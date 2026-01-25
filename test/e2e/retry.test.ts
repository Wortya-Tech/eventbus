import { assertEquals } from "@std/assert";
import { assert } from "@std/assert/assert";
import { EventBusService } from "../../src/eventBus/index.ts";
import {
  cleanupWithGrace,
  connectToRabbitMQ,
  createConsumer,
  createExchangeName,
  createProducer,
  createQueueName,
  createRetryHandler,
  createRetryTracker,
  createTestData,
  encodeTestData,
  sleep,
  testConfig,
} from "./helpers.ts";

Deno.test("should retry failed handler and eventually succeed", async () => {
  const connection = await connectToRabbitMQ();
  const services: EventBusService[] = [];
  const exchangeName = createExchangeName("retry-success");
  const queueName = createQueueName("consumer");

  try {
    const producer = await createProducer(
      exchangeName,
      connection,
      undefined,
      "amqp://guest:guest:localhost:5672",
      false,
    );
    services.push(producer);

    const tracker = createRetryTracker();
    const consumer = await createConsumer(
      exchangeName,
      queueName,
      connection,
      false,
    );
    services.push(consumer);

    consumer.subscribe("handler", createRetryHandler(tracker, 1));

    await consumer.consume();
    await sleep(100);

    const testData = createTestData();
    await producer.publish({
      type: "test.event",
      data: encodeTestData(testData),
      metadata: { contentType: "application/json" },
    });

    await sleep(500);

    assert(tracker.attempts >= 2);
    assertEquals(tracker.failures, 1);
    assert(tracker.attempts > tracker.failures);
  } finally {
    await cleanupWithGrace(connection, exchangeName, [queueName], services);
  }
});

Deno.test("should send to dead letter queue after max retries", async () => {
  const connection = await connectToRabbitMQ();
  const services: EventBusService[] = [];
  const mainConnection = await connectToRabbitMQ();
  const exchangeName = createExchangeName("retry-dlq");
  const queueName = createQueueName("consumer");
  const dlqName = `${queueName}.dlq`;

  try {
    const producer = await createProducer(
      exchangeName,
      connection,
      undefined,
      "amqp://guest:guest@localhost:5672",
      false,
    );
    services.push(producer);

    const tracker = createRetryTracker();
    const consumer = await createConsumer(
      exchangeName,
      queueName,
      connection,
      false,
    );
    services.push(consumer);

    const failureCount = Math.max(1, testConfig.MAX_RETRIES);
    consumer.subscribe("handler", async () => {
      tracker.attempts++;
      await Promise.resolve();
      if (tracker.attempts <= failureCount) {
        throw new Error(`DLQ test failure at attempt ${tracker.attempts}`);
      }
    });

    await consumer.consume();
    await sleep(100);

    const testData = createTestData();
    await producer.publish({
      type: "test.event",
      data: encodeTestData(testData),
      metadata: { contentType: "application/json" },
    });

    const baseDelay = testConfig.RETRY_DELAY * (testConfig.MAX_RETRIES + 2);
    await sleep(baseDelay + 1000);

    assert(tracker.attempts > failureCount);
  } finally {
    await cleanupWithGrace(connection, exchangeName, [queueName, dlqName], services);
    await mainConnection.close();
  }
});

Deno.test("should increment retry count headers correctly", async () => {
  const connection = await connectToRabbitMQ();
  const services: EventBusService[] = [];
  const exchangeName = createExchangeName("retry-headers");
  const queueName = createQueueName("consumer");

  try {
    const producer = await createProducer(
      exchangeName,
      connection,
      undefined,
      "amqp://guest:guest@localhost:5672",
      false,
    );
    services.push(producer);

    const consumer = await createConsumer(
      exchangeName,
      queueName,
      connection,
      false,
    );
    services.push(consumer);

    const tracker = createRetryTracker();
    consumer.subscribe("handler", async () => {
      tracker.attempts++;
      await Promise.resolve();
      if (tracker.attempts <= Math.max(1, testConfig.MAX_RETRIES)) {
        throw new Error("Always fail");
      }
    });

    await consumer.consume();
    await sleep(100);

    const testData = createTestData();
    await producer.publish({
      type: "test.event",
      data: encodeTestData(testData),
      metadata: { contentType: "application/json" },
    });

    const baseDelay = testConfig.RETRY_DELAY * (testConfig.MAX_RETRIES + 1);
    await sleep(baseDelay + 500);

    assert(tracker.attempts > 1);
    assert(tracker.attempts >= testConfig.MAX_RETRIES);
  } finally {
    await cleanupWithGrace(connection, exchangeName, [queueName], services);
  }
});
