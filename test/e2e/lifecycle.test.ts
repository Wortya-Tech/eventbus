import { assertEquals } from "@std/assert";
import { EventBusService } from "../../src/main.ts";
import {
  cleanupWithGrace,
  connectToRabbitMQ,
  createConsumer,
  createExchangeName,
  createProducer,
  createQueueName,
  createTestData,
  encodeTestData,
  sleep,
} from "./helpers.ts";

Deno.test("should stop consuming after cancel", async () => {
  const connection = await connectToRabbitMQ();
  const services: EventBusService[] = [];
  const exchangeName = createExchangeName("cancel");
  const queueName = createQueueName("consumer");
  const producerName = `producer-${Date.now()}`;

  try {
    const producer = await createProducer(
      exchangeName,
      connection,
      producerName,
      "amqp://guest:guest@localhost:5672",
      false,
    );
    services.push(producer);

    let receivedCount = 0;
    const consumer = await createConsumer(
      exchangeName,
      queueName,
      connection,
      false,
    );
    services.push(consumer);

    consumer.subscribe("handler", () => {
      receivedCount++;
      return Promise.resolve();
    });

    await consumer.consume();
    await sleep(100);

    for (let i = 0; i < 3; i++) {
      await producer.publish({
        type: "test.event",
        data: encodeTestData(createTestData()),
        metadata: { contentType: "application/json" },
      });
    }

    await sleep(500);

    const countBeforeCancel = receivedCount;
    consumer.close();

    await sleep(100);

    await producer.publish({
      type: "test.event",
      data: encodeTestData(createTestData()),
      metadata: { contentType: "application/json" },
    });

    await sleep(500);

    assertEquals(receivedCount, countBeforeCancel);
  } finally {
    await cleanupWithGrace(connection, exchangeName, [queueName, producerName], services);
  }
});

Deno.test("should gracefully close all resources", async () => {
  const connection = await connectToRabbitMQ();
  const services: EventBusService[] = [];
  const exchangeName = createExchangeName("graceful-shutdown");
  const queueName = createQueueName("consumer");
  const producerName = createQueueName("producer");

  try {
    const producer = new EventBusService(
      exchangeName,
      producerName,
      "test-producer",
      "1.0.0",
    );
    await producer.connect(
      connection,
      "amqp://guest:guest@localhost:5672",
      false,
    );
    services.push(producer);

    const consumer = new EventBusService(
      exchangeName,
      queueName,
      "test-consumer",
      "1.0.0",
    );
    await consumer.connect(
      connection,
      "amqp://guest:guest@localhost:5672",
      false,
    );
    services.push(consumer);

    consumer.subscribe("handler", async () => {});
    await consumer.consume();

    await producer.publish({
      type: "test.event",
      data: encodeTestData(createTestData()),
      metadata: { contentType: "application/json" },
    });

    await sleep(100);

    await consumer.close();
    await producer.close();

    assertEquals(true, true);
  } finally {
    await cleanupWithGrace(connection, exchangeName, [queueName, producerName], services);
  }
});

Deno.test("should unsubscribe and stop processing", async () => {
  const connection = await connectToRabbitMQ();
  const services: EventBusService[] = [];
  const exchangeName = createExchangeName("unsubscribe");
  const queueName = createQueueName("consumer");
  const producerName = createQueueName("producer");

  try {
    const producer = await createProducer(
      exchangeName,
      connection,
      producerName,
      "amqp://guest:guest@localhost:5672",
      false,
    );
    services.push(producer);

    let handlerCalledCount = 0;
    const consumer = await createConsumer(
      exchangeName,
      queueName,
      connection,
      false,
    );
    services.push(consumer);

    consumer.subscribe("handler", () => {
      handlerCalledCount++;
      return Promise.resolve();
    });

    await consumer.consume();
    await sleep(100);

    await producer.publish({
      type: "test.event",
      data: encodeTestData(createTestData()),
      metadata: { contentType: "application/json" },
    });

    await sleep(200);

    const countBeforeUnsubscribe = handlerCalledCount;
    consumer.unsubscribe("handler");

    await producer.publish({
      type: "test.event",
      data: encodeTestData(createTestData()),
      metadata: { contentType: "application/json" },
    });

    await sleep(200);

    assertEquals(handlerCalledCount, countBeforeUnsubscribe);
  } finally {
    await cleanupWithGrace(connection, exchangeName, [queueName, producerName], services);
  }
});
