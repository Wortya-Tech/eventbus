import { assertEquals } from "@std/assert";
import { TextDecoder } from "node:util";
import { EventBusService } from "../../src/eventBus/index.ts";
import type { TestData } from "./helpers.ts";
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
  waitForMessages,
} from "./helpers.ts";

Deno.test("should handle concurrent publishing", async () => {
  const connection = await connectToRabbitMQ();
  const services: EventBusService[] = [];
  const exchangeName = createExchangeName("concurrent");
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

    const receivedMessages: string[] = [];
    const consumer = await createConsumer(
      exchangeName,
      queueName,
      connection,
      false,
    );
    services.push(consumer);

    consumer.subscribe("handler", async (data) => {
      const parsed = JSON.parse(new TextDecoder().decode(data)) as { id: string };
      receivedMessages.push(parsed.id);
      await Promise.resolve();
    });

    await consumer.consume();
    await sleep(100);

    const messageCount = 10;
    const promises = [];
    for (let i = 0; i < messageCount; i++) {
      const testData = createTestData(`msg-${i}`);
      promises.push(
        producer.publish({
          type: "test.event",
          data: encodeTestData(testData),
          metadata: { contentType: "application/json" },
        }),
      );
    }

    await Promise.all(promises);
    await sleep(1500);

    await waitForMessages(
      messageCount,
      () => receivedMessages.length,
      5000,
    );

    assertEquals(receivedMessages.length, messageCount);
  } finally {
    await cleanupWithGrace(connection, exchangeName, [queueName, producerName], services);
  }
});

Deno.test("should maintain message order during retries", async () => {
  const connection = await connectToRabbitMQ();
  const services: EventBusService[] = [];
  const exchangeName = createExchangeName("order");
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

    const receivedOrder: string[] = [];
    const consumer = await createConsumer(
      exchangeName,
      queueName,
      connection,
      false,
    );
    services.push(consumer);

    let failCounter = 0;
    consumer.subscribe("handler", async (data) => {
      const parsed = JSON.parse(new TextDecoder().decode(data)) as { id: string };
      receivedOrder.push(parsed.id);

      await Promise.resolve();
      if (parsed.id === "order-1" && failCounter === 0) {
        failCounter++;
        throw new Error("First message fails once");
      }
    });

    await consumer.consume();
    await sleep(100);

    await producer.publish({
      type: "test.event",
      data: encodeTestData(createTestData("order-0")),
      metadata: { contentType: "application/json" },
    });

    await sleep(50);

    await producer.publish({
      type: "test.event",
      data: encodeTestData(createTestData("order-1")),
      metadata: { contentType: "application/json" },
    });

    await sleep(50);

    await producer.publish({
      type: "test.event",
      data: encodeTestData(createTestData("order-2")),
      metadata: { contentType: "application/json" },
    });

    await sleep(1500);

    assertEquals(receivedOrder[0], "order-0");
    assertEquals(receivedOrder[receivedOrder.length - 1], "order-1");
  } finally {
    await cleanupWithGrace(connection, exchangeName, [queueName, producerName], services);
  }
});

Deno.test("should handle message publish before consumer is ready", async () => {
  const connection = await connectToRabbitMQ();
  const services: EventBusService[] = [];
  const exchangeName = createExchangeName("pre-publish");
  const producerName = `producer-${Date.now()}`;
  const consumerName = `consumer-${Date.now()}`;

  try {
    const producer = await createProducer(
      exchangeName,
      connection,
      producerName,
      "amqp://guest:guest@localhost:5672",
      false,
    );
    services.push(producer);

    const consumer = await createConsumer(
      exchangeName,
      consumerName,
      connection,
      false,
    );
    services.push(consumer);

    let receivedData: TestData | null = null;
    consumer.subscribe("handler", (data) => {
      receivedData = JSON.parse(new TextDecoder().decode(data)) as TestData;
      return Promise.resolve();
    });

    await consumer.consume();
    await sleep(100);

    const testData = createTestData("pre-consumer");
    await producer.publish({
      type: "test.event",
      data: encodeTestData(testData),
      metadata: { contentType: "application/json" },
    });

    await sleep(300);

    if (receivedData === null) {
      throw new Error("Should have received message");
    }
    const data = receivedData as TestData;
    assertEquals(data.id, testData.id);
  } finally {
    await cleanupWithGrace(connection, exchangeName, [producerName, consumerName], services);
  }
});

Deno.test("should include all message metadata", async () => {
  const connection = await connectToRabbitMQ();
  const services: EventBusService[] = [];
  const exchangeName = createExchangeName("metadata");
  const queueName = createQueueName("consumer");
  const producerName = createQueueName("producer");

  try {
    const producer = new EventBusService(
      exchangeName,
      producerName,
      "my-service",
      "2.0.0",
    );
    await producer.connect(
      connection,
      "amqp://guest:guest@localhost:5672",
      false,
    );
    services.push(producer);

    let receivedProps: Record<string, string | number> | null = null;
    const consumer = await createConsumer(
      exchangeName,
      queueName,
      connection,
      false,
    );
    services.push(consumer);

    consumer.subscribe("handler", (_data, properties) => {
      receivedProps = properties;
      return Promise.resolve();
    });

    await consumer.consume();
    await sleep(100);

    const correlationId = "test-correlation-123";
    await producer.publish({
      type: "test.event",
      data: encodeTestData(createTestData()),
      metadata: {
        contentType: "application/json",
        contentEncoding: "utf-8",
        correlationId,
      },
    });

    await sleep(1000);

    assertEquals(receivedProps!.appId, "my-service@2.0.0+" + exchangeName);
    assertEquals(receivedProps!.contentType, "application/json");
    assertEquals(receivedProps!.contentEncoding, "utf-8");
    assertEquals(receivedProps!.correlationId, correlationId);
    assertEquals(typeof receivedProps!.messageId, "string");
    assertEquals(typeof receivedProps!.timestamp, "number");
  } finally {
    await cleanupWithGrace(connection, exchangeName, [queueName, producerName], services);
  }
});
