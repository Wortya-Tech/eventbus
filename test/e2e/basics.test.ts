import { assertEquals } from "@std/assert";
import type { EventBusService } from "../../src/eventBus/index.ts";
import {
  cleanupWithGrace,
  connectToRabbitMQ,
  createConsumer,
  createExchangeName,
  createProducer,
  createQueueName,
  createTestData,
  decodeTestData,
  encodeTestData,
  sleep,
  waitForMessage,
} from "./helpers.ts";

Deno.test("should publish and consume using event bus service", async () => {
  const connection = await connectToRabbitMQ();
  const services: EventBusService[] = [];
  const exchangeName = createExchangeName("pub-consume");
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

    const consumer = await createConsumer(
      exchangeName,
      queueName,
      connection,
      false,
      "amqp://guest:guest@localhost:5672",
    );
    services.push(consumer);

    const testData = createTestData();
    let receivedData: typeof testData | null = null;

    consumer.subscribe("handler", async (data) => {
      await Promise.resolve(receivedData = decodeTestData(data));
    });

    await consumer.consume();
    await sleep(100);

    await producer.publish({
      type: "test.event",
      data: encodeTestData(testData),
      metadata: { contentType: "application/json" },
    });

    await waitForMessage(() => receivedData);

    assertEquals(receivedData!.id, testData.id);
  } finally {
    await cleanupWithGrace(connection, exchangeName, [queueName, producerName], services);
  }
});

Deno.test("should execute multiple subscribers on same consumer", async () => {
  const connection = await connectToRabbitMQ();
  const services: EventBusService[] = [];
  const exchangeName = createExchangeName("multi-sub");
  const queueName = createQueueName("consumer");
  const producerName = createQueueName("producer");

  try {
    const producer = await createProducer(
      exchangeName,
      connection,
      producerName,
      "amqp://guest:guest:localhost:5672",
      false,
    );
    services.push(producer);

    const consumer = await createConsumer(
      exchangeName,
      queueName,
      connection,
      false,
      "amqp://guest:guest:localhost:5672",
    );
    services.push(consumer);

    const testData = createTestData();
    let handler1Called = false;
    let handler2Called = false;

    consumer.subscribe("handler1", async () => {
      await Promise.resolve(handler1Called = true);
    });

    consumer.subscribe("handler2", async () => {
      await Promise.resolve(handler2Called = true);
    });

    await consumer.consume();
    await sleep(100);

    await producer.publish({
      type: "test.event",
      data: encodeTestData(testData),
      metadata: { contentType: "application/json" },
    });

    await sleep(500);

    assertEquals(handler1Called, true);
    assertEquals(handler2Called, true);
  } finally {
    await cleanupWithGrace(connection, exchangeName, [queueName, producerName], services);
  }
});

Deno.test("should fanout to multiple independent consumers", async () => {
  const connection = await connectToRabbitMQ();
  const services: EventBusService[] = [];
  const exchangeName = createExchangeName("fanout");
  const queueName1 = createQueueName("consumer-1");
  const queueName2 = createQueueName("consumer-2");
  const producerName = createQueueName("producer");

  try {
    const producer = await createProducer(
      exchangeName,
      connection,
      producerName,
      "amqp://guest:guest:localhost:5672",
      false,
    );
    services.push(producer);

    const consumer1 = await createConsumer(
      exchangeName,
      queueName1,
      connection,
      false,
      "amqp://guest:guest:localhost:5672",
    );
    services.push(consumer1);

    const consumer2 = await createConsumer(
      exchangeName,
      queueName2,
      connection,
      false,
      "amqp://guest:guest:localhost:5672",
    );
    services.push(consumer2);

    const testData = createTestData();
    let received1: typeof testData | null = null;
    let received2: typeof testData | null = null;

    consumer1.subscribe("handler1", async (data) => {
      await Promise.resolve(received1 = decodeTestData(data));
    });

    consumer2.subscribe("handler2", async (data) => {
      await Promise.resolve(received2 = decodeTestData(data));
    });

    await consumer1.consume();
    await consumer2.consume();
    await sleep(100);

    await producer.publish({
      type: "test.event",
      data: encodeTestData(testData),
      metadata: { contentType: "application/json" },
    });

    await waitForMessage(() => received1 && received2);

    assertEquals(received1!.id, testData.id);
    assertEquals(received2!.id, testData.id);
  } finally {
    await cleanupWithGrace(
      connection,
      exchangeName,
      [queueName1, queueName2, producerName],
      services,
    );
  }
});
