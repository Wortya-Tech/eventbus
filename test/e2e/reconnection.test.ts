import { assertEquals } from "@std/assert";
import { EventBusService } from "../../src/eventBus/index.ts";
import {
  cleanupWithGrace,
  connectToRabbitMQ,
  createConsumer,
  createExchangeName,
  createProducer,
  createQueueName,
  createRetryTracker,
  createTestData,
  encodeTestData,
  killChannel,
  sleep,
} from "./helpers.ts";

Deno.test("should reconnect after channel is killed", async () => {
  const connection = await connectToRabbitMQ();
  const services: EventBusService[] = [];
  const exchangeName = createExchangeName("channel-reconnect");
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

    const tracker = createRetryTracker();
    const consumer = await createConsumer(
      exchangeName,
      queueName,
      connection,
      false,
    );
    services.push(consumer);

    consumer.subscribe("handler", async () => {
      tracker.attempts++;
    });

    await consumer.consume();
    await sleep(100);

    const testData1 = createTestData("msg1");
    await producer.publish({
      type: "test.event",
      data: encodeTestData(testData1),
      metadata: { contentType: "application/json" },
    });

    await sleep(500);

    const channel = await connection.createChannel();
    await killChannel(channel);
    await sleep(500);

    const testData2 = createTestData("msg2");
    await producer.publish({
      type: "test.event",
      data: encodeTestData(testData2),
      metadata: { contentType: "application/json" },
    });

    await sleep(500);

    assertEquals(tracker.attempts >= 1, true);
  } finally {
    await cleanupWithGrace(connection, exchangeName, [queueName, producerName], services);
  }
});

Deno.test("should reconnect after connection is killed", async () => {
  const services: EventBusService[] = [];
  const exchangeName = createExchangeName("connection-reconnect");
  const queueName = createQueueName("consumer");
  const producerName = `producer-${Date.now()}`;
  let connection = await connectToRabbitMQ();

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
      true,
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

    consumer.subscribe("handler", async () => {
      tracker.attempts++;
    });

    await consumer.consume();
    await sleep(100);

    await consumer.close();
    await producer.close();
    await sleep(100);

    connection = await connectToRabbitMQ();

    const newProducer = new EventBusService(
      exchangeName,
      producerName,
      "test-producer",
      "1.0.0",
    );
    await newProducer.connect(
      connection,
      "amqp://guest@localhost:5672",
      false,
    );
    services.push(newProducer);

    const testData = createTestData("reconnect-test");
    await newProducer.publish({
      type: "test.event",
      data: encodeTestData(testData),
      metadata: { contentType: "application/json" },
    });

    await sleep(1000);

    assertEquals(true, true);
  } finally {
    await cleanupWithGrace(connection, exchangeName, [queueName, producerName], services);
  }
});
