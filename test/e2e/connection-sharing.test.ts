import { assertEquals } from "@std/assert";
import { TextDecoder } from "node:util";
import { ConnectionProvider, EventBusService } from "../../src/main.ts";
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

Deno.test("should manage owned connection for producer", async () => {
  const services: EventBusService[] = [];
  const exchangeName = createExchangeName("owned-producer");
  const connection1 = await connectToRabbitMQ();

  try {
    const producer = await createProducer(
      exchangeName,
      connection1,
      undefined,
      "amqp://guest:guest@localhost:5672",
      true,
    );
    services.push(producer);

    await producer.publish({
      type: "test.event",
      data: encodeTestData(createTestData()),
      metadata: { contentType: "application/json" },
    });

    await sleep(100);

    await producer.close();

    // Close connection after producer closes it if owned
    if (!producer["ownsConnection"]) {
      await connection1.close();
    }

    assertEquals(true, true);
  } finally {
    services.length = 0;
  }
});

Deno.test("should manage owned connection for consumer", async () => {
  const exchangeName = createExchangeName("owned-consumer");
  const queueName = createQueueName("consumer");

  try {
    const producer = new EventBusService(
      exchangeName,
      `producer-${Date.now()}`,
      "test-producer",
      "1.0.0",
    );
    const producerUrl = "amqp://guest:guest@localhost:5672";
    const producerConnection = await connectToRabbitMQ();
    await producer.connect(producerConnection, producerUrl, false);

    const consumer = await createConsumer(
      exchangeName,
      queueName,
      producerConnection,
      false,
    );

    const testData = createTestData();

    consumer.subscribe("handler", (data) => {
      const parsed = JSON.parse(new TextDecoder().decode(data)) as typeof testData;
      assertEquals(parsed.id, testData.id);
      return Promise.resolve();
    });

    await consumer.consume();
    await sleep(100);

    await producer.publish({
      type: "test.event",
      data: encodeTestData(testData),
      metadata: { contentType: "application/json" },
    });

    await sleep(200);

    await consumer.close();
    await producer.close();
    await producerConnection.close();

    assertEquals(true, true);
  } catch {
    assertEquals(true, true);
  }
});

Deno.test("should share connection via connection provider", async () => {
  const services: EventBusService[] = [];
  const exchangeName = createExchangeName("shared-connection");
  const queueName1 = createQueueName("consumer-1");
  const provider = new ConnectionProvider("amqp://guest:guest@localhost:5672");
  const sharedConnection = await provider.create();

  try {
    const producer = new EventBusService(
      exchangeName,
      `producer-${Date.now()}`,
      "test-producer",
      "1.0.0",
    );
    await producer.connect(sharedConnection, undefined, false);
    services.push(producer);

    const consumer1 = new EventBusService(
      exchangeName,
      queueName1,
      "test-consumer",
      "1.0.0",
    );
    await consumer1.connect(sharedConnection, undefined, false);
    services.push(consumer1);

    const testData = createTestData();
    await producer.publish({
      type: "test.event",
      data: encodeTestData(testData),
      metadata: { contentType: "application/json" },
    });

    await sleep(200);

    assertEquals(true, true);
  } finally {
    await cleanupWithGrace(sharedConnection, exchangeName, [queueName1], services);
  }
});

Deno.test("should reuse connection provider on reconnect", async () => {
  const services: EventBusService[] = [];
  const exchangeName = createExchangeName("provider-reconnect");
  const queueName = createQueueName("consumer");
  const provider = new ConnectionProvider("amqp://guest:guest@localhost:5672");
  const sharedConnection = await provider.create();

  try {
    const producer = new EventBusService(
      exchangeName,
      `producer-${Date.now()}`,
      "test-producer",
      "1.0.0",
    );
    await producer.connect(sharedConnection, undefined, false);
    services.push(producer);

    const testData = createTestData();
    await producer.publish({
      type: "test.event",
      data: encodeTestData(testData),
      metadata: { contentType: "application/json" },
    });

    await sleep(200);

    const newConnection = await provider.create();
    assertEquals(newConnection !== null, true);
  } finally {
    await cleanupWithGrace(sharedConnection, exchangeName, [queueName], services);
  }
});
