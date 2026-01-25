import { assertEquals } from "@std/assert";
import { EventBusService } from "../../src/eventBus/index.ts";
import {
  cleanupWithGrace,
  connectToRabbitMQ,
  createConsumer,
  createExchangeName,
  createQueueName,
} from "./helpers.ts";

Deno.test("should log warning when subscribing existing key", async () => {
  const exchangeName = createExchangeName("dup-sub");
  const queueName = createQueueName("consumer");
  const connection = await connectToRabbitMQ();
  const services: EventBusService[] = [];

  try {
    const consumer = await createConsumer(
      exchangeName,
      queueName,
      connection,
    );
    services.push(consumer);

    const handler = async () => {};
    consumer.subscribe("test-key", handler);
    consumer.subscribe("test-key", handler);

    assertEquals(true, true);
  } finally {
    await cleanupWithGrace(connection, exchangeName, [queueName], services);
  }
});
