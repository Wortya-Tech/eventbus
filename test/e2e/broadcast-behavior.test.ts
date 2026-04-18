import { assertEquals } from "@std/assert";
import { EventBusService } from "../../src/main.ts";
import {
  cleanupWithGrace,
  connectToRabbitMQ,
  createExchangeName,
  createQueueName,
} from "./helpers.ts";
import { Buffer } from "node:buffer";
import { setTimeout as setTimeout2 } from "node:timers/promises";

Deno.test("Fanout Exchange: one published event is consumed by multiple queues", async () => {
  const exchangeName = createExchangeName("broadcast");
  const queueNameA = createQueueName("consumer-a");
  const queueNameB = createQueueName("consumer-b");
  const connection = await connectToRabbitMQ();

  const services: EventBusService[] = [];

  try {
    // Create two consumers (services) connected to SAME exchange but DIFFERENT queues
    const serviceA = new EventBusService(
      exchangeName,
      queueNameA,
      "service-a",
      "1.0.0",
    );

    const serviceB = new EventBusService(
      exchangeName,
      queueNameB,
      "service-b",
      "1.0.0",
    );

    // Track which services receive messages
    const aReceivedMessages: number[] = [];
    const bReceivedMessages: number[] = [];

    // Set up subscribers on both services
    serviceA.subscribe("collector", async () => {
      aReceivedMessages.push(1);
    });

    serviceB.subscribe("collector", async () => {
      bReceivedMessages.push(1);
    });

    // Connect and start consuming
    await serviceA.connect(connection);
    services.push(serviceA);
    await serviceA.consume();

    await serviceB.connect(connection);
    services.push(serviceB);
    await serviceB.consume();

    // Publish a SINGLE event to the shared exchange
    await serviceA.publish({
      type: "broadcast.test",
      data: Buffer.from("test-data"),
      metadata: { contentType: "text/plain" },
    });

    // Wait for processing
    await setTimeout2(2000);

    // Verify: BOTH services should have received the message (Fanout behavior)
    assertEquals(aReceivedMessages.length, 1, "Service A should receive 1 message");
    assertEquals(bReceivedMessages.length, 1, "Service B should receive 1 message");
  } finally {
    const queueNames = [queueNameA, queueNameB];
    await cleanupWithGrace(connection, exchangeName, queueNames, services);
  }
});
