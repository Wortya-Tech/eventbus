import { assertEquals, assertGreater } from "@std/assert";
import { EventBusService } from "../../src/main.ts";
import {
  cleanupWithGrace,
  connectToRabbitMQ,
  createExchangeName,
  createQueueName,
} from "./helpers.ts";
import { setTimeout } from "node:timers/promises";
import { Buffer } from "node:buffer";

Deno.test("Load Balancing: shared queue prevents duplicate message processing", async () => {
  const exchangeName = createExchangeName("load-balance");
  // Shared queue name - KEY PART OF THIS TEST
  const sharedQueueName = createQueueName("shared-queue");
  const connection = await connectToRabbitMQ();

  const services: EventBusService[] = [];
  const messageIdsA: string[] = [];
  const messageIdsB: string[] = [];

  try {
    // Create Service A
    const serviceA = new EventBusService(
      exchangeName,
      sharedQueueName,
      "service-a",
      "1.0.0",
    );
    services.push(serviceA);

    // Create Service B - SAME QUEUE NAME
    const serviceB = new EventBusService(
      exchangeName,
      sharedQueueName,
      "service-b",
      "1.0.0",
    );
    services.push(serviceB);

    // Set up subscribers to track which service gets which message
    serviceA.subscribe("collector", (_data, props) => {
      messageIdsA.push(props.messageId!);
      return Promise.resolve();
    });

    serviceB.subscribe("collector", (_data, props) => {
      messageIdsB.push(props.messageId!);
      return Promise.resolve();
    });

    // Connect and start consuming
    await serviceA.connect(connection);
    await serviceA.consume();

    await serviceB.connect(connection);
    await serviceB.consume();

    // Publish 10 distinct messages
    for (let i = 0; i < 10; i++) {
      await serviceA.publish({
        type: "test.msg",
        data: Buffer.from(JSON.stringify({ idx: i })),
        metadata: { contentType: "application/json" },
      });
    }

    // Wait for processing
    await setTimeout(2000);

    const totalProcessed = messageIdsA.length + messageIdsB.length;

    // Assertion 1: Total should be 10 (no lost messages)
    assertEquals(
      totalProcessed,
      10,
      `Should have processed 10 messages total, got ${totalProcessed}`,
    );

    // Assertion 2: No message should be processed by BOTH services (no duplication)
    // Check for intersection of messageIdsA and messageIdsB
    const duplicates = messageIdsA.filter((id) => messageIdsB.includes(id));
    assertEquals(
      duplicates.length,
      0,
      `Messages should not be duplicated across services. Found ${duplicates.length} duplicates`,
    );

    // Assertion 3: Load balancing verification (messages distributed between services)
    // This is optional in the sense that it tests *distribution*, not just no-dup,
    // but it confirms the work queue pattern is active
    messageIdsA.length > 0 && messageIdsB.length > 0;
    assertGreater(messageIdsA.length, 0);
    assertGreater(messageIdsB.length, 0);
  } finally {
    await cleanupWithGrace(connection, exchangeName, [sharedQueueName], services);
  }
});
