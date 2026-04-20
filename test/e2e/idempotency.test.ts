import { assertEquals } from "@std/assert";
import { EventBusService } from "../../src/main.ts";
import {
  cleanupWithGrace,
  connectToRabbitMQ,
  createExchangeName,
  createQueueName,
} from "./helpers.ts";
import { setTimeout } from "node:timers/promises";
import { Buffer } from "node:buffer";

Deno.test("Idempotency - subscribe() same key should overwrite not duplicate", async () => {
  const exchangeName = createExchangeName("idemp-sub");
  const queueName = createQueueName("idemp-sub");
  const connection = await connectToRabbitMQ();
  const service = new EventBusService(
    exchangeName,
    queueName,
    "test-service",
    "1.0.0",
  );

  await service.connect(connection);

  try {
    let callCount = 0;
    const handler = () => {
      callCount++;
      return Promise.resolve();
    };

    // Subscribe twice with same key
    service.subscribe("test.key", handler);
    service.subscribe("test.key", handler);

    // Publish event
    await service.consume();

    await service.publish({
      type: "test.event",
      data: Buffer.from("test"),
      metadata: { contentType: "text/plain" },
    });

    await setTimeout(500);

    // Should be called ONCE because the second subscribe overwrote the first
    assertEquals(callCount, 1, "Handler should be called exactly once despite double subscription");
  } finally {
    await cleanupWithGrace(connection, exchangeName, [queueName], [service]);
  }
});

Deno.test("At-Least-Once Delivery - identical payloads produce distinct events", async () => {
  const exchangeName = createExchangeName("idemp-dup");
  const queueName = createQueueName("idemp-dup");
  const connection = await connectToRabbitMQ();
  const service = new EventBusService(
    exchangeName,
    queueName,
    "test-service",
    "1.0.0",
  );

  await service.connect(connection);

  try {
    const receivedIds: string[] = [];

    service.subscribe("collector", (_data, props) => {
      receivedIds.push(props.messageId as string);
      return Promise.resolve();
    });

    await service.consume();

    // Publish identical data twice - different timestamps/content-encoding
    await service.publish({
      type: "test.dup",
      data: Buffer.from("same-data"),
      metadata: {
        contentType: "text/plain",
        timestamp: Date.now(),
      },
    });

    await service.publish({
      type: "test.dup",
      data: Buffer.from("same-data"),
      metadata: {
        contentType: "text/plain",
        timestamp: Date.now(),
      },
    });

    await setTimeout(500);

    // Should receive both because they have different messageIds
    assertEquals(receivedIds.length, 2, "Should receive 2 events for 2 publishes");
    assertEquals(receivedIds[0] !== receivedIds[1], true, "messageIds should be different");
  } finally {
    await cleanupWithGrace(connection, exchangeName, [queueName], [service]);
  }
});
