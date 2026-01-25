import { Buffer } from "node:buffer";
import { TextDecoder } from "node:util";

import { assertEquals } from "@std/assert";
import amqplib from "npm:amqplib@0.10.9";
import { setTimeout as setTimeoutFn } from "node:timers/promises";
import type { Message } from "npm:amqplib@0.10.9";

const rabbitMQUrl = "amqp://guest:guest@localhost:5672";

Deno.test("should publish and consume message", async () => {
  const exchangeName = `test.simple.publish.${Date.now()}`;
  const connection = await amqplib.connect(rabbitMQUrl);
  const channel = await connection.createChannel();

  await channel.assertExchange(exchangeName, "fanout", { durable: true });

  const queueName = `simple.queue.${Date.now()}`;
  await channel.assertQueue(queueName, { durable: true });
  await channel.bindQueue(queueName, exchangeName, "");

  const testPayload = { test: "data" };
  let receivedData: any = null;

  const consumerTag = (await channel.consume(queueName, async (msg: Message | null) => {
    if (msg) {
      receivedData = JSON.parse(new TextDecoder().decode(msg.content));
      await channel.ack(msg);
    }
  })).consumerTag;

  await setTimeoutFn(100);

  const payloadStr = JSON.stringify(testPayload);
  const payloadBuffer = Buffer.from(payloadStr);
  await channel.publish(exchangeName, "", payloadBuffer);

  await setTimeoutFn(500);

  assertEquals(receivedData.test, "data");

  await channel.cancel(consumerTag);
  await channel.close();
  await connection.close();
});
