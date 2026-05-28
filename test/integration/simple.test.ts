import test from "node:test";
import assert from "node:assert/strict";
import { connect as amqpConnect } from "amqplib";
import type { ChannelModel, Message } from "amqplib";

const URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672";

test("simple raw publish/consume", async (t) => {
    let connection: ChannelModel;

    t.before(async () => { connection = await amqpConnect(URL); });
    t.after(async () => { await connection.close(); });

    await t.test("should publish and consume message", async () => {
        const channel = await connection.createChannel();

        const ex = "test.simple.publish";
        const q = "test.simple.queue";
        await channel.assertExchange(ex, "fanout", { durable: true });
        await channel.assertQueue(q, { durable: true });
        await channel.bindQueue(q, ex, "");

        let receivedData: { test: string } | null = null;
        const { consumerTag } = await channel.consume(q, async (msg: Message | null) => {
            if (msg) { receivedData = JSON.parse(new TextDecoder().decode(msg.content)); await channel.ack(msg); }
        });

        await new Promise(r => setTimeout(r, 100));
        await channel.publish(ex, "", Buffer.from(JSON.stringify({ test: "data" })));
        await new Promise(r => setTimeout(r, 500));

        if (!receivedData) throw new Error("no data received");
        assert.equal((receivedData as { test: string }).test, "data");
        await channel.cancel(consumerTag);
    });
});
