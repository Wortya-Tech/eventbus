import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { connect as amqpConnect } from "amqplib";
import type { ChannelModel, Message } from "amqplib";

const URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672";

describe("simple raw publish/consume", () => {
    let connection: ChannelModel;

    before(async () => { connection = await amqpConnect(URL); });
    after(async () => { await connection.close(); });

    it("should publish and consume message", async (t) => {
        const channel = await connection.createChannel();
        t.after(async () => { await channel.close(); });

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
