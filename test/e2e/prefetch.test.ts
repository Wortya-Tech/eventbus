import test from "node:test";
import assert from "node:assert/strict";
import { connect as amqpConnect } from "amqplib";
import type { ChannelModel } from "amqplib";
import { EventBusService } from "../../src/main.js";

const URL = process.env.RABBITMQ_URL!;

test("prefetch - limits unacked messages", async (t) => {
    let connection: ChannelModel;
    let producer: EventBusService;
    let consumer: EventBusService;

    t.before(async () => {
        connection = await amqpConnect(URL);
        producer = new EventBusService("test.prefetch.limit", "test.prefetch.limit.p", "test", "1.0.0", undefined, 2, 100);
        await producer.connect(connection, URL, false);
        consumer = new EventBusService("test.prefetch.limit", "test.prefetch.limit.q", "test", "1.0.0", undefined, 2, 100, 3, 100, 1);
        await consumer.connect(connection, URL, false);
    });

    t.after(async () => {
        await consumer.close();
        await producer.close();
        await connection.close();
    });

    await t.test("should deliver only one message at a time with prefetch=1", async () => {
        const acks: Array<() => void> = [];
        let received = 0;

        consumer.subscribe("h", async () => {
            received++;
            await new Promise<void>((resolve) => acks.push(resolve));
        });
        await consumer.consume();
        await new Promise((r) => setTimeout(r, 200));

        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ n: 1 })), metadata: { contentType: "application/json" } });
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ n: 2 })), metadata: { contentType: "application/json" } });
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ n: 3 })), metadata: { contentType: "application/json" } });

        await new Promise((r) => setTimeout(r, 300));

        assert.equal(received, 1, "only first message should be delivered while unacked");

        acks[0]();
        await new Promise((r) => setTimeout(r, 300));
        assert.equal(received, 2, "second message should arrive after first ack");

        acks[1]();
        await new Promise((r) => setTimeout(r, 300));
        assert.equal(received, 3, "third message should arrive after second ack");

        acks[2]();
    });
});

test("prefetch - batched delivery with 100 messages", async (t) => {
    let connection: ChannelModel;
    let producer: EventBusService;
    let consumer: EventBusService;

    t.before(async () => {
        connection = await amqpConnect(URL);
        producer = new EventBusService("test.prefetch.batch", "test.prefetch.batch.p", "test", "1.0.0", undefined, 2, 100);
        await producer.connect(connection, URL, false);
        consumer = new EventBusService("test.prefetch.batch", "test.prefetch.batch.q", "test", "1.0.0", undefined, 2, 100, 3, 100, 3);
        await consumer.connect(connection, URL, false);
    });

    t.after(async () => {
        await consumer.close();
        await producer.close();
        await connection.close();
    });

    await t.test("should deliver messages in batches of 3 with prefetch=3", async () => {
        const acks: Array<() => void> = [];
        let received = 0;

        consumer.subscribe("h", async () => {
            received++;
            await new Promise<void>((resolve) => acks.push(resolve));
        });
        await consumer.consume();
        await new Promise((r) => setTimeout(r, 300));

        const total = 100;
        const batchSize = 3;

        for (let i = 0; i < total; i++) {
            await producer.publish({
                type: "test.event",
                data: Buffer.from(JSON.stringify({ n: i })),
                metadata: { contentType: "application/json" },
            });
        }

        await new Promise((r) => setTimeout(r, 500));

        assert.equal(received, batchSize, "initial batch should be 3 messages");

        let expected = batchSize;
        for (let batch = 2; batch * batchSize <= total; batch++) {
            for (let i = 0; i < batchSize; i++) {
                acks[expected - 3 + i]();
            }
            await new Promise((r) => setTimeout(r, 300));
            expected += batchSize;
            assert.equal(received, expected, `after batch ${batch}, should have ${expected} messages`);
        }

        const remaining = total - expected;
        if (remaining > 0) {
            for (let i = 0; i < remaining; i++) {
                acks[expected - 3 + i]();
            }
            await new Promise((r) => setTimeout(r, 300));
            expected += remaining;
            assert.equal(received, expected, `remaining messages should arrive`);
        }

        for (let i = expected - 3; i < acks.length; i++) {
            acks[i]();
        }
    });
});
