import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { connect as amqpConnect } from "amqplib";
import type { ChannelModel } from "amqplib";
import { EventBusService } from "../../src/main.js";

const URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672";

describe("advanced - concurrent publishing", () => {
    let connection: ChannelModel;
    let producer: EventBusService;
    let consumer: EventBusService;

    before(async () => {
        connection = await amqpConnect(URL);
        producer = new EventBusService("test.advanced.concurrent", "test.advanced.concurrent.p", "test", "1.0.0", undefined, 2, 100);
        await producer.connect(connection, URL, false);
        consumer = new EventBusService("test.advanced.concurrent", "test.advanced.concurrent.q", "test", "1.0.0", undefined, 2, 100, 3, 100);
        await consumer.connect(connection, URL, false);
    });

    after(async () => {
        await consumer.close();
        await producer.close();
        await connection.close();
    });

    it("should handle concurrent publishing", async () => {
        const msgs: string[] = [];
        consumer.subscribe("h", async (buf) => { msgs.push(JSON.parse(new TextDecoder().decode(buf)).id); });
        await consumer.consume(); await new Promise(r => setTimeout(r, 100));
        const N = 10;
        await Promise.all(Array.from({ length: N }, (_, i) =>
            producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ id: `msg-${i}` })), metadata: { contentType: "application/json" } })
        ));
        await new Promise(r => setTimeout(r, 1500));
        const start = Date.now();
        while (Date.now() - start < 5000) { if (msgs.length >= N) break; await new Promise(r => setTimeout(r, 50)); }
        assert.equal(msgs.length, N);
    });
});

describe("advanced - message order", () => {
    let connection: ChannelModel;
    let producer: EventBusService;
    let consumer: EventBusService;

    before(async () => {
        connection = await amqpConnect(URL);
        producer = new EventBusService("test.advanced.order", "test.advanced.order.p", "test", "1.0.0", undefined, 2, 100);
        await producer.connect(connection, URL, false);
        consumer = new EventBusService("test.advanced.order", "test.advanced.order.q", "test", "1.0.0", undefined, 2, 100, 3, 100);
        await consumer.connect(connection, URL, false);
    });

    after(async () => {
        await consumer.close();
        await producer.close();
        await connection.close();
    });

    it("should maintain message order during retries", async () => {
        const order: string[] = [];
        let fail = 0;
        consumer.subscribe("h", async (buf) => { const { id } = JSON.parse(new TextDecoder().decode(buf)); order.push(id); if (id === "order-1" && fail === 0) { fail++; throw new Error("fail"); } });
        await consumer.consume(); await new Promise(r => setTimeout(r, 100));
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ id: "order-0" })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 50));
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ id: "order-1" })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 50));
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ id: "order-2" })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 3000));
        assert.equal(order[0], "order-0"); assert.equal(order[order.length - 1], "order-1");
    });
});

describe("advanced - pre publish", () => {
    let connection: ChannelModel;
    let producer: EventBusService;
    let consumer: EventBusService;

    before(async () => {
        connection = await amqpConnect(URL);
        producer = new EventBusService("test.advanced.prepub", "test.advanced.prepub.p", "test", "1.0.0", undefined, 2, 100);
        await producer.connect(connection, URL, false);
        consumer = new EventBusService("test.advanced.prepub", "test.advanced.prepub.q", "test", "1.0.0", undefined, 2, 100, 3, 100);
        await consumer.connect(connection, URL, false);
    });

    after(async () => {
        await consumer.close();
        await producer.close();
        await connection.close();
    });

    it("should handle message publish before consumer is ready", async () => {
        let received: { id: string } | null = null;
        consumer.subscribe("h", (buf) => { received = JSON.parse(new TextDecoder().decode(buf)); return Promise.resolve(); });
        await consumer.consume(); await new Promise(r => setTimeout(r, 100));
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ id: "pre" })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 300));
        if (!received) throw new Error("no message");
        assert.equal((received as { id: string }).id, "pre");
    });
});

describe("advanced - metadata", () => {
    let connection: ChannelModel;
    let producer: EventBusService;
    let consumer: EventBusService;

    before(async () => {
        connection = await amqpConnect(URL);
        producer = new EventBusService("test.advanced.metadata", "test.advanced.metadata.p", "my-service", "2.0.0");
        await producer.connect(connection, URL, false);
        consumer = new EventBusService("test.advanced.metadata", "test.advanced.metadata.q", "test", "1.0.0", undefined, 2, 100, 3, 100);
        await consumer.connect(connection, URL, false);
    });

    after(async () => {
        await consumer.close();
        await producer.close();
        await connection.close();
    });

    it("should include all message metadata", async () => {
        let props: Record<string, unknown> | null = null;
        consumer.subscribe("h", (_d, p) => { props = p as unknown as Record<string, unknown>; return Promise.resolve(); });
        await consumer.consume(); await new Promise(r => setTimeout(r, 100));
        const cid = "corr-123";
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ x: 1 })), metadata: { contentType: "application/json", contentEncoding: "utf-8", correlationId: cid } });
        await new Promise(r => setTimeout(r, 1000));
        assert.equal(props!.appId, "my-service@2.0.0+test.advanced.metadata");
        assert.equal(props!.contentType, "application/json");
        assert.equal(props!.contentEncoding, "utf-8");
        assert.equal(props!.correlationId, cid);
        assert.equal(typeof props!.messageId, "string");
        assert.equal(typeof props!.timestamp, "number");
    });
});
