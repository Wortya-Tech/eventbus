import test from "node:test";
import assert from "node:assert/strict";
import { connect as amqpConnect } from "amqplib";
import type { ChannelModel } from "amqplib";
import { EventBusService } from "../../src/main.js";

const URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672";

test("basics - publish and consume", async (t) => {
    let connection: ChannelModel;
    let producer: EventBusService;
    let consumer: EventBusService;

    t.before(async () => {
        connection = await amqpConnect(URL);
        producer = new EventBusService("test.basics.pub", "test.basics.pub.p", "test", "1.0.0", undefined, 2, 100);
        await producer.connect(connection, URL, false);
        consumer = new EventBusService("test.basics.pub", "test.basics.pub.q", "test", "1.0.0", undefined, 2, 100, 3, 100);
        await consumer.connect(connection, URL, false);
    });

    t.after(async () => {
        await consumer.close();
        await producer.close();
        await connection.close();
    });

    await t.test("should publish and consume using event bus service", async () => {
        const data = { id: "basics-1", value: 1, timestamp: Date.now() };
        let received: typeof data | null = null;
        consumer.subscribe("h", async (buf) => { received = JSON.parse(new TextDecoder().decode(buf)); });
        await consumer.consume(); await new Promise(r => setTimeout(r, 100));
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify(data)), metadata: { contentType: "application/json" } });
        const start = Date.now();
        while (Date.now() - start < 5000) { if (received) break; await new Promise(r => setTimeout(r, 50)); }
        if (!received) throw new Error("timeout");
        assert.equal(received.id, data.id);
    });
});

test("basics - multiple subscribers", async (t) => {
    let connection: ChannelModel;
    let producer: EventBusService;
    let consumer: EventBusService;

    t.before(async () => {
        connection = await amqpConnect(URL);
        producer = new EventBusService("test.basics.multi", "test.basics.multi.p", "test", "1.0.0", undefined, 2, 100);
        await producer.connect(connection, URL, false);
        consumer = new EventBusService("test.basics.multi", "test.basics.multi.q", "test", "1.0.0", undefined, 2, 100, 3, 100);
        await consumer.connect(connection, URL, false);
    });

    t.after(async () => {
        await consumer.close();
        await producer.close();
        await connection.close();
    });

    await t.test("should execute multiple subscribers on same consumer", async () => {
        let a = false, b = false;
        consumer.subscribe("h1", async () => { a = true; }); consumer.subscribe("h2", async () => { b = true; });
        await consumer.consume(); await new Promise(r => setTimeout(r, 100));
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ x: 1 })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 500));
        assert.equal(a, true); assert.equal(b, true);
    });
});

test("basics - fanout", async (t) => {
    let connection: ChannelModel;
    let producer: EventBusService;
    let c1: EventBusService;
    let c2: EventBusService;

    t.before(async () => {
        connection = await amqpConnect(URL);
        producer = new EventBusService("test.basics.fanout", "test.basics.fanout.p", "test", "1.0.0", undefined, 2, 100);
        await producer.connect(connection, URL, false);
        c1 = new EventBusService("test.basics.fanout", "c1", "test", "1.0.0", undefined, 2, 100, 3, 100);
        await c1.connect(connection, URL, false);
        c2 = new EventBusService("test.basics.fanout", "c2", "test", "1.0.0", undefined, 2, 100, 3, 100);
        await c2.connect(connection, URL, false);
    });

    t.after(async () => {
        await c1.close();
        await c2.close();
        await producer.close();
        await connection.close();
    });

    await t.test("should fanout to multiple independent consumers", async () => {
        const data = { id: "fanout", value: 1, timestamp: Date.now() };
        let r1: typeof data | null = null, r2: typeof data | null = null;
        c1.subscribe("h1", async (buf) => { r1 = JSON.parse(new TextDecoder().decode(buf)); });
        c2.subscribe("h2", async (buf) => { r2 = JSON.parse(new TextDecoder().decode(buf)); });
        await c1.consume(); await c2.consume(); await new Promise(r => setTimeout(r, 100));
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify(data)), metadata: { contentType: "application/json" } });
        const start = Date.now();
        while (Date.now() - start < 5000) { if (r1 && r2) break; await new Promise(r => setTimeout(r, 50)); }
        if (!r1 || !r2) throw new Error("timeout");
        assert.equal(r1.id, data.id); assert.equal(r2.id, data.id);
    });
});

test("basics - subscribe overwrite", async (t) => {
    let connection: ChannelModel;
    let producer: EventBusService;
    let consumer: EventBusService;

    t.before(async () => {
        connection = await amqpConnect(URL);
        producer = new EventBusService("test.basics.overwrite", "test.basics.overwrite.p", "test", "1.0.0", undefined, 2, 100);
        await producer.connect(connection, URL, false);
        consumer = new EventBusService("test.basics.overwrite", "test.basics.overwrite.q", "test", "1.0.0", undefined, 2, 100, 3, 100);
        await consumer.connect(connection, URL, false);
    });

    t.after(async () => {
        await consumer.close();
        await producer.close();
        await connection.close();
    });

    await t.test("subscribe() should not overwrite existing handler", async () => {
        let h1c = false, h2c = false;
        consumer.subscribe("k", async () => { h1c = true; });
        consumer.subscribe("k", async () => { h2c = true; });
        await consumer.consume(); await new Promise(r => setTimeout(r, 100));
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ x: 1 })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 500));
        assert.equal(h1c, true); assert.equal(h2c, false);
    });
});
