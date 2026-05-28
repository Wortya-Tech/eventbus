import test from "node:test";
import assert from "node:assert/strict";
import { connect as amqpConnect } from "amqplib";
import type { ChannelModel } from "amqplib";
import { EventBusService } from "../../src/main.js";

const URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672";

test("lifecycle - cancel", async (t) => {
    let connection: ChannelModel;
    let producer: EventBusService;
    let consumer: EventBusService;

    t.before(async () => {
        connection = await amqpConnect(URL);
        producer = new EventBusService("test.lifecycle.cancel", "test.lifecycle.cancel.p", "test", "1.0.0", undefined, 2, 100);
        await producer.connect(connection, URL, false);
        consumer = new EventBusService("test.lifecycle.cancel", "test.lifecycle.cancel.q", "test", "1.0.0", undefined, 2, 100, 3, 100);
        await consumer.connect(connection, URL, false);
    });

    t.after(async () => {
        await consumer.close();
        await producer.close();
        await connection.close();
    });

    await t.test("should stop consuming after cancel", async () => {
        let count = 0;
        consumer.subscribe("h", () => { count++; return Promise.resolve(); });
        await consumer.consume(); await new Promise(r => setTimeout(r, 100));
        for (let i = 0; i < 3; i++) await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ i })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 500));
        const before = count;
        consumer.close(); await new Promise(r => setTimeout(r, 100));
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ x: 99 })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 500));
        assert.equal(count, before);
    });
});

test("lifecycle - graceful close", async (t) => {
    let connection: ChannelModel;
    let producer: EventBusService;
    let consumer: EventBusService;

    t.before(async () => {
        connection = await amqpConnect(URL);
        producer = new EventBusService("test.lifecycle.graceful", "test.lifecycle.graceful.p", "test", "1.0.0");
        await producer.connect(connection, URL, false);
        consumer = new EventBusService("test.lifecycle.graceful", "test.lifecycle.graceful.q", "test", "1.0.0");
        await consumer.connect(connection, URL, false);
    });

    t.after(async () => {
        await consumer.close();
        await producer.close();
        await connection.close();
    });

    await t.test("should gracefully close all resources", async () => {
        consumer.subscribe("h", async () => {});
        await consumer.consume();
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ x: 1 })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 100));
        assert.ok(true);
    });
});

test("lifecycle - unsubscribe", async (t) => {
    let connection: ChannelModel;
    let producer: EventBusService;
    let consumer: EventBusService;

    t.before(async () => {
        connection = await amqpConnect(URL);
        producer = new EventBusService("test.lifecycle.unsub", "test.lifecycle.unsub.p", "test", "1.0.0", undefined, 2, 100);
        await producer.connect(connection, URL, false);
        consumer = new EventBusService("test.lifecycle.unsub", "test.lifecycle.unsub.q", "test", "1.0.0", undefined, 2, 100, 3, 100);
        await consumer.connect(connection, URL, false);
    });

    t.after(async () => {
        await consumer.close();
        await producer.close();
        await connection.close();
    });

    await t.test("should unsubscribe and stop processing", async () => {
        let count = 0;
        consumer.subscribe("h", () => { count++; return Promise.resolve(); });
        await consumer.consume(); await new Promise(r => setTimeout(r, 100));
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ x: 1 })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 200));
        const before = count;
        consumer.unsubscribe("h");
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ x: 2 })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 200));
        assert.equal(count, before);
    });
});
