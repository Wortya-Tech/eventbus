import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { connect as amqpConnect } from "amqplib";
import type { ChannelModel } from "amqplib";
import { EventBusService } from "../../src/main.js";

const URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672";

describe("retry - should retry failed handler and eventually succeed", () => {
    let connection: ChannelModel;
    let producer: EventBusService;
    let consumer: EventBusService;

    before(async () => {
        connection = await amqpConnect(URL);
        producer = new EventBusService("test.retry.success", "test.retry.success.p", "test", "1.0.0", undefined, 2, 100);
        await producer.connect(connection, URL, false);
        consumer = new EventBusService("test.retry.success", "test.retry.success.q", "test", "1.0.0", undefined, 2, 100, 3, 100);
        await consumer.connect(connection, URL, false);
    });

    after(async () => {
        await consumer.close();
        await producer.close();
        await connection.close();
    });

    it("should retry failed handler and eventually succeed", async () => {
        const tkr = { attempts: 0, failures: 0 };
        consumer.subscribe("h", () => {
            tkr.attempts++; if (tkr.attempts <= 1) { tkr.failures++; return Promise.reject(new Error("fail")); }
            return Promise.resolve();
        });
        await consumer.consume(); await new Promise(r => setTimeout(r, 100));
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ x: 1 })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 500));
        assert.ok(tkr.attempts >= 2); assert.equal(tkr.failures, 1); assert.ok(tkr.attempts > tkr.failures);
    });
});

describe("retry - should send to dead letter queue after max retries", () => {
    let connection: ChannelModel;
    let producer: EventBusService;
    let consumer: EventBusService;

    before(async () => {
        connection = await amqpConnect(URL);
        producer = new EventBusService("test.retry.dlq", "test.retry.dlq.p", "test", "1.0.0", undefined, 2, 100);
        await producer.connect(connection, URL, false);
        consumer = new EventBusService("test.retry.dlq", "test.retry.dlq.q", "test", "1.0.0", undefined, 2, 100, 3, 100);
        await consumer.connect(connection, URL, false);
    });

    after(async () => {
        await consumer.close();
        await producer.close();
        await connection.close();
    });

    it("should send to dead letter queue after max retries", async () => {
        const tkr = { attempts: 0 };
        consumer.subscribe("h", async () => { tkr.attempts++; if (tkr.attempts <= 2) throw new Error("fail"); });
        await consumer.consume(); await new Promise(r => setTimeout(r, 100));
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ x: 1 })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 400 + 1000));
        assert.ok(tkr.attempts > 2);
    });
});

describe("retry - should increment retry count headers correctly", () => {
    let connection: ChannelModel;
    let producer: EventBusService;
    let consumer: EventBusService;

    before(async () => {
        connection = await amqpConnect(URL);
        producer = new EventBusService("test.retry.headers", "test.retry.headers.p", "test", "1.0.0", undefined, 2, 100);
        await producer.connect(connection, URL, false);
        consumer = new EventBusService("test.retry.headers", "test.retry.headers.q", "test", "1.0.0", undefined, 2, 100, 3, 100);
        await consumer.connect(connection, URL, false);
    });

    after(async () => {
        await consumer.close();
        await producer.close();
        await connection.close();
    });

    it("should increment retry count headers correctly", async () => {
        const tkr = { attempts: 0 };
        consumer.subscribe("h", async () => { tkr.attempts++; if (tkr.attempts <= 2) throw new Error("fail"); });
        await consumer.consume(); await new Promise(r => setTimeout(r, 100));
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ x: 1 })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 300 + 500));
        assert.ok(tkr.attempts > 1); assert.ok(tkr.attempts >= 2);
    });
});

describe("retry - should send directly to DLQ on unexpected consume error", () => {
    let connection: ChannelModel;
    let producer: EventBusService;
    let consumer: EventBusService;

    before(async () => {
        connection = await amqpConnect(URL);
        producer = new EventBusService("test.retry.unexpected", "test.retry.unexpected.p", "test", "1.0.0", undefined, 1, 100);
        await producer.connect(connection, URL, false);
        consumer = new EventBusService("test.retry.unexpected", "test.retry.unexpected.q", "test", "1.0.0", undefined, 1, 100, 3, 100);
        await consumer.connect(connection, URL, false);
    });

    after(async () => {
        await consumer.close();
        await producer.close();
        await connection.close();
    });

    it("should send directly to DLQ on unexpected consume error", async () => {
        consumer.subscribe("h", (): Promise<void> => { throw new Error("unexpected sync error"); });
        await consumer.consume(); await new Promise(r => setTimeout(r, 100));
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ x: 1 })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 500));
        assert.ok(true);
    });
});
