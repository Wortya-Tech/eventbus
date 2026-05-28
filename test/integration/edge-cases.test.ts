import test from "node:test";
import assert from "node:assert/strict";
import { connect as amqpConnect } from "amqplib";
import type { ChannelModel } from "amqplib";
import { EventBusService } from "../../src/main.js";

const URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672";

test("edge-cases - duplicate subscribe", async (t) => {
    let connection: ChannelModel;
    let consumer: EventBusService;

    t.before(async () => {
        connection = await amqpConnect(URL);
        consumer = new EventBusService("test.edge.dup", "test.edge.dup.q", "test", "1.0.0", undefined, 2, 100, 3, 100);
        await consumer.connect(connection);
    });

    t.after(async () => {
        await consumer.close();
        await connection.close();
    });

    await t.test("should not throw when subscribing with existing key", () => {
        const h = async () => {};
        consumer.subscribe("test-key", h); consumer.subscribe("test-key", h);
        assert.ok(true);
    });
});

test("edge-cases - publish fail", async (t) => {
    let svc: EventBusService;

    t.before(() => {
        svc = new EventBusService("test.edge.publish.fail", "test.edge.publish.fail.q", "test", "1.0.0", undefined, 2, 100);
    });

    t.after(async () => {
        await svc.close();
    });

    await t.test("publish() should return false when not connected", async () => {
        const result = await svc.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ x: 1 })), metadata: { contentType: "application/json" } });
        assert.equal(result, false);
    });
});

test("edge-cases - close error", async (t) => {
    let connection: ChannelModel;
    let svc: EventBusService;

    t.before(async () => {
        connection = await amqpConnect(URL);
        svc = new EventBusService("test.edge.close.error", "test.edge.close.error.q", "test", "1.0.0", undefined, 2, 100, 3, 100);
        await svc.connect(connection, URL, false);
    });

    t.after(async () => {
        await svc.close();
        await connection.close();
    });

    await t.test("close() should handle double close gracefully", async () => {
        await svc.close();
        await svc.close();
        assert.ok(true);
    });
});
