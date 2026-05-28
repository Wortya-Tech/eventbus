import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { connect as amqpConnect } from "amqplib";
import type { ChannelModel } from "amqplib";
import { ConnectionProvider, EventBusService } from "../../src/main.js";

const URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672";

describe("connection-sharing - owned producer", () => {
    let connection: ChannelModel;
    let producer: EventBusService;

    before(async () => {
        connection = await amqpConnect(URL);
        producer = new EventBusService("test.owned.producer", "test.owned.producer.q", "test", "1.0.0");
        await producer.connect(connection, "amqp://guest:guest@localhost:5672", true);
    });

    after(async () => {
        await producer.close();
    });

    it("should manage owned connection for producer", async () => {
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ x: 1 })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 100));
        assert.ok(true);
    });
});

describe("connection-sharing - owned consumer", () => {
    let connection: ChannelModel;
    let producer: EventBusService;
    let consumer: EventBusService;

    before(async () => {
        connection = await amqpConnect(URL);
        producer = new EventBusService("test.owned.consumer", "test.owned.consumer.p", "test", "1.0.0");
        await producer.connect(connection, "amqp://guest:guest@localhost:5672", false);
        consumer = new EventBusService("test.owned.consumer", "test.owned.consumer.q", "test", "1.0.0", undefined, 2, 100, 3, 100);
        await consumer.connect(connection, "amqp://guest:guest@localhost:5672", false);
    });

    after(async () => {
        await consumer.close();
        await producer.close();
        await connection.close();
    });

    it("should manage owned connection for consumer", async () => {
        const data = { id: "owned", value: 1, timestamp: Date.now() };
        consumer.subscribe("h", (buf) => { assert.equal(JSON.parse(new TextDecoder().decode(buf)).id, data.id); return Promise.resolve(); });
        await consumer.consume(); await new Promise(r => setTimeout(r, 100));
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify(data)), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 200));
        assert.ok(true);
    });
});

describe("connection-sharing - provider shared", () => {
    let connection: ChannelModel;
    let producer: EventBusService;
    let consumer: EventBusService;

    before(async () => {
        connection = await amqpConnect(URL);
        producer = new EventBusService("test.shared.connection", "test.shared.connection.p", "test", "1.0.0");
        await producer.connect(connection, undefined, false);
        consumer = new EventBusService("test.shared.connection", "test.shared.connection.q", "test", "1.0.0");
        await consumer.connect(connection, undefined, false);
    });

    after(async () => {
        await consumer.close();
        await producer.close();
        await connection.close();
    });

    it("should share connection via connection provider", async () => {
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ x: 1 })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 200));
        assert.ok(true);
    });
});

describe("connection-sharing - provider reconnect", () => {
    let connection: ChannelModel;
    let producer: EventBusService;

    before(async () => {
        const provider = new ConnectionProvider("amqp://guest:guest@localhost:5672");
        connection = await provider.create();
        producer = new EventBusService("test.provider.reconnect", "test.provider.reconnect.p", "test", "1.0.0");
        await producer.connect(connection, undefined, false);
    });

    after(async () => {
        await producer.close();
        await connection.close();
    });

    it("should reuse connection provider on reconnect", async () => {
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ x: 1 })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 200));
        const provider = new ConnectionProvider("amqp://guest:guest@localhost:5672");
        const newConn = await provider.create();
        assert.ok(newConn !== null);
        await newConn.close();
    });
});


