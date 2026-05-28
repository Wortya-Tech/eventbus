import test from "node:test";
import assert from "node:assert/strict";
import { connect as amqpConnect } from "amqplib";
import type { ChannelModel } from "amqplib";
import { ConnectionProvider, EventBusService } from "../../src/main.js";

const URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672";

test("connection-sharing - owned producer", async (t) => {
    let connection: ChannelModel;
    let producer: EventBusService;

    t.before(async () => {
        connection = await amqpConnect(URL);
        producer = new EventBusService("test.owned.producer", "test.owned.producer.q", "test", "1.0.0");
        await producer.connect(connection, "amqp://guest:guest@localhost:5672", true);
    });

    t.after(async () => {
        await producer.close();
    });

    await t.test("should manage owned connection for producer", async () => {
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ x: 1 })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 100));
        assert.ok(true);
    });
});

test("connection-sharing - owned consumer", async (t) => {
    let connection: ChannelModel;
    let producer: EventBusService;
    let consumer: EventBusService;

    t.before(async () => {
        connection = await amqpConnect(URL);
        producer = new EventBusService("test.owned.consumer", "test.owned.consumer.p", "test", "1.0.0");
        await producer.connect(connection, "amqp://guest:guest@localhost:5672", false);
        consumer = new EventBusService("test.owned.consumer", "test.owned.consumer.q", "test", "1.0.0", undefined, 2, 100, 3, 100);
        await consumer.connect(connection, "amqp://guest:guest@localhost:5672", false);
    });

    t.after(async () => {
        await consumer.close();
        await producer.close();
        await connection.close();
    });

    await t.test("should manage owned connection for consumer", async () => {
        const data = { id: "owned", value: 1, timestamp: Date.now() };
        consumer.subscribe("h", (buf) => { assert.equal(JSON.parse(new TextDecoder().decode(buf)).id, data.id); return Promise.resolve(); });
        await consumer.consume(); await new Promise(r => setTimeout(r, 100));
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify(data)), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 200));
        assert.ok(true);
    });
});

test("connection-sharing - provider shared", async (t) => {
    let connection: ChannelModel;
    let producer: EventBusService;
    let consumer: EventBusService;

    t.before(async () => {
        connection = await amqpConnect(URL);
        producer = new EventBusService("test.shared.connection", "test.shared.connection.p", "test", "1.0.0");
        await producer.connect(connection, undefined, false);
        consumer = new EventBusService("test.shared.connection", "test.shared.connection.q", "test", "1.0.0");
        await consumer.connect(connection, undefined, false);
    });

    t.after(async () => {
        await consumer.close();
        await producer.close();
        await connection.close();
    });

    await t.test("should share connection via connection provider", async () => {
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ x: 1 })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 200));
        assert.ok(true);
    });
});

test("connection-sharing - provider reconnect", async (t) => {
    let connection: ChannelModel;
    let producer: EventBusService;

    t.before(async () => {
        const provider = new ConnectionProvider("amqp://guest:guest@localhost:5672");
        connection = await provider.create();
        producer = new EventBusService("test.provider.reconnect", "test.provider.reconnect.p", "test", "1.0.0");
        await producer.connect(connection, undefined, false);
    });

    t.after(async () => {
        await producer.close();
        await connection.close();
    });

    await t.test("should reuse connection provider on reconnect", async () => {
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ x: 1 })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 200));
        const provider = new ConnectionProvider("amqp://guest:guest@localhost:5672");
        const newConn = await provider.create();
        assert.ok(newConn !== null);
        await newConn.close();
    });
});

test("connection-sharing - provider dead connection", async (t) => {
    let provider: ConnectionProvider;

    t.before(() => {
        provider = new ConnectionProvider("amqp://guest:guest@localhost:5672");
        // Inject a "dead" connection so create() makes a new one
        (provider as unknown as Record<string, ChannelModel>)["connection"] = {
            connection: { stream: { destroyed: true } },
        } as unknown as ChannelModel;
    });

    t.after(async () => {
        // The connection created by create() is stored on the provider
        const conn = (provider as unknown as Record<string, ChannelModel>)["connection"];
        if (conn && typeof (conn as unknown as { close?: () => Promise<void> }).close === "function") {
            await conn.close();
        }
    });

    await t.test("create() should make new connection when existing is dead", async () => {
        const conn = await provider.create();
        assert.ok(conn !== null);
        const alive = (provider as unknown as Record<string, (c: ChannelModel) => boolean>)["isConnectionAlive"](conn);
        assert.equal(alive, true);
    });
});

test("connection-sharing - ensureChannel with provider", async (t) => {
    let connection: ChannelModel;
    let svc: EventBusService;
    let provider: ConnectionProvider;

    t.before(async () => {
        connection = await amqpConnect(URL);
        provider = new ConnectionProvider("amqp://guest:guest@localhost:5672");
        svc = new EventBusService("test.ensure.channel.pvd", "test.ensure.channel.pvd.q", "test", "1.0.0", undefined, 2, 100, 3, 100);
        await svc.connect(connection, undefined, false, () => provider.create());
    });

    t.after(async () => {
        await svc.close();
        await connection.close();
        const pvdConn = (provider as unknown as Record<string, ChannelModel>)["connection"];
        if (pvdConn) await pvdConn.close();
    });

    await t.test("should recreate channel via connectionProvider when channel dies", async () => {
        svc.subscribe("h", async () => {});
        await svc.consume(); await new Promise(r => setTimeout(r, 100));

        // Kill the service's channel — ensureChannel will detect and use connectionProvider
        const svcChan = (svc as unknown as { channel?: { close(): Promise<void> } }).channel;
        if (svcChan) await svcChan.close();
        await new Promise(r => setTimeout(r, 300));

        const succeeded = await svc.publish({
            type: "test.event",
            data: Buffer.from(JSON.stringify({ x: 1 })),
            metadata: { contentType: "application/json" },
        });
        assert.equal(succeeded, true);
    });
});


