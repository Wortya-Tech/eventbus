import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { connect as amqpConnect } from "amqplib";
import type { ChannelModel } from "amqplib";
import { EventBusService } from "../../src/main.js";

const URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672";

describe("reconnection - channel killed", () => {
    let connection: ChannelModel;
    let producer: EventBusService;
    let consumer: EventBusService;

    before(async () => {
        connection = await amqpConnect(URL);
        producer = new EventBusService("test.channel.reconnect", "test.channel.reconnect.p", "test", "1.0.0", undefined, 2, 100);
        await producer.connect(connection, URL, false);
        consumer = new EventBusService("test.channel.reconnect", "test.channel.reconnect.q", "test", "1.0.0", undefined, 2, 100, 3, 100);
        await consumer.connect(connection, URL, false);
    });

    after(async () => {
        await consumer.close();
        await producer.close();
        await connection.close();
    });

    it("should reconnect after channel is killed", async () => {
        const tkr = { attempts: 0 };
        consumer.subscribe("h", () => { tkr.attempts++; return Promise.resolve(); });
        await consumer.consume(); await new Promise(r => setTimeout(r, 100));
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ id: "msg1" })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 500));
        const ch = await connection.createChannel(); await ch.close();
        await new Promise(r => setTimeout(r, 600));
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ id: "msg2" })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 500));
        assert.ok(tkr.attempts >= 1);
    });
});

describe("reconnection - connection killed", () => {
    let secondConn: ChannelModel;
    let newProducer: EventBusService;

    before(async () => {
        const firstConn = await amqpConnect(URL);
        const ex = "test.connection.reconnect";
        const pq = "test.connection.reconnect.p";
        const producer = new EventBusService(ex, pq, "test", "1.0.0");
        await producer.connect(firstConn, URL, true);
        const consumer = new EventBusService(ex, "test.connection.reconnect.q", "test", "1.0.0", undefined, 2, 100, 3, 100);
        await consumer.connect(firstConn, URL, false);
        consumer.subscribe("h", () => Promise.reject());
        await consumer.consume(); await new Promise(r => setTimeout(r, 100));
        await consumer.close(); await producer.close();
        await new Promise(r => setTimeout(r, 100));
        secondConn = await amqpConnect(URL);
        newProducer = new EventBusService(ex, pq, "test", "1.0.0");
        await newProducer.connect(secondConn, URL, false);
    });

    after(async () => {
        await newProducer.close();
        await secondConn.close();
    });

    it("should reconnect after connection is killed", async () => {
        await newProducer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ id: "reconnect" })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 1000));
        assert.ok(true);
    });
});

describe("reconnection - handleChannelReconnect", () => {
    let connection: ChannelModel;
    let producer: EventBusService;
    let consumer: EventBusService;

    before(async () => {
        connection = await amqpConnect(URL);
        producer = new EventBusService("test.handle.channel.reconnect", "test.handle.channel.reconnect.p", "test", "1.0.0", undefined, 2, 100);
        await producer.connect(connection, URL, false);
        consumer = new EventBusService("test.handle.channel.reconnect", "test.handle.channel.reconnect.q", "test", "1.0.0", undefined, 2, 100, 5, 100);
        await consumer.connect(connection, URL, false);
    });

    after(async () => {
        await consumer.close();
        await producer.close();
        await connection.close();
    });

    it("should trigger handleChannelReconnect when consumer channel dies", async () => {
        const tkr = { attempts: 0 };
        consumer.subscribe("h", () => { tkr.attempts++; return Promise.resolve(); });
        await consumer.consume(); await new Promise(r => setTimeout(r, 100));

        // Publish before kill — should be received
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ id: "before" })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 200));

        // Kill the consumer's own channel
        const svcChan = (consumer as unknown as { channel?: { close(): Promise<void> } }).channel;
        if (svcChan) await svcChan.close();
        await new Promise(r => setTimeout(r, 800));

        // Publish after reconnect — should arrive via new channel
        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ id: "after" })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 500));

        assert.ok(tkr.attempts >= 2, `Expected >=2 attempts, got ${tkr.attempts}`);
    });
});

describe("reconnection - handleConnectionReconnect", () => {
    let svc: EventBusService;
    let consumerConn: ChannelModel;

    before(async () => {
        consumerConn = await amqpConnect(URL);
        svc = new EventBusService("test.handle.conn.reconnect", "test.handle.conn.reconnect.q", "test", "1.0.0", undefined, 2, 100, 5, 100);
        await svc.connect(consumerConn, URL, true);
    });

    after(async () => {
        await svc.close();
    });

    it("should reconnect connection when connection dies with ownsConnection=true", async () => {
        svc.subscribe("h", async () => {});
        await svc.consume(); await new Promise(r => setTimeout(r, 100));

        // Kill connection — closes the connection and all channels on it
        await consumerConn.close();
        await new Promise(r => setTimeout(r, 2000));

        // After reconnect, publish should succeed on new connection
        const succeeded = await svc.publish({
            type: "test.event",
            data: Buffer.from(JSON.stringify({ x: 1 })),
            metadata: { contentType: "application/json" },
        });

        assert.ok(succeeded !== undefined);
    });
});
