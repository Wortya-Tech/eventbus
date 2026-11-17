import test from "node:test";
import assert from "node:assert/strict";
import { connect as amqpConnect } from "amqplib";
import type { Channel, ChannelModel } from "amqplib";
import { EventBusService } from "../../src/main.ts";

const URL = process.env.RABBITMQ_URL!;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

type ProviderMock = (() => Promise<ChannelModel>) & {
    mock: { callCount(): number };
};

type Internals = {
    channel?: Channel;
    connection?: ChannelModel;
    connectionRetryCount: number;
    isReconnecting: boolean;
    connectionProvider?: () => Promise<ChannelModel>;
    isConnectionAlive: (connection: ChannelModel) => boolean;
    isChannelHealthy: (channel?: Channel) => boolean;
    ensureChannel: () => Promise<Channel>;
};

function emitEvent(emitter: object, event: string, payload?: unknown): void {
    (emitter as unknown as {
        emit(event: string, ...args: unknown[]): boolean;
    }).emit(event, payload);
}

function stripListeners(emitter: object): void {
    const e = emitter as unknown as {
        removeAllListeners(event: string): unknown;
    };
    e.removeAllListeners("close");
    e.removeAllListeners("error");
}

test("ensureChannel - recreates an unhealthy channel", { skip: !URL ? "requires RABBITMQ_URL" : undefined }, async (t) => {
    let connection: ChannelModel;
    let svc: EventBusService;
    let internals: Internals;
    let providerMock: ProviderMock;
    const deadChannels: Channel[] = [];

    t.before(async () => {
        connection = await amqpConnect(URL);
        svc = new EventBusService("test.ensure.recreate", "test.ensure.recreate.q", "test", "1.0.0", undefined, 2, 100, 5, 100);
        providerMock = t.mock.fn(async () => connection) as ProviderMock;
        await svc.connect(connection, URL, false, providerMock);
        internals = svc as unknown as Internals;
    });

    t.after(async () => {
        for (const ch of deadChannels) stripListeners(ch);
        await svc.close();
        await connection.close();
    });

    await t.test("should recreate the channel using the connectionProvider when the channel is unhealthy", async () => {
        const oldChannel = internals.channel;
        const healthy = t.mock.method(internals, "isChannelHealthy", () => false);

        const recreated = await internals.ensureChannel();

        assert.equal(healthy.mock.callCount(), 1);
        assert.equal(providerMock.mock.callCount(), 1);
        assert.notEqual(recreated, oldChannel);
        assert.equal(internals.channel, recreated);
        assert.equal(internals.isConnectionAlive(internals.connection!), true);
    });

    await t.test("should fall back to the existing connection when the connectionProvider rejects", async () => {
        const previousChannel = internals.channel!;
        deadChannels.push(previousChannel);
        const healthy = t.mock.method(internals, "isChannelHealthy", () => false);
        const failingProvider = t.mock.fn(async () => {
            throw new Error("provider down");
        }) as ProviderMock;
        internals.connectionProvider = failingProvider;

        const recreated = await internals.ensureChannel();

        assert.equal(healthy.mock.callCount(), 1);
        assert.equal(failingProvider.mock.callCount(), 1);
        assert.notEqual(recreated, previousChannel);
        assert.equal(internals.channel, recreated);

        // Restore the working provider so later subtests can reconnect
        internals.connectionProvider = providerMock;
    });

    await t.test("should reconnect when the ensureChannel-recreated channel error handler fires", async () => {
        const current = internals.channel!;
        deadChannels.push(current);

        emitEvent(current, "error", new Error("synthetic error on recreated channel"));
        await sleep(900);

        assert.notEqual(internals.channel, current);
        assert.equal(internals.isReconnecting, false);
    });

    await t.test("should reconnect when the ensureChannel-recreated channel close handler fires unexpectedly", async () => {
        const previous = internals.channel!;
        deadChannels.push(previous);
        const healthy = t.mock.method(internals, "isChannelHealthy", () => false);
        const recreated = await internals.ensureChannel();
        deadChannels.push(recreated);
        assert.equal(healthy.mock.callCount(), 1);

        emitEvent(recreated, "close");
        await sleep(900);

        assert.notEqual(internals.channel, recreated);
        assert.equal(internals.isReconnecting, false);
    });

    await t.test("should run the intentional-close path when close() closes an ensureChannel-recreated channel", async () => {
        const previous = internals.channel!;
        deadChannels.push(previous);
        const healthy = t.mock.method(internals, "isChannelHealthy", () => false);
        const recreated = await internals.ensureChannel();
        assert.equal(healthy.mock.callCount(), 1);
        assert.equal(internals.channel, recreated);

        // close() marks the channel as intentionally closed, so the close
        // handler takes the intentional path instead of reconnecting
        await svc.close();

        assert.equal(internals.channel, undefined);
    });
});

test("connect() - channel event handlers trigger reconnection", { skip: !URL ? "requires RABBITMQ_URL" : undefined }, async (t) => {
    let connection: ChannelModel;
    let svc: EventBusService;
    let internals: Internals;
    const deadChannels: Channel[] = [];

    t.before(async () => {
        connection = await amqpConnect(URL);
        svc = new EventBusService("test.ch.events", "test.ch.events.q", "test", "1.0.0", undefined, 2, 100, 5, 100);
        await svc.connect(connection, URL, false);
        internals = svc as unknown as Internals;
    });

    t.after(async () => {
        for (const ch of deadChannels) stripListeners(ch);
        await svc.close();
        await connection.close();
    });

    await t.test("should reconnect the channel when the connect() error handler fires", async () => {
        const oldChannel = internals.channel!;
        deadChannels.push(oldChannel);

        emitEvent(oldChannel, "error", new Error("synthetic channel error"));
        await sleep(900);

        assert.notEqual(internals.channel, oldChannel);
        assert.equal(internals.connectionRetryCount, 0);
        assert.equal(internals.isReconnecting, false);
    });

    await t.test("should reconnect again when the re-created channel error handler fires", async () => {
        const current = internals.channel!;
        deadChannels.push(current);

        emitEvent(current, "error", new Error("synthetic channel error again"));
        await sleep(900);

        assert.notEqual(internals.channel, current);
        assert.equal(internals.connectionRetryCount, 0);
    });

    await t.test("should reconnect when the re-created channel close handler fires unexpectedly", async () => {
        const current = internals.channel!;
        deadChannels.push(current);

        emitEvent(current, "close");
        await sleep(900);

        assert.notEqual(internals.channel, current);
        assert.equal(internals.isReconnecting, false);
    });
});

test("connect() - connection event handlers trigger reconnection", { skip: !URL ? "requires RABBITMQ_URL" : undefined }, async (t) => {
    let oldConnection: ChannelModel;
    let producerConn: ChannelModel;
    let producer: EventBusService;
    let svc: EventBusService;
    let internals: Internals;

    t.before(async () => {
        oldConnection = await amqpConnect(URL);
        producerConn = await amqpConnect(URL);
        svc = new EventBusService("test.conn.events", "test.conn.events.q", "test", "1.0.0", undefined, 2, 100, 5, 100);
        await svc.connect(oldConnection, URL, true);
        producer = new EventBusService("test.conn.events", "test.conn.events.p", "test", "1.0.0", undefined, 2, 100);
        await producer.connect(producerConn, URL, false);
        internals = svc as unknown as Internals;
    });

    t.after(async () => {
        await producer.close();
        await producerConn.close();
        await svc.close();
        stripListeners(oldConnection);
        await oldConnection.close();
    });

    await t.test("should create a new connection, restart the consumer and keep consuming when the connect() error handler fires", async () => {
        const tkr = { attempts: 0 };
        svc.subscribe("h", async () => { tkr.attempts++; });
        await svc.consume(); await sleep(200);

        emitEvent(oldConnection, "error", new Error("synthetic connection error"));
        await sleep(1200);

        const newConnection = internals.connection;
        assert.ok(newConnection);
        assert.notEqual(newConnection, oldConnection);
        assert.equal(internals.isConnectionAlive(newConnection!), true);
        assert.equal(internals.connectionRetryCount, 0);
        assert.equal(internals.isReconnecting, false);

        await producer.publish({ type: "test.event", data: Buffer.from(JSON.stringify({ x: 1 })), metadata: { contentType: "application/json" } });
        await sleep(700);
        assert.ok(tkr.attempts >= 1, `Expected at least 1 attempt, got ${tkr.attempts}`);
    });
});
