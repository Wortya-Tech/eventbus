import test from "node:test";
import assert from "node:assert/strict";
import { EventBusService } from "../../src/main.js";
import type { Channel, ChannelModel } from "amqplib";
import { pino } from "pino";

const testLogger = pino({ level: "silent" });

class TestEventBusService extends EventBusService {
    public readonly testIsConnectionAlive: (conn: ChannelModel) => boolean;
    public readonly testIsChannelHealthy: (ch: Channel | undefined) => boolean;

    constructor(
        exchangeName: string,
        queueName: string,
        source: string,
        version: string,
        maxRetries?: number,
        retryDelay?: number,
        maxConnectionRetries?: number,
        initialReconnectDelay?: number,
    ) {
        super(
            exchangeName,
            queueName,
            source,
            version,
            testLogger,
            maxRetries,
            retryDelay,
            maxConnectionRetries,
            initialReconnectDelay,
        );
        this.testIsConnectionAlive = (
            this as unknown as Record<
                string,
                (conn: ChannelModel) => boolean
            >
        )["isConnectionAlive"].bind(this);
        this.testIsChannelHealthy = (
            this as unknown as Record<
                string,
                (ch: Channel | undefined) => boolean
            >
        )["isChannelHealthy"].bind(this);
    }
}

test("isConnectionAlive", async (t) => {
    await t.test("detects null connections", () => {
        const service = new TestEventBusService(
            "test-exchange",
            "test-queue",
            "test-source",
            "1.0.0",
        );
        assert.equal(
            service.testIsConnectionAlive(null as unknown as ChannelModel),
            false,
        );
    });

    await t.test("handles malformed connection objects", () => {
        const service = new TestEventBusService(
            "test-exchange",
            "test-queue",
            "test-source",
            "1.0.0",
        );

        assert.equal(
            service.testIsConnectionAlive({} as ChannelModel),
            false,
        );
        assert.equal(
            service.testIsConnectionAlive({
                connection: undefined,
            } as unknown as ChannelModel),
            false,
        );
        assert.equal(
            service.testIsConnectionAlive({
                connection: null,
            } as unknown as ChannelModel),
            false,
        );
        assert.equal(
            service.testIsConnectionAlive({
                connection: { stream: undefined },
            } as unknown as ChannelModel),
            false,
        );
        assert.equal(
            service.testIsConnectionAlive({
                connection: { stream: null },
            } as unknown as ChannelModel),
            false,
        );
        assert.equal(
            service.testIsConnectionAlive({
                connection: { stream: { destroyed: true } },
            } as unknown as ChannelModel),
            false,
        );
        assert.equal(
            service.testIsConnectionAlive({
                connection: { stream: { destroyed: false } },
            } as unknown as ChannelModel),
            true,
        );
    });

    await t.test("catch block: accessing stream on non-object connection throws", () => {
        const service = new TestEventBusService(
            "test-exchange",
            "test-queue",
            "test-source",
            "1.0.0",
        );
        const obj = { connection: {} };
        Object.defineProperty(obj.connection, "stream", {
            get() { throw new Error("access error"); },
        });
        assert.equal(
            service.testIsConnectionAlive(obj as unknown as ChannelModel),
            false,
        );
    });
});

test("isChannelHealthy", async (t) => {
    await t.test("handles malformed states", async () => {
        const service = new TestEventBusService(
            "test-exchange",
            "test-queue",
            "test-source",
            "1.0.0",
        );

        assert.equal(
            service.testIsChannelHealthy(null as unknown as Channel),
            false,
        );
        assert.equal(
            service.testIsChannelHealthy({} as Channel),
            false,
        );
        assert.equal(
            service.testIsChannelHealthy({
                connection: null,
            } as unknown as Channel),
            false,
        );
        assert.equal(
            service.testIsChannelHealthy({
                connection: { stream: undefined },
            } as unknown as Channel),
            false,
        );
        assert.equal(
            service.testIsChannelHealthy({
                connection: { stream: null },
            } as unknown as Channel),
            false,
        );
        assert.equal(
            service.testIsChannelHealthy({
                connection: { stream: { destroyed: true } },
            } as unknown as Channel),
            false,
        );
        assert.equal(
            service.testIsChannelHealthy({
                connection: { stream: { destroyed: false } },
            } as unknown as Channel),
            true,
        );
    });

    await t.test("catch block: accessing stream on non-object channel throws", () => {
        const service = new TestEventBusService(
            "test-exchange",
            "test-queue",
            "test-source",
            "1.0.0",
        );
        const obj = { connection: {} };
        Object.defineProperty(obj.connection, "stream", {
            get() { throw new Error("access error"); },
        });
        assert.equal(
            service.testIsChannelHealthy(obj as unknown as Channel),
            false,
        );
    });
});
