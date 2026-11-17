import test from "node:test";
import assert from "node:assert/strict";
import { ConnectionProvider } from "../../src/main.ts";
import type { ChannelModel } from "amqplib";
import { assertLog, captureLogger } from "../fixtures/capture-logger.ts";

test("ConnectionProvider", async (t) => {
    await t.test("should store the URL", () => {
        const provider = new ConnectionProvider("amqp://localhost:5672");
        assert.equal(provider.url, "amqp://localhost:5672");
    });

    await t.test("should use default logger if not provided", () => {
        const provider = new ConnectionProvider("amqp://localhost:5672");
        assert.equal(
            typeof (provider as unknown as Record<string, unknown>)["logger"],
            "object",
        );
    });

    await t.test("should accept custom logger", () => {
        const customLogger = {
            info: () => {},
            debug: () => {},
            warn: () => {},
            error: () => {},
        };
        const provider = new ConnectionProvider(
            "amqp://localhost:5672",
            customLogger as unknown as import("pino").Logger,
        );
        assert.strictEqual(
            (provider as unknown as Record<string, unknown>)["logger"],
            customLogger,
        );
    });

    await t.test("create() should return existing connection if alive", async () => {
        const provider = new ConnectionProvider("amqp://localhost:5672");
        const mockConn = { connection: { stream: { destroyed: false } } };
        (provider as unknown as Record<string, ChannelModel>)["connection"] =
            mockConn as unknown as ChannelModel;

        const result = await provider.create();
        assert.strictEqual(
            result as unknown as typeof mockConn,
            mockConn,
        );
    });

    await t.test("isConnectionAlive() should return false when inspection throws", () => {
        const provider = new ConnectionProvider("amqp://localhost:5672");
        const throwingConn = new Proxy({}, {
            get() {
                throw new Error("boom");
            },
        });
        const isConnectionAlive = (
            provider as unknown as {
                isConnectionAlive: (conn: unknown) => boolean;
            }
        )["isConnectionAlive"].bind(provider);

        assert.equal(isConnectionAlive(throwingConn), false);
    });

    await t.test("isConnectionAlive() should return false for null connection", () => {
        const provider = new ConnectionProvider("amqp://localhost:5672");
        const isConnectionAlive = (
            provider as unknown as {
                isConnectionAlive: (conn: unknown) => boolean;
            }
        )["isConnectionAlive"].bind(provider);

        assert.equal(isConnectionAlive(null), false);
    });

    await t.test("isConnectionAlive() should return false when connection object is missing", () => {
        const provider = new ConnectionProvider("amqp://localhost:5672");
        const isConnectionAlive = (
            provider as unknown as {
                isConnectionAlive: (conn: unknown) => boolean;
            }
        )["isConnectionAlive"].bind(provider);

        assert.equal(isConnectionAlive({}), false);
    });

    await t.test("isConnectionAlive() should return false when stream is missing", () => {
        const provider = new ConnectionProvider("amqp://localhost:5672");
        const isConnectionAlive = (
            provider as unknown as {
                isConnectionAlive: (conn: unknown) => boolean;
            }
        )["isConnectionAlive"].bind(provider);

        assert.equal(isConnectionAlive({ connection: {} }), false);
    });

    await t.test("isConnectionAlive() should return false when the stream is destroyed", () => {
        const provider = new ConnectionProvider("amqp://localhost:5672");
        const isConnectionAlive = (
            provider as unknown as {
                isConnectionAlive: (conn: unknown) => boolean;
            }
        )["isConnectionAlive"].bind(provider);

        assert.equal(
            isConnectionAlive({ connection: { stream: { destroyed: true } } }),
            false,
        );
    });

    await t.test("create() should log when creating a new connection", async () => {
        const logs = captureLogger();
        const provider = new ConnectionProvider("amqp://localhost:1", logs.logger);

        // Port 1 refuses connections fast, so create() fails after logging
        await assert.rejects(() => provider.create());

        assertLog(
            logs.records,
            "Creating new RabbitMQ connection from provider",
            "info",
        );
        assertLog(logs.records, "Connection domain", "debug");
    });
});
