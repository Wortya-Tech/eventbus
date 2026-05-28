import test from "node:test";
import assert from "node:assert/strict";
import { ConnectionProvider } from "../../src/main.js";

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
});
