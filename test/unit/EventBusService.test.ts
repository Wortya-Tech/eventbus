import test from "node:test";
import assert from "node:assert/strict";
import { EventBusService } from "../../src/main.js";
import type { ChannelModel } from "amqplib";
import type { MessageHandler } from "../../src/main.js";

test("EventBusService", async (t) => {
    await t.test("should use default logger if not provided", () => {
        const service = new EventBusService(
            "test-exchange",
            "test-queue",
            "test-source",
            "1.0.0",
        );
        assert.equal(
            typeof (service as unknown as Record<string, unknown>)["logger"],
            "object",
        );
    });

    await t.test("should use default retry configuration", () => {
        const service = new EventBusService(
            "test-exchange",
            "test-queue",
            "test-source",
            "1.0.0",
        );
        assert.equal(
            (service as unknown as Record<string, unknown>)["MAX_RETRIES"],
            3,
        );
        assert.equal(
            (service as unknown as Record<string, unknown>)["RETRY_DELAY"],
            5000,
        );
    });

    await t.test("should allow custom retry configuration via parameters", () => {
        const service = new EventBusService(
            "test-exchange",
            "test-queue",
            "test-source",
            "1.0.0",
            undefined,
            5,
            10000,
        );
        assert.equal(
            (service as unknown as Record<string, unknown>)["MAX_RETRIES"],
            5,
        );
        assert.equal(
            (service as unknown as Record<string, unknown>)["RETRY_DELAY"],
            10000,
        );
    });

    await t.test("should allow custom connection retry configuration", () => {
        const service = new EventBusService(
            "test-exchange",
            "test-queue",
            "test-source",
            "1.0.0",
            undefined,
            3,
            5000,
            7,
            2000,
        );
        assert.equal(
            (service as unknown as Record<string, unknown>)[
                "MAX_CONNECTION_RETRIES"
            ],
            7,
        );
        assert.equal(
            (service as unknown as Record<string, unknown>)[
                "INITIAL_RECONNECT_DELAY"
            ],
            2000,
        );
    });

    await t.test("connect() should reject with null connection", async () => {
        const service = new EventBusService(
            "test-exchange",
            "test-queue",
            "test-source",
            "1.0.0",
        );
        await assert.rejects(
            () => service.connect(null as unknown as ChannelModel),
        );
    });

    await t.test("subscribe() should keep original handler when key already exists", () => {
        const service = new EventBusService(
            "test-exchange",
            "test-queue",
            "test-source",
            "1.0.0",
        );
        const h1: MessageHandler = async () => {};
        const h2: MessageHandler = async () => {};
        service.subscribe("k", h1);
        service.subscribe("k", h2);
        const subs = (service as unknown as Record<string, Map<string, MessageHandler>>)["subscribers"];
        assert.equal(subs.size, 1);
        assert.strictEqual(subs.get("k"), h1);
    });

    await t.test("unsubscribe() should remove handler", () => {
        const service = new EventBusService(
            "test-exchange",
            "test-queue",
            "test-source",
            "1.0.0",
        );
        service.subscribe("k", async () => {});
        service.unsubscribe("k");
        const subs = (service as unknown as Record<string, Map<string, MessageHandler>>)["subscribers"];
        assert.equal(subs.size, 0);
    });
});
