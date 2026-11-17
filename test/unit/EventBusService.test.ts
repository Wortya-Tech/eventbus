import test from "node:test";
import assert from "node:assert/strict";
import { EventBusService } from "../../src/main.ts";
import type { ChannelModel } from "amqplib";
import type { MessageHandler } from "../../src/main.ts";

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

    await t.test("should use default prefetch of 10", () => {
        const service = new EventBusService(
            "test-exchange",
            "test-queue",
            "test-source",
            "1.0.0",
        );
        assert.equal(
            (service as unknown as Record<string, unknown>)["PREFETCH"],
            10,
        );
    });

    await t.test("should allow custom prefetch via parameter", () => {
        const service = new EventBusService(
            "test-exchange",
            "test-queue",
            "test-source",
            "1.0.0",
            undefined,
            3,
            5000,
            10,
            1000,
            5,
        );
        assert.equal(
            (service as unknown as Record<string, unknown>)["PREFETCH"],
            5,
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

    await t.test("close() should handle error when canceling consumer", async () => {
        const service = new EventBusService(
            "test-exchange",
            "test-queue",
            "test-source",
            "1.0.0",
        );
        const priv = service as unknown as Record<string, unknown>;
        priv["consumerTag"] = "test-tag";
        priv["channel"] = {
            cancel: () => Promise.reject(new Error("cancel failed")),
            close: () => Promise.resolve(),
        };

        await service.close();
        assert.ok(true);
    });

    await t.test("handleChannelReconnect() should skip when already reconnecting", async () => {
        const service = new EventBusService(
            "test-exchange",
            "test-queue",
            "test-source",
            "1.0.0",
        );
        const priv = service as unknown as {
            isReconnecting: boolean;
            connectionRetryCount: number;
            handleChannelReconnect: () => Promise<void>;
        };
        priv.isReconnecting = true;

        await priv.handleChannelReconnect();

        assert.equal(priv.connectionRetryCount, 0);
        assert.equal(priv.isReconnecting, true);
    });

    await t.test("handleConnectionReconnect() should skip when already reconnecting", async () => {
        const service = new EventBusService(
            "test-exchange",
            "test-queue",
            "test-source",
            "1.0.0",
        );
        const priv = service as unknown as {
            isReconnecting: boolean;
            connectionRetryCount: number;
            handleConnectionReconnect: () => Promise<void>;
        };
        priv.isReconnecting = true;

        await priv.handleConnectionReconnect();

        assert.equal(priv.connectionRetryCount, 0);
        assert.equal(priv.isReconnecting, true);
    });

    await t.test("handleConnectionReconnect() should give up after max retries when no URL is available", async () => {
        const service = new EventBusService(
            "test-exchange",
            "test-queue",
            "test-source",
            "1.0.0",
            undefined,
            3,
            100,
            3,
            10,
        );
        const priv = service as unknown as {
            isReconnecting: boolean;
            connectionRetryCount: number;
            handleConnectionReconnect: () => Promise<void>;
        };

        // No rabbitmqUrl: every attempt throws, recursion retries until the cap
        await priv.handleConnectionReconnect();

        assert.equal(priv.connectionRetryCount, 3);
        assert.equal(priv.isReconnecting, false);
    });

    await t.test("createQueue() should reject when channel is not initialized", async () => {
        const service = new EventBusService(
            "test-exchange",
            "test-queue",
            "test-source",
            "1.0.0",
        );
        const priv = service as unknown as {
            createQueue: () => Promise<void>;
        };

        await assert.rejects(
            () => priv.createQueue(),
            /Channel not initialized/,
        );
    });

    await t.test("handleMessage() should ignore null messages", async () => {
        const service = new EventBusService(
            "test-exchange",
            "test-queue",
            "test-source",
            "1.0.0",
        );
        const priv = service as unknown as {
            handleMessage: (msg: null) => Promise<void>;
        };

        await priv.handleMessage(null);

        assert.equal(
            (service as unknown as Record<string, unknown>)["channel"],
            undefined,
        );
    });
});
