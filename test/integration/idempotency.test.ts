import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { connect as amqpConnect } from "amqplib";
import type { ChannelModel } from "amqplib";
import { EventBusService } from "../../src/main.js";

const URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672";

describe("idempotency - subscribe same key", () => {
    let connection: ChannelModel;
    let svc: EventBusService;

    before(async () => {
        connection = await amqpConnect(URL);
        svc = new EventBusService("test.idempotent.sub", "test.idempotent.sub.q", "test", "1.0.0");
        await svc.connect(connection);
    });

    after(async () => {
        await svc.close();
        await connection.close();
    });

    it("subscribe() same key should overwrite not duplicate", async () => {
        let count = 0;
        const h = () => { count++; return Promise.resolve(); };
        svc.subscribe("k", h); svc.subscribe("k", h);
        await svc.consume();
        await svc.publish({ type: "test.event", data: Buffer.from("test"), metadata: { contentType: "text/plain" } });
        await new Promise(r => setTimeout(r, 500));
        assert.equal(count, 1);
    });
});

describe("idempotency - distinct events", () => {
    let connection: ChannelModel;
    let svc: EventBusService;

    before(async () => {
        connection = await amqpConnect(URL);
        svc = new EventBusService("test.idempotent.dup", "test.idempotent.dup.q", "test", "1.0.0");
        await svc.connect(connection);
    });

    after(async () => {
        await svc.close();
        await connection.close();
    });

    it("identical payloads produce distinct events", async () => {
        const ids: string[] = [];
        svc.subscribe("c", (_d, props) => { if (props.messageId) ids.push(props.messageId); return Promise.resolve(); });
        await svc.consume();
        await svc.publish({ type: "test.dup", data: Buffer.from("same"), metadata: { contentType: "text/plain", timestamp: Date.now() } });
        await svc.publish({ type: "test.dup", data: Buffer.from("same"), metadata: { contentType: "text/plain", timestamp: Date.now() } });
        await new Promise(r => setTimeout(r, 500));
        assert.equal(ids.length, 2); assert.notEqual(ids[0], ids[1]);
    });
});
