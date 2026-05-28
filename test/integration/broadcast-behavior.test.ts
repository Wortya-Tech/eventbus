import test from "node:test";
import assert from "node:assert/strict";
import { connect as amqpConnect } from "amqplib";
import type { ChannelModel } from "amqplib";
import { EventBusService } from "../../src/main.js";

const URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672";

test("broadcast-behavior", async (t) => {
    let connection: ChannelModel;
    let svcA: EventBusService;
    let svcB: EventBusService;

    t.before(async () => {
        connection = await amqpConnect(URL);
        svcA = new EventBusService("test.broadcast", "test.broadcast.a", "a", "1.0.0");
        svcB = new EventBusService("test.broadcast", "test.broadcast.b", "b", "1.0.0");
        await svcA.connect(connection); await svcA.consume();
        await svcB.connect(connection); await svcB.consume();
    });

    t.after(async () => {
        await svcA.close();
        await svcB.close();
        await connection.close();
    });

    await t.test("one published event is consumed by multiple queues", async () => {
        const ma: number[] = []; const mb: number[] = [];
        svcA.subscribe("c", () => { ma.push(1); return Promise.resolve(); });
        svcB.subscribe("c", () => { mb.push(1); return Promise.resolve(); });
        await svcA.publish({ type: "broadcast.test", data: Buffer.from("test"), metadata: { contentType: "text/plain" } });
        await new Promise(r => setTimeout(r, 2000));
        assert.equal(ma.length, 1); assert.equal(mb.length, 1);
    });
});
