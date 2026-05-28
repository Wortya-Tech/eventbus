import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { connect as amqpConnect } from "amqplib";
import type { ChannelModel } from "amqplib";
import { EventBusService } from "../../src/main.js";

const URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672";

describe("load-balancing", () => {
    let connection: ChannelModel;
    let svcA: EventBusService;
    let svcB: EventBusService;

    before(async () => {
        connection = await amqpConnect(URL);
        svcA = new EventBusService("test.load.balance", "test.load.balance.q", "a", "1.0.0");
        svcB = new EventBusService("test.load.balance", "test.load.balance.q", "b", "1.0.0");
        await svcA.connect(connection); await svcA.consume();
        await svcB.connect(connection); await svcB.consume();
    });

    after(async () => {
        await svcA.close();
        await svcB.close();
        await connection.close();
    });

    it("shared queue prevents duplicate message processing", async () => {
        const idsA: string[] = []; const idsB: string[] = [];
        svcA.subscribe("c", (_d, props) => { if (props.messageId) idsA.push(props.messageId); return Promise.resolve(); });
        svcB.subscribe("c", (_d, props) => { if (props.messageId) idsB.push(props.messageId); return Promise.resolve(); });
        for (let i = 0; i < 10; i++) await svcA.publish({ type: "test.msg", data: Buffer.from(JSON.stringify({ idx: i })), metadata: { contentType: "application/json" } });
        await new Promise(r => setTimeout(r, 2000));
        assert.equal(idsA.length + idsB.length, 10);
        assert.equal(idsA.filter(id => idsB.includes(id)).length, 0);
        assert.ok(idsA.length > 0 && idsB.length > 0);
    });
});
