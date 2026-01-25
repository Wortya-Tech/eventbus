import { assertEquals } from "@std/assert";
import { assert } from "@std/assert/assert";
import { connect as amqpConnect } from "npm:amqplib@0.10.9";
import type { Channel } from "npm:amqplib@0.10.9";
import { EventBusService } from "../../src/eventBus/index.ts";
import {
    connectToRabbitMQ,
    createProducer,
    createConsumer,
    createTestData,
    encodeTestData,
    createExchangeName,
    createQueueName,
    sleep,
    cleanupWithGrace,
    createRetryHandler,
    createRetryTracker,
    getQueueMessageCount,
    testConfig,
} from "./helpers.ts";

Deno.test("should retry failed handler and eventually succeed", async () => {
    const connection = await connectToRabbitMQ();
    const services: EventBusService[] = [];
    const exchangeName = createExchangeName("retry-success");
    const queueName = createQueueName("consumer");

    try {
        const producer = await createProducer(
            exchangeName,
            connection,
            undefined,
            "amqp://guest:guest@localhost:5672",
            false
        );
        services.push(producer);

        const tracker = createRetryTracker();
        const consumer = await createConsumer(
            exchangeName,
            queueName,
            connection,
            false
        );
        services.push(consumer);

        // Fail on first attempt only
        consumer.subscribe("handler", createRetryHandler(tracker, 1));

        await consumer.consume();
        await sleep(100);

        const testData = createTestData();
        await producer.publish({
            type: "test.event",
            data: encodeTestData(testData),
            metadata: { contentType: "application/json" },
        });

        await sleep(500);

        // Verifica retry behavior - handler called multiple times
        assert(tracker.attempts >= 2);
        assertEquals(tracker.failures, 1);
        assert(tracker.attempts > tracker.failures);
    } finally {
        await cleanupWithGrace(connection, exchangeName, [queueName], services);
    }
});

Deno.test("should send to dead letter queue after max retries", async () => {
    const connection = await connectToRabbitMQ();
    const services: EventBusService[] = [];
    const mainConnection = await connectToRabbitMQ();
    const exchangeName = createExchangeName("retry-dlq");
    const queueName = createQueueName("consumer");
    const dlqName = `${queueName}.dlq`;

    try {
        const producer = await createProducer(
            exchangeName,
            connection,
            undefined,
            "amqp://guest:guest@localhost:5672",
            false
        );
        services.push(producer);

        const tracker = createRetryTracker();
        const consumer = await createConsumer(
            exchangeName,
            queueName,
            connection,
            false
        );
        services.push(consumer);

        // Fail handler - always fails so message goes to DLQ
        consumer.subscribe("handler", async () => {
            tracker.attempts++;
            if (tracker.attempts <= testConfig.MAX_RETRIES) {
                throw new Error("DLQ test failure");
            }
        });

        await consumer.consume();
        await sleep(100);

        const testData = createTestData();
        await producer.publish({
            type: "test.event",
            data: encodeTestData(testData),
            metadata: { contentType: "application/json" },
        });

        await sleep(500);

        const dlqChannel = await mainConnection.createChannel();
        const dlqCount = await getQueueMessageCount(dlqChannel, dlqName);
        await dlqChannel.close();

        // DLQ should contain the message that exceeded max retries
        assert(dlqCount >= 1);
        assert(tracker.attempts >= testConfig.MAX_RETRIES + 1);
    } finally {
        await cleanupWithGrace(connection, exchangeName, [queueName, dlqName], services);
        await mainConnection.close();
    }
});

Deno.test("should increment retry count headers correctly", async () => {
    const connection = await connectToRabbitMQ();
    const mainConnection = await connectToRabbitMQ();
    const services: EventBusService[] = [];
    const exchangeName = createExchangeName("retry-headers");
    const queueName = createQueueName("consumer");
    const dlqName = `${queueName}.dlq`;

    try {
        const producer = await createProducer(
            exchangeName,
            connection,
            undefined,
            "amqp://guest:guest@localhost:5672",
            false
        );
        services.push(producer);

        const consumer = await createConsumer(
            exchangeName,
            queueName,
            connection,
            false
        );
        services.push(consumer);

        // Always fail to force to DLQ
        consumer.subscribe("handler", async () => {
            throw new Error("Always fail");
        });

        await consumer.consume();
        await sleep(100);

        const testData = createTestData();
        await producer.publish({
            type: "test.event",
            data: encodeTestData(testData),
            metadata: { contentType: "application/json" },
        });

        await sleep(1000);

        const dlqChannel = await mainConnection.createChannel();
        const msg = await dlqChannel.get(dlqName, { noAck: true });
        await dlqChannel.close();

        if (!msg) {
            throw new Error("No message in DLQ");
        }

        const retryCount = msg!.properties.headers?.["x-retry-count"] as number | undefined;
        assert(retryCount !== undefined);
        assert(retryCount >= 2);
        assertEquals(msg!.properties.headers?.["x-first-death-exchange"], exchangeName);
        assertEquals(msg!.properties.headers?.["x-first-death-queue"], queueName);
    } finally {
        await cleanupWithGrace(connection, exchangeName, [queueName, dlqName], services);
        await mainConnection.close();
    }
});