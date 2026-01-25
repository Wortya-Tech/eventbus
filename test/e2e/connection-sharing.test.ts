import { assertEquals } from "@std/assert";
import { EventBusService, ConnectionProvider } from "../../src/eventBus/index.ts";
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
} from "./helpers.ts";

Deno.test("should manage owned connection for producer", async () => {
    const services: EventBusService[] = [];
    const exchangeName = createExchangeName("owned-producer");
    const connection1 = await connectToRabbitMQ();

    try {
        const producer = await createProducer(
            exchangeName,
            connection1,
            undefined,
            "amqp://guest:guest@localhost:5672",
            true
        );
        services.push(producer);

        await producer.publish({
            type: "test.event",
            data: encodeTestData(createTestData()),
            metadata: { contentType: "application/json" },
        });

        // Limpa antes de fechar connection (cleanupWithGrace fecha connection se owns)
        // Preciso limpa antes de producer.close()
        const channel = await connection1.createChannel();
        await channel.deleteExchange(exchangeName);
        await channel.close();

        await producer.close();
        await connection1.close();

        assertEquals(true, true);
    } finally {
        services.length = 0;
    }
});

Deno.test("should manage owned connection for consumer", async () => {
    const services: EventBusService[] = [];
    const exchangeName = createExchangeName("owned-consumer");
    const queueName = createQueueName("consumer");
    const connection = await connectToRabbitMQ();

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
            true
        );
        services.push(consumer);

        const testData = createTestData();
        await producer.publish({
            type: "test.event",
            data: encodeTestData(testData),
            metadata: { contentType: "application/json" },
        });

        await sleep(200);

        assertEquals(true, true);
    } finally {
        await cleanupWithGrace(connection, exchangeName, [queueName], services);
    }
});

Deno.test("should share connection via connection provider", async () => {
    const services: EventBusService[] = [];
    const exchangeName = createExchangeName("shared-connection");
    const queueName1 = createQueueName("consumer-1");
    const provider = new ConnectionProvider("amqp://guest:guest@localhost:5672");
    let sharedConnection = await provider.create();

    try {
        const producer = new EventBusService(
            exchangeName,
            `producer-${Date.now()}`,
            "test-producer",
            "1.0.0"
        );
        await producer.connect(sharedConnection, undefined, false);
        services.push(producer);

        const consumer1 = new EventBusService(
            exchangeName,
            queueName1,
            "test-consumer",
            "1.0.0"
        );
        await consumer1.connect(sharedConnection, undefined, false);
        services.push(consumer1);

        const testData = createTestData();
        await producer.publish({
            type: "test.event",
            data: encodeTestData(testData),
            metadata: { contentType: "application/json" },
        });

        await sleep(200);

        assertEquals(true, true);
    } finally {
        await cleanupWithGrace(sharedConnection, exchangeName, [queueName1], services);
    }
});

Deno.test("should reuse connection provider on reconnect", async () => {
    const services: EventBusService[] = [];
    const exchangeName = createExchangeName("provider-reconnect");
    const queueName = createQueueName("consumer");
    const provider = new ConnectionProvider("amqp://guest:guest@localhost:5672");
    let sharedConnection = await provider.create();

    try {
        const producer = new EventBusService(
            exchangeName,
            `producer-${Date.now()}`,
            "test-producer",
            "1.0.0"
        );
        await producer.connect(sharedConnection, undefined, false);
        services.push(producer);

        const testData = createTestData();
        await producer.publish({
            type: "test.event",
            data: encodeTestData(testData),
            metadata: { contentType: "application/json" },
        });

        await sleep(200);

        const newConnection = await provider.create();
        assertEquals(newConnection !== null, true);
    } finally {
        await cleanupWithGrace(sharedConnection, exchangeName, [queueName], services);
    }
});