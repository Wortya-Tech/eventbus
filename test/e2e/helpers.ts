import { Buffer } from "node:buffer";
import { TextDecoder } from "node:util";
import { setTimeout as nodeSetTimeout } from "node:timers/promises";
import { connect as amqpConnect } from "amqplib";
import type { Channel, ChannelModel } from "amqplib";
import { EventBusService } from "../../src/eventBus/index.ts";

export const rabbitMQUrl = "amqp://guest:guest@localhost:5672";

export const testConfig = {
  RETRY_DELAY: 100,
  MAX_RETRIES: 2,
  INITIAL_RECONNECT_DELAY: 100,
  MAX_CONNECTION_RETRIES: 3,
};

export async function connectToRabbitMQ(): Promise<ChannelModel> {
  await waitForRabbitMQ();
  return await amqpConnect(rabbitMQUrl);
}

export async function waitForRabbitMQ(maxAttempts = 30): Promise<void> {
  for (let i = 0; i < maxAttempts; i++) {
    try {
      const conn = await amqpConnect(rabbitMQUrl);
      await conn.close();
      return;
    } catch {
      await sleep(1000);
    }
  }
  throw new Error("RabbitMQ not available");
}

export function createExchangeName(suffix: string): string {
  return `test.${suffix}.${Date.now()}`;
}

export function createQueueName(suffix: string): string {
  return `${suffix}-${Date.now()}`;
}

export function sleep(ms: number): Promise<void> {
  return nodeSetTimeout(ms);
}

export async function waitForMessage<T>(
  getter: () => T | null | undefined,
  timeout = 5000,
): Promise<T> {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    const result = getter();
    if (result !== null && result !== undefined) {
      await sleep(50);
      return result;
    }
    await sleep(50);
  }
  throw new Error("Timeout waiting for message");
}

export async function waitForMessages(
  count: number,
  getter: () => number,
  timeout = 5000,
): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    if (getter() >= count) {
      await sleep(50);
      return;
    }
    await sleep(50);
  }
  throw new Error(`Timeout waiting for ${count} messages`);
}

export function cleanupService(service: EventBusService): Promise<void> {
  return service.close();
}

export async function cleanupServices(services: EventBusService[]): Promise<void> {
  await Promise.allSettled(
    services.map((s) => cleanupService(s)),
  );
}

export function cleanupExchange(
  channel: Channel,
  exchangeName: string,
): Promise<void> {
  return channel.deleteExchange(exchangeName);
}

export function cleanupQueue(
  channel: Channel,
  queueName: string,
): Promise<void> {
  return channel.deleteQueue(queueName);
}

export async function cleanupExchangeAndQueues(
  channel: Channel,
  exchangeName: string,
  queueNames: string[],
): Promise<void> {
  await cleanupExchange(channel, exchangeName);

  const allQueues: string[] = [];
  for (const q of queueNames) {
    allQueues.push(q, `${q}.retry`, `${q}.dlq`);
  }

  await Promise.all(
    allQueues.map((q) => cleanupQueue(channel, q)),
  );
}

export async function createChannelSafely(
  connection: ChannelModel,
): Promise<Channel | null> {
  return await connection.createChannel();
}

export async function cleanupWithGrace(
  connection: ChannelModel,
  exchangeName: string,
  queueNames: string[],
  services: EventBusService[],
): Promise<void> {
  // Limpa queues/exchanges PRIMEIRO (enquanto connection está aberta)
  const channel = await createChannelSafely(connection);
  if (channel) {
    try {
      await cleanupExchangeAndQueues(channel, exchangeName, queueNames);
    } finally {
      await channel.close();
    }
  }

  // Fecha serviços depois de cleanup
  await cleanupServices(services);

  // Fecha connection por último
  await connection.close();
}

export async function createProducer(
  exchangeName: string,
  connection: ChannelModel,
  producerQueueName?: string,
  rabbitmqUrl?: string,
  ownsConnection = false,
  logger?: import("pino").Logger,
): Promise<EventBusService> {
  const config = testConfig;
  const queueName = producerQueueName || createQueueName("producer");
  const service = new EventBusService(
    exchangeName,
    queueName,
    "test-producer",
    "1.0.0",
    logger,
    config.MAX_RETRIES,
    config.RETRY_DELAY,
  );
  await service.connect(connection, rabbitmqUrl, ownsConnection);
  return service;
}

export async function createConsumer(
  exchangeName: string,
  queueName: string,
  connection: ChannelModel,
  ownsConnection = false,
  rabbitmqUrl?: string,
  logger?: import("pino").Logger,
): Promise<EventBusService> {
  const config = testConfig;
  const service = new EventBusService(
    exchangeName,
    queueName,
    "test-consumer",
    "1.0.0",
    logger,
    config.MAX_RETRIES,
    config.RETRY_DELAY,
    config.MAX_CONNECTION_RETRIES,
    config.INITIAL_RECONNECT_DELAY,
  );
  await service.connect(connection, rabbitmqUrl, ownsConnection);
  return service;
}

export interface TestData {
  id: string;
  value: number;
  timestamp: number;
}

export function createTestData(id?: string): TestData {
  return {
    id: id || `test-${Date.now()}-${Math.random()}`,
    value: Math.random(),
    timestamp: Date.now(),
  };
}

export function encodeTestData(data: TestData): Buffer {
  return Buffer.from(JSON.stringify(data));
}

export function decodeTestData(buffer: Buffer): TestData {
  return JSON.parse(new TextDecoder().decode(buffer));
}

type RetryTracker = {
  attempts: number;
  failures: number;
  lastError?: Error;
};

export function createRetryTracker(): RetryTracker {
  return { attempts: 0, failures: 0 };
}

export function createRetryHandler(
  tracker: RetryTracker,
  failUntilAttempt: number,
): (data: Buffer) => Promise<void> {
  function handler(_data: Buffer): Promise<void> {
    tracker.attempts++;
    if (tracker.attempts <= failUntilAttempt) {
      tracker.failures++;
      return Promise.reject(`Intentional failure at attempt ${tracker.attempts}`);
    }
    return Promise.resolve();
  }
  return handler;
}

export async function killChannel(channel: Channel) {
  await channel.close();
  await sleep(100);
}
