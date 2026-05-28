import type {
  Channel,
  ChannelModel,
  Message,
  MessageProperties,
} from "amqplib";
import type { Logger } from "pino";
import { connect as rabbitmqConnect } from "amqplib";
import { randomUUID } from "node:crypto";
import { pino } from "pino";
import { setTimeout } from "node:timers/promises";

/**
 * Event payload to be published to the event bus.
 */
export type EventPublish = {
  type: string;
  data: Buffer;
  metadata: {
    contentType: string;
    timestamp?: number;
    contentEncoding?: string;
    correlationId?: string;
    persistent?: boolean;
  };
};

/**
 * Message handler function for processing consumed messages.
 */
export type MessageHandler = (
  data: Buffer,
  metadata: MessageProperties,
) => Promise<void>;

// Store intentional close state at connection level using WeakMap
const intentionalCloseMap = new WeakMap<ChannelModel, boolean>();

// Store intentional close state at channel level using WeakMap
const intentionalChannelCloseMap = new WeakMap<Channel, boolean>();

/**
 * Provides a shared RabbitMQ connection that can be reused across multiple services.
 * @example
 * ```ts
 * const provider = new ConnectionProvider("amqp://localhost");
 * const connection = await provider.create();
 * ```
 */
export class ConnectionProvider {
  private connection?: ChannelModel;
  private readonly logger: Logger;

  constructor(
    public readonly url: string,
    logger?: Logger,
  ) {
    this.logger = logger ?? pino({ level: "silent" });
  }

  private isConnectionAlive(conn: ChannelModel): boolean {
    try {
      if (!conn) return false;
      const con = conn as { connection?: { stream?: { destroyed?: boolean } } };
      if (!con.connection) return false;
      if (!con.connection.stream) return false;
      return !con.connection.stream.destroyed;
    } catch {
      return false;
    }
  };

  /**
   * Creates or returns an existing RabbitMQ connection.
   * Reuses the connection if it's still alive, otherwise creates a new one.
   * @returns Promise resolving to a RabbitMQ ChannelModel (Connection)
   * @example
   * ```ts
   * const connection = await provider.create();
   * ```
   */
  async create(): Promise<ChannelModel> {
    if (this.connection && this.isConnectionAlive(this.connection)) {
      return this.connection;
    }
    // If connection is dead, create a new one
    this.logger.info("Creating new RabbitMQ connection from provider");
    const url = new URL(this.url);
    this.logger.debug({ url: url.host }, "Connection domain");
    const connection = await rabbitmqConnect(this.url, {
      timeout: 30000,
    });
    this.connection = connection;
    return connection;
  };
}

/**
 * Event bus service for publishing and consuming RabbitMQ messages with retry and dead letter queue support.
 * Uses a fanout exchange strategy where each event has a dedicated queue.
 * @example
 * ```ts
 * const bus = new EventBusService(
 *   "user-events",
 *   "email-service-queue",
 *   "email-service",
 *   "1.0.0"
 * );
 * ```
 */
export class EventBusService {
  private connection?: ChannelModel;
  private channel?: Channel;
  private readonly deadLetterExchange: string;
  private readonly retryExchange: string;
  private readonly subscribers: Map<string, MessageHandler>;
  private readonly MAX_RETRIES: number;
  private readonly RETRY_DELAY: number;
  private readonly logger: Logger;
  private readonly MAX_CONNECTION_RETRIES: number;
  private readonly INITIAL_RECONNECT_DELAY: number;
  private connectionRetryCount: number;
  private isReconnecting: boolean;
  private rabbitmqUrl?: string;
  private consumerTag?: string;
  private ownsConnection: boolean;
  private connectionProvider?: () => Promise<ChannelModel>;

  /**
   * Creates a new event bus service instance.
   * @param exchangeName - The name of the RabbitMQ exchange to use.
   * @param queueName - The name of the queue to bind messages to.
   * @param source - The source of the events (e.g., service name).
   * @param version - The version of the event source.
   * @param logger - Optional logger instance. Defaults to silent pino logger.
   * @param maxRetries - Maximum number of retry attempts for failed messages. Defaults to 3.
   * @param retryDelay - Delay between retry attempts in milliseconds. Defaults to 5000.
   * @param maxConnectionRetries - Maximum number of connection retry attempts. Defaults to 10.
   * @param initialReconnectDelay - Initial delay between reconnection attempts in milliseconds. Defaults to 1000.
   * @example
   * ```ts
   * const bus = new EventBusService(
   *   "order-exchange",
   *   "inventory-queue",
   *   "inventory-service",
   *   "1.0.0"
   * );
   * ```
   */
  constructor(
    private readonly exchangeName: string,
    private readonly queueName: string,
    private readonly source: string,
    private readonly version: string,
    logger?: Logger,
    maxRetries: number = 3,
    retryDelay: number = 5000,
    maxConnectionRetries: number = 10,
    initialReconnectDelay: number = 1000,
  ) {
    this.MAX_RETRIES = maxRetries;
    this.RETRY_DELAY = retryDelay;
    this.MAX_CONNECTION_RETRIES = maxConnectionRetries;
    this.INITIAL_RECONNECT_DELAY = initialReconnectDelay;
    this.deadLetterExchange = `${this.queueName}.dlx`;
    this.retryExchange = `${this.queueName}.retry`;
    this.subscribers = new Map();
    this.logger = logger ?? pino({ level: "silent" });
    this.connectionRetryCount = 0;
    this.isReconnecting = false;
    this.ownsConnection = false;
  }

  /**
   * Subscribes a message handler to a specific key.
   * If the key already has a handler, it will be overwritten.
   * @param key - The key to associate the handler with.
   * @param handler - The async message handler function.
   * @example
   * ```ts
   * bus.subscribe("user.created", async (data, metadata) => {
   *   const user = JSON.parse(data.toString());
   *   console.log("User created:", user);
   * });
   * ```
   */
  subscribe(key: string, handler: MessageHandler): void {
    if (this.subscribers.has(key)) {
      this.logger.info(`channel ${key} alredy subscribed`);
    } else {
      this.logger.info(`channel ${key} subscribed`);
      this.subscribers.set(key, handler);
    }
  }

  /**
   * Unsubscribes the handler associated with the given key.
   * @param key - The key to unsubscribe from.
   * @example
   * ```ts
   * bus.unsubscribe("user.created");
   * ```
   */
  unsubscribe(key: string): void {
    this.subscribers.delete(key);
  }

  /**
   * Connects to RabbitMQ and sets up exchanges, queues, and event handlers.
   * @param connection - The RabbitMQ connection to use.
   * @param rabbitmqUrl - Optional RabbitMQ URL for reconnection.
   * @param ownsConnection - Whether this service owns the connection. Defaults to false.
   * @param connectionProvider - Optional function to provide fresh connections for reconnection.
   * @example
   * ```ts
   * const connection = await amqp.connect("amqp://localhost");
   * await bus.connect(connection, "amqp://localhost", true);
   * ```
   */
  async connect(
    connection: ChannelModel,
    rabbitmqUrl?: string,
    ownsConnection: boolean = false,
    connectionProvider?: () => Promise<ChannelModel>,
  ): Promise<void> {
    this.connection = connection;
    this.ownsConnection = ownsConnection;
    this.connectionProvider = connectionProvider;
    if (rabbitmqUrl) {
      this.rabbitmqUrl = rabbitmqUrl;
    }
    if (!this.connection) {
      throw new Error("Cannot connect");
    }
    this.channel = await this.connection.createChannel();

    // Setup channel event handlers for all services
    this.channel.on("error", async (err: unknown) => {
      this.logger.error({ err }, "RabbitMQ channel error");
      const isIntentional = intentionalChannelCloseMap.get(this.channel!);
      if (!isIntentional) {
        await this.handleChannelReconnect();
      }
    });

    this.channel.on("close", async () => {
      const isIntentional = intentionalChannelCloseMap.get(this.channel!);
      if (isIntentional) {
        this.logger.info("RabbitMQ channel closed intentionally");
      } else {
        this.logger.warn("RabbitMQ channel closed unexpectedly");
        await this.handleChannelReconnect();
      }
    });

    // Setup main exchange
    await this.channel.assertExchange(this.exchangeName, "fanout", {
      durable: true,
    });

    // Setup DLX exchange (direct to avoid redistributing to all queues)
    await this.channel.assertExchange(this.deadLetterExchange, "direct", {
      durable: true,
    });

    // Setup retry exchange
    await this.channel.assertExchange(this.retryExchange, "fanout", {
      durable: true,
    });

    // Setup queue
    await this.createQueue();

    // Only setup connection event handlers if this service owns the connection
    if (this.ownsConnection) {
      // Handle connection errors
      this.connection.on("error", async (err: unknown) => {
        this.logger.error({ err }, "RabbitMQ connection error");
        const isIntentional = intentionalCloseMap.get(this.connection!);
        if (!isIntentional) {
          await this.handleConnectionReconnect();
        }
      });

      this.connection.on("close", async () => {
        const isIntentional = intentionalCloseMap.get(this.connection!);
        if (!isIntentional) {
          await this.handleConnectionReconnect();
        }
      });

      this.connection.on("close", async () => {
        const isIntentional = intentionalCloseMap.get(this.connection!);
        if (isIntentional) {
          this.logger.info("RabbitMQ connection closed intentionally");
        } else {
          this.logger.warn("RabbitMQ connection closed unexpectedly");
          await this.handleConnectionReconnect();
        }
      });
    }

    // Reset retry count on successful connection
    this.connectionRetryCount = 0;
    this.isReconnecting = false;
  }

  private isConnectionAlive(connection: ChannelModel): boolean {
    try {
      if (!connection) return false;
      const con = connection as {
        connection?: { stream?: { destroyed?: boolean } };
      };
      if (!con.connection) return false;
      if (!con.connection.stream) return false;
      return !con.connection.stream.destroyed;
    } catch {
      return false;
    }
  }

  private isChannelHealthy(channel?: Channel): boolean {
    try {
      if (!channel) return false;
      const ch = channel as {
        connection?: { stream?: { destroyed?: boolean } };
      };
      if (!ch.connection) return false;
      if (!ch.connection.stream) return false;
      return !ch.connection.stream.destroyed;
    } catch {
      return false;
    }
  }

  private async ensureChannel(): Promise<Channel> {
    // If channel is healthy, return it
    if (this.channel && this.isChannelHealthy(this.channel)) {
      return this.channel;
    }

    this.logger.warn("Channel is not healthy, attempting to recreate...");

    // Try to get a fresh connection if provider is available
    if (this.connectionProvider) {
      try {
        this.connection = await this.connectionProvider();
      } catch (error) {
        this.logger.error({ error }, "Failed to get connection from provider");
      }
    }

    // Check if connection is alive
    if (!this.connection || !this.isConnectionAlive(this.connection)) {
      throw new Error("No healthy connection available");
    }

    // Create a new channel
    this.channel = await this.connection.createChannel();

    // Re-setup channel event handlers
    this.channel.on("error", async (err: unknown) => {
      this.logger.error({ err }, "RabbitMQ channel error");
      const isIntentional = intentionalChannelCloseMap.get(this.channel!);
      if (!isIntentional) {
        await this.handleChannelReconnect();
      }
    });

    this.channel.on("close", async () => {
      const isIntentional = intentionalChannelCloseMap.get(this.channel!);
      if (isIntentional) {
        this.logger.info("RabbitMQ channel closed intentionally");
      } else {
        this.logger.warn("RabbitMQ channel closed unexpectedly");
        await this.handleChannelReconnect();
      }
    });

    // Re-setup exchanges and queues
    await this.channel.assertExchange(this.exchangeName, "fanout", {
      durable: true,
    });
    await this.channel.assertExchange(this.deadLetterExchange, "direct", {
      durable: true,
    });
    await this.channel.assertExchange(this.retryExchange, "fanout", {
      durable: true,
    });

    // Create queues using the new channel
    await this.createQueueInternal(this.channel);

    this.logger.info("Channel recreated successfully");
    return this.channel;
  }

  private async handleChannelReconnect(): Promise<void> {
    if (this.isReconnecting) {
      this.logger.info("Channel reconnection already in progress, skipping");
      return;
    }

    if (this.connectionRetryCount >= this.MAX_CONNECTION_RETRIES) {
      this.logger.error(
        `Maximum channel retry attempts (${this.MAX_CONNECTION_RETRIES}) reached. Giving up.`,
      );
      return;
    }

    this.isReconnecting = true;
    this.connectionRetryCount++;

    const delay =
      this.INITIAL_RECONNECT_DELAY * Math.pow(2, this.connectionRetryCount - 1);
    this.logger.info(
      `Attempting to reconnect channel (attempt ${this.connectionRetryCount}/${this.MAX_CONNECTION_RETRIES}) in ${delay}ms`,
    );

    await setTimeout(delay);

    try {
      // First, try to get a fresh connection if provider is available
      if (this.connectionProvider) {
        this.logger.info("Getting fresh connection from provider...");
        this.connection = await this.connectionProvider();
      }

      // Check if connection is still alive
      if (!this.connection || !this.isConnectionAlive(this.connection)) {
        throw new Error("Connection is not available or destroyed");
      }

      this.logger.info("Creating new channel...");
      this.channel = await this.connection.createChannel();

      // Re-setup channel event handlers
      this.channel.on("error", async (err: unknown) => {
        this.logger.error({ err }, "RabbitMQ channel error");
        const isIntentional = intentionalChannelCloseMap.get(this.channel!);
        if (!isIntentional) {
          await this.handleChannelReconnect();
        }
      });

      this.channel.on("close", async () => {
        const isIntentional = intentionalChannelCloseMap.get(this.channel!);
        if (isIntentional) {
          this.logger.info("RabbitMQ channel closed intentionally");
        } else {
          this.logger.warn("RabbitMQ channel closed unexpectedly");
          await this.handleChannelReconnect();
        }
      });

      // Re-setup exchanges and queues
      await this.channel.assertExchange(this.exchangeName, "fanout", {
        durable: true,
      });
      await this.channel.assertExchange(this.deadLetterExchange, "direct", {
        durable: true,
      });
      await this.channel.assertExchange(this.retryExchange, "fanout", {
        durable: true,
      });
      await this.createQueue();

      // Restart consumer if there are subscribers
      if (this.subscribers.size > 0) {
        this.logger.info("Restarting consumer after channel reconnection");
        await this.consume();
      }

      this.logger.info("Successfully reconnected channel");
      this.connectionRetryCount = 0;
      this.isReconnecting = false;
    } catch (error) {
      this.logger.error({ error }, "Failed to reconnect channel");
      this.isReconnecting = false;
      // Recursively retry the reconnection
      await this.handleChannelReconnect();
    }
  }

  private async handleConnectionReconnect(): Promise<void> {
    if (this.isReconnecting) {
      this.logger.info("Connection reconnection already in progress, skipping");
      return;
    }

    if (this.connectionRetryCount >= this.MAX_CONNECTION_RETRIES) {
      this.logger.error(
        `Maximum connection retry attempts (${this.MAX_CONNECTION_RETRIES}) reached. Giving up.`,
      );
      return;
    }

    this.isReconnecting = true;
    this.connectionRetryCount++;

    const delay =
      this.INITIAL_RECONNECT_DELAY * Math.pow(2, this.connectionRetryCount - 1);
    this.logger.info(
      `Attempting to reconnect (attempt ${this.connectionRetryCount}/${this.MAX_CONNECTION_RETRIES}) in ${delay}ms`,
    );

    await setTimeout(delay);

    try {
      if (!this.rabbitmqUrl) {
        throw new Error("No RabbitMQ URL available for reconnection");
      }

      this.logger.info("Creating new RabbitMQ connection...");
      const newConnection = await rabbitmqConnect(this.rabbitmqUrl, {
        timeout: 30000,
      });

      this.logger.info("Reconnecting to RabbitMQ...");
      await this.connect(
        newConnection,
        this.rabbitmqUrl,
        this.ownsConnection,
        this.connectionProvider,
      );

      // Restart consumer if there are subscribers
      if (this.subscribers.size > 0) {
        this.logger.info("Restarting consumer after reconnection");
        await this.consume();
      }

      this.logger.info("Successfully reconnected to RabbitMQ");
    } catch (error) {
      this.logger.error({ error }, "Failed to reconnect to RabbitMQ");
      this.isReconnecting = false;
      // Recursively retry the reconnection
      await this.handleConnectionReconnect();
    }
  }

  private async createQueueInternal(channel: Channel): Promise<void> {
    // Create DLQ (bind with queueName as routing key to prevent redistribution)
    const dlqName = `${this.queueName}.dlq`;
    await channel.assertQueue(dlqName, {
      durable: true,
    });
    await channel.bindQueue(dlqName, this.deadLetterExchange, this.queueName);

    // Create retry queue (route back to main exchange and queue on retry)
    const retryQueueName = `${this.queueName}.retry`;
    await channel.assertQueue(retryQueueName, {
      durable: true,
      arguments: {
        "x-dead-letter-exchange": this.exchangeName,
        "x-dead-letter-routing-key": this.queueName,
        "x-message-ttl": this.RETRY_DELAY,
      },
    });
    await channel.bindQueue(retryQueueName, this.retryExchange, "");

    // Create main queue with DLQ configuration (use queueName as routing key)
    await channel.assertQueue(this.queueName, {
      durable: true,
      arguments: {
        "x-dead-letter-exchange": this.deadLetterExchange,
        "x-dead-letter-routing-key": this.queueName,
      },
    });

    // Bind queue to exchange
    await channel.bindQueue(this.queueName, this.exchangeName, "");
  }

  private async createQueue(): Promise<void> {
    if (!this.channel) throw new Error("Channel not initialized");
    await this.createQueueInternal(this.channel);
  }

  /**
   * Starts consuming messages from the queue and dispatching them to registered handlers.
   * Implements retry logic with configurable delay and dead letter queue for failed messages.
   * @example
   * ```ts
   * await bus.consume();
   * ```
   */
  async consume(): Promise<void> {
    // Ensure we have a healthy channel before consuming
    const channel = await this.ensureChannel();

    const result = await channel.consume(
      this.queueName,
      async (msg: Message | null) => {
        if (!msg) return;

        this.logger.info(
          msg.properties,
          `Received message of type ${msg.properties.contentType}`,
        );

        try {
          const retryCount = (msg.properties.headers?.["x-retry-count"] ||
            0) as number;

          // Get all handlers for this message type
          const handlers = this.subscribers.values();

          // Execute all handlers
          this.logger.info(
            msg.properties,
            `Received message of type ${msg.properties.contentType}`,
          );
          const results = await Promise.allSettled(
            Array.from(handlers).map((handler) =>
              handler(msg.content, msg.properties),
            ),
          );

          // Check if any handlers failed
          const hasFailures = results.some(
            (result) => result.status === "rejected",
          );

          if (hasFailures) {
            if (retryCount >= this.MAX_RETRIES) {
              // Send to dead letter queue by rejecting without requeue
              this.logger.warn(
                msg.properties,
                `Message exceeded maximum retries (${this.MAX_RETRIES}), sending to DLQ`,
              );
              this.channel?.nack(msg, false, false);
            } else {
              // Publish to retry exchange with incremented counter
              this.logger.info(
                msg.properties,
                `Retrying message (attempt ${retryCount + 1} of ${this.MAX_RETRIES})`,
              );
              const headers = {
                ...msg.properties.headers,
                "x-retry-count": retryCount + 1,
                "x-first-death-exchange": this.exchangeName,
                "x-first-death-queue": this.queueName,
              };

              this.channel?.publish(this.retryExchange, "", msg.content, {
                ...msg.properties,
                headers,
              });
              this.channel?.ack(msg);
            }
          } else {
            this.logger.info(msg.properties, "Message success delivey");
            this.channel?.ack(msg);
          }
        } catch (error) {
          this.logger.error(
            { error, propreties: msg.properties },
            `Error processing message`,
          );
          // On unexpected errors, send directly to DLQ
          this.channel?.nack(msg, false, false);
        }
      },
    );

    this.consumerTag = result.consumerTag;
  }

  /**
   * Publishes an event to the exchange.
   * @param event - The event payload to publish.
   * @returns True if the event was published successfully.
   * @example
   * ```ts
   * await bus.publish({
   *   type: "user.created",
   *   data: Buffer.from(JSON.stringify({ id: 1, name: "Alice" })),
   *   metadata: {
   *     contentType: "application/json",
   *     correlationId: "12345"
   *   }
   * });
   * ```
   */
  async publish(event: EventPublish): Promise<boolean> {
    try {
      // Ensure we have a healthy channel before publishing
      const channel = await this.ensureChannel();
      return channel.publish(this.exchangeName, event.type, event.data, {
        type: event.type,
        appId: `${this.source}@${this.version}+${this.exchangeName}`,
        contentEncoding: event.metadata.contentEncoding,
        timestamp: event.metadata?.timestamp ?? Date.now(),
        persistent: event.metadata?.persistent ?? false,
        contentType: event.metadata.contentType,
        messageId: randomUUID(),
        correlationId: event.metadata.correlationId,
      });
    } catch (error) {
      this.logger.error(
        { error, eventType: event.type },
        "Failed to publish event",
      );
      return false;
    }
  }

  /**
   * Closes the channel and optionally the connection.
   * Cancels active consumers and marks connections as intentionally closed to prevent reconnection.
   * @example
   * ```ts
   * await bus.close();
   * ```
   */
  async close(): Promise<void> {
    // Cancel consumer if active
    if (this.channel && this.consumerTag) {
      try {
        await this.channel.cancel(this.consumerTag);
        this.consumerTag = undefined;
      } catch (error) {
        this.logger.error({ error }, "Error canceling consumer");
      }
    }

    // Close channel
    if (this.channel) {
      try {
        // Mark this channel as intentionally closed
        intentionalChannelCloseMap.set(this.channel, true);
        await this.channel.close();
        this.channel = undefined;
      } catch (error) {
        this.logger.error({ error }, "Error closing channel");
      }
    }

    // Only close connection if this service owns it
    if (this.connection && this.ownsConnection) {
      try {
        // Mark this connection as intentionally closed
        intentionalCloseMap.set(this.connection, true);
        await this.connection.close();
        this.connection = undefined;
      } catch (error) {
        this.logger.error({ error }, "Error closing connection");
      }
    }
  }
}
