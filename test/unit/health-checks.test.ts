import { assertEquals } from "@std/assert";
import { EventBusService } from "../../src/eventBus/index.ts";
import type { Channel, ChannelModel } from "amqplib";
import { pino } from "pino";

const testLogger = pino({ level: "silent" });

// Mock de serviço com overrides para testar health checks
class TestEventBusService extends EventBusService {
  public readonly testIsConnectionAlive: (conn: ChannelModel) => boolean;

  public readonly testIsChannelHealthy: (ch: Channel | undefined) => boolean;

  constructor(
    exchangeName: string,
    queueName: string,
    source: string,
    version: string,
    maxRetries?: number,
    retryDelay?: number,
    maxConnectionRetries?: number,
    initialReconnectDelay?: number,
  ) {
    super(
      exchangeName,
      queueName,
      source,
      version,
      testLogger,
      maxRetries,
      retryDelay,
      maxConnectionRetries,
      initialReconnectDelay,
    );
    this.testIsConnectionAlive = this["isConnectionAlive"].bind(this);
    this.testIsChannelHealthy = this["isChannelHealthy"].bind(this);
  }
}

Deno.test("isConnectionAlive detects null connections", async () => {
  // Test connection is null
  const service = new TestEventBusService(
    "test-exchange",
    "test-queue",
    "test-source",
    "1.0.0",
  );
  assertEquals(service.testIsConnectionAlive(null), false);
});

Deno.test("isConnectionAlive handles malformed connection objects", async () => {
  const service = new TestEventBusService(
    "test-exchange",
    "test-queue",
    "test-source",
    "1.0.0",
  );

  // Test connection without .connection property
  assertEquals(service.testIsConnectionAlive({}), false);

  // Test connection with undefined .connection
  assertEquals(service.testIsConnectionAlive({ connection: undefined }), false);

  // Test connection with null .connection
  assertEquals(service.testIsConnectionAlive({ connection: null }), false);

  // Test connection with undefined stream
  assertEquals(service.testIsConnectionAlive({ connection: { stream: undefined } }), false);

  // Test connection with null stream
  assertEquals(service.testIsConnectionAlive({ connection: { stream: null } }), false);

  // Test connection with destroyed stream
  assertEquals(
    service.testIsConnectionAlive({
      connection: { stream: { destroyed: true } },
    }),
    false,
  );

  // Test healthy connection
  assertEquals(
    service.testIsConnectionAlive({
      connection: { stream: { destroyed: false } },
    }),
    true,
  );
});

Deno.test("isChannelHealthy handles malformed states", async () => {
  const service = new TestEventBusService(
    "test-exchange",
    "test-queue",
    "test-source",
    "1.0.0",
  );

  // Test channel is null
  assertEquals(service.testIsChannelHealthy(null), false);

  // Test channel with undefined .connection
  assertEquals(
    service.testIsChannelHealthy({}),
    false,
  );

  // Test channel with null .connection
  assertEquals(
    service.testIsChannelHealthy({ connection: null }),
    false,
  );

  // Test channel with undefined stream
  assertEquals(
    service.testIsChannelHealthy({ connection: { stream: undefined } }),
    false,
  );

  // Test channel with null stream
  assertEquals(
    service.testIsChannelHealthy({ connection: { stream: null } }),
    false,
  );

  // Test channel with destroyed stream
  assertEquals(
    service.testIsChannelHealthy({
      connection: { stream: { destroyed: true } },
    }),
    false,
  );

  // Test healthy channel
  assertEquals(
    service.testIsChannelHealthy({
      connection: { stream: { destroyed: false } },
    }),
    true,
  );
});

Deno.test("isChannelHealthy handles exceptions gracefully", async () => {
  const service = new TestEventBusService(
    "test-exchange",
    "test-queue",
    "test-source",
    "1.0.0",
  );

  // Test with objects that throw on property access
  const errorThrower = {
    get connection() {
      throw new Error("Property access error");
    },
  };
  // Should return false on exception
  assertEquals(service.testIsChannelHealthy(errorThrower as Channel), false);

  const errorThrower2 = {
    connection: {
      get stream() {
        throw new Error("Stream access error");
      },
    },
  };
  assertEquals(service.testIsChannelHealthy(errorThrower2 as Channel), false);
});
