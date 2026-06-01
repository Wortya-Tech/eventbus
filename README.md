# eventbus

RabbitMQ event bus library using fanout exchange strategy with per-consumer retry and dead letter queue (DLQ) isolation.

## Installation

```bash
npm install @eventbus/eventbus
```

## Why eventbus?

Node.js library for reliable RabbitMQ messaging with key advantages over BullMQ:

- **Consumer Isolation**: Each consumer owns its dedicated queue, retry, and DLQ. BullMQ uses shared queues without per-consumer isolation.
- **Multi-Handler**: `Promise.allSettled` executes all registered handlers for every message. BullMQ allows a single handler per job.
- **Reliability**: Durable exchanges and queues with ACK/NACK delivery. Failed messages retry or go to DLQ for post-mortem analysis.
- **Fanout**: One publish reaches all independent consumers — each with its own retry and DLQ.

**Choose eventbus when delivery guarantees and consumer isolation are critical.**

## Architecture

```
Exchange (fanout)
  ├── Queue A → Handler(s) → ack
  │   ├── .retry → (TTL) → back to Exchange
  │   └── .dlq   → dead messages
  └── Queue B → Handler(s) → ack
      ├── .retry → (TTL) → back to Exchange
      └── .dlq   → dead messages
```

```mermaid
graph TD
    P[Publisher] --> E[Exchange: fanout]
    E --> Q1[Queue: consumer-a]
    E --> Q2[Queue: consumer-b]
    Q1 --> H1[Handler(s)]
    H1 -->|fail| R1[Retry Queue<br>.retry]
    H1 -->|max retries| D1[DLQ<br>.dlq]
    R1 -->|TTL expires| E
    Q2 --> H2[Handler(s)]
    H2 -->|fail| R2[Retry Queue<br>.retry]
    H2 -->|max retries| D2[DLQ<br>.dlq]
    R2 -->|TTL expires| E
```

## API

### `EventBusService`

Main class for publishing and consuming events.

```ts
new EventBusService(
  exchangeName: string,
  queueName: string,
  source: string,
  version: string,
  logger?: Logger,           // default: silent pino
  maxRetries?: number,        // default: 3
  retryDelay?: number,        // default: 5000ms
  maxConnectionRetries?: number,      // default: 10
  initialReconnectDelay?: number,     // default: 1000ms
)
```

#### Methods

| Method | Description |
|--------|-------------|
| `connect(connection, rabbitmqUrl?, ownsConnection?, connectionProvider?)` | Connect to RabbitMQ, set up exchanges/queues/event handlers |
| `subscribe(key, handler)` | Register a handler. Idempotent — same key does not overwrite |
| `unsubscribe(key)` | Remove a handler by key |
| `consume()` | Start consuming messages. Dispatches to all registered handlers via `Promise.allSettled` |
| `publish(event)` | Publish an event to the exchange. Returns `boolean` |
| `close()` | Cancel consumer, close channel. Closes connection if `ownsConnection=true` |

#### Publish Event Shape

```ts
type EventPublish = {
  type: string;
  data: Buffer;
  metadata: {
    contentType: string;
    timestamp?: number;
    contentEncoding?: string;
    correlationId?: string;
    persistent?: boolean;
  };
}
```

#### Message Handler

```ts
type MessageHandler = (
  data: Buffer,
  metadata: MessageProperties,
) => Promise<void>;
```

### `ConnectionProvider`

Manages a shared RabbitMQ connection across multiple `EventBusService` instances.

```ts
new ConnectionProvider(url: string, logger?: Logger)
```

#### Methods

| Method | Description |
|--------|-------------|
| `create()` | Returns existing connection if alive, otherwise creates a new one (30s timeout) |

## Usage

### Basic Producer

```ts
import { EventBusService } from "eventbus";
import { connect } from "amqplib";

const connection = await connect("amqp://localhost");
const bus = new EventBusService("orders", "email-queue", "email-svc", "1.0.0");
await bus.connect(connection, "amqp://localhost", true);

await bus.publish({
  type: "order.created",
  data: Buffer.from(JSON.stringify({ id: "123" })),
  metadata: { contentType: "application/json" },
});
```

### Basic Consumer

```ts
import { EventBusService } from "eventbus";

const bus = new EventBusService("orders", "email-queue", "email-svc", "1.0.0");
await bus.connect(connection);

bus.subscribe("send-email", async (data, props) => {
  const order = JSON.parse(data.toString());
  await sendEmail(order);
});

await bus.consume();
```

### Multiple Handlers (Same Consumer)

```ts
bus.subscribe("log", async (data) => { console.log("log:", data); });
bus.subscribe("notify", async (data) => { await sendNotification(data); });

await bus.consume();
// Both handlers run via Promise.allSettled for every message
```

### Custom Retry Configuration

```ts
const bus = new EventBusService(
  "orders", "email-queue", "email-svc", "1.0.0",
  undefined,   // logger
  5,          // maxRetries (default: 3)
  10000,      // retryDelay in ms (default: 5000)
  10,         // maxConnectionRetries (default: 10)
  2000,       // initialReconnectDelay in ms (default: 1000)
);
```

### Shared Connection via `ConnectionProvider`

```ts
import { ConnectionProvider, EventBusService } from "eventbus";

const provider = new ConnectionProvider("amqp://localhost");
const sharedConn = await provider.create();

const producer = new EventBusService("events", "prod-q", "prod", "1.0.0");
await producer.connect(sharedConn, undefined, false);

const consumer = new EventBusService("events", "cons-q", "cons", "1.0.0");
await consumer.connect(sharedConn, undefined, false);
```

### Connection Provider for Auto-Reconnection

```ts
const provider = new ConnectionProvider("amqp://localhost");

const bus = new EventBusService("events", "my-queue", "my-svc", "1.0.0");
await bus.connect(
  await provider.create(),
  "amqp://localhost",
  false,
  () => provider.create()   // called on channel death
);

await bus.consume();
// If channel dies, ensureChannel calls the provider to get a fresh connection
```

### Custom Logger

```ts
import pino from "pino";

const logger = pino({ level: "info" });
const bus = new EventBusService("events", "q", "src", "1.0.0", logger);
```

## Features

### Core Architecture
- **Isolated execution**: failures never cascade across consumers
- **WeakMap close tracking**: distinguishes intentional vs unintentional closes
- **Multi-handler processing**: `Promise.allSettled` — one failure does not block others
- **Per-consumer retry + DLQ**: complete error isolation per queue

### Message Handling
- **Retry**: Configurable attempts (default 3) and delay (default 5s). Tracks via `x-retry-count` header
- **DLQ**: Failed messages stored for post-failure analysis
- **Routing headers**: `x-retry-count`, `x-first-death-exchange`, `x-first-death-queue` for lifecycle tracking
- **Idempotent subscribers**: `subscribe()` with same key does not overwrite
- **Unexpected error guard**: sync throws in handlers → direct to DLQ without retry

### Connection Management
- **ConnectionProvider**: share one connection across multiple services, auto-recreate if dead
- **Owned and shared connections**: `ownsConnection` flag controls who closes the connection
- **Exponential backoff**: `initialReconnectDelay * 2^(attempt - 1)`
- **Automatic reconnection**: channel death triggers `handleChannelReconnect`, connection death triggers `handleConnectionReconnect`
- **`connectionProvider` callback**: supply fresh connections on channel failure
- **Graceful close**: `close()` cancels consumer, closes channel, marks as intentional to prevent reconnect loops

### Exchanges and Queues (created automatically by `connect()`)

| Resource | Type | Name |
|----------|------|------|
| Main exchange | fanout, durable | `{exchangeName}` |
| DLX exchange | direct, durable | `{queueName}.dlx` |
| Retry exchange | fanout, durable | `{queueName}.retry` |
| Main queue | durable, DLQ-bound | `{queueName}` |
| Retry queue | durable, TTL | `{queueName}.retry` |
| Dead letter queue | durable | `{queueName}.dlq` |

## Development

```bash
npm install       # dependencies
npm run check     # type-check
npm run lint      # eslint
npm run test:unit # unit tests (no RabbitMQ)
npm run test      # all tests

# Integration/E2E tests require RabbitMQ:
npm run rabbitmq:start
npm run test:integration
npm run test:e2e
npm run rabbitmq:stop

# Build
npm run build     # → dist/main.js + dist/main.d.ts

# Coverage
npm run coverage  # spec reporter + coverage/lcov.info
```

RabbitMQ management UI: http://localhost:15672 (guest/guest)

## License

LGPL-3.0-or-later
