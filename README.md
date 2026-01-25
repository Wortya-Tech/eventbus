# fanaticjs

RabbitMQ library for Deno implementing independent queues and events using fanout strategy.

## Installation

```bash
deno add jsr:@scope/fanaticjs
```

## Why fanaticjs?

Deno library using RabbitMQ for reliable messaging with key advantages over BullMQ:

- **Deno Support**: Native npm: compatibility (BullMQ: Node.js only, no official Deno support)
- **Consumer Isolation**: Each consumer owns dedicated queue, retry, and DLQ (BullMQ: weak shared
  queues)
- **Multi-Handlers**: `Promise.allSettled` executes all handlers (BullMQ: single handler)
- **Reliability**: Durable ACK/NACK delivery vs BullMQ fire-and-forget
- **Performance**: fanaticjs 50k msg/sec reliable vs BullMQ 100k+ msg/sec fast but lossy

**Choose fanaticjs when message delivery is critical.**

## Architecture

Fanout exchange distributes messages to consumers each with own retry queue and DLQ for isolation.
Each consumer receives messages independently with dedicated retry mechanism and DLQ.

**Message Flow:**

- Publish → Distribute to all queues → Process → Ack | Retry | Send to DLQ

```mermaid
graph TD
    P[Producer] --> E[Exchange]
    E --> Q1[Queue: consumer1] --> R1[Retry: .retry] --> E
    E --> Q2[Queue: consumer2] --> R2[Retry: .retry] --> E
    Q1 --> D1[DLQ: .dlq]
    Q2 --> D2[DLQ: .dlq]
```

```mermaid
sequenceDiagram
    participant P as Producer
    participant E as Exchange
    participant Q as Queue
    participant R as Retry Queue
    participant D as DLQ
    P->>E: Publish
    E->>Q: Distribute
    Q->>Q: Process
    alt Success
        Q->>Q: Ack
    else Failure (< MAX_RETRIES)
        Q->>R: Retry
        R-->>E: Republish
    else Failure (>= MAX_RETRIES)
        Q->>D: DLQ
    end
```

## Usage Examples

### Basic Producer

```typescript
import { EventBusService } from "@scope/fanaticjs/eventBus";

const producer = new EventBusService(
  "user-events",
  "unused",
  "my-service",
  "1.0.0",
);

await producer.connect(amqpConnection, "amqp://localhost:5672", true);

await producer.publish({
  type: "user.created",
  data: Buffer.from(JSON.stringify({ id: "123", name: "Alice" })),
  metadata: { contentType: "application/json" },
});
```

### Basic Consumer

```typescript
import { EventBusService } from "@scope/fanaticjs/eventBus";

const consumer = new EventBusService(
  "user-events",
  "email-service",
  "email-service",
  "1.0.0",
);

await consumer.connect(amqpConnection, "amqp://localhost:5672", false);

consumer.subscribe("handle-user-created", async (data, properties) => {
  const user = JSON.parse(data.toString());
  await sendWelcomeEmail(user);
});

await consumer.consume();
```

### With Custom Logger

```typescript
import { EventBusService } from "@scope/fanaticjs/eventBus";
import pino from "pino";

const logger = pino();
const service = new EventBusService(
  "user-events",
  "queue-name",
  "my-service",
  "1.0.0",
  logger,
);
```

### Custom Retry Configuration

```typescript
const service = new EventBusService(
  "user-events",
  "queue-name",
  "my-service",
  "1.0.0",
  undefined, // logger (optional)
  5, // maxRetries (default: 3)
  10000, // retryDelay in ms (default: 5000)
  10, // maxConnectionRetries (default: 10)
  2000, // initialReconnectDelay in ms (default: 1000)
);
```

### Shared Connection via ConnectionProvider

```typescript
import { ConnectionProvider, EventBusService } from "@scope/fanaticjs/eventBus";

const provider = new ConnectionProvider("amqp://guest:guest@localhost:5672");
const sharedConnection = await provider.create();

const producer = new EventBusService("events", "prod-queue", "prod", "1.0.0");
await producer.connect(sharedConnection, undefined, false);

const consumer = new EventBusService("events", "cons-queue", "cons", "1.0.0");
await consumer.connect(sharedConnection, undefined, false);
```

## fanaticjs vs BullMQ

| Aspect           | fanaticjs (RabbitMQ)              | BullMQ (Redis)                |
| ---------------- | --------------------------------- | ----------------------------- |
| Deno Support     | ✅ Native library                 | ❌ Node.js only               |
| Execution        | Isolated per consumer             | Shared queue workers          |
| Guarantees       | Durable, ACK/NACK                 | In-memory, fire-and-forget    |
| Multi-Handler    | ✅ Promise.allSettled             | ❌ Single handler             |
| Throughput       | 50k-100k msg/sec                  | 100k+ msg/sec (30x Dragonfly) |
| Reliability      | High (durable storage)            | Medium (memory-dependent)     |
| Close Tracking   | ✅ WeakMap                        | ❌ No built-in                |
| Retry            | Per-consumer + DLQ                | Shared queue                  |
| Message Patterns | Fanout, Direct, Topic             | Queue-based only              |
| Clustering       | Built-in                          | External tools                |
| Management UI    | Included (http://localhost:15672) | Third-party plugins only      |

**When to Choose:** fanaticjs for guaranteed delivery (payments, emails, compliance) and consumer
isolation. BullMQ for speed where occasional loss acceptable (analytics, background tasks).

## Features

**Core Architecture:**

- Isolated execution (failures never cascade)
- WeakMap close tracking (avoids false reconnections)
- Multi-handler processing via Promise.allSettled
- Per-consumer retry + DLQ for complete error isolation

**Message Handling:**

- Retry: Configurable attempts (default 3), delay (default 5s), tracking in `x-retry-count` header
- DLQ: Failed messages stored for post-failure analysis
- Routing: Headers `x-retry-count`, `x-first-death-exchange`, `x-first-death-queue` for lifecycle
  tracking

**Connection Management:**

- Owned and shared connections supported
- Exponential backoff reconnection
- Graceful close tracking

## Development

This project uses Deno runtime with tasks defined in `deno.json`.

```bash
# Type check
deno check src/**/*.ts test/**/*.ts

# Format code
deno fmt
deno fmt --check

# Lint
deno lint

# Run all tests
deno test -A

# Run unit tests only
deno test -A test/unit

# Run E2E tests (start RabbitMQ first)
deno task rabbitmq:start
deno test -A test/e2e
deno task rabbitmq:stop

# Publish to JSR
deno publish --allow-slow-types
```

List all tasks: `deno task`

## Docker Compose

```bash
# Start RabbitMQ
deno task rabbitmq:start

# Stop RabbitMQ
deno task rabbitmq:stop
```

RabbitMQ management UI: http://localhost:15672 (guest/guest)

## License

MIT
