# fanaticjs

RabbitMQ library implementing independent queues and events using fanout strategy.

## Installation

```bash
bun install @scope/fanaticjs
# or
npm install @scope/fanaticjs
```

## Usage

### Basic Usage (with auto-logger)

```typescript
import { EventBusService } from "@scope/fanaticjs/eventBus";

const producer = new EventBusService(
  "user-events",
  "unused",
  "my-service",
  "1.0.0"
);

await producer.connect(amqpConnection, "amqp://localhost:5672", true);

await producer.publish({
  type: "user.created",
  data: Buffer.from(JSON.stringify({ id: "123", name: "Alice" })),
  metadata: { contentType: "application/json" }
});
```

### Publishing Events (with custom logger)

```typescript
import { EventBusService } from "@scope/fanaticjs/eventBus";
import pino from "pino";

const logger = pino();
const producer = new EventBusService(
  "user-events",
  "unused",
  "my-service",
  "1.0.0",
  logger
);

await producer.connect(amqpConnection, "amqp://localhost:5672", true);

await producer.publish({
  type: "user.created",
  data: Buffer.from(JSON.stringify({ id: "123", name: "Alice" })),
  metadata: { contentType: "application/json" }
});
```

### Consuming Events

```typescript
import { EventBusService } from "@scope/fanaticjs/eventBus";

const consumer = new EventBusService(
  "user-events",
  "email-service",
  "email-service",
  "1.0.0"
);

await consumer.connect(amqpConnection, "amqp://localhost:5672", false);

consumer.subscribe("handle-user-created", async (data, properties) => {
  const user = JSON.parse(data.toString());
  await sendWelcomeEmail(user);
});

await consumer.consume();
```

## Features

- Fanout exchange strategy for independent consumer queues
- Automatic retry with configurable delay
- Dead Letter Queue (DLQ) support
- Exponential backoff for reconnection
- Support for both owned and shared connections
- Optional logger (uses silent logger if not provided)

## Development

```bash
# Install dependencies
bun install

# Type-check
bun run typecheck

# Run tests
bun test

# Run tests with coverage
bun test --coverage

# Run E2E tests (starts RabbitMQ automatically with Docker Compose)
bun run test:e2e

# Build
bun run build
```

## Docker Compose (for local development/testing)

A docker-compose.yml is included for running RabbitMQ locally:

```bash
# Start RabbitMQ
docker-compose up -d

# Stop RabbitMQ
docker-compose down
```

RabbitMQ management UI is available at http://localhost:15672 (guest/guest)

## License

MIT