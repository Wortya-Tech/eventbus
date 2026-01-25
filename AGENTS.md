# Agents Guide - fanaticjs

## Overview

RabbitMQ library implementing independent queues and events using fanout strategy. Each event has
dedicated queue with retry and DLQ (Dead Letter Queue) support.

## Project Structure

```
fanaticjs/
├── eventBus/
│   ├── index.ts         # Core EventBusService implementation
│   └── README.md        # Architecture documentation
└── AGENTS.md            # This file
```

## Development Commands

### Setup

```bash
# Type-check
deno check src/**/*.ts test/**/*.ts

# Lint
deno lint

# Format
deno fmt

# Format check
deno fmt --check
```

### Testing

```bash
# Run all tests
deno test -A

# Run only unit tests
deno test -A test/unit

# Run E2E tests with Docker Compose (starts RabbitMQ automatically)
deno task rabbitmq:start
deno test -A test/e2e
deno task rabbitmq:stop

# Run specific test file
deno test -A test/unit/EventBusService.test.ts

# Start RabbitMQ for manual E2E testing
deno task rabbitmq:start

# Stop RabbitMQ
deno task rabbitmq:stop
```

### Publishing

```bash
# Publish to JSR
deno publish --allow-slow-types
```

## Code Style Guidelines

### Import Style

- Keep imports at top of file, grouped by type:
  1. Node/core imports (e.g., `node:crypto`, `node:crypto`)
  2. External package imports
  3. Internal type imports
  4. Internal value imports

Example:

```typescript
import type { Channel, ChannelModel } from "amqplib";
import { connect as rabbitmqConnect } from "amqplib";
import { randomUUID } from "node:crypto";
import type { Logger } from "pino";
```

### Formatting

- Indentation: 4 spaces (not tabs)
- Quotes: Double quotes for strings and object keys
- Semicolons: Omitted (not used)
- Trailing commas: Omitted
- Line length: Try to stay under 100 characters
- Max empty lines: 2 between blocks

### Types

- Use `interface` for object shapes
- Use `type` for unions, primitives, and mapped types
- Type assertions: Use `as type` sparingly; prefer type guards
- ESLint disable for necessary `any` types:
  `// eslint-disable-next-line @typescript-eslint/no-explicit-any`

### Naming Conventions

- Classes: PascalCase (`EventBusService`, `ConnectionProvider`)
- Functions/methods: camelCase (`createQueue`, `handleConnectionReconnect`)
- Constants: SCREAMING_SNAKE_CASE (`MAX_RETRIES`, `RETRY_DELAY`)
- Private properties: Use TypeScript `private` keyword (no underscore prefix)
- Map/Set variables: Descriptive names indicating structure (e.g., `subscribers`,
  `intentionalCloseMap`)

### Error Handling

- Use try/catch for async operations that may fail
- Always log errors with context using the pino logger:
  ```typescript
  try {
    await operation();
  } catch (error) {
    this.logger.error({ error, context }, "Descriptive error message");
    throw error;
  }
  ```
- Use `Promise.allSettled()` for parallel operations where partial success is acceptable
- For channel/connection errors, check if closure was intentional using WeakMap

### RabbitMQ Patterns

**Exchange Setup:**

```typescript
await channel.assertExchange(exchangeName, "fanout", { durable: true });
```

**Queue Creation with DLQ:**

```typescript
await channel.assertQueue(queueName, {
  durable: true,
  arguments: {
    "x-dead-letter-exchange": deadLetterExchange,
    "x-dead-letter-routing-key": "",
  },
});
```

**Retry Queue with TTL:**

```typescript
await channel.assertQueue(retryQueueName, {
  durable: true,
  arguments: {
    "x-dead-letter-exchange": originalExchange,
    "x-message-ttl": retryDelay,
  },
});
```

**Message Publishing:**

```typescript
await channel.publish(exchange, routingKey, content, {
  type,
  appId: `${source}@${version}+${exchange}`,
  timestamp,
  persistent,
  contentType,
  messageId: randomUUID(),
  correlationId,
});
```

**Message Consumption:**

- Use `channel.consume()` with async handler
- Acknowledge on success: `this.channel?.ack(msg)`
- On retry: publish to retry exchange, then ack original
- On permanent failure: `this.channel?.nack(msg, false, false)`

**Connection Management:**

- Track intentional closes using WeakMap to avoid spurious reconnection
- Implement exponential backoff for reconnection
- Always check connection/channel health before operations
- Support both owned and shared connections via `ConnectionProvider`

### WeakMap Usage

Use WeakMap for tracking intentional closure to prevent memory leaks:

```typescript
const intentionalCloseMap = new WeakMap<ChannelModel, boolean>();
// Mark as intentionally closed
intentionalCloseMap.set(connection, true);
// Check before reconnecting
const isIntentional = intentionalCloseMap.get(connection);
```

### Logging

- Use pino logger (passed to classes)
- Log levels: `info` for normal ops, `warn` for unexpected but recoverable, `error` for failures
- Provide context object first: `this.logger.info({ context }, "message")`
- Include relevant metadata in context objects

### Architecture Patterns

**Fanout Exchange Strategy:**

- Single exchange publishes to all bound queues
- Each consumer gets independent queue with own retry/DLQ
- Failures in one consumer don't affect others

**Retry Mechanism:**

- Track retry count in message headers: `x-retry-count`
- Increment on each retry attempt
- After MAX_RETRIES exceeded, send to DLQ
- Configurable delay between retries in retry queue TTL

**Reconnection Logic:**

- Implement exponential backoff: `INITIAL_RECONNECT_DELAY * 2^(retryCount - 1)`
- Limit max reconnection attempts
- Use flags to prevent concurrent reconnections
- Restart consumers after successful reconnect

## Testing Commands

```bash
# Run all tests
deno task test

# Run only unit tests
deno task test:unit

# Run E2E tests (automatically starts RabbitMQ with Docker Compose)
deno task test:e2e

# Run specific test file
deno test test/unit/EventBusService.test.ts
```

## Dependencies

- `amqplib` (npm): RabbitMQ client library
- `pino` (npm): Logger
- `node:crypto`: UUID generation (Deno's Node.js compatibility layer)
- `@std/assert` (jsr): Testing utilities

## Notes

- ES modules using `node:` protocol for Node.js compatibility layer in Deno
- No explicit TypeScript config needed (configured in deno.json)
