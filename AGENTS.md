# Agents Guide - fanaticjs

## Overview
RabbitMQ library implementing independent queues and events using fanout strategy. Each event has dedicated queue with retry and DLQ (Dead Letter Queue) support.

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
# Install dependencies
bun install

# Type-check
bun run typecheck

# Lint
bun run lint

# Format
bun run format

# Format check
bun run format:check
```

### Testing
```bash
# Run all tests (coverage included)
bun test --coverage

# Run only unit tests
bun run test:unit

# Run only integration tests (requires RabbitMQ running locally)
bun run test:integration

# Run E2E tests with Docker Compose (starts RabbitMQ automatically)
bun run test:e2e

# Run tests in watch mode
bun test --watch

# Run specific test file
bun test test/unit/EventBusService.test.ts

# Cleanup Docker containers if needed
bun run test:e2e:cleanup
```

### Building
```bash
# Clean dist directory
bun run clean

# Build library (ESM and CJS outputs)
bun run build

# Build types only
bun run build:types

# Build bundles only
bun run build:bundles
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
- ESLint disable for necessary `any` types: `// eslint-disable-next-line @typescript-eslint/no-explicit-any`

### Naming Conventions
- Classes: PascalCase (`EventBusService`, `ConnectionProvider`)
- Functions/methods: camelCase (`createQueue`, `handleConnectionReconnect`)
- Constants: SCREAMING_SNAKE_CASE (`MAX_RETRIES`, `RETRY_DELAY`)
- Private properties: Use TypeScript `private` keyword (no underscore prefix)
- Map/Set variables: Descriptive names indicating structure (e.g., `subscribers`, `intentionalCloseMap`)

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
        "x-dead-letter-routing-key": ""
    }
});
```

**Retry Queue with TTL:**
```typescript
await channel.assertQueue(retryQueueName, {
    durable: true,
    arguments: {
        "x-dead-letter-exchange": originalExchange,
        "x-message-ttl": retryDelay
    }
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
    correlationId
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

## Build/Lint/Test Commands (Future)
When test framework is configured:
```bash
# Run all tests
npm test

# Run single test file (once configuration exists)
npm test path/to/test.spec.ts

# Run tests matching pattern
npm test -- --run "**/test*.ts"

# Watch mode
npm test -- --watch
```

## Dependencies
- `amqplib`: RabbitMQ client library
- `pino`: Logger
- `node:crypto`: UUID generation

## Notes
- Portuguese README documents architecture with mermaid diagrams
- ES modules (uses `node:` protocol for Node modules)
- Node.js native crypto module for UUID generation
- No explicit TypeScript config yet; add `tsconfig.json` when needed