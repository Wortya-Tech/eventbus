# Agents Guide - eventbus

## Overview

RabbitMQ event bus library implementing independent queues and events using fanout strategy. Each event has
dedicated queue with retry and DLQ (Dead Letter Queue) support.

## Project Structure

```
eventbus/
├── src/
│   └── main.ts          # EventBusService + ConnectionProvider
├── test/
│   ├── unit/            # Unit tests (no RabbitMQ required)
│   ├── integration/     # Integration tests (require RabbitMQ)
│   └── e2e/             # End-to-end tests (full workflows)
├── doc/                 # Documentation
├── coverage/            # LCOV coverage output
├── dist/                # Build output (js + d.ts)
├── tsconfig.json
├── tsconfig.prod.json
├── eslint.config.js
└── AGENTS.md            # This file
```

## Development Commands

```bash
# Type-check
npm run check

# Lint
npm run lint

# Run all tests (sequential, with coverage)
npm test

# Run only unit tests
npm run test:unit

# Run integration tests
npm run test:integration

# Run E2E tests
npm run test:e2e

# Build for production
npm run build

# Generate LCOV coverage report
npm run coverage
```

## Dependencies

- `amqplib` (npm): RabbitMQ client library
- `pino` (npm): Logger
- `node:crypto`: UUID generation

## Dev Dependencies

- `typescript`: Type checking and compilation
- `eslint` + `typescript-eslint` + `globals`: Linting
- `tsx`: TypeScript test execution
- `@types/node`: Node.js type definitions

## Architecture

### Classes

| Class | Responsibility |
|-------|---------------|
| `ConnectionProvider` | Reusable RabbitMQ connection (reconnects if dead) |
| `EventBusService` | Publish/consume with retry, DLQ, and auto-reconnection |

### Key Patterns

- **WeakMap close tracking**: `intentionalCloseMap` and `intentionalChannelCloseMap` distinguish intentional vs unintentional channel/connection closes to prevent reconnection loops
- **Exponential backoff**: `initialReconnectDelay * 2^(attempt - 1)` for both channel and connection reconnection
- **`connectionProvider` callback**: allows injecting external connection sources (e.g., `ConnectionProvider.create`) for `ensureChannel`
- **Promise.allSettled**: all registered handlers execute for every message; individual failures don't block others

### Testing

Tests use `node:test` with spec reporter. All tests run with `--test-concurrency 1` (sequential).

**Unit tests** use `test()` + `t.test()`:
```typescript
import test from "node:test";
import assert from "node:assert/strict";

test("EventBusService", async (t) => {
    await t.test("should use default logger", () => {
        assert.equal(typeof svc["logger"], "object");
    });
});
```

**Integration/E2E tests** use `describe()`/`it()` + before()/after():
```typescript
import { describe, it, before, after } from "node:test";

describe("retry - success", () => {
    let connection, producer, consumer;

    before(async () => {
        connection = await amqpConnect(URL);
        producer = new EventBusService(...);
        await producer.connect(connection);
        consumer = new EventBusService(...);
        await consumer.connect(connection);
    });

    after(async () => {
        await consumer.close();
        await producer.close();
        await connection.close();
    });

    it("should retry failed handler", () => {
        consumer.subscribe("h", ...);
        // test body
    });
});
```

Rules:
- Each test case is its own `describe()` block
- `before()` creates all resources (connection + services)
- `after()` destroys all resources
- `before` and `after` are always paired
- No resource sharing between test cases
- No try/catch or empty catch blocks in tests
- Exchange/queue names are static string literals per test

### RabbitMQ for integration/E2E tests

```bash
npm run rabbitmq:start
npm run test:integration
npm run test:e2e
npm run rabbitmq:stop
```

## Code Style Guidelines

- Indentation: 4 spaces
- Quotes: Double quotes
- Semicolons: Omitted
- Module system: ESM with `nodenext` resolution
- Relative imports use `.js` extension
