# Agents Guide - fanaticjs

## Overview

RabbitMQ library implementing independent queues and events using fanout strategy. Each event has
dedicated queue with retry and DLQ (Dead Letter Queue) support.

## Project Structure

```
fanaticjs/
├── src/
│   └── main.ts          # Core EventBusService + ConnectionProvider
├── test/
│   ├── unit/            # Unit tests (no RabbitMQ required)
│   ├── integration/     # Integration tests (require RabbitMQ)
│   └── e2e/             # End-to-end tests (full workflows)
├── doc/                 # Documentation
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

# Run all tests (sequential)
npm test

# Run only unit tests
npm run test:unit

# Run integration tests
npm run test:integration

# Run E2E tests
npm run test:e2e
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

## Testing

All tests run with `--test-concurrency 1` (sequential). Tests use `node:test` with TAP reporter.

### Test Patterns

**Unit tests** use `test()` + `t.test()` subtests:
```typescript
import test from "node:test";
import assert from "node:assert/strict";

test("EventBusService", async (t) => {
    await t.test("should use default logger", () => {
        assert.equal(typeof svc["logger"], "object");
    });
});
```

**Integration/E2E tests** use `describe()`/`it()` + `before()`/`after()`:
```typescript
import { describe, it, before, after } from "node:test";

describe("retry - success", () => {
    let connection: ChannelModel;
    let producer: EventBusService;
    let consumer: EventBusService;

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

    it("should retry failed handler", async () => {
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
