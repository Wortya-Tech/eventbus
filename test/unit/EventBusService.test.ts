import { assertEquals } from "@std/assert";
import { EventBusService } from "../../src/eventBus/index.ts";

Deno.test("should use default logger if not provided", () => {
  const service = new EventBusService(
    "test-exchange",
    "test-queue",
    "test-source",
    "1.0.0"
  );
  assertEquals(typeof service["logger"], "object");
});

Deno.test("should use default retry configuration", () => {
  const service = new EventBusService(
    "test-exchange",
    "test-queue",
    "test-source",
    "1.0.0"
  );
  assertEquals(service["MAX_RETRIES"], 3);
  assertEquals(service["RETRY_DELAY"], 5000);
});

Deno.test("should allow custom retry configuration via parameters", () => {
  const service = new EventBusService(
    "test-exchange",
    "test-queue",
    "test-source",
    "1.0.0",
    undefined,
    5,
    10000
  );
  assertEquals(service["MAX_RETRIES"], 5);
  assertEquals(service["RETRY_DELAY"], 10000);
});

Deno.test("should allow custom connection retry configuration", () => {
  const service = new EventBusService(
    "test-exchange",
    "test-queue",
    "test-source",
    "1.0.0",
    undefined,
    3,
    5000,
    7,
    2000
  );
  assertEquals(service["MAX_CONNECTION_RETRIES"], 7);
  assertEquals(service["INITIAL_RECONNECT_DELAY"], 2000);
});
