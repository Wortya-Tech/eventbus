/// <reference lib="deno.ns" />
import { assertEquals, assertExists } from "@std/assert";
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