import assert from "node:assert/strict";
import { Writable } from "node:stream";
import { pino } from "pino";
import type { Logger } from "pino";

/**
 * A single pino log record as captured from the JSON line output.
 */
export type LogRecord = { level: number; msg: string } & Record<string, unknown>;

const LEVEL_NUMBERS: Record<string, number> = {
    trace: 10,
    debug: 20,
    info: 30,
    warn: 40,
    error: 50,
    fatal: 60,
};

/**
 * Creates a real pino logger that writes every record into an in-memory
 * array, so tests can assert on the emitted log messages.
 */
export function captureLogger(level = "trace") {
    const records: LogRecord[] = [];
    const stream = new Writable({
        write(chunk, _enc, cb) {
            records.push(JSON.parse(chunk.toString()) as LogRecord);
            cb();
        },
    });
    const logger = pino({ level }, stream) as unknown as Logger;
    return { logger, records };
}

/**
 * Asserts that at least one captured record matches the pattern and,
 * when given, the exact level name ("trace", "debug", "info", "warn",
 * "error", "fatal").
 */
export function assertLog(
    records: LogRecord[],
    pattern: string | RegExp,
    level?: keyof typeof LEVEL_NUMBERS,
): void {
    const levelNumber = level ? LEVEL_NUMBERS[level] : undefined;
    const matches = records.filter(
        (r) =>
            (levelNumber === undefined || r.level === levelNumber) &&
            (typeof pattern === "string" ? r.msg === pattern : pattern.test(r.msg)),
    );
    assert.ok(
        matches.length > 0,
        `expected a ${level ?? "(any)"} log record matching ${pattern} in:\n` +
            records.map((r) => `${r.level}: ${r.msg}`).join("\n"),
    );
}
