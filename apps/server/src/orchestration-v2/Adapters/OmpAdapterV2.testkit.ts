import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ProviderReplayEntry,
  ProviderInstanceId,
  type ResolvedProviderInstanceEnvironment,
  type ProviderReplayTranscript,
  type ProviderReplayEntry as ProviderReplayEntryType,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import { ProviderAdapterDriverCreateError } from "@t3tools/provider-core/server/adapterDriver";
import * as ProviderAdapterRegistry from "../ProviderAdapterRegistry.ts";
import type { OrchestratorV2ProviderReplayHarness } from "../testkit/ProviderReplayHarness.ts";
import { OMP_PROVIDER, OmpAdapterV2Driver } from "@t3tools/provider-omp/server";

export const OMP_RPC_REPLAY_PROTOCOL = "omp.rpc-ui-jsonl";
export const OMP_REPLAY_ANY = "<any>";

const OmpRpcReplayTranscript = Schema.Struct({
  provider: Schema.Literal(OMP_PROVIDER),
  protocol: Schema.Literal(OMP_RPC_REPLAY_PROTOCOL),
  version: Schema.String,
  scenario: Schema.String,
  metadata: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
  entries: Schema.Array(ProviderReplayEntry),
});
type OmpRpcReplayTranscript = typeof OmpRpcReplayTranscript.Type;
const decodeOmpRpcReplayTranscript = Schema.decodeUnknownEffect(OmpRpcReplayTranscript);

class OmpReplayTranscriptDecodeError extends Schema.TaggedError<OmpReplayTranscriptDecodeError>()(
  "OmpReplayTranscriptDecodeError",
  {
    driver: Schema.optional(Schema.String),
    protocol: Schema.optional(Schema.String),
    scenario: Schema.optional(Schema.String),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to decode OMP RPC replay transcript for scenario ${this.scenario ?? "<unknown>"}.`;
  }
}

class OmpReplayMismatchError extends Schema.TaggedError<OmpReplayMismatchError>()(
  "OmpReplayMismatchError",
  {
    scenario: Schema.String,
    cursor: Schema.Number,
    process: Schema.Number,
    label: Schema.optional(Schema.String),
    actualType: Schema.optional(Schema.String),
    expected: Schema.Unknown,
    actual: Schema.Unknown,
  },
) {
  override get message(): string {
    return `OMP replay frame mismatch at cursor ${this.cursor} (expected ${this.label ?? "<none>"}, received ${this.actualType ?? "<untyped>"} from process ${this.process}) in scenario ${this.scenario}.`;
  }
}

class OmpReplayIncompleteError extends Schema.TaggedError<OmpReplayIncompleteError>()(
  "OmpReplayIncompleteError",
  {
    scenario: Schema.String,
    cursor: Schema.Number,
    remaining: Schema.Number,
    nextLabel: Schema.optional(Schema.String),
  },
) {
  override get message(): string {
    return `OMP replay ended with ${this.remaining} unconsumed entries at cursor ${this.cursor} (next ${this.nextLabel ?? "<none>"}) in scenario ${this.scenario}.`;
  }
}

function entryLabel(entry: ProviderReplayEntryType | undefined): string | undefined {
  if (entry === undefined) return undefined;
  return entry.type === "runtime_exit" ? "runtime_exit" : (entry.label ?? entry.type);
}

const OmpOrchestratorReplayHarnessError = Schema.Union([
  OmpReplayTranscriptDecodeError,
  ProviderAdapterDriverCreateError,
]);
type OmpOrchestratorReplayHarnessError = typeof OmpOrchestratorReplayHarnessError.Type;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function arrayValuesMatch(expected: ReadonlyArray<unknown>, actual: unknown): boolean {
  return (
    Array.isArray(actual) &&
    expected.length === actual.length &&
    expected.every((entry, index) => replayValueMatches(entry, actual[index]))
  );
}

function recordValuesMatch(expected: Record<string, unknown>, actual: unknown): boolean {
  if (!isRecord(actual)) return false;
  const keys = new Set([...Object.keys(expected), ...Object.keys(actual)]);
  return [...keys].every((key) => replayValueMatches(expected[key], actual[key]));
}

function replayValueMatches(expected: unknown, actual: unknown): boolean {
  if (expected === OMP_REPLAY_ANY) return true;
  if (Array.isArray(expected)) return arrayValuesMatch(expected, actual);
  if (isRecord(expected)) return recordValuesMatch(expected, actual);
  return Object.is(expected, actual);
}

const ADAPTER_REQUEST_ID = /^t3-(?:prompt-|abort-)?\d+$/u;

function adapterRequestId(frame: unknown): string | undefined {
  const id = isRecord(frame) ? frame.id : undefined;
  return typeof id === "string" && ADAPTER_REQUEST_ID.test(id) ? id : undefined;
}

/** Rebind adapter request ids without pinning counters; dialog ids that replies must echo stay exact. */
function withRequestId(expected: unknown, actual: unknown): unknown {
  const actualId = adapterRequestId(actual);
  return isRecord(expected) && actualId !== undefined && adapterRequestId(expected) !== undefined
    ? { ...expected, id: actualId }
    : expected;
}

function startsTurn(entry: ProviderReplayEntryType): boolean {
  if (entry.type !== "expect_outbound") return false;
  const type = isRecord(entry.frame) ? entry.frame.type : undefined;
  return type === "prompt" || type === "compact" || type === "process_start";
}

function processStartFrame(command: ChildProcess.Command) {
  const args = ChildProcess.isStandardCommand(command) ? command.args : [];
  return { type: "process_start", args: [...args] };
}

function entryProcess(entry: ProviderReplayEntryType): number {
  const label = entry.type === "runtime_exit" ? undefined : entry.label;
  const match = label === undefined ? null : /@p(\d+)$/u.exec(label);
  return match === null ? 1 : Number(match[1]);
}

function frameLabel(frame: unknown): string | undefined {
  if (!isRecord(frame)) return undefined;
  if (frame.type === "response" && typeof frame.command === "string") {
    return `response:${frame.command}`;
  }
  return typeof frame.type === "string" ? frame.type : undefined;
}

function recordLabel(frame: unknown, process: number): string | undefined {
  const base = frameLabel(frame);
  if (base === undefined || process === 1) return base;
  return `${base}@p${process}`;
}

const decodeJsonLine = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const encodeJsonLine = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

function makeLineSplitter() {
  let buffer = "";
  const decoder = new TextDecoder();
  return (chunk: Uint8Array): ReadonlyArray<string> => {
    buffer += decoder.decode(chunk, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() ?? "";
    return lines.map((line) => line.replace(/\r$/u, "")).filter((line) => line.length > 0);
  };
}

interface OmpReplayProcess {
  readonly ordinal: number;
  readonly stdout: Queue.Queue<Uint8Array, Cause.Done>;
  readonly ids: Map<string, string>;
  open: boolean;
}

/** Adapter fibers can reorder stdin writes; hold matches within the turn until reached, and fail unmatched writes. */
class OmpReplayController {
  private cursor = 0;
  private failure: unknown = null;
  private readonly consumed = new Set<number>();
  private readonly held: Array<{ readonly process: OmpReplayProcess; readonly actual: unknown }> =
    [];
  private readonly processes: Array<OmpReplayProcess> = [];
  private readonly transcript: OmpRpcReplayTranscript;

  constructor(transcript: OmpRpcReplayTranscript) {
    this.transcript = transcript;
  }

  spawn(
    command: ChildProcess.Command,
    stdout: Queue.Queue<Uint8Array, Cause.Done>,
  ): OmpReplayProcess {
    const process: OmpReplayProcess = {
      ordinal: this.processes.length + 1,
      stdout,
      ids: new Map(),
      open: true,
    };
    this.processes.push(process);
    this.receive(process, processStartFrame(command));
    return process;
  }

  receive(process: OmpReplayProcess, actual: unknown): void {
    if (this.failure !== null) return;
    if (!this.isExpectedWithinTurn(process, actual)) {
      this.failMismatch(process, actual);
      return;
    }
    this.held.push({ process, actual });
    this.advance();
  }

  close(process: OmpReplayProcess): void {
    process.open = false;
    Queue.endUnsafe(process.stdout);
  }

  assertComplete(): void {
    if (this.failure !== null) throw this.failure;
    if (this.cursor !== this.transcript.entries.length || this.held.length > 0) {
      const nextLabel = entryLabel(this.transcript.entries[this.cursor]);
      throw new OmpReplayIncompleteError({
        scenario: this.transcript.scenario,
        cursor: this.cursor,
        remaining: this.transcript.entries.length - this.cursor,
        ...(nextLabel === undefined ? {} : { nextLabel }),
      });
    }
  }

  private pendingOutbound(index: number): ProviderReplayEntryType | undefined {
    const entry = this.transcript.entries[index];
    return entry?.type === "expect_outbound" && !this.consumed.has(index) ? entry : undefined;
  }

  private matchesEntry(
    entry: ProviderReplayEntryType,
    process: OmpReplayProcess,
    actual: unknown,
  ): boolean {
    return (
      entry.type === "expect_outbound" &&
      entryProcess(entry) === process.ordinal &&
      replayValueMatches(withRequestId(entry.frame, actual), actual)
    );
  }

  /** A write may run ahead of inbound frames, but never past the next turn-starting write. */
  private isExpectedWithinTurn(process: OmpReplayProcess, actual: unknown): boolean {
    for (let index = this.cursor; index < this.transcript.entries.length; index += 1) {
      const entry = this.pendingOutbound(index);
      if (entry === undefined) continue;
      if (this.matchesEntry(entry, process, actual)) return true;
      if (startsTurn(entry)) return false;
    }
    return false;
  }

  private failMismatch(process: OmpReplayProcess, actual: unknown): void {
    const expected = this.transcript.entries[this.cursor];
    const label = entryLabel(expected);
    const actualType = isRecord(actual) ? actual.type : undefined;
    this.fail(
      new OmpReplayMismatchError({
        scenario: this.transcript.scenario,
        cursor: this.cursor,
        process: process.ordinal,
        ...(label === undefined ? {} : { label }),
        ...(typeof actualType === "string" ? { actualType } : {}),
        expected: expected ?? null,
        actual,
      }),
    );
  }

  private advance(): void {
    let progressed = true;
    while (progressed && this.failure === null) {
      progressed = this.drainInbound() || this.consumeHeldWrite();
    }
  }

  private consumeHeldWrite(): boolean {
    const heldIndex = this.held.findIndex((write) =>
      this.consumePending(write.process, write.actual),
    );
    if (heldIndex === -1) return false;
    this.held.splice(heldIndex, 1);
    return true;
  }

  private consumePending(process: OmpReplayProcess, actual: unknown): boolean {
    const entries = this.transcript.entries;
    for (let index = this.cursor; index < entries.length; index += 1) {
      const entry = entries[index]!;
      if (entry.type !== "expect_outbound") return false;
      if (this.consumed.has(index) || !this.matchesEntry(entry, process, actual)) continue;
      this.rememberRequestId(process, entry.frame, actual);
      this.consumed.add(index);
      while (this.consumed.has(this.cursor)) this.cursor += 1;
      return true;
    }
    return false;
  }

  private rememberRequestId(process: OmpReplayProcess, expected: unknown, actual: unknown): void {
    const recordedId = adapterRequestId(expected);
    const actualId = adapterRequestId(actual);
    if (recordedId !== undefined && actualId !== undefined) process.ids.set(recordedId, actualId);
  }

  private drainInbound(): boolean {
    let emitted = false;
    while (this.failure === null && this.emitNextInbound()) emitted = true;
    return emitted;
  }

  private emitNextInbound(): boolean {
    const entry = this.transcript.entries[this.cursor];
    if (entry?.type === "runtime_exit") {
      this.cursor += 1;
      const process = this.processes.at(-1);
      if (process !== undefined) this.close(process);
      return true;
    }
    if (entry?.type !== "emit_inbound") return false;
    const process = this.processes[entryProcess(entry) - 1];
    if (process === undefined) return false;
    this.cursor += 1;
    if (process.open) this.writeStdout(process, entry.frame);
    return true;
  }

  private writeStdout(process: OmpReplayProcess, frame: unknown): void {
    const recordedId = isRecord(frame) ? frame.id : undefined;
    const reboundId = typeof recordedId === "string" ? process.ids.get(recordedId) : undefined;
    const rebound =
      isRecord(frame) && reboundId !== undefined ? { ...frame, id: reboundId } : frame;
    Queue.offerUnsafe(process.stdout, new TextEncoder().encode(`${encodeJsonLine(rebound)}\n`));
  }

  private fail(cause: unknown): void {
    if (this.failure !== null) return;
    this.failure = cause;
    for (const process of this.processes) Queue.endUnsafe(process.stdout);
  }
}

/** Deliberately outside the valid pid range so OmpRpc's group kill never lands. */
const REPLAY_PID = 999_999_999;

function makeOmpReplaySpawner(
  controller: OmpReplayController,
): ChildProcessSpawner.ChildProcessSpawner["Service"] {
  return ChildProcessSpawner.make((command) =>
    Effect.gen(function* () {
      const process = controller.spawn(command, yield* Queue.unbounded<Uint8Array, Cause.Done>());
      yield* Effect.addFinalizer(() => Effect.sync(() => controller.close(process)));
      const split = makeLineSplitter();
      return ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(REPLAY_PID),
        exitCode: Effect.never,
        isRunning: Effect.succeed(true),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.forEach((chunk: Uint8Array) =>
          Effect.sync(() => {
            for (const line of split(chunk)) controller.receive(process, decodeJsonLine(line));
          }),
        ),
        stdout: Stream.fromQueue(process.stdout),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      });
    }),
  );
}

/** Replay configures no T3 MCP endpoint, so host tools never call out. */
const layerUnreachableMcp = Layer.succeed(
  HttpClient.HttpClient,
  HttpClient.make((request) =>
    Effect.succeed(HttpClientResponse.fromWeb(request, new Response(null, { status: 503 }))),
  ),
);

function layerProviderHost(stateDir: string | undefined) {
  const testHost = TestProviderHost.layer().pipe(Layer.provide(NodeServices.layer));
  if (stateDir === undefined) return testHost;
  return Layer.effect(
    ProviderHost.ProviderHost,
    Effect.gen(function* () {
      const host = yield* ProviderHost.ProviderHost;
      return ProviderHost.ProviderHost.of({ ...host, paths: { ...host.paths, stateDir } });
    }),
  ).pipe(Layer.provide(testHost));
}

export function layer<E, R>(input: {
  readonly spawner: Layer.Layer<ChildProcessSpawner.ChildProcessSpawner, E, R>;
  readonly binaryPath: string;
  readonly launchArgs: string;
  readonly environment?: ResolvedProviderInstanceEnvironment;
  /** Where omp keeps session files and overlays; a recording keeps them out of the system temp dir. */
  readonly stateDir?: string;
}) {
  return ProviderAdapterRegistry.layerFromAdaptersEffect(
    OmpAdapterV2Driver.create({
      instanceId: ProviderInstanceId.make(OMP_PROVIDER),
      environment: input.environment ?? [],
      displayName: undefined,
      enabled: true,
      config: {
        ...OmpAdapterV2Driver.defaultConfig(),
        binaryPath: input.binaryPath,
        launchArgs: input.launchArgs,
      },
    }).pipe(Effect.map((adapter) => [adapter])),
  ).pipe(
    Layer.provide(input.spawner),
    Layer.provide(
      Layer.mergeAll(
        layerProviderHost(input.stateDir),
        layerUnreachableMcp,
        NodeServices.layer,
        IdAllocator.layer,
      ),
    ),
  );
}

function metadataString(transcript: OmpRpcReplayTranscript, key: string): string {
  const value = transcript.metadata?.[key];
  return typeof value === "string" ? value : "";
}

export const OmpOrchestratorReplayHarness: OrchestratorV2ProviderReplayHarness<
  OmpRpcReplayTranscript,
  OmpOrchestratorReplayHarnessError
> = {
  driver: OMP_PROVIDER,
  decodeTranscript: (transcript: ProviderReplayTranscript) =>
    decodeOmpRpcReplayTranscript(transcript).pipe(
      Effect.mapError(
        (cause) =>
          new OmpReplayTranscriptDecodeError({
            driver: transcript.provider,
            protocol: transcript.protocol,
            scenario: transcript.scenario,
            cause,
          }),
      ),
    ),
  makeProviderAdapterRegistryLayer: (transcript) =>
    layer({
      binaryPath: "omp",
      launchArgs: metadataString(transcript, "launchArgs"),
      spawner: Layer.effect(
        ChildProcessSpawner.ChildProcessSpawner,
        Effect.gen(function* () {
          const controller = new OmpReplayController(transcript);
          yield* Effect.addFinalizer(() => Effect.sync(() => controller.assertComplete()));
          return makeOmpReplaySpawner(controller);
        }),
      ),
    }),
};

/**
 * Records both directions of every spawned omp process. When `refuseInbound` returns true for a
 * frame (for example one from a model the recording must not use), the frame is dropped and omp is killed.
 */
export function makeOmpRecordingSpawner(
  live: ChildProcessSpawner.ChildProcessSpawner["Service"],
  entries: Array<ProviderReplayEntryType>,
  refuseInbound: (frame: unknown) => boolean = () => false,
): ChildProcessSpawner.ChildProcessSpawner["Service"] {
  let spawned = 0;
  return ChildProcessSpawner.make((command) =>
    Effect.gen(function* () {
      const handle = yield* live.spawn(command);
      const process = ++spawned;
      const record = (type: "expect_outbound" | "emit_inbound", frame: unknown) => {
        const label = recordLabel(frame, process);
        entries.push({ type, ...(label === undefined ? {} : { label }), frame });
      };
      const parseLines = (lines: ReadonlyArray<string>) =>
        lines.flatMap((line) => {
          try {
            return [decodeJsonLine(line)];
          } catch {
            // OmpRpc drops non-JSON stdout lines, so the transcript does too.
            return [];
          }
        });
      const recordInbound = (lines: ReadonlyArray<string>) => {
        const frames = parseLines(lines);
        const refused = frames.some(refuseInbound);
        if (!refused) for (const frame of frames) record("emit_inbound", frame);
        return refused ? handle.kill({ killSignal: "SIGKILL" }).pipe(Effect.ignore) : Effect.void;
      };
      record("expect_outbound", processStartFrame(command));
      const splitIn = makeLineSplitter();
      const splitOut = makeLineSplitter();
      return ChildProcessSpawner.makeHandle({
        ...handle,
        stdin: handle.stdin.pipe(
          Sink.mapInput((chunk: Uint8Array) => {
            for (const frame of parseLines(splitIn(chunk))) record("expect_outbound", frame);
            return chunk;
          }),
        ),
        stdout: handle.stdout.pipe(
          Stream.tap((chunk) => Effect.suspend(() => recordInbound(splitOut(chunk)))),
        ),
      });
    }),
  );
}
