// @effect-diagnostics nodeBuiltinImport:off
import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Logger from "effect/Logger";
import * as Option from "effect/Option";
import * as PlatformError from "effect/PlatformError";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as NodeFS from "node:fs";
import * as HostProcess from "@t3tools/shared/HostProcess";

import {
  makeOmpChunkDecoder,
  makeOmpJsonlFramer,
  makeOmpRpcConnection,
  stripOmpBashTrace,
  type OmpChunkDecodeResult,
} from "./rpc.ts";
import {
  encodeFakeOmpChunks,
  FAKE_OMP_IDLE_STATE,
  FAKE_OMP_READY_FRAME,
  FAKE_OMP_UNANSWERED,
  fakeOmpFailure,
  fakeOmpSuccess,
  makeFakeOmpProcess,
  type FakeOmpProcess,
  type FakeOmpScript,
} from "./rpc.testkit.ts";
import {
  OMP_MAX_FRAME_BYTES,
  OMP_MAX_REASSEMBLED_FRAME_BYTES,
  type OmpReadyFrame,
  type OmpRecord,
} from "./wire.ts";

interface RecordedLine {
  readonly dir: "in" | "out";
  readonly frame: OmpRecord;
  readonly chunk?: boolean;
}

const decodeJson = Schema.decodeSync(Schema.fromJsonString(Schema.Unknown));
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const jsonLine = (value: unknown) => `${encodeJson(value)}\n`;
const utf8Length = (text: string) => new TextEncoder().encode(text).byteLength;

/** Phase 0 recordings, trimmed: `{t, dir, frame}` per line, inbound chunks as metadata only. */
function readRecording(name: string): ReadonlyArray<RecordedLine> {
  return NodeFS.readFileSync(new URL(`./__fixtures__/rpc/${name}`, import.meta.url), "utf8")
    .split("\n")
    .filter((line) => line.length > 0)
    .map((line) => decodeJson(line) as RecordedLine);
}

function inbound(name: string): ReadonlyArray<RecordedLine> {
  return readRecording(name).filter((line) => line.dir === "in");
}

/** Rebinds a recorded response id to the one T3 wrote, as the replay testkits do. */
function rebind(frame: OmpRecord, recordedId: string, id: string): OmpRecord {
  return frame["id"] === recordedId ? { ...frame, id } : frame;
}

const OMP_SPAWN = { command: "omp", args: ["--mode", "rpc-ui"], cwd: undefined, env: {} };

const connect = (fake: FakeOmpProcess) =>
  makeOmpRpcConnection(OMP_SPAWN).pipe(
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner),
  );

const connectFake = (script?: FakeOmpScript) =>
  Effect.gen(function* () {
    const fake = yield* makeFakeOmpProcess(script);
    return { fake, connection: yield* connect(fake) };
  });

const chunkRecords = (frame: unknown, chunkId?: string): Array<OmpRecord> =>
  encodeFakeOmpChunks(frame, chunkId)
    .split("\n")
    .filter((line) => line.length > 0)
    .map((line) => decodeJson(line) as OmpRecord);

/** A response whose UTF-8 encoding is exactly `byteLength` bytes. */
function responseOfSize(byteLength: number): OmpRecord {
  const withPadding = (padding: string) => ({
    type: "response",
    id: "p0-3",
    command: "get_available_models",
    success: true,
    data: { padding },
  });
  return withPadding("x".repeat(byteLength - utf8Length(encodeJson(withPadding("")))));
}

const pushAll = (records: ReadonlyArray<OmpRecord>): Array<OmpChunkDecodeResult> => {
  const decoder = makeOmpChunkDecoder();
  return records.map((record) => decoder.push(record));
};

const invalid = (reason: string): OmpChunkDecodeResult => ({ _tag: "Invalid", reason });

/** An extension's `session_start` dialog; omp holds the negotiate ack until it is answered. */
const STARTUP_DIALOG: OmpRecord = {
  type: "extension_ui_request",
  id: "startup-dialog-1",
  method: "confirm",
  title: "Project extensions",
  message: "Load this project's extensions?",
};

describe("makeOmpJsonlFramer", () => {
  it("splits on LF only, strips a trailing CR and drops records over the limit", () => {
    const frame = makeOmpJsonlFramer(16);

    assert.deepStrictEqual(frame('{"a":1}\r\n{"b":'), { lines: ['{"a":1}'], droppedLengths: [] });
    assert.deepStrictEqual(frame("2} \n"), { lines: ['{"b":2} '], droppedLengths: [] });
    assert.deepStrictEqual(frame("y".repeat(10)), { lines: [], droppedLengths: [] });
    assert.deepStrictEqual(frame(`${"y".repeat(10)}\n{}\n`), {
      lines: ["{}"],
      droppedLengths: [20],
    });
  });
});

describe("makeOmpChunkDecoder", () => {
  const frame = responseOfSize(OMP_MAX_FRAME_BYTES + 300_000);
  const chunks = chunkRecords(frame);
  const [first, second, third] = chunks as [OmpRecord, OmpRecord, OmpRecord];
  const withLength = (delta: number) =>
    chunks.map((chunk) => ({ ...chunk, byteLength: Number(chunk["byteLength"]) + delta }));

  it("reassembles an in-order sequence and passes other frames through", () => {
    assert.strictEqual(chunks.length, 6);
    assert.deepStrictEqual(pushAll([...chunks, { type: "agent_start" }]), [
      ...chunks.slice(1).map((): OmpChunkDecodeResult => ({ _tag: "Pending" })),
      { _tag: "Frame", frame },
      { _tag: "Frame", frame: { type: "agent_start" } },
    ]);
  });

  it.each([
    ["a frame between chunks", [first, { type: "agent_start" }], "rpc chunk sequence interrupted"],
    ["another chunkId", [first, { ...second, chunkId: "rpc-2" }], "rpc chunk sequence mismatch"],
    ["a skipped index", [first, third], "rpc chunk sequence mismatch"],
    ["a sequence that starts late", [second], "rpc chunk sequence must start at index 0"],
    ["non-base64 data", [{ ...first, data: "not base64!" }], "invalid rpc chunk data"],
    ["non-canonical base64", [{ ...first, data: "QR==" }], "invalid rpc chunk data"],
    ["a single-chunk count", [{ ...first, count: 1 }], "invalid rpc chunk metadata"],
    [
      "a frame small enough to send whole",
      [{ ...first, byteLength: OMP_MAX_FRAME_BYTES - 1 }],
      "invalid rpc chunk metadata",
    ],
    ["more bytes than declared", withLength(-1), "rpc chunk sequence exceeds declared length"],
    ["fewer bytes than declared", withLength(1), "rpc chunk sequence length mismatch"],
    [
      "a payload that is not a JSON object",
      chunkRecords(["x".repeat(OMP_MAX_FRAME_BYTES)]),
      "rpc frame must be an object",
    ],
  ])("rejects %s", (_name, records, reason) => {
    assert.deepStrictEqual(pushAll(records).at(-1), invalid(reason));
  });

  it("accepts the chunk geometry omp 19.1.1 recorded for get_available_models", () => {
    const recorded = inbound("harness-smoke.jsonl").filter((line) => line.chunk === true);
    const byteLength = Number(recorded[0]?.frame["byteLength"]);
    const encoded = chunkRecords(responseOfSize(byteLength));

    assert.deepStrictEqual(
      encoded.map(({ data, ...metadata }) => ({
        ...metadata,
        dataBase64Length: String(data).length,
      })),
      recorded.map((line) => line.frame),
    );
    assert.strictEqual(pushAll(encoded).at(-1)?._tag, "Frame");
  });
});

describe("stripOmpBashTrace", () => {
  it("removes bash details.trace from every frame that carries it (TL/02)", () => {
    const frames = inbound("bash-trace.jsonl").map((line) => line.frame);
    const withoutTraceKeys = (value: unknown) =>
      decodeJson(JSON.stringify(value, (key, item) => (key === "trace" ? undefined : item)));

    assert.isTrue(frames.slice(1).every((frame) => JSON.stringify(frame).includes('"trace"')));
    for (const frame of frames) {
      assert.deepStrictEqual(stripOmpBashTrace(frame), withoutTraceKeys(frame));
    }
    assert.deepStrictEqual(
      frames.filter((frame) => stripOmpBashTrace(frame) === frame),
      frames.slice(0, 1),
    );
  });

  it("strips get_messages results and leaves other tools' details alone", () => {
    const readEnd = {
      type: "tool_execution_end",
      toolName: "read",
      result: { details: { trace: "kept" } },
    };
    const messages = {
      type: "response",
      id: "t3-4",
      command: "get_messages",
      success: true,
      data: { messages: [{ role: "toolResult", toolName: "bash", details: { trace: {}, a: 1 } }] },
    };

    assert.strictEqual(stripOmpBashTrace(readEnd), readEnd);
    assert.deepStrictEqual(stripOmpBashTrace({ type: "toString" }), { type: "toString" });
    assert.deepStrictEqual(stripOmpBashTrace(messages), {
      ...messages,
      data: { messages: [{ role: "toolResult", toolName: "bash", details: { a: 1 } }] },
    });
  });
});

describe("makeOmpRpcConnection handshake", () => {
  it.effect("replays omp's startup with early commands (TL/01:5-20)", () =>
    Effect.gen(function* () {
      const [ready, ...rest] = inbound("startup-before-ready.jsonl").map((line) => line.frame);
      const fake = yield* makeFakeOmpProcess({
        ready: ready as OmpReadyFrame,
        reply: (command) =>
          command.type === "negotiate_protocol" ? FAKE_OMP_UNANSWERED : undefined,
      });
      const connecting = yield* connect(fake).pipe(Effect.forkChild);
      const negotiate = yield* fake.takeCommand("negotiate_protocol");
      yield* fake.emitRaw(
        rest.map((frame) => jsonLine(rebind(frame, "neg-2", negotiate.id))).join(""),
      );
      const connection = yield* Fiber.join(connecting);

      const consumed = (frame: OmpRecord) =>
        frame["type"] === "response" && (frame["id"] === undefined || frame["id"] === "neg-2");
      const expected = rest.filter((frame) => !consumed(frame));
      assert.strictEqual(negotiate.protocolVersion, 2);
      assert.deepStrictEqual(connection.ready.supportedProtocolVersions, [1, 2]);
      assert.deepStrictEqual(
        expected.map((frame) => `${frame["type"]}:${frame["id"] ?? ""}`),
        [
          "extension_ui_request:15dbc38de5acec10",
          "advisor_cost_changed:",
          "available_commands_update:",
          "response:early-state",
          "response:early-unknown",
          "response:v1-models",
          "response:neg-3",
          "response:neg-2-again",
        ],
      );
      assert.deepStrictEqual(yield* Queue.takeN(connection.events, expected.length), expected);
      assert.isTrue(Option.isNone(yield* Queue.poll(connection.events)));
    }).pipe(Effect.scoped),
  );

  it.effect("replays a session with a chunked get_available_models (harness-smoke)", () =>
    Effect.gen(function* () {
      const [ready, ...rest] = inbound("harness-smoke.jsonl");
      const byteLength = Number(rest.find((line) => line.chunk === true)?.frame["byteLength"]);
      const models = responseOfSize(byteLength);
      const frames = rest
        .filter((line) => line.chunk !== true)
        .map((line) => (line.frame["command"] === "get_available_models" ? models : line.frame));
      const fake = yield* makeFakeOmpProcess({
        ready: ready?.frame as OmpReadyFrame,
        reply: (command) =>
          command.type === "negotiate_protocol" ? FAKE_OMP_UNANSWERED : undefined,
      });
      const connecting = yield* connect(fake).pipe(Effect.forkChild);
      const negotiate = yield* fake.takeCommand("negotiate_protocol");
      yield* fake.emitRaw(
        frames
          .map((frame) =>
            frame === models
              ? encodeFakeOmpChunks(frame)
              : jsonLine(rebind(frame, "p0-1", negotiate.id)),
          )
          .join(""),
      );
      const connection = yield* Fiber.join(connecting);

      const expected = frames.filter((frame) => frame["id"] !== "p0-1");
      assert.deepStrictEqual(yield* Queue.takeN(connection.events, expected.length), expected);
    }).pipe(Effect.scoped),
  );

  it.effect("refuses an omp without protocol v2 or that rejects it", () =>
    Effect.gen(function* () {
      const unsupported = yield* connectFake({
        ready: { ...FAKE_OMP_READY_FRAME, supportedProtocolVersions: [1] },
      }).pipe(Effect.flip, Effect.scoped);
      const otherLimits = yield* connectFake({
        ready: { ...FAKE_OMP_READY_FRAME, maxFrameBytes: 2 * OMP_MAX_FRAME_BYTES },
      }).pipe(Effect.flip, Effect.scoped);
      const refused = yield* connectFake({
        reply: (command) =>
          command.type === "negotiate_protocol"
            ? fakeOmpFailure("Unsupported RPC protocol version: 2")
            : undefined,
      }).pipe(Effect.flip, Effect.scoped);

      assert.isTrue(
        unsupported._tag === "OmpRpcHandshakeError" &&
          unsupported.reason === "protocol_unsupported",
      );
      assert.deepStrictEqual(
        unsupported._tag === "OmpRpcHandshakeError" ? unsupported.supportedProtocolVersions : [],
        [1],
      );
      assert.isTrue(
        otherLimits._tag === "OmpRpcHandshakeError" &&
          otherLimits.reason === "protocol_unsupported",
        "frame limits other than the framer's are refused, as upstream's client does",
      );
      assert.isTrue(
        refused._tag === "OmpRpcHandshakeError" && refused.reason === "negotiation_refused",
      );
    }),
  );

  it.effect("opens on a dialog omp raises while starting and negotiates once it is answered", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess({
        startup: [STARTUP_DIALOG],
        reply: (command) =>
          command.type === "negotiate_protocol" ? FAKE_OMP_UNANSWERED : undefined,
      });
      const connection = yield* connect(fake);
      const negotiate = yield* fake.takeCommand("negotiate_protocol");
      const heldOpen = yield* connection.isNegotiated;
      const queued = yield* Queue.take(connection.events);
      const negotiating = yield* connection.negotiated.pipe(Effect.forkChild);
      yield* fake.respond(negotiate, { protocolVersion: 2 });
      yield* Fiber.join(negotiating);

      assert.isFalse(heldOpen);
      assert.deepStrictEqual(queued, STARTUP_DIALOG);
      assert.isTrue(yield* connection.isNegotiated);
    }).pipe(Effect.scoped),
  );

  it.effect("fails the transport when omp refuses protocol v2 after a startup dialog", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess({
        startup: [STARTUP_DIALOG],
        reply: (command) =>
          command.type === "negotiate_protocol" ? FAKE_OMP_UNANSWERED : undefined,
      });
      const connection = yield* connect(fake);
      yield* Queue.take(connection.events);
      yield* fake.respondFailure(
        yield* fake.takeCommand("negotiate_protocol"),
        "Unsupported RPC protocol version: 2",
      );
      const refused = yield* connection.negotiated.pipe(Effect.flip);
      const transportDown = yield* Queue.take(connection.events).pipe(Effect.flip);

      assert.strictEqual(refused.operation, "negotiate_protocol");
      assert.strictEqual(transportDown, refused);
    }).pipe(Effect.scoped),
  );

  it.effect("gives up when omp never writes ready", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess({ ready: null, reply: () => FAKE_OMP_UNANSWERED });
      const connecting = yield* connect(fake).pipe(Effect.flip, Effect.forkChild);
      yield* fake.takeCommand("negotiate_protocol");
      yield* TestClock.adjust(Duration.seconds(30));
      const error = yield* Fiber.join(connecting);

      assert.isTrue(error._tag === "OmpRpcHandshakeError" && error.reason === "no_ready_frame");
    }).pipe(Effect.scoped),
  );

  it.effect("reports a nonzero exit code, not stderr, when omp exits before ready", () =>
    Effect.gen(function* () {
      const secret = "API_KEY=super-secret\n";
      const spawner = ChildProcessSpawner.make(() =>
        Effect.succeed(
          ChildProcessSpawner.makeHandle({
            pid: ChildProcessSpawner.ProcessId(999_999_999),
            exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
            isRunning: Effect.succeed(false),
            kill: () => Effect.void,
            unref: Effect.succeed(Effect.void),
            stdin: Sink.drain,
            stdout: Stream.empty,
            stderr: Stream.make(new TextEncoder().encode(secret)),
            all: Stream.empty,
            getInputFd: () => Sink.drain,
            getOutputFd: () => Stream.empty,
          }),
        ),
      );
      const error = yield* makeOmpRpcConnection(OMP_SPAWN).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.flip,
      );

      assert.isTrue(error._tag === "OmpRpcError" && error.operation === "read");
      assert.strictEqual(error.message, "omp RPC read failed: omp process exited with code 1.");
    }).pipe(Effect.scoped),
  );
});

describe("makeOmpRpcConnection responses", () => {
  it.effect(
    "settles request ids once and queues caller-id responses in stream order (TL/04:24-27)",
    () =>
      Effect.gen(function* () {
        const busy = (id: string) => ({
          type: "response",
          id,
          command: "prompt",
          success: false,
          error: "Agent is already processing.",
        });
        const promptResult = (id: string) => ({
          type: "prompt_result",
          id,
          agentInvoked: false,
          status: "error",
          sessionSettled: false,
          error: { message: "Agent is already processing.", retryable: false },
        });
        const { connection } = yield* connectFake({
          reply: (command) => {
            if (command.type === "prompt") {
              return fakeOmpSuccess(undefined, {
                before: [busy(command.id)],
                after: [promptResult(command.id)],
              });
            }
            if (command.type !== "get_state") return undefined;
            return fakeOmpSuccess(FAKE_OMP_IDLE_STATE, {
              after: [
                { ...busy(command.id), command: "get_state" },
                { type: "response", command: "parse", success: false, error: "Failed to parse" },
              ],
            });
          },
        });

        const state = yield* connection.request({ type: "get_state" });
        yield* connection.send({ type: "prompt", id: "t3-prompt-0", message: "zeta" });
        const reserved = yield* connection.send({ type: "abort", id: "t3-7" }).pipe(Effect.flip);

        assert.deepStrictEqual(state, { id: "t3-1", data: FAKE_OMP_IDLE_STATE });
        assert.deepStrictEqual(yield* Queue.takeN(connection.events, 3), [
          busy("t3-prompt-0"),
          { type: "response", id: "t3-prompt-0", command: "prompt", success: true },
          promptResult("t3-prompt-0"),
        ]);
        assert.strictEqual(reserved.detail, "id t3-7 is reserved");
      }).pipe(Effect.scoped),
  );

  it.effect("fails the transport and pending requests on an invalid chunk", () =>
    Effect.gen(function* () {
      const { fake, connection } = yield* connectFake({
        reply: (command) => (command.type === "get_state" ? FAKE_OMP_UNANSWERED : undefined),
      });
      const pending = yield* connection
        .request({ type: "get_state" })
        .pipe(Effect.flip, Effect.forkChild);
      yield* fake.takeCommand("get_state");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ ...chunkRecords(responseOfSize(OMP_MAX_FRAME_BYTES + 1))[0], data: "@" });

      assert.strictEqual((yield* Queue.take(connection.events))["type"], "agent_start");
      const error = yield* Queue.take(connection.events).pipe(Effect.flip);
      assert.strictEqual(error.message, "omp RPC read failed: invalid rpc chunk data.");
      assert.strictEqual((yield* Fiber.join(pending)).message, error.message);
    }).pipe(Effect.scoped),
  );

  it.effect("drops non-JSON and oversized stdout records with a warning", () => {
    const warnings: Array<unknown> = [];
    const logCapture = Logger.make<unknown, void>(({ logLevel, message }) => {
      if (logLevel === "Warn") warnings.push(message);
    });
    return Effect.gen(function* () {
      const { fake, connection } = yield* connectFake();
      const oversized = `{"type":"message_update","text":"${"x".repeat(OMP_MAX_FRAME_BYTES)}"}`;
      yield* fake.emitRaw('{"type":"agent_');
      yield* fake.emitRaw('start"}\r\n{"type":"turn_start"}\nnot json\n');
      yield* fake.emitRaw(`${oversized}\n{"type":"turn_end"}\n`);

      assert.deepStrictEqual(
        (yield* Queue.takeN(connection.events, 3)).map((frame) => frame["type"]),
        ["agent_start", "turn_start", "turn_end"],
      );
      assert.deepStrictEqual(warnings, [
        [
          "Dropping oversized omp stdout record.",
          { recordLength: oversized.length, maxRecordLength: OMP_MAX_FRAME_BYTES },
        ],
      ]);
    }).pipe(
      Effect.scoped,
      Effect.provide(Logger.layer([logCapture], { mergeWithExisting: false })),
    );
  });
});

describe("makeOmpRpcConnection teardown", () => {
  // Speaks just enough of the protocol to connect; records how it was asked to stop.
  const fakeOmpScript = `
const fs = require("node:fs");
const [cuesPath, onEof] = process.argv.slice(1);
const note = (cue) => fs.appendFileSync(cuesPath, cue + "\\n");
const write = (frame) => process.stdout.write(JSON.stringify(frame) + "\\n");
write({ type: "ready", supportedProtocolVersions: [1, 2], maxFrameBytes: ${OMP_MAX_FRAME_BYTES}, maxReassembledFrameBytes: ${OMP_MAX_REASSEMBLED_FRAME_BYTES} });
let pending = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (text) => {
  const lines = (pending + text).split("\\n");
  pending = lines.pop();
  for (const line of lines) {
    const command = JSON.parse(line);
    write({ type: "response", id: command.id, command: command.type, success: true, data: { protocolVersion: 2 } });
  }
});
process.stdin.on("end", () => {
  note("stdin end");
  if (onEof === "exit") process.exit(0);
  if (onEof === "exit-later") setTimeout(() => process.exit(0), 50);
});
process.on("SIGTERM", () => { note("SIGTERM"); process.exit(0); });
setInterval(() => {}, 1 << 30);
`;

  const spawnWatched = Effect.gen(function* () {
    const realSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const childExit = yield* Deferred.make<
      ChildProcessSpawner.ExitCode,
      PlatformError.PlatformError
    >();
    const stdinClosed = yield* Deferred.make<void>();
    const spawner = ChildProcessSpawner.make((command) =>
      realSpawner.spawn(command).pipe(
        Effect.tap((handle) => handle.exitCode.pipe(Deferred.into(childExit), Effect.forkDetach)),
        Effect.map((handle) =>
          ChildProcessSpawner.makeHandle({
            ...handle,
            stdin: handle.stdin.pipe(Sink.ensuring(Deferred.succeed(stdinClosed, undefined))),
          }),
        ),
      ),
    );
    const fileSystem = yield* FileSystem.FileSystem;
    const cuesPath = `${yield* fileSystem.makeTempDirectoryScoped()}/cues`;
    const connectReal = (onEof: "exit" | "exit-later" | "ignore") =>
      makeOmpRpcConnection({
        command: process.execPath,
        args: ["-e", fakeOmpScript, cuesPath, onEof],
        cwd: undefined,
        env: {},
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
    const cues = fileSystem
      .readFileString(cuesPath)
      .pipe(Effect.map((text) => text.split("\n").filter(Boolean)));
    return { childExit, stdinClosed, connectReal, cues };
  });

  it.effect.skipIf(HostProcess.Platform.defaultValue() === "win32")(
    "closes stdin on scope close and never signals an omp that exits",
    () =>
      Effect.gen(function* () {
        const { childExit, connectReal, cues } = yield* spawnWatched;
        yield* connectReal("exit").pipe(Effect.scoped);

        assert.strictEqual(yield* Deferred.await(childExit), 0);
        assert.deepStrictEqual(yield* cues, ["stdin end"]);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect.skipIf(HostProcess.Platform.defaultValue() === "win32")(
    "signals omp only once it outlives the stdin grace",
    () =>
      Effect.gen(function* () {
        const { childExit, stdinClosed, connectReal, cues } = yield* spawnWatched;
        const scope = yield* Scope.make();
        const connection = yield* connectReal("ignore").pipe(Scope.provide(scope));

        const terminating = yield* connection.terminate.pipe(Effect.forkChild);
        yield* Deferred.await(stdinClosed);
        yield* TestClock.adjust(Duration.seconds(2));
        assert.strictEqual(yield* Deferred.await(childExit), 0);
        yield* TestClock.adjust(Duration.seconds(1));
        yield* Fiber.join(terminating);
        assert.strictEqual(yield* connection.exited, 0);
        yield* Scope.close(scope, Exit.void);

        assert.sameMembers([...(yield* cues)], ["stdin end", "SIGTERM"]);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
  it.effect.skipIf(HostProcess.Platform.defaultValue() === "win32")(
    "waits the caller's grace before signalling an omp still shutting down",
    () =>
      Effect.gen(function* () {
        const { stdinClosed, connectReal, cues } = yield* spawnWatched;
        const scope = yield* Scope.make();
        const connection = yield* connectReal("exit-later").pipe(Scope.provide(scope));

        const terminating = yield* connection
          .terminateWithin(Duration.seconds(60))
          .pipe(Effect.forkChild);
        yield* Deferred.await(stdinClosed);
        yield* TestClock.adjust(Duration.seconds(2));
        yield* Fiber.join(terminating);
        assert.strictEqual(yield* connection.exited, 0);
        yield* Scope.close(scope, Exit.void);

        assert.deepStrictEqual(yield* cues, ["stdin end"]);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
