import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CheckpointId,
  EnvironmentId,
  MessageId,
  NodeId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderRef,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type RuntimeMode,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import type * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import type * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as HttpClient from "effect/http/HttpClient";
import type * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import type { OmpSettings } from "../settings.ts";
import advisorFrames from "./__fixtures__/advisor/frames.json" with { type: "json" };
import frames from "./__fixtures__/adapter/frames.json" with { type: "json" };
import goalFrames from "./__fixtures__/goal/frames.json" with { type: "json" };
import subagentFrames from "./__fixtures__/subagents/frames.json" with { type: "json" };
import todoFrames from "./__fixtures__/todos/frames.json" with { type: "json" };
import { makeOmpAdapterV2, OMP_PROVIDER, type OmpAdapterV2Options } from "./adapter.ts";
import { ompOverlayPath, renderOmpUnattendedOverlay } from "./launch.ts";
import {
  FAKE_OMP_IDLE_STATE,
  FAKE_OMP_SESSION_FILE,
  FAKE_OMP_UNANSWERED,
  fakeOmpFailure,
  fakeOmpSuccess,
  makeFakeOmpProcess,
  type FakeOmpProcess,
  type FakeOmpReply,
  type FakeOmpScript,
} from "./rpc.testkit.ts";
import { ompSessionDirForCwd } from "./sessions.ts";
import { ompContentText, ompRecordField, ompRecordString, type OmpCommandFrame } from "./wire.ts";

type AdapterEvent = ProviderAdapter.ProviderAdapterV2Event;
type TerminalEvent = Extract<AdapterEvent, { readonly type: "turn.terminal" }>;

const OMP_INSTANCE_ID = ProviderInstanceId.make("omp");
const THREAD_ID = ThreadId.make("thread-omp-test");
const SESSION_ID = ProviderSessionId.make("provider-session-omp-test");
const SETTINGS: OmpSettings = {
  enabled: true,
  binaryPath: "omp",
  launchArgs: "",
  customModels: [],
};
const FIRST_USER_ENTRY = "e1078788";
const SECOND_USER_ENTRY = "22d686f8";
const FORK_ENTRY = "00633b09";

const layerTest = Layer.mergeAll(
  NodeServices.layer,
  IdAllocator.layer,
  McpProviderSessions.layer,
  TestProviderHost.layer().pipe(Layer.provide(NodeServices.layer)),
);

const isTerminal = (event: AdapterEvent): event is TerminalEvent => event.type === "turn.terminal";
const isProviderTurnUpdate = (
  event: AdapterEvent,
): event is Extract<AdapterEvent, { readonly type: "provider_turn.updated" }> =>
  event.type === "provider_turn.updated";

const isRuntimeRequestUpdate = (
  event: AdapterEvent,
): event is Extract<AdapterEvent, { readonly type: "runtime_request.updated" }> =>
  event.type === "runtime_request.updated";
const userMessageStart = (text: string, steering = false) => ({
  type: "message_start",
  message: {
    role: "user",
    content: [{ type: "text", text }],
    attribution: "user",
    ...(steering ? { steering: true } : {}),
  },
});

/** Delta-mode frames of one assistant message with a single text block. */
const assistantReply = (text: string) => {
  const update = (assistantMessageEvent: Record<string, unknown>) => ({
    type: "message_update",
    assistantMessageEvent,
    message: { role: "assistant" },
  });
  return [
    { type: "message_start", message: { role: "assistant", content: [] } },
    update({ type: "text_start", contentIndex: 0 }),
    update({ type: "text_delta", contentIndex: 0, delta: text }),
    update({ type: "text_end", contentIndex: 0, content: text }),
    {
      type: "message_end",
      message: { role: "assistant", content: [{ type: "text", text }], stopReason: "stop" },
    },
  ];
};

const emitAll = (fake: FakeOmpProcess, records: ReadonlyArray<Record<string, unknown>>) =>
  Effect.forEach(records, (record) => fake.emit(record), { discard: true });

const assistantTexts = (seen: ReadonlyArray<AdapterEvent>, providerTurnId: ProviderTurnId) => {
  const latest = new Map<string, string>();
  for (const event of seen) {
    if (event.type !== "turn_item.updated" || event.turnItem.type !== "assistant_message") continue;
    if (event.turnItem.providerTurnId !== providerTurnId) continue;
    latest.set(event.turnItem.id, event.turnItem.text);
  }
  return [...latest.values()];
};

/** Lists `subagents` for `get_subagents` and confirms every `cancel_subagent`. */
const subagentReply = (
  command: OmpCommandFrame,
  subagents: ReadonlyArray<Record<string, unknown>>,
) => {
  if (command.type === "get_subagents") return fakeOmpSuccess({ subagents });
  return command.type === "cancel_subagent"
    ? fakeOmpSuccess(frames.cancelSubagentResult)
    : undefined;
};

/** omp's delivery of a cancelled child's result (round-a/subagent-stop-abort-then-cancel.jsonl:103-104). */
const cancelledSubagentResult = (jobId: string) => {
  const message = {
    role: "custom",
    customType: "async-result",
    content: `<system-notice>\nBackground job ${jobId} has completed.\n</system-notice>`,
    display: true,
    details: { jobs: [{ jobId, type: "task", status: "failed" }] },
  };
  return [
    { type: "message_start", message },
    { type: "message_end", message },
  ];
};

const sessionStatuses = (seen: ReadonlyArray<AdapterEvent>) =>
  seen.flatMap((event) =>
    event.type === "provider_session.updated" ? [event.providerSession.status] : [],
  );

const steerInput = (session: OmpTestSession, providerTurnId: ProviderTurnId, text: string) => ({
  threadId: THREAD_ID,
  runId: RunId.make(`run:${THREAD_ID}:1`),
  providerThread: session.providerThread,
  providerTurnId,
  message: {
    messageId: MessageId.make(`message:${THREAD_ID}:${text}`),
    text,
    attachments: [],
    createdBy: "user" as const,
    creationSource: "web" as const,
  },
});

/** The Stop budget (7 s), plus the second TestClock steps cannot divide finer. */
const OMP_TEST_STOP_SECONDS = 8;

/** omp 19.2.0 backgrounds running bash and eval on `abort_and_restore_queue` (CHANGELOG 19.2.0). */
const STOP_COMMANDS: ReadonlySet<string> = new Set([
  "remove_queued_message",
  "abort",
  "abort_and_restore_queue",
]);

const isAssistantItemUpdate = (
  event: AdapterEvent,
): event is Extract<AdapterEvent, { readonly type: "turn_item.updated" }> =>
  event.type === "turn_item.updated" && event.turnItem.type === "assistant_message";
const isApprovalItemUpdate = (
  event: AdapterEvent,
): event is Extract<AdapterEvent, { readonly type: "turn_item.updated" }> =>
  event.type === "turn_item.updated" && event.turnItem.type === "approval_request";
const isSteerUpdate = (
  event: AdapterEvent,
): event is Extract<AdapterEvent, { readonly type: "steer.updated" }> =>
  event.type === "steer.updated";
const isBackgroundRelease = (event: AdapterEvent): event is AdapterEvent =>
  event.type === "background_work.updated" && !event.pending;
const isSessionStopped = (event: AdapterEvent): event is AdapterEvent =>
  event.type === "provider_session.updated" && event.providerSession.status === "stopped";

const firstProviderTurnId = (seen: ReadonlyArray<AdapterEvent>) => {
  const started = seen.find(isProviderTurnUpdate);
  if (started === undefined) throw new Error("no provider turn was started");
  return started.providerTurn.id;
};

/** Drains the stream to its end, which a failed session reaches with an error. */
const matchesNothing = (_event: AdapterEvent): _event is never => false;

const modelSelection = (model: string, thinking?: string): ModelSelection => ({
  instanceId: OMP_INSTANCE_ID,
  model,
  ...(thinking === undefined ? {} : { options: [{ id: "thinking", value: thinking }] }),
});

const runtimePolicy = (runtimeMode: RuntimeMode, cwd: string) =>
  ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
    runtimeMode,
    interactionMode: "default",
    cwd,
  });

/** A session file whose header (line 2) names `cwd`, then `entries`, as omp writes it (R§44). */
const writeSessionFile = Effect.fnUntraced(function* (
  file: string,
  cwd: string,
  entries: ReadonlyArray<unknown> = [],
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const header = { type: "session", version: 3, id: "s1", timestamp: "2026-11-25T00:00:00Z", cwd };
  const lines = [{ type: "title", v: 1, title: "" }, header, ...entries];
  yield* fileSystem.writeFileString(
    file,
    lines.map((line) => `${JSON.stringify(line)}\n`).join(""),
  );
  return file;
});

const stateWith = (patch: Record<string, unknown>) => ({ ...FAKE_OMP_IDLE_STATE, ...patch });

/** Sends `--fork` launches to `forkFake`, everything else to `mainFake` (Pi's adapter test pattern). */
const routeForkSpawns = (mainFake: FakeOmpProcess, forkFake: FakeOmpProcess) =>
  ChildProcessSpawner.make((command) =>
    ChildProcess.isStandardCommand(command) && command.args.includes("--fork")
      ? forkFake.spawner.spawn(command)
      : mainFake.spawner.spawn(command),
  );

const unavailableMcp = HttpClient.make((request) =>
  Effect.succeed(HttpClientResponse.fromWeb(request, new Response(null, { status: 503 }))),
);

interface OpenOptions {
  readonly runtimeMode?: RuntimeMode;
  readonly cwd?: string;
  readonly http?: HttpClient.HttpClient;
  readonly spawner?: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly initialNativeThreadId?: string;
  readonly continuationRequests?: OmpAdapterV2Options["continuationRequests"];
  readonly providerSessionId?: ProviderSessionId;
}

/** Opens a session without registering a thread, so tests can observe what omp did at startup. */
const openSession = Effect.fnUntraced(function* (fake: FakeOmpProcess, open: OpenOptions = {}) {
  const fileSystem = yield* FileSystem.FileSystem;
  const home = yield* fileSystem.makeTempDirectoryScoped({ prefix: "omp-adapter-home-" });
  const cwd = open.cwd ?? (yield* fileSystem.makeTempDirectoryScoped({ prefix: "omp-adapter-" }));
  const policy = runtimePolicy(open.runtimeMode ?? "full-access", cwd);
  const adapter = yield* makeOmpAdapterV2({
    instanceId: OMP_INSTANCE_ID,
    settings: SETTINGS,
    environment: { HOME: home },
    ...(open.continuationRequests === undefined
      ? {}
      : { continuationRequests: open.continuationRequests }),
  }).pipe(
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, open.spawner ?? fake.spawner),
    Effect.provideService(HttpClient.HttpClient, open.http ?? unavailableMcp),
  );
  const runtime = yield* adapter.openSession({
    threadId: THREAD_ID,
    providerSessionId: open.providerSessionId ?? SESSION_ID,
    modelSelection: modelSelection("default"),
    runtimePolicy: policy,
    ...(open.initialNativeThreadId === undefined
      ? {}
      : { initialNativeThreadId: open.initialNativeThreadId }),
  });
  const emitted = yield* Queue.unbounded<
    AdapterEvent,
    ProviderAdapter.ProviderAdapterV2Error | Cause.Done
  >();
  yield* Stream.runIntoQueue(runtime.events, emitted).pipe(Effect.forkScoped);
  const seen: Array<AdapterEvent> = [];
  const takeEvent = Effect.fnUntraced(function* <A extends AdapterEvent>(
    predicate: (event: AdapterEvent) => event is A,
  ) {
    while (true) {
      const event = yield* Queue.take(emitted);
      seen.push(event);
      if (predicate(event)) return event;
    }
  });
  const ensureThread = runtime.ensureThread({
    threadId: THREAD_ID,
    modelSelection: modelSelection("default"),
    runtimePolicy: policy,
  });
  return { runtime, policy, cwd, emitted, seen, takeEvent, ensureThread };
});

const openRuntime = Effect.fnUntraced(function* (fake: FakeOmpProcess, open: OpenOptions = {}) {
  const session = yield* openSession(fake, open);
  return { ...session, providerThread: yield* session.ensureThread };
});

type OmpTestSession = Effect.Success<ReturnType<typeof openRuntime>>;

/** An extension's `session_start` dialog; omp holds the negotiate ack until it is answered. */
const STARTUP_DIALOG = {
  type: "extension_ui_request",
  id: "startup-dialog-1",
  method: "confirm",
  title: "Project extensions",
  message: "Load this project's extensions?",
} as const;

const makeAppThread = Effect.fnUntraced(function* (selection: ModelSelection) {
  const now = yield* DateTime.now;
  return {
    createdBy: "user",
    creationSource: "web",
    id: THREAD_ID,
    projectId: "project:fixture:omp" as OrchestrationV2AppThread["projectId"],
    title: "omp test thread",
    providerInstanceId: OMP_INSTANCE_ID,
    modelSelection: selection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: THREAD_ID },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  } satisfies OrchestrationV2AppThread;
});

interface TurnOptions {
  readonly ordinal?: number;
  readonly text?: string;
  readonly selection?: ModelSelection;
  readonly runtimeMode?: RuntimeMode;
  /** A provider-initiated continuation, as the orchestrator dispatches for a wake. */
  readonly continuation?: boolean;
}

const startTurn = Effect.fnUntraced(function* (session: OmpTestSession, turn: TurnOptions = {}) {
  const ordinal = turn.ordinal ?? 1;
  const selection = turn.selection ?? modelSelection("default");
  const runId = RunId.make(`run:${THREAD_ID}:${ordinal}`);
  yield* session.runtime.startTurn({
    appThread: yield* makeAppThread(selection),
    threadId: THREAD_ID,
    runId,
    runOrdinal: ordinal,
    providerTurnOrdinal: ordinal,
    attemptId: RunAttemptId.make(`run-attempt:${runId}:1`),
    rootNodeId: NodeId.make(`node:${runId}:root`),
    providerThread: session.providerThread,
    message: {
      messageId: MessageId.make(`message:${THREAD_ID}:${ordinal}`),
      text: turn.text ?? "Hello omp",
      attachments: [],
      createdBy: turn.continuation === true ? "agent" : "user",
      creationSource: turn.continuation === true ? "provider" : "web",
    },
    modelSelection: selection,
    runtimePolicy:
      turn.runtimeMode === undefined
        ? session.policy
        : runtimePolicy(turn.runtimeMode, session.cwd),
  });
});

const promptResult = (id: string, patch: Record<string, unknown> = {}) => ({
  type: "prompt_result",
  id,
  agentInvoked: true,
  status: "completed",
  sessionSettled: true,
  ...patch,
});

/** Starts a turn and completes its prompt with a `prompt_result`. */
const runTurn = Effect.fnUntraced(function* (
  session: OmpTestSession,
  fake: FakeOmpProcess,
  turn: TurnOptions = {},
) {
  yield* startTurn(session, turn);
  const prompt = yield* fake.takeCommand("prompt");
  yield* fake.emit(promptResult(prompt.id));
  return yield* session.takeEvent(isTerminal);
});

const commandsOf = <Type extends OmpCommandFrame["type"]>(fake: FakeOmpProcess, type: Type) =>
  fake
    .commands()
    .filter(
      (command): command is Extract<OmpCommandFrame, { type: Type }> => command.type === type,
    );

const lastToolRowStatus = (seen: ReadonlyArray<AdapterEvent>, toolCallId: string) =>
  seen
    .flatMap((event) =>
      event.type === "turn_item.updated" && event.turnItem.nativeItemRef?.nativeId === toolCallId
        ? [event.turnItem.status]
        : [],
    )
    .at(-1);

const nativeIdOf = (event: AdapterEvent) => {
  if (event.type === "turn_item.updated") return event.turnItem.nativeItemRef?.nativeId;
  return event.type === "node.updated" ? event.node.nativeItemRef?.nativeId : undefined;
};

type SubagentUpdate = Extract<AdapterEvent, { readonly type: "subagent.updated" }>;

/** Updates of the row for omp child `childId`, which omp names by its id. */
const isSubagentUpdateOf =
  (childId: string) =>
  (event: AdapterEvent): event is SubagentUpdate =>
    event.type === "subagent.updated" && event.subagent.nativeTaskRef?.nativeId === childId;

const subagentUpdates = (seen: ReadonlyArray<AdapterEvent>, childId: string) =>
  seen.filter(isSubagentUpdateOf(childId)).map((event) => event.subagent);

const lastSubagent = (seen: ReadonlyArray<AdapterEvent>, childId: string) =>
  subagentUpdates(seen, childId).at(-1);

/** What a subagent row shows and where it hangs. */
const subagentRow = (subagent: SubagentUpdate["subagent"] | undefined) =>
  subagent === undefined
    ? undefined
    : {
        status: subagent.status,
        title: subagent.title,
        result: subagent.result,
        origin: subagent.origin,
        childThreadId: subagent.childThreadId,
        runId: subagent.runId,
        parentNodeId: subagent.parentNodeId,
      };

const nodeIdOf = (seen: ReadonlyArray<AdapterEvent>, nativeItemId: string) => {
  const update = seen.find(
    (event) => event.type === "node.updated" && nativeIdOf(event) === nativeItemId,
  );
  if (update?.type !== "node.updated") throw new Error(`omp item ${nativeItemId} has no node`);
  return update.node.id;
};

/** Each subagent node update as [omp child id, status]. */
const subagentNodeStatuses = (seen: ReadonlyArray<AdapterEvent>) =>
  seen.flatMap((event) =>
    event.type === "node.updated" && event.node.kind === "subagent"
      ? [[String(nativeIdOf(event)).split(":").at(-1), event.node.status]]
      : [],
  );

const backgroundWorkPending = (seen: ReadonlyArray<AdapterEvent>) =>
  seen.flatMap((event) => (event.type === "background_work.updated" ? [event.pending] : []));

const todoPlans = (seen: ReadonlyArray<AdapterEvent>) =>
  seen.flatMap((event) => (event.type === "plan.updated" ? [event.plan] : []));

const todoListItems = (seen: ReadonlyArray<AdapterEvent>) =>
  seen.flatMap((event) =>
    event.type === "turn_item.updated" && event.turnItem.type === "todo_list"
      ? [event.turnItem]
      : [],
  );

/** Each todo snapshot emits its node, then its plan, then its turn item. */
const TODO_PROJECTION = ["node", "plan", "item"] as const;

const todoProjectionStep = (event: AdapterEvent): ReadonlyArray<string> => {
  if (event.type === "node.updated") return event.node.kind === "todo_list" ? ["node"] : [];
  if (event.type === "plan.updated") return ["plan"];
  const isTodoItem = event.type === "turn_item.updated" && event.turnItem.type === "todo_list";
  return isTodoItem ? ["item"] : [];
};

const runtimeRequestStatuses = (seen: ReadonlyArray<AdapterEvent>) =>
  seen.flatMap((event) =>
    event.type === "runtime_request.updated" ? [event.runtimeRequest.status] : [],
  );

const providerTurn = (
  providerThread: OrchestrationV2ProviderThread,
  ordinal: number,
  nativeTurnRef: OrchestrationV2ProviderTurn["nativeTurnRef"],
): OrchestrationV2ProviderTurn => ({
  id: ProviderTurnId.make(`turn-${ordinal}`),
  providerThreadId: providerThread.id,
  nodeId: NodeId.make(`node-${ordinal}`),
  runAttemptId: null,
  nativeTurnRef,
  ordinal,
  status: "completed",
  startedAt: DateTime.makeUnsafe(ordinal * 1_000),
  completedAt: DateTime.makeUnsafe(ordinal * 1_000 + 500),
});

const strongRef = (nativeId: string): OrchestrationV2ProviderRef => ({
  driver: OMP_PROVIDER,
  nativeId,
  strength: "strong",
});

describe("OmpAdapterV2", () => {
  it.effect.each([
    {
      name: "a cost cap",
      result: frames.costCapResult,
      usageLimit: undefined,
      expected: { class: "provider_error", code: "cost_cap", resetAt: undefined },
    },
    {
      name: "a quota error",
      result: {
        ...frames.providerErrorResult,
        error: { message: "Rate limit exceeded.", httpStatus: 429, retryable: true },
      },
      usageLimit: { stage: "wrap_up", resetsAtSec: 1_795_600_000, extraUsage: false },
      expected: { class: "usage_limit", code: null, resetAt: "2026-11-25T09:46:40.000Z" },
    },
  ])("fails the turn on $name and classifies usage limits", ({ result, usageLimit, expected }) =>
    Effect.gen(function* () {
      let state: Record<string, unknown> = stateWith({});
      const fake = yield* makeFakeOmpProcess({
        reply: (command) => (command.type === "get_state" ? fakeOmpSuccess(state) : undefined),
      });
      const session = yield* openRuntime(fake);

      yield* startTurn(session);
      const prompt = yield* fake.takeCommand("prompt");
      if (usageLimit !== undefined) state = stateWith({ usageLimit });
      yield* fake.emit({ ...result, id: prompt.id });
      const terminal = yield* session.takeEvent(isTerminal);

      assert.strictEqual(terminal.status, "failed");
      const failure = terminal.failure;
      assert.deepStrictEqual<unknown>(
        { class: failure?.class, code: failure?.code, resetAt: failure?.resetAt },
        expected,
      );
      assert.strictEqual(session.runtime.providerSession.status, "error");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([true, false])(
    "completes once and holds background work until omp settles (sessionSettled=%s)",
    (sessionSettled) =>
      Effect.gen(function* () {
        const fake = yield* makeFakeOmpProcess();
        const session = yield* openRuntime(fake);

        yield* startTurn(session);
        const prompt = yield* fake.takeCommand("prompt");
        const result = { ...frames.yieldedPromptResult, id: prompt.id, sessionSettled };
        yield* fake.emit(result);
        yield* session.takeEvent(isTerminal);
        // Repeats must not complete, settle or reopen the turn.
        yield* fake.emit(result);
        yield* fake.emit(frames.sessionSettled);
        yield* runTurn(session, fake, { ordinal: 2 });

        const terminals = session.seen.filter(isTerminal);
        assert.deepStrictEqual(
          terminals.map((event) => event.status),
          ["completed", "completed"],
        );
        const background = session.seen.flatMap((event, index) =>
          event.type === "background_work.updated" ? [{ pending: event.pending, index }] : [],
        );
        const firstTerminal = session.seen.indexOf(terminals[0]!);
        if (sessionSettled) {
          assert.deepStrictEqual(background, []);
        } else {
          assert.deepStrictEqual(
            background.map((entry) => entry.pending),
            [true, false],
          );
          assert.isBelow(background[0]!.index, firstTerminal);
        }
        assert.isFalse(yield* session.runtime.hasPendingBackgroundWork!);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("ignores a session_settled and a prompt_result that no turn owns", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess();
      const session = yield* openRuntime(fake);

      yield* fake.emit(frames.sessionSettled);
      yield* fake.emit(promptResult("t3-prompt-99", { status: "error" }));
      const terminal = yield* runTurn(session, fake);

      assert.strictEqual(terminal.status, "completed");
      assert.lengthOf(session.seen.filter(isTerminal), 1);
      assert.isFalse(yield* session.runtime.hasPendingBackgroundWork!);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([
    {
      name: "a local command",
      text: "/context",
      before: [frames.contextCommandOutput],
      ack: frames.localCommandAck.data,
      notice: "Context window: 100000 tokens (11% used)",
    },
    {
      name: "an intercepted command",
      text: "/stats",
      before: [],
      ack: undefined,
      notice: "/stats opens a local web dashboard",
    },
  ])("completes $name without a prompt_result", ({ text, before, ack, notice }) =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess({
        reply: (command) =>
          command.type === "prompt" ? fakeOmpSuccess(ack, { before }) : undefined,
      });
      const session = yield* openRuntime(fake);

      yield* startTurn(session, { text });
      const terminal = yield* session.takeEvent(isTerminal);

      assert.strictEqual(terminal.status, "completed");
      const notices = session.seen.flatMap((event) =>
        event.type === "turn_item.updated" && event.turnItem.type === "system_notice"
          ? [event.turnItem.message]
          : [],
      );
      assert.isTrue(notices.some((message) => message.startsWith(notice)));
      assert.isFalse(notices.some((message) => message.includes("\u001b[")));
      assert.lengthOf(commandsOf(fake, "prompt"), ack === undefined ? 0 : 1);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "sends a catalog command named like a terminal built-in and declines command steers",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeOmpProcess({
          startup: [
            {
              type: "available_commands_update",
              commands: [
                { name: "plan", source: "custom" },
                { name: "model", source: "builtin" },
              ],
            },
          ],
        });
        const session = yield* openRuntime(fake);

        yield* startTurn(session, { text: "/plan draft the migration" });
        const prompt = yield* fake.takeCommand("prompt");
        const started = yield* session.takeEvent(isProviderTurnUpdate);
        const steer = (text: string) =>
          session.runtime.steerTurn({
            threadId: THREAD_ID,
            runId: RunId.make(`run:${THREAD_ID}:1`),
            providerThread: session.providerThread,
            providerTurnId: started.providerTurn.id,
            message: {
              messageId: MessageId.make(`message:${THREAD_ID}:steer`),
              text,
              attachments: [],
              createdBy: "user",
              creationSource: "web",
            },
          });
        const declined = yield* Effect.flip(steer("/model openai/gpt-5"));
        yield* steer("/src/index.ts is the entry point");
        const steered = yield* fake.takeCommand("prompt");
        yield* fake.emit(promptResult(steered.id));
        yield* fake.emit(promptResult(prompt.id));
        yield* session.takeEvent(isTerminal);

        assert.include(prompt.message, "/plan draft the migration");
        assert.strictEqual(declined._tag, "ProviderAdapterSteerDeclinedError");
        assert.deepStrictEqual(
          commandsOf(fake, "prompt").map((command) => command.streamingBehavior),
          ["followUp", "steer"],
        );
        assert.include(steered.message, "/src/index.ts");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([
    { tool: "bash", title: frames.bashApproval.title, kind: "command" },
    {
      tool: "write",
      title: "Allow tool: write\nPath: seven.txt\nContent:\nseven\n",
      kind: "file-change",
    },
  ])("reports a $tool approval as a $kind request", ({ tool, title, kind }) =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess();
      const session = yield* openRuntime(fake);
      yield* startTurn(session);
      yield* fake.takeCommand("prompt");

      yield* fake.emit({ ...frames.bashApproval, title, options: ["Approve", "Deny"] });
      const request = yield* session.takeEvent(isRuntimeRequestUpdate);

      assert.strictEqual(request.runtimeRequest.kind, kind);
      assert.deepStrictEqual(request.runtimeRequest.responseCapability, {
        type: "live",
        providerSessionId: SESSION_ID,
        supportsFeedback: true,
      });
      const item = yield* session.takeEvent(isApprovalItemUpdate);
      assert.strictEqual(item.turnItem.title, `Allow ${tool}`);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("asks omp's confirm dialog as a question and replies in omp's shape", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess();
      const session = yield* openRuntime(fake);
      const dialogId = "15dbc451eb016398";

      yield* startTurn(session);
      const prompt = yield* fake.takeCommand("prompt");
      yield* fake.emit({
        type: "extension_ui_request",
        id: dialogId,
        method: "confirm",
        title: "Deploy",
        message: "Deploy to staging?",
        timeout: 60_000,
      });
      const asked = yield* session.takeEvent(isRuntimeRequestUpdate);
      const refused = yield* Effect.flip(
        session.runtime.respondToRuntimeRequest({
          requestId: asked.runtimeRequest.id,
          answers: { [dialogId]: "maybe" },
        }),
      );
      yield* session.runtime.respondToRuntimeRequest({
        requestId: asked.runtimeRequest.id,
        answers: { [dialogId]: "yes" },
      });
      const reply = yield* fake.takeFrame("extension_ui_response");
      yield* fake.emit(promptResult(prompt.id));
      yield* session.takeEvent(isTerminal);

      assert.strictEqual(asked.runtimeRequest.kind, "user_input");
      assert.strictEqual(refused._tag, "ProviderAdapterRuntimeRequestResponseError");
      assert.deepStrictEqual(reply, {
        type: "extension_ui_response",
        id: dialogId,
        confirmed: true,
      });
      assert.deepStrictEqual(runtimeRequestStatuses(session.seen), ["pending", "resolved"]);
      assert.lengthOf(
        fake.frames().filter((frame) => frame.type === "extension_ui_response"),
        1,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("asks a dialog omp raises while starting and configures omp once it is answered", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess({
        startup: [STARTUP_DIALOG],
        reply: (command) =>
          command.type === "negotiate_protocol" ? FAKE_OMP_UNANSWERED : undefined,
      });
      const session = yield* openSession(fake);
      const asked = yield* session.takeEvent(isRuntimeRequestUpdate);
      const held = yield* Effect.flip(session.ensureThread);
      const writtenWhileHeld = fake.commands().map((command) => command.type);
      yield* session.runtime.respondToRuntimeRequest({
        requestId: asked.runtimeRequest.id,
        answers: { [STARTUP_DIALOG.id]: "yes" },
      });
      const reply = yield* fake.takeFrame("extension_ui_response");
      yield* fake.respond(yield* fake.takeCommand("negotiate_protocol"), { protocolVersion: 2 });
      yield* fake.takeCommand("set_host_tools");
      const providerThread = yield* session.ensureThread;

      assert.strictEqual(asked.threadId, THREAD_ID);
      assert.strictEqual(asked.runtimeRequest.kind, "user_input");
      assert.include(
        held._tag === "ProviderAdapterEnsureThreadError" && held.cause instanceof Error
          ? held.cause.message
          : "",
        "waiting for an answer to a dialog it opened while starting",
      );
      assert.deepStrictEqual(writtenWhileHeld, ["negotiate_protocol"]);
      assert.deepStrictEqual(reply, {
        type: "extension_ui_response",
        id: STARTUP_DIALOG.id,
        confirmed: true,
      });
      assert.deepStrictEqual(
        fake
          .commands()
          .slice(0, 6)
          .map((command) => command.type),
        [
          "negotiate_protocol",
          "set_event_filter",
          "set_subagent_subscription",
          "set_ask_dialog",
          "set_interrupt_mode",
          "set_host_tools",
        ],
      );
      assert.strictEqual(providerThread.nativeThreadRef?.nativeId, FAKE_OMP_SESSION_FILE);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("reads the active branch's user and assistant text as the thread snapshot", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess({
        reply: (command) =>
          command.type === "get_messages"
            ? fakeOmpSuccess({
                messages: [
                  { role: "user", content: "Rename the module", timestamp: 1_000 },
                  { role: "toolResult", content: [{ type: "text", text: "ok" }] },
                  {
                    role: "assistant",
                    content: [
                      { type: "thinking", thinking: "hidden" },
                      { type: "text", text: "Renamed." },
                    ],
                    timestamp: 2_000,
                  },
                ],
              })
            : undefined,
      });
      const session = yield* openRuntime(fake);

      const snapshot = yield* session.runtime.readThreadSnapshot!({
        providerThread: session.providerThread,
      });

      assert.deepStrictEqual(
        snapshot.messages.map((message) => [message.role, message.text]),
        [
          ["user", "Rename the module"],
          ["assistant", "Renamed."],
        ],
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("waits for the prompt's ack before aborting an early Stop", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess({
        reply: (command) => (command.type === "prompt" ? FAKE_OMP_UNANSWERED : undefined),
      });
      const session = yield* openRuntime(fake);
      yield* startTurn(session);
      const prompt = yield* fake.takeCommand("prompt");
      const providerTurnId = (yield* session.takeEvent(isProviderTurnUpdate)).providerTurn.id;

      const stop = yield* session.runtime
        .interruptTurn({ providerThread: session.providerThread, providerTurnId })
        .pipe(Effect.forkChild({ startImmediately: true }));
      // Writes reach omp in order, so once this rename is answered an earlier abort would show.
      yield* session.runtime.applyThreadTitle!({
        providerThread: session.providerThread,
        title: "write barrier",
      });
      assert.lengthOf(commandsOf(fake, "abort"), 0);
      yield* fake.respond(prompt);
      yield* fake.takeCommand("abort");
      yield* Fiber.join(stop);
      yield* fake.emit(promptResult(prompt.id, { status: "aborted" }));

      assert.strictEqual((yield* session.takeEvent(isTerminal)).status, "interrupted");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("reports a command omp aborts on Stop with restart as interrupted", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess({
        reply: (command) => (command.type === "abort" ? FAKE_OMP_UNANSWERED : undefined),
      });
      const session = yield* openRuntime(fake);
      yield* startTurn(session);
      yield* fake.takeCommand("prompt");
      const providerTurnId = (yield* session.takeEvent(isProviderTurnUpdate)).providerTurn.id;
      yield* fake.emit(frames.bashStart);
      const stop = yield* session.runtime
        .interruptTurn({
          providerThread: session.providerThread,
          providerTurnId,
          requestRuntimeRestart: true,
        })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const abort = yield* fake.takeCommand("abort");
      yield* fake.emit({
        type: "tool_execution_end",
        toolCallId: frames.bashStart.toolCallId,
        toolName: "bash",
        result: { content: [{ type: "text", text: "Command aborted" }], details: {} },
        isError: true,
      });
      yield* fake.respond(abort);
      yield* Fiber.join(stop);
      yield* session.takeEvent(isTerminal);

      assert.strictEqual(
        lastToolRowStatus(session.seen, frames.bashStart.toolCallId),
        "interrupted",
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("records the stopped turn's user entry before Stop ends omp", () =>
    Effect.gen(function* () {
      let branch: ReadonlyArray<{ entryId: string; text: string }> = [];
      const fake = yield* makeFakeOmpProcess({
        reply: (command) =>
          command.type === "get_branch_messages" ? fakeOmpSuccess({ messages: branch }) : undefined,
      });
      const session = yield* openRuntime(fake);
      branch = [{ entryId: FIRST_USER_ENTRY, text: "Hello omp" }];
      yield* runTurn(session, fake);
      yield* startTurn(session, { ordinal: 2, text: "Second" });
      yield* fake.takeCommand("prompt");
      const started = yield* session.takeEvent(isProviderTurnUpdate);
      branch = [...branch, { entryId: SECOND_USER_ENTRY, text: "Second" }];

      yield* session.runtime.interruptTurn({
        providerThread: session.providerThread,
        providerTurnId: started.providerTurn.id,
        requestRuntimeRestart: true,
      });
      const terminal = yield* session.takeEvent(isTerminal);

      const stopped = session.seen
        .filter(isProviderTurnUpdate)
        .filter((event) => event.providerTurn.id === started.providerTurn.id)
        .at(-1);
      assert.strictEqual(terminal.status, "interrupted");
      assert.deepStrictEqual(stopped?.providerTurn.nativeTurnRef, strongRef(SECOND_USER_ENTRY));
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([false, true])(
    "takes the turn's prompts back from omp's queues, then aborts so a running command stops (restart %s)",
    (restart) =>
      Effect.gen(function* () {
        const fake = yield* makeFakeOmpProcess();
        const session = yield* openRuntime(fake);
        yield* startTurn(session);
        const prompt = yield* fake.takeCommand("prompt");
        const started = yield* session.takeEvent(isProviderTurnUpdate);
        yield* fake.emit(userMessageStart("Hello omp"));
        yield* fake.emit(frames.bashStart);
        yield* session.runtime.steerTurn({
          threadId: THREAD_ID,
          runId: RunId.make(`run:${THREAD_ID}:1`),
          providerThread: session.providerThread,
          providerTurnId: started.providerTurn.id,
          message: {
            messageId: MessageId.make(`message:${THREAD_ID}:steer`),
            text: "Change course",
            attachments: [],
            createdBy: "user",
            creationSource: "web",
          },
        });
        const steer = yield* fake.takeCommand("prompt");
        yield* session.takeEvent(isSteerUpdate);

        yield* session.runtime.interruptTurn({
          providerThread: session.providerThread,
          providerTurnId: started.providerTurn.id,
          ...(restart ? { requestRuntimeRestart: true } : {}),
        });
        if (!restart) {
          yield* fake.emit(promptResult(steer.id, { status: "aborted" }));
          yield* fake.emit(promptResult(prompt.id, { status: "aborted" }));
        }
        const terminal = yield* session.takeEvent(isTerminal);

        assert.strictEqual(terminal.status, "interrupted");
        assert.deepStrictEqual(
          fake
            .commands()
            .filter((command) => STOP_COMMANDS.has(command.type))
            .map((command) => command.type),
          ["remove_queued_message", "remove_queued_message", "abort"],
        );
        assert.deepStrictEqual(
          commandsOf(fake, "remove_queued_message").map((command) => [
            command.message,
            command.queue,
          ]),
          [
            [prompt.message, "followUp"],
            ["Change course", "steering"],
          ],
        );
        assert.deepStrictEqual(
          session.seen.filter(isSteerUpdate).map((event) => event.status),
          ["pending", "cancelled"],
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "queues a turn's prompt behind a run omp started itself and completes it on its result",
    () =>
      Effect.gen(function* () {
        const wakes =
          yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
        const fake = yield* makeFakeOmpProcess();
        const session = yield* openRuntime(fake, {
          continuationRequests: {
            offer: (request) => Queue.offer(wakes, request).pipe(Effect.asVoid),
          },
        });
        const text = frames.followUpUserMessageStart.message.content[0]!.text;
        yield* fake.emit(frames.wakeAgentStart);
        yield* Queue.take(wakes);
        yield* fake.emit(frames.goalContinuationStart);
        yield* emitAll(fake, assistantReply("ok"));

        yield* startTurn(session, { text });
        const prompt = yield* fake.takeCommand("prompt");
        yield* fake.emit(frames.followUpQueued);
        yield* fake.emit(frames.followUpUserMessageStart);
        yield* emitAll(fake, assistantReply("three"));
        yield* fake.emit({ type: "agent_end", messages: [] });
        yield* fake.emit(promptResult(prompt.id));
        const terminal = yield* session.takeEvent(isTerminal);

        assert.strictEqual(prompt.streamingBehavior, "followUp");
        assert.strictEqual(prompt.message, text);
        assert.strictEqual(terminal.status, "completed");
        assert.deepStrictEqual(assistantTexts(session.seen, terminal.providerTurnId), [
          "ok",
          "three",
        ]);
        assert.lengthOf(commandsOf(fake, "prompt"), 1);
        assert.lengthOf(session.seen.filter(isTerminal), 1);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("takes a turn's prompt back from omp's queue before aborting the run it waits on", () =>
    Effect.gen(function* () {
      const wakes =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      const fake = yield* makeFakeOmpProcess({
        reply: (command) =>
          command.type === "remove_queued_message"
            ? fakeOmpSuccess(frames.removeQueuedMessageResult)
            : undefined,
      });
      const session = yield* openRuntime(fake, {
        continuationRequests: {
          offer: (request) => Queue.offer(wakes, request).pipe(Effect.asVoid),
        },
      });
      const text = frames.followUpUserMessageStart.message.content[0]!.text;
      yield* fake.emit(frames.wakeAgentStart);
      yield* Queue.take(wakes);
      yield* startTurn(session, { text });
      const prompt = yield* fake.takeCommand("prompt");
      const started = yield* session.takeEvent(isProviderTurnUpdate);
      yield* fake.emit(frames.followUpQueued);

      yield* session.runtime.interruptTurn({
        providerThread: session.providerThread,
        providerTurnId: started.providerTurn.id,
      });
      yield* fake.emit(promptResult(prompt.id, { status: "aborted" }));
      const terminal = yield* session.takeEvent(isTerminal);

      assert.strictEqual(terminal.status, "interrupted");
      assert.deepStrictEqual(
        fake
          .commands()
          .filter((command) => STOP_COMMANDS.has(command.type))
          .map((command) => command.type),
        ["remove_queued_message", "abort"],
      );
      assert.deepInclude(commandsOf(fake, "remove_queued_message")[0], {
        message: text,
        queue: "followUp",
      });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("withdraws input omp hands back to a stopped run when it resumes the run for it", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess();
      const session = yield* openRuntime(fake);
      yield* startTurn(session);
      const prompt = yield* fake.takeCommand("prompt");
      const started = yield* session.takeEvent(isProviderTurnUpdate);
      yield* fake.emit(frames.wakeAgentStart);
      yield* fake.emit(userMessageStart("Hello omp"));
      // A second loop of the same prompt, such as a todo reminder's, is not a resumed run.
      yield* fake.emit(frames.wakeAgentStart);
      yield* emitAll(fake, assistantReply("Working"));
      yield* session.takeEvent(isAssistantItemUpdate);

      yield* session.runtime.interruptTurn({
        providerThread: session.providerThread,
        providerTurnId: started.providerTurn.id,
      });
      yield* fake.emit(frames.wakeAgentStart);
      yield* fake.takeCommand("abort_and_restore_queue");
      yield* fake.emit(promptResult(prompt.id, { status: "aborted" }));
      const terminal = yield* session.takeEvent(isTerminal);

      assert.strictEqual(terminal.status, "interrupted");
      assert.deepStrictEqual(
        fake
          .commands()
          .filter((command) => STOP_COMMANDS.has(command.type))
          .map((command) => command.type),
        ["remove_queued_message", "abort", "abort_and_restore_queue"],
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "cancels every subagent on Stop, an earlier turn's too, and ends their rows interrupted",
    () =>
      Effect.gen(function* () {
        const stoppedTask = "toolu_stopped_task";
        const fake = yield* makeFakeOmpProcess({
          reply: (command) =>
            subagentReply(command, [
              frames.runningSubagent,
              { ...frames.runningSubagent, id: "Quick", parentToolCallId: stoppedTask },
              { ...frames.runningSubagent, id: "Unparented", parentToolCallId: undefined },
            ]),
        });
        const session = yield* openRuntime(fake);
        yield* startTurn(session);
        const first = yield* fake.takeCommand("prompt");
        yield* fake.emit(frames.taskStart);
        yield* fake.emit(frames.taskEnd);
        yield* fake.emit({ ...frames.yieldedPromptResult, id: first.id });
        const earlierTurnId = (yield* session.takeEvent(isTerminal)).providerTurnId;
        yield* startTurn(session, { ordinal: 2, text: "Second" });
        const second = yield* fake.takeCommand("prompt");
        const started = yield* session.takeEvent(isProviderTurnUpdate);
        yield* fake.emit(userMessageStart("Second"));
        yield* fake.emit({ ...frames.taskStart, toolCallId: stoppedTask });
        yield* fake.emit({ ...frames.taskEnd, toolCallId: stoppedTask });

        const stop = yield* session.runtime
          .interruptTurn({
            providerThread: session.providerThread,
            providerTurnId: started.providerTurn.id,
          })
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* fake.takeCommand("abort");
        // The abort latches omp's user interrupt, so it settles while its children still run.
        yield* fake.emit(promptResult(second.id, { status: "aborted" }));
        yield* fake.emit(frames.sessionSettled);
        yield* Fiber.join(stop);
        yield* session.takeEvent(
          (event): event is AdapterEvent =>
            isBackgroundRelease(event) &&
            event.type === "background_work.updated" &&
            event.providerTurnId === earlierTurnId,
        );

        const types = fake.commands().map((command) => command.type);
        assert.isBelow(types.indexOf("abort"), types.indexOf("get_subagents"));
        assert.deepStrictEqual(
          commandsOf(fake, "cancel_subagent").map((command) => command.subagentId),
          [frames.runningSubagent.id, "Quick", "Unparented"],
        );
        assert.strictEqual(
          lastToolRowStatus(session.seen, frames.taskEnd.toolCallId),
          "interrupted",
        );
        assert.isFalse(yield* session.runtime.hasPendingBackgroundWork!);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "sends every subagent's cancel at once, so omp cancels them all after Stop stops waiting",
    () =>
      Effect.gen(function* () {
        const children = [frames.runningSubagent.id, "Quick", "Slow"];
        const fake = yield* makeFakeOmpProcess({
          reply: (command) =>
            command.type === "cancel_subagent"
              ? FAKE_OMP_UNANSWERED
              : subagentReply(
                  command,
                  children.map((id) => ({ ...frames.runningSubagent, id })),
                ),
        });
        const session = yield* openRuntime(fake);
        yield* startTurn(session);
        const prompt = yield* fake.takeCommand("prompt");
        const started = yield* session.takeEvent(isProviderTurnUpdate);

        const stop = yield* session.runtime
          .interruptTurn({
            providerThread: session.providerThread,
            providerTurnId: started.providerTurn.id,
          })
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* fake.takeCommand("cancel_subagent");
        // Writes reach omp in order, so once this rename is answered every cancel sent shows.
        yield* session.runtime.applyThreadTitle!({
          providerThread: session.providerThread,
          title: "write barrier",
        });
        const sentBeforeAnyAnswer = commandsOf(fake, "cancel_subagent").length;
        // omp answers each cancel about 1.3 s after it (STG/03:117-132); none answers in time.
        yield* TestClock.adjust(Duration.seconds(OMP_TEST_STOP_SECONDS));
        yield* Fiber.join(stop);
        yield* fake.emit(promptResult(prompt.id, { status: "aborted" }));

        assert.strictEqual((yield* session.takeEvent(isTerminal)).status, "interrupted");
        assert.strictEqual(sentBeforeAnyAnswer, children.length);
        assert.deepStrictEqual(
          commandsOf(fake, "cancel_subagent").map((command) => command.subagentId),
          children,
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("takes a steer back from omp's queue only once omp acknowledged it", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess({
        reply: (command) =>
          command.type === "prompt" && command.streamingBehavior === "steer"
            ? FAKE_OMP_UNANSWERED
            : undefined,
      });
      const session = yield* openRuntime(fake);
      yield* startTurn(session);
      const prompt = yield* fake.takeCommand("prompt");
      const started = yield* session.takeEvent(isProviderTurnUpdate);
      yield* session.runtime.steerTurn(
        steerInput(session, started.providerTurn.id, "Change course"),
      );
      const steer = yield* fake.takeCommand("prompt");

      const stop = yield* session.runtime
        .interruptTurn({
          providerThread: session.providerThread,
          providerTurnId: started.providerTurn.id,
        })
        .pipe(Effect.forkChild({ startImmediately: true }));
      // omp admits a steer in the background, so a removal sent before its ack finds nothing.
      yield* session.runtime.applyThreadTitle!({
        providerThread: session.providerThread,
        title: "write barrier",
      });
      assert.lengthOf(commandsOf(fake, "remove_queued_message"), 0);
      yield* fake.respond(steer);
      yield* fake.takeCommand("abort");
      yield* Fiber.join(stop);
      yield* fake.emit(promptResult(steer.id, { status: "aborted" }));
      yield* fake.emit(promptResult(prompt.id, { status: "aborted" }));

      assert.strictEqual((yield* session.takeEvent(isTerminal)).status, "interrupted");
      assert.deepStrictEqual(
        commandsOf(fake, "remove_queued_message").map((command) => command.queue),
        ["followUp", "steering"],
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("ends an unresponsive omp on Stop with restart well inside the interrupt timeout", () =>
    Effect.gen(function* () {
      let answering = true;
      const fake = yield* makeFakeOmpProcess({
        reply: () => (answering ? undefined : FAKE_OMP_UNANSWERED),
      });
      const session = yield* openRuntime(fake);
      yield* startTurn(session);
      yield* fake.takeCommand("prompt");
      const started = yield* session.takeEvent(isProviderTurnUpdate);
      answering = false;
      for (const text of ["First steer", "Second steer"]) {
        yield* session.runtime.steerTurn(steerInput(session, started.providerTurn.id, text));
        yield* fake.takeCommand("prompt");
      }

      const stop = yield* session.runtime
        .interruptTurn({
          providerThread: session.providerThread,
          providerTurnId: started.providerTurn.id,
          requestRuntimeRestart: true,
        })
        .pipe(Effect.forkChild({ startImmediately: true }));
      let seconds = 0;
      // ProviderSessionManager gives up on an interrupt after 15 s, before omp would be ended.
      while (stop.pollUnsafe() === undefined && seconds < 15) {
        yield* TestClock.adjust(Duration.seconds(1));
        seconds += 1;
      }

      assert.isDefined(stop.pollUnsafe());
      assert.isAtMost(seconds, OMP_TEST_STOP_SECONDS);
      assert.strictEqual((yield* session.takeEvent(isTerminal)).status, "interrupted");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("withdraws input omp resumed a run on before Stop with restart ends omp", () =>
    Effect.gen(function* () {
      let aborted = false;
      const fake = yield* makeFakeOmpProcess({
        reply: (command) => {
          if (command.type === "abort") aborted = true;
          // A live-steered message the abort hands back resumes the run (agent.ts:1590-1600).
          return command.type === "get_state" && aborted
            ? fakeOmpSuccess(stateWith({ isStreaming: true, isSettled: false }))
            : undefined;
        },
      });
      const session = yield* openRuntime(fake);
      yield* startTurn(session);
      yield* fake.takeCommand("prompt");
      const started = yield* session.takeEvent(isProviderTurnUpdate);
      yield* fake.emit(userMessageStart("Hello omp"));

      yield* session.runtime.interruptTurn({
        providerThread: session.providerThread,
        providerTurnId: started.providerTurn.id,
        requestRuntimeRestart: true,
      });
      yield* session.takeEvent(isTerminal);

      const types = fake.commands().map((command) => command.type);
      assert.deepStrictEqual(types.slice(types.indexOf("abort")), [
        "abort",
        "get_state",
        "get_branch_messages",
        "abort_and_restore_queue",
        "get_subagents",
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("cancels every subagent before Stop with restart ends omp", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess({
        reply: (command) => subagentReply(command, [frames.runningSubagent]),
      });
      const session = yield* openRuntime(fake);
      yield* startTurn(session);
      yield* fake.takeCommand("prompt");
      const started = yield* session.takeEvent(isProviderTurnUpdate);
      yield* fake.emit(userMessageStart("Hello omp"));
      yield* fake.emit(frames.taskStart);
      yield* fake.emit(frames.taskEnd);

      yield* session.runtime.interruptTurn({
        providerThread: session.providerThread,
        providerTurnId: started.providerTurn.id,
        requestRuntimeRestart: true,
      });
      const terminal = yield* session.takeEvent(isTerminal);

      yield* session.takeEvent(isBackgroundRelease);
      assert.strictEqual(terminal.status, "interrupted");
      const types = fake.commands().map((command) => command.type);
      // The stopped turn's entry is read first; cancelling children ends right before omp does.
      assert.deepStrictEqual(types.slice(types.indexOf("abort")), [
        "abort",
        "get_state",
        "get_branch_messages",
        "get_subagents",
        "cancel_subagent",
      ]);
      assert.deepStrictEqual(
        commandsOf(fake, "cancel_subagent").map((command) => command.subagentId),
        [frames.runningSubagent.id],
      );
      assert.strictEqual(lastToolRowStatus(session.seen, frames.taskEnd.toolCallId), "interrupted");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("cancels subagents before ending omp for work a settled turn left running", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess({
        reply: (command) => subagentReply(command, [frames.runningSubagent]),
      });
      const session = yield* openRuntime(fake);
      yield* startTurn(session);
      const prompt = yield* fake.takeCommand("prompt");
      const started = yield* session.takeEvent(isProviderTurnUpdate);
      yield* fake.emit(frames.taskStart);
      yield* fake.emit(frames.taskEnd);
      yield* fake.emit({ ...frames.yieldedPromptResult, id: prompt.id });
      yield* session.takeEvent(isTerminal);
      assert.isTrue(yield* session.runtime.hasPendingBackgroundWork!);

      yield* session.runtime.interruptTurn({
        providerThread: session.providerThread,
        providerTurnId: started.providerTurn.id,
        requestRuntimeRestart: true,
      });
      yield* session.takeEvent(isSessionStopped);

      assert.deepStrictEqual(
        commandsOf(fake, "cancel_subagent").map((command) => command.subagentId),
        [frames.runningSubagent.id],
      );
      assert.strictEqual(lastToolRowStatus(session.seen, frames.taskEnd.toolCallId), "interrupted");
      assert.isFalse(yield* session.runtime.hasPendingBackgroundWork!);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "aborts the run omp starts with a cancelled subagent's result while Stop ends a settled turn's work",
    () =>
      Effect.gen(function* () {
        let cancels = 0;
        const fake = yield* makeFakeOmpProcess({
          reply: (command) => {
            if (command.type !== "cancel_subagent") {
              return subagentReply(command, [
                frames.runningSubagent,
                { ...frames.runningSubagent, id: "Quick" },
              ]);
            }
            cancels += 1;
            // An idle omp starts a run to deliver the cancelled child's result (STG/03:126-140).
            return cancels === 1
              ? fakeOmpSuccess(frames.cancelSubagentResult, {
                  after: [frames.wakeAgentStart, ...cancelledSubagentResult("Napper")],
                })
              : FAKE_OMP_UNANSWERED;
          },
        });
        const session = yield* openRuntime(fake);
        yield* startTurn(session);
        const prompt = yield* fake.takeCommand("prompt");
        const started = yield* session.takeEvent(isProviderTurnUpdate);
        yield* fake.emit(frames.taskStart);
        yield* fake.emit(frames.taskEnd);
        yield* fake.emit({ ...frames.yieldedPromptResult, id: prompt.id });
        yield* session.takeEvent(isTerminal);

        const stop = yield* session.runtime
          .interruptTurn({
            providerThread: session.providerThread,
            providerTurnId: started.providerTurn.id,
            requestRuntimeRestart: true,
          })
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* fake.takeCommand("cancel_subagent");
        const second = yield* fake.takeCommand("cancel_subagent");
        // omp answers a cancel about 1.3 s after it was sent, so the first child's run starts first.
        yield* fake.takeCommand("abort");
        const runAbort = yield* fake.takeCommand("abort");
        // The aborted run settles omp while the second child is still being cancelled.
        yield* emitAll(fake, [{ type: "agent_end", messages: [] }, frames.sessionSettled]);
        yield* session.takeEvent(isBackgroundRelease);
        yield* fake.respond(second, frames.cancelSubagentResult);
        yield* Fiber.join(stop);
        yield* Effect.flip(session.takeEvent(matchesNothing));

        assert.match(runAbort.id, /^t3-abort-/);
        assert.deepStrictEqual(sessionStatuses(session.seen).slice(-1), ["stopped"]);
        assert.notInclude(sessionStatuses(session.seen), "error");
        assert.strictEqual(
          lastToolRowStatus(session.seen, frames.taskEnd.toolCallId),
          "interrupted",
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("stops a wake run omp already started before Stop cancels its subagents", () =>
    Effect.gen(function* () {
      const wakes =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      const fake = yield* makeFakeOmpProcess({
        reply: (command) => subagentReply(command, [frames.runningSubagent]),
      });
      const session = yield* openRuntime(fake, {
        continuationRequests: {
          offer: (request) => Queue.offer(wakes, request).pipe(Effect.asVoid),
        },
      });
      yield* fake.emit(frames.wakeAgentStart);
      yield* Queue.take(wakes);

      yield* session.runtime.interruptTurn({
        providerThread: session.providerThread,
        providerTurnId: ProviderTurnId.make("turn-unadopted-wake"),
        requestRuntimeRestart: true,
      });
      yield* session.takeEvent(isSessionStopped);

      const types = fake.commands().map((command) => command.type);
      assert.deepStrictEqual(types.slice(types.indexOf("abort")), [
        "abort",
        "get_subagents",
        "cancel_subagent",
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("streams truncated bash output as deltas and leaves the row to the tool's end", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess();
      const session = yield* openRuntime(fake);
      const toolCallId = frames.bashStart.toolCallId;
      const bashUpdate = (text: string) => ({
        type: "tool_execution_update",
        toolCallId,
        toolName: "bash",
        partialResult: { content: [{ type: "text", text }] },
      });
      const isToolRowUpdate = (event: AdapterEvent): event is AdapterEvent =>
        event.type === "turn_item.updated" && event.turnItem.nativeItemRef?.nativeId === toolCallId;
      const isOutputDelta = (
        event: AdapterEvent,
      ): event is Extract<AdapterEvent, { readonly type: "command_output.delta" }> =>
        event.type === "command_output.delta";

      yield* startTurn(session);
      const prompt = yield* fake.takeCommand("prompt");
      yield* fake.emit(frames.bashStart);
      yield* fake.emit(bashUpdate("1\n2\n3\n4\n"));
      yield* fake.emit(bashUpdate("1\n2\n[…2ln elided…]\n5\n6\n"));
      yield* fake.emit(bashUpdate("1\n2\n[…3ln elided…]\n6\n7\n"));
      yield* fake.emit({
        type: "tool_execution_end",
        toolCallId,
        toolName: "bash",
        result: { content: [{ type: "text", text: "1\n2\n[…3ln elided…]\n6\n7\n" }], details: {} },
      });
      yield* fake.emit(promptResult(prompt.id));
      yield* session.takeEvent(isTerminal);

      assert.deepStrictEqual(
        session.seen.filter(isOutputDelta).map((event) => event.chunk),
        ["1\n2\n3\n4\n", "[…2ln elided…]\n5\n6\n", "7\n"],
      );
      assert.deepStrictEqual(
        session.seen
          .filter(isToolRowUpdate)
          .map((event) => (event.type === "turn_item.updated" ? event.turnItem.status : undefined)),
        ["running", "completed"],
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("shows omp's todo snapshots as the turn's todo list instead of tool rows", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess();
      const session = yield* openRuntime(fake);
      const calls = Object.values(todoFrames.createCompleteDrop);

      yield* startTurn(session);
      const prompt = yield* fake.takeCommand("prompt");
      yield* emitAll(fake, calls);
      yield* fake.emit(promptResult(prompt.id));
      yield* session.takeEvent(isTerminal);

      const callIds = new Set(calls.map((call) => call.toolCallId));
      assert.isFalse(session.seen.some((event) => callIds.has(nativeIdOf(event) ?? "")));
      assert.deepStrictEqual(
        session.seen.flatMap(todoProjectionStep),
        calls.flatMap((call) => (call.type === "tool_execution_end" ? TODO_PROJECTION : [])),
      );
      const plans = todoPlans(session.seen);
      assert.deepStrictEqual(
        plans.map((plan) => plan.status),
        ["active", "active", "active", "completed"],
      );
      const items = todoListItems(session.seen);
      assert.strictEqual(new Set(plans.map((plan) => plan.id)).size, 1);
      assert.isTrue(items.every((item) => item.planId === plans[0]?.id));
      assert.strictEqual(new Set(items.map((item) => item.id)).size, 1);
      assert.deepStrictEqual(
        items.at(-1)?.steps.map((step) => [step.text, step.status]),
        [
          ["Alpha", "completed"],
          ["Beta", "completed"],
          ["Gamma", "completed"],
        ],
      );
      assert.isTrue(session.runtime.providerSession.capabilities.planning.emitsTodoList);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "supersedes the last turn's todo plan and skips views, failed calls and reminders",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeOmpProcess();
        const session = yield* openRuntime(fake);
        const { initStart, initEnd } = todoFrames.createCompleteDrop;
        yield* startTurn(session);
        const first = yield* fake.takeCommand("prompt");
        yield* emitAll(fake, [initStart, initEnd]);
        yield* fake.emit(promptResult(first.id));
        yield* session.takeEvent(isTerminal);
        const [firstPlan] = todoPlans(session.seen);
        const [firstItem] = todoListItems(session.seen);
        const firstTurnEvents = session.seen.length;

        yield* startTurn(session, { ordinal: 2 });
        const second = yield* fake.takeCommand("prompt");
        yield* emitAll(fake, Object.values(todoFrames.twoPhases));
        yield* fake.emit(promptResult(second.id));
        yield* session.takeEvent(isTerminal);

        const secondTurn = session.seen.slice(firstTurnEvents);
        assert.deepStrictEqual(
          todoPlans(secondTurn).map((plan) => [plan.id === firstPlan?.id, plan.status]),
          [
            [true, "superseded"],
            [false, "active"],
            [false, "active"],
          ],
        );
        const items = todoListItems(secondTurn);
        assert.lengthOf(items, 2);
        assert.notStrictEqual(items[0]?.id, firstItem?.id);
        assert.strictEqual(items.at(-1)?.explanation, "Build → Ship");
        assert.deepStrictEqual(
          items.at(-1)?.steps.map((step) => step.status),
          ["completed", "running", "pending"],
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("shows no row for a todo call Stop cut off before its snapshot", () =>
    Effect.gen(function* () {
      const { initStart } = todoFrames.createCompleteDrop;
      const fake = yield* makeFakeOmpProcess();
      const session = yield* openRuntime(fake);
      yield* startTurn(session);
      const prompt = yield* fake.takeCommand("prompt");
      const started = yield* session.takeEvent(isProviderTurnUpdate);
      yield* emitAll(fake, [frames.bashStart, initStart]);

      yield* session.runtime.interruptTurn({
        providerThread: session.providerThread,
        providerTurnId: started.providerTurn.id,
      });
      yield* fake.emit(promptResult(prompt.id, { status: "aborted" }));
      const terminal = yield* session.takeEvent(isTerminal);

      assert.strictEqual(terminal.status, "interrupted");
      assert.strictEqual(
        lastToolRowStatus(session.seen, frames.bashStart.toolCallId),
        "interrupted",
      );
      assert.isUndefined(lastToolRowStatus(session.seen, initStart.toolCallId));
      assert.lengthOf(todoListItems(session.seen), 0);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "shows a task call's children as subagent rows under it and interrupts the ones Stop ends",
    () =>
      Effect.gen(function* () {
        const { parentAbort } = subagentFrames;
        const taskCallId = parentAbort.taskStart.toolCallId;
        const fake = yield* makeFakeOmpProcess({
          reply: (command) =>
            subagentReply(command, [{ id: "Napper", parentToolCallId: taskCallId }]),
        });
        const session = yield* openRuntime(fake);
        yield* startTurn(session);
        const prompt = yield* fake.takeCommand("prompt");
        const started = yield* session.takeEvent(isProviderTurnUpdate);
        // omp reports both children before the task call that spawns them (P2:62-79).
        yield* emitAll(fake, [
          parentAbort.napperStarted,
          parentAbort.napperRunning,
          parentAbort.quickStarted,
          parentAbort.quickRunning,
          parentAbort.taskStart,
          parentAbort.taskEnd,
          parentAbort.napperDescribed,
          parentAbort.quickDescribed,
          parentAbort.waitStart,
          parentAbort.napperBash,
          parentAbort.quickCompletedProgress,
          parentAbort.quickCompleted,
          parentAbort.quickJobUpdate,
        ]);
        yield* session.takeEvent(isSubagentUpdateOf("Quick"));
        yield* session.takeEvent(isSubagentUpdateOf("Quick"));

        yield* session.runtime.interruptTurn({
          providerThread: session.providerThread,
          providerTurnId: started.providerTurn.id,
        });
        yield* emitAll(fake, [
          parentAbort.waitAborted,
          parentAbort.quickResultStart,
          parentAbort.quickResultEnd,
          promptResult(prompt.id, { status: "aborted" }),
          frames.sessionSettled,
        ]);
        const terminal = yield* session.takeEvent(isTerminal);
        yield* session.takeEvent(isBackgroundRelease);

        // Clients order rows by arrival, so the call's row must arrive before its children's.
        const firstTaskRow = session.seen.findIndex(
          (event) => event.type === "turn_item.updated" && nativeIdOf(event) === taskCallId,
        );
        const firstSubagentRow = session.seen.findIndex(
          (event) => event.type === "turn_item.updated" && event.turnItem.type === "subagent",
        );
        const underTaskCall = {
          origin: "provider_native" as const,
          childThreadId: null,
          runId: RunId.make(`run:${THREAD_ID}:1`),
          parentNodeId: nodeIdOf(session.seen, taskCallId),
        };
        assert.strictEqual(terminal.status, "interrupted");
        assert.deepStrictEqual(
          commandsOf(fake, "set_subagent_subscription").map((command) => command.level),
          ["progress"],
        );
        assert.deepStrictEqual(subagentRow(lastSubagent(session.seen, "Napper")), {
          ...underTaskCall,
          status: "interrupted",
          title: "Run sleep command and report its output",
          result: null,
        });
        assert.deepStrictEqual(subagentRow(lastSubagent(session.seen, "Quick")), {
          ...underTaskCall,
          status: "completed",
          title: "Run echo quick-done command and report output",
          result: "quick-done",
        });
        assert.strictEqual(
          lastSubagent(session.seen, "Quick")?.prompt,
          parentAbort.quickRunning.payload.assignment,
        );
        assert.deepStrictEqual(subagentNodeStatuses(session.seen), [
          ["Napper", "running"],
          ["Quick", "running"],
          ["Quick", "completed"],
          ["Napper", "interrupted"],
        ]);
        assert.isAtLeast(firstTaskRow, 0);
        assert.isBelow(firstTaskRow, firstSubagentRow);
        assert.strictEqual(lastToolRowStatus(session.seen, taskCallId), "interrupted");
        assert.isTrue(session.runtime.providerSession.capabilities.subagents.supportsSubagents);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("coalesces a child's progress to one update per half second and keeps the latest", () =>
    Effect.gen(function* () {
      const { parentAbort } = subagentFrames;
      const fake = yield* makeFakeOmpProcess();
      const session = yield* openRuntime(fake);
      yield* startTurn(session);
      yield* fake.takeCommand("prompt");
      yield* emitAll(fake, [
        parentAbort.napperStarted,
        parentAbort.napperRunning,
        parentAbort.napperDescribed,
        parentAbort.napperBash,
        parentAbort.napperBashAgain,
        parentAbort.taskStart,
      ]);
      // The task call's start shows omp handled every progress frame sent before it.
      yield* session.takeEvent(
        (event): event is AdapterEvent =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "dynamic_tool" &&
          JSON.stringify(event.turnItem.input) === JSON.stringify(parentAbort.taskStart.args),
      );
      const beforeFlush = subagentUpdates(session.seen, "Napper");

      yield* TestClock.adjust(Duration.millis(500));
      const flushed = (yield* session.takeEvent(isSubagentUpdateOf("Napper"))).subagent;
      yield* TestClock.adjust(Duration.seconds(5));
      yield* fake.emit(parentAbort.napperBashAgain);
      yield* emitAll(fake, [parentAbort.taskEnd]);
      yield* session.takeEvent(
        (event): event is AdapterEvent =>
          event.type === "turn_item.updated" &&
          nativeIdOf(event) === parentAbort.taskEnd.toolCallId &&
          event.turnItem.status === "running" &&
          event.turnItem.completedAt === null &&
          event.turnItem.type === "dynamic_tool" &&
          event.turnItem.output !== undefined,
      );

      assert.deepStrictEqual(
        beforeFlush.map((subagent) => [subagent.title, subagent.prompt, subagent.progress]),
        [["Napper", "", undefined]],
      );
      assert.deepStrictEqual(
        [flushed.title, flushed.prompt, flushed.progress],
        [
          "Run sleep command and report its output",
          parentAbort.napperRunning.payload.assignment,
          "bash: sleep 15; echo nap-done",
        ],
      );
      assert.lengthOf(subagentUpdates(session.seen, "Napper"), 2);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("takes a waited child's result from the wait call's end", () =>
    Effect.gen(function* () {
      const { waited } = subagentFrames;
      const fake = yield* makeFakeOmpProcess();
      const session = yield* openRuntime(fake);
      yield* startTurn(session);
      const prompt = yield* fake.takeCommand("prompt");
      yield* emitAll(fake, [
        waited.counterStarted,
        waited.counterRunning,
        waited.taskStart,
        waited.taskEnd,
        waited.waitStart,
        waited.counterCompleted,
        waited.taskCompletion,
        waited.waitEnd,
      ]);
      yield* fake.emit(promptResult(prompt.id));
      yield* session.takeEvent(isTerminal);

      const counter = lastSubagent(session.seen, "Counter");
      assert.deepStrictEqual([counter?.status, counter?.result], ["completed", "2"]);
      assert.isNotNull(counter?.completedAt);
      assert.deepStrictEqual(backgroundWorkPending(session.seen), [true, false]);
      assert.lengthOf(commandsOf(fake, "get_subagent_messages"), 0);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "holds a finished child's row past its turn until omp delivers its result in a wake",
    () =>
      Effect.gen(function* () {
        const { detachedNoWait } = subagentFrames;
        const wakes =
          yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
        const fake = yield* makeFakeOmpProcess();
        const session = yield* openRuntime(fake, {
          continuationRequests: {
            offer: (request) => Queue.offer(wakes, request).pipe(Effect.asVoid),
          },
        });
        yield* startTurn(session);
        const prompt = yield* fake.takeCommand("prompt");
        yield* emitAll(fake, [
          detachedNoWait.listerStarted,
          detachedNoWait.listerRunning,
          detachedNoWait.taskStart,
          detachedNoWait.taskEnd,
        ]);
        yield* fake.emit({ ...frames.yieldedPromptResult, id: prompt.id });
        yield* session.takeEvent(isTerminal);
        yield* emitAll(fake, [
          detachedNoWait.listerBash,
          detachedNoWait.listerCompletedProgress,
          detachedNoWait.listerCompleted,
          detachedNoWait.taskCompletion,
        ]);
        const ended = (yield* session.takeEvent(
          (event): event is SubagentUpdate =>
            isSubagentUpdateOf("Lister")(event) && event.subagent.status === "completed",
        )).subagent;
        yield* fake.emit(detachedNoWait.wakeAgentStart);
        const wake = yield* Queue.take(wakes);
        assert.isTrue(yield* session.runtime.hasPendingBackgroundWork!);
        yield* emitAll(fake, [
          detachedNoWait.resultStart,
          detachedNoWait.resultEnd,
          detachedNoWait.sessionSettled,
        ]);
        yield* wake.dispatchIfCurrent!(Effect.void);
        yield* startTurn(session, { ordinal: 2, continuation: true });
        yield* session.takeEvent(isTerminal);

        const lister = lastSubagent(session.seen, "Lister");
        const resultIndex = session.seen.findLastIndex(isSubagentUpdateOf("Lister"));
        const releaseIndex = session.seen.findIndex(isBackgroundRelease);
        assert.isNull(ended.result);
        assert.deepStrictEqual(
          [lister?.status, lister?.runId, lister?.result],
          [
            "completed",
            RunId.make(`run:${THREAD_ID}:1`),
            "Output of `sleep 4; ls -A` in the current directory:\n\n- `.git`\n- `NOTES.md`",
          ],
        );
        assert.deepStrictEqual(backgroundWorkPending(session.seen), [true, false]);
        assert.isTrue(resultIndex < releaseIndex);
        assert.isFalse(yield* session.runtime.hasPendingBackgroundWork!);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("reads a child's result from its transcript when omp settles without one", () =>
    Effect.gen(function* () {
      const { waited } = subagentFrames;
      const fake = yield* makeFakeOmpProcess({
        reply: (command) =>
          command.type === "get_subagent_messages"
            ? fakeOmpSuccess(waited.messagesReply)
            : undefined,
      });
      const session = yield* openRuntime(fake);
      yield* startTurn(session);
      const prompt = yield* fake.takeCommand("prompt");
      yield* emitAll(fake, [
        waited.counterStarted,
        waited.counterRunning,
        waited.taskStart,
        waited.taskEnd,
        waited.counterCompleted,
        waited.taskCompletion,
      ]);
      yield* fake.emit(promptResult(prompt.id));
      yield* session.takeEvent(isTerminal);
      yield* fake.emit(frames.sessionSettled);
      yield* session.takeEvent(isBackgroundRelease);

      assert.deepStrictEqual(
        commandsOf(fake, "get_subagent_messages").map((command) => command.subagentId),
        ["Counter"],
      );
      const counter = lastSubagent(session.seen, "Counter");
      assert.deepStrictEqual([counter?.status, counter?.result], ["completed", "2"]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("shows a child omp cancelled without a Stop from T3 as cancelled", () =>
    Effect.gen(function* () {
      const { cancelled } = subagentFrames;
      const fake = yield* makeFakeOmpProcess();
      const session = yield* openRuntime(fake);
      yield* startTurn(session);
      const prompt = yield* fake.takeCommand("prompt");
      yield* emitAll(fake, [
        cancelled.sleeperStarted,
        cancelled.sleeperRunning,
        cancelled.sleeperAborted,
        cancelled.sleeperResultEnd,
      ]);
      yield* fake.emit(promptResult(prompt.id));
      yield* session.takeEvent(isTerminal);

      const sleeper = lastSubagent(session.seen, "Sleeper");
      assert.deepStrictEqual(
        [sleeper?.status, sleeper?.result],
        ["cancelled", "(no output) after 2 req"],
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("shows a child a later turn's Stop cancels as interrupted", () =>
    Effect.gen(function* () {
      const { stopCancel } = subagentFrames;
      const fake = yield* makeFakeOmpProcess({
        reply: (command) =>
          command.type === "cancel_subagent"
            ? fakeOmpSuccess(frames.cancelSubagentResult, {
                after: [stopCancel.napperAborted, stopCancel.napperResultEnd],
              })
            : subagentReply(command, [{ id: "Napper" }]),
      });
      const session = yield* openRuntime(fake);
      yield* startTurn(session);
      const first = yield* fake.takeCommand("prompt");
      yield* emitAll(fake, [stopCancel.taskStart, stopCancel.napperStarted, stopCancel.taskEnd]);
      yield* fake.emit({ ...frames.yieldedPromptResult, id: first.id });
      yield* session.takeEvent(isTerminal);

      yield* startTurn(session, { ordinal: 2 });
      const second = yield* fake.takeCommand("prompt");
      const secondTurn = yield* session.takeEvent(isProviderTurnUpdate);
      yield* session.runtime.interruptTurn({
        providerThread: session.providerThread,
        providerTurnId: secondTurn.providerTurn.id,
      });
      yield* fake.emit(promptResult(second.id, { status: "aborted" }));
      yield* session.takeEvent(isTerminal);

      const napper = lastSubagent(session.seen, "Napper");
      assert.deepStrictEqual(
        [napper?.status, napper?.runId, napper?.result],
        ["interrupted", RunId.make(`run:${THREAD_ID}:1`), "(no output) after 2 req"],
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("ends an earlier turn's child that Stop is still cancelling once omp settles", () =>
    Effect.gen(function* () {
      const { stopCancel } = subagentFrames;
      const fake = yield* makeFakeOmpProcess({
        reply: (command) =>
          command.type === "cancel_subagent"
            ? FAKE_OMP_UNANSWERED
            : subagentReply(command, [{ id: "Napper" }]),
      });
      const session = yield* openRuntime(fake);
      yield* startTurn(session);
      const first = yield* fake.takeCommand("prompt");
      yield* emitAll(fake, [stopCancel.taskStart, stopCancel.napperStarted, stopCancel.taskEnd]);
      yield* fake.emit({ ...frames.yieldedPromptResult, id: first.id });
      yield* session.takeEvent(isTerminal);
      yield* startTurn(session, { ordinal: 2 });
      const second = yield* fake.takeCommand("prompt");
      const secondTurn = yield* session.takeEvent(isProviderTurnUpdate);

      const stop = yield* session.runtime
        .interruptTurn({
          providerThread: session.providerThread,
          providerTurnId: secondTurn.providerTurn.id,
        })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const cancel = yield* fake.takeCommand("cancel_subagent");
      // omp settles right after the abort, before the cancel has stopped the child (P2:154-157).
      yield* emitAll(fake, [promptResult(second.id, { status: "aborted" }), frames.sessionSettled]);
      yield* session.takeEvent(isBackgroundRelease);
      const settled = lastSubagent(session.seen, "Napper");
      yield* fake.respond(cancel, frames.cancelSubagentResult);
      yield* Fiber.join(stop);

      assert.strictEqual(settled?.status, "interrupted");
      assert.isFalse(yield* session.runtime.hasPendingBackgroundWork!);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("fails an earlier turn's running child when omp exits", () =>
    Effect.gen(function* () {
      const { stopCancel } = subagentFrames;
      const fake = yield* makeFakeOmpProcess();
      const session = yield* openRuntime(fake);
      yield* startTurn(session);
      const prompt = yield* fake.takeCommand("prompt");
      yield* emitAll(fake, [stopCancel.taskStart, stopCancel.napperStarted, stopCancel.taskEnd]);
      yield* fake.emit({ ...frames.yieldedPromptResult, id: prompt.id });
      yield* session.takeEvent(isTerminal);

      yield* fake.exit(1);
      yield* Effect.flip(session.takeEvent(matchesNothing));

      assert.strictEqual(lastSubagent(session.seen, "Napper")?.status, "failed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("holds a child a task call ran inline until omp settles, then reads its result", () =>
    Effect.gen(function* () {
      const { waited } = subagentFrames;
      const { async: _job, ...inlineDetails } = waited.taskEnd.result.details;
      // A `blocking` agent runs inline; its task call ends with no job (task/index.ts:1114-1122).
      const inlineEnd = {
        ...waited.taskEnd,
        result: { ...waited.taskEnd.result, details: inlineDetails },
      };
      const fake = yield* makeFakeOmpProcess({
        reply: (command) =>
          command.type === "get_subagent_messages"
            ? fakeOmpSuccess(waited.messagesReply)
            : undefined,
      });
      const session = yield* openRuntime(fake);
      yield* startTurn(session);
      const prompt = yield* fake.takeCommand("prompt");
      yield* emitAll(fake, [
        waited.counterStarted,
        waited.counterRunning,
        waited.taskStart,
        waited.counterCompleted,
        inlineEnd,
      ]);
      yield* fake.emit(promptResult(prompt.id));
      yield* session.takeEvent(isTerminal);
      const pendingAtTurnEnd = backgroundWorkPending(session.seen);
      yield* fake.emit(frames.sessionSettled);
      yield* session.takeEvent(isBackgroundRelease);

      assert.deepStrictEqual(pendingAtTurnEnd, [true]);
      assert.strictEqual(lastToolRowStatus(session.seen, waited.taskStart.toolCallId), "completed");
      assert.deepStrictEqual(
        commandsOf(fake, "get_subagent_messages").map((command) => command.subagentId),
        ["Counter"],
      );
      const counter = lastSubagent(session.seen, "Counter");
      assert.deepStrictEqual([counter?.status, counter?.result], ["completed", "2"]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps one row per child through a repeated start or end and clears its progress", () =>
    Effect.gen(function* () {
      const { parentAbort } = subagentFrames;
      const endedAgain = {
        ...parentAbort.quickCompleted,
        payload: { ...parentAbort.quickCompleted.payload, status: "aborted" },
      };
      const fake = yield* makeFakeOmpProcess();
      const session = yield* openRuntime(fake);
      yield* startTurn(session);
      const prompt = yield* fake.takeCommand("prompt");
      yield* emitAll(fake, [
        parentAbort.quickStarted,
        parentAbort.quickRunning,
        parentAbort.quickBash,
        parentAbort.quickStarted,
        parentAbort.quickCompleted,
        endedAgain,
        parentAbort.quickResultStart,
        parentAbort.quickResultEnd,
      ]);
      yield* fake.emit(promptResult(prompt.id));
      yield* session.takeEvent(isTerminal);

      const quick = lastSubagent(session.seen, "Quick");
      assert.deepStrictEqual(subagentNodeStatuses(session.seen), [
        ["Quick", "running"],
        ["Quick", "completed"],
      ]);
      assert.deepStrictEqual(
        [quick?.status, quick?.progress, quick?.result, quick?.prompt],
        ["completed", undefined, "quick-done", parentAbort.quickRunning.payload.assignment],
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps a task call's row start time from when its first child was reported", () =>
    Effect.gen(function* () {
      const { parentAbort } = subagentFrames;
      const taskCallId = parentAbort.taskStart.toolCallId;
      const fake = yield* makeFakeOmpProcess();
      const session = yield* openRuntime(fake);
      yield* startTurn(session);
      yield* fake.takeCommand("prompt");
      yield* fake.emit(parentAbort.napperStarted);
      yield* session.takeEvent(isSubagentUpdateOf("Napper"));
      yield* TestClock.adjust(Duration.seconds(1));
      yield* fake.emit(parentAbort.taskStart);
      yield* session.takeEvent(
        (event): event is AdapterEvent =>
          event.type === "turn_item.updated" &&
          nativeIdOf(event) === taskCallId &&
          event.turnItem.type === "dynamic_tool" &&
          event.turnItem.input !== undefined,
      );

      const taskRowStarts = session.seen.flatMap((event) =>
        event.type === "turn_item.updated" && nativeIdOf(event) === taskCallId
          ? [
              event.turnItem.startedAt === null
                ? null
                : DateTime.toEpochMillis(event.turnItem.startedAt),
            ]
          : [],
      );
      assert.lengthOf(taskRowStarts, 2);
      assert.notInclude(taskRowStarts, null);
      assert.strictEqual(new Set(taskRowStarts).size, 1);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("shows no row for a child's own child, whose task call omp never reports", () =>
    Effect.gen(function* () {
      const { nested } = subagentFrames;
      const fake = yield* makeFakeOmpProcess();
      const session = yield* openRuntime(fake);
      yield* startTurn(session);
      const prompt = yield* fake.takeCommand("prompt");
      yield* emitAll(fake, [
        nested.taskStart,
        nested.outerStarted,
        nested.outerRunning,
        nested.taskEnd,
        nested.waitStart,
        nested.innerStarted,
        nested.innerRunning,
        nested.innerCompletedProgress,
        nested.innerCompleted,
        nested.outerCompletedProgress,
        nested.outerCompleted,
        nested.waitEnd,
      ]);
      yield* fake.emit(promptResult(prompt.id));
      yield* session.takeEvent(isTerminal);
      yield* fake.emit(frames.sessionSettled);
      yield* session.takeEvent(isBackgroundRelease);

      const innerTaskCall = nested.innerStarted.payload.parentToolCallId;
      const shownChildren = session.seen.flatMap((event) =>
        event.type === "subagent.updated" ? [event.subagent.nativeTaskRef?.nativeId] : [],
      );
      assert.deepStrictEqual([...new Set(shownChildren)], ["Outer"]);
      assert.isFalse(session.seen.some((event) => nativeIdOf(event) === innerTaskCall));
      assert.strictEqual(
        lastSubagent(session.seen, "Outer")?.result,
        "Inner reported the output: `inner-done`",
      );
      assert.deepStrictEqual(backgroundWorkPending(session.seen), [true, false]);
      assert.lengthOf(commandsOf(fake, "get_subagent_messages"), 0);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "puts a child omp starts after its turn ended on the turn whose task call made it",
    () =>
      Effect.gen(function* () {
        const { stopCancel } = subagentFrames;
        const taskCallId = stopCancel.taskStart.toolCallId;
        const fake = yield* makeFakeOmpProcess();
        const session = yield* openRuntime(fake);
        yield* startTurn(session);
        const first = yield* fake.takeCommand("prompt");
        yield* emitAll(fake, [stopCancel.taskStart, stopCancel.taskEnd]);
        yield* fake.emit({ ...frames.yieldedPromptResult, id: first.id });
        const firstTerminal = yield* session.takeEvent(isTerminal);

        yield* startTurn(session, { ordinal: 2 });
        const second = yield* fake.takeCommand("prompt");
        // A batch past omp's concurrency limit starts its later children once others end.
        yield* fake.emit(stopCancel.napperStarted);
        yield* session.takeEvent(isSubagentUpdateOf("Napper"));
        yield* fake.emit(promptResult(second.id));
        yield* session.takeEvent(isTerminal);

        const taskRowTurns = session.seen.flatMap((event) =>
          event.type === "turn_item.updated" && nativeIdOf(event) === taskCallId
            ? [event.turnItem.providerTurnId]
            : [],
        );
        assert.deepStrictEqual(subagentRow(lastSubagent(session.seen, "Napper")), {
          status: "running",
          title: "Napper",
          result: null,
          origin: "provider_native",
          childThreadId: null,
          runId: RunId.make(`run:${THREAD_ID}:1`),
          parentNodeId: nodeIdOf(session.seen, taskCallId),
        });
        assert.isTrue(taskRowTurns.every((turnId) => turnId === firstTerminal.providerTurnId));
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([
    { ending: "settled completion", terminal: "completed", subagent: "running" },
    { ending: "Stop", terminal: "interrupted", subagent: "interrupted" },
    { ending: "provider error", terminal: "failed", subagent: "running" },
    { ending: "process exit", terminal: "failed", subagent: "failed" },
  ] as const)(
    "leaves a running subagent to omp or settles it on $ending",
    ({ ending, terminal: expectedStatus, subagent: expectedSubagent }) =>
      Effect.gen(function* () {
        const { parentAbort } = subagentFrames;
        const fake = yield* makeFakeOmpProcess({
          reply: (command) => subagentReply(command, [{ id: "Napper" }]),
        });
        const session = yield* openRuntime(fake);
        yield* startTurn(session);
        const prompt = yield* fake.takeCommand("prompt");
        yield* emitAll(fake, [
          parentAbort.napperStarted,
          parentAbort.taskStart,
          parentAbort.taskEnd,
        ]);
        yield* session.takeEvent(isSubagentUpdateOf("Napper"));

        if (ending === "settled completion") {
          yield* fake.emit({ ...frames.yieldedPromptResult, id: prompt.id });
        }
        if (ending === "provider error") {
          yield* fake.emit({ ...frames.providerErrorResult, id: prompt.id });
        }
        if (ending === "process exit") yield* fake.exit(1);
        if (ending === "Stop") {
          yield* session.runtime.interruptTurn({
            providerThread: session.providerThread,
            providerTurnId: firstProviderTurnId(session.seen),
          });
          yield* fake.emit(promptResult(prompt.id, { status: "aborted" }));
        }
        const terminal = yield* session.takeEvent(isTerminal);

        assert.strictEqual(terminal.status, expectedStatus);
        assert.strictEqual(lastSubagent(session.seen, "Napper")?.status, expectedSubagent);
        assert.strictEqual(
          terminal.status === "failed" ? terminal.childTurnsContinue : undefined,
          ending === "provider error" ? true : undefined,
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([
    { ending: "settled completion", terminal: "completed", tool: "completed" },
    { ending: "Stop", terminal: "interrupted", tool: "interrupted" },
    { ending: "provider error", terminal: "failed", tool: "interrupted" },
    { ending: "process exit", terminal: "failed", tool: "interrupted" },
  ] as const)(
    "settles open tools and requests on $ending",
    ({ ending, terminal: expectedStatus, tool }) =>
      Effect.gen(function* () {
        const fake = yield* makeFakeOmpProcess();
        const session = yield* openRuntime(fake);
        yield* startTurn(session);
        const prompt = yield* fake.takeCommand("prompt");
        yield* fake.emit(frames.bashStart);
        yield* fake.emit(frames.bashApproval);
        yield* session.takeEvent(
          (event): event is AdapterEvent => event.type === "runtime_request.updated",
        );

        if (ending === "settled completion") yield* fake.emit(promptResult(prompt.id));
        if (ending === "provider error") {
          yield* fake.emit({ ...frames.providerErrorResult, id: prompt.id });
        }
        if (ending === "process exit") yield* fake.exit(1);
        if (ending === "Stop") {
          const providerTurnId = firstProviderTurnId(session.seen);
          yield* session.runtime.interruptTurn({
            providerThread: session.providerThread,
            providerTurnId,
          });
          yield* fake.emit(promptResult(prompt.id, { status: "aborted" }));
        }
        const terminal = yield* session.takeEvent(isTerminal);

        assert.strictEqual(terminal.status, expectedStatus);
        assert.strictEqual(lastToolRowStatus(session.seen, frames.bashStart.toolCallId), tool);
        assert.deepStrictEqual(runtimeRequestStatuses(session.seen), ["pending", "cancelled"]);
        if (ending === "process exit") {
          assert.strictEqual(terminal.failure?.class, "transport_error");
          const streamEnd = yield* Effect.flip(session.takeEvent(matchesNothing));
          assert.strictEqual(streamEnd._tag, "ProviderAdapterEventStreamError");
        }
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([
    { ending: "an answer", request: "resolved", job: "completed" },
    { ending: "Stop", request: "cancelled", job: "interrupted" },
  ] as const)(
    "keeps a nested tool-command approval open past its yielded turn until $ending",
    ({ ending, request, job }) =>
      Effect.gen(function* () {
        const fake = yield* makeFakeOmpProcess();
        const session = yield* openRuntime(fake, { runtimeMode: "approval-required" });
        const approval = frames.nestedCommandApproval;

        yield* startTurn(session);
        const prompt = yield* fake.takeCommand("prompt");
        yield* fake.emit(frames.nestedCommandStart);
        yield* fake.emit(approval);
        // omp backgrounds the bash row while the nested approval waits (TSR§5).
        yield* fake.emit(frames.nestedCommandEnd);
        yield* fake.emit({ ...frames.yieldedPromptResult, id: prompt.id });
        const terminal = yield* session.takeEvent(isTerminal);
        const requestId = session.seen.find(isRuntimeRequestUpdate)!.runtimeRequest.id;

        assert.strictEqual(terminal.status, "completed");
        assert.deepStrictEqual(terminal.retainedRuntimeRequestIds, [requestId]);
        assert.deepStrictEqual(runtimeRequestStatuses(session.seen), ["pending"]);
        if (ending === "an answer") {
          yield* session.runtime.respondToRuntimeRequest({ requestId, decision: "accept" });
          yield* fake.emit(frames.sessionSettled);
        } else {
          yield* session.runtime.interruptTurn({
            providerThread: session.providerThread,
            providerTurnId: terminal.providerTurnId,
            requestRuntimeRestart: true,
          });
        }
        yield* session.takeEvent(isBackgroundRelease);

        assert.deepStrictEqual(runtimeRequestStatuses(session.seen), ["pending", request]);
        assert.strictEqual(
          lastToolRowStatus(session.seen, frames.nestedCommandStart.toolCallId),
          job,
        );
        assert.deepStrictEqual(
          fake.frames().filter((frame) => frame.type === "extension_ui_response"),
          ending === "an answer"
            ? [{ type: "extension_ui_response", id: approval.id, value: "Approve" }]
            : [],
        );
        assert.isFalse(yield* session.runtime.hasPendingBackgroundWork!);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("stops a manual compaction by ending omp, since abort queues behind compact", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess({
        reply: (command) => (command.type === "compact" ? FAKE_OMP_UNANSWERED : undefined),
      });
      const session = yield* openRuntime(fake);
      yield* startTurn(session, { text: "/compact" });
      yield* fake.takeCommand("compact");
      yield* fake.emit(frames.manualCompactionStart);
      const providerTurnId = (yield* session.takeEvent(isProviderTurnUpdate)).providerTurn.id;

      yield* session.runtime.interruptTurn({
        providerThread: session.providerThread,
        providerTurnId,
      });
      const terminal = yield* session.takeEvent(isTerminal);
      yield* session.takeEvent(isSessionStopped);

      assert.strictEqual(terminal.status, "interrupted");
      assert.lengthOf(commandsOf(fake, "abort"), 0);
      assert.strictEqual(
        session.seen
          .flatMap((event) =>
            event.type === "turn_item.updated" && event.turnItem.type === "compaction"
              ? [event.turnItem.status]
              : [],
          )
          .at(-1),
        "cancelled",
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("renames the session after a manual compaction instead of timing out behind it", () =>
    Effect.gen(function* () {
      let compacting = false;
      const fake = yield* makeFakeOmpProcess({
        // omp runs one command at a time, so a command written behind `compact` waits for it.
        reply: (command) => {
          if (command.type === "compact") compacting = true;
          const queued = command.type === "compact" || command.type === "set_session_name";
          return compacting && queued ? FAKE_OMP_UNANSWERED : undefined;
        },
      });
      const session = yield* openRuntime(fake);
      yield* startTurn(session, { text: "/compact" });
      const compact = yield* fake.takeCommand("compact");
      const renaming = yield* session.runtime.applyThreadTitle!({
        providerThread: session.providerThread,
        title: "Compacted thread",
      }).pipe(Effect.forkChild);
      yield* TestClock.adjust(Duration.minutes(2));
      compacting = false;
      yield* fake.respond(compact);
      yield* session.takeEvent(isTerminal);
      yield* Fiber.join(renaming);

      assert.deepStrictEqual(
        commandsOf(fake, "set_session_name").map((command) => command.name),
        ["omp test thread", "Compacted thread"],
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("holds a backgrounded job past its turn and adopts omp's wake as a continuation", () =>
    Effect.gen(function* () {
      const wakes =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      const fake = yield* makeFakeOmpProcess();
      const session = yield* openRuntime(fake, {
        continuationRequests: {
          offer: (request) => Queue.offer(wakes, request).pipe(Effect.asVoid),
        },
      });
      const job = frames.backgroundedStart.toolCallId;

      yield* startTurn(session);
      const prompt = yield* fake.takeCommand("prompt");
      yield* fake.emit(frames.backgroundedStart);
      yield* fake.emit(frames.backgroundedEnd);
      yield* fake.emit({ ...frames.yieldedPromptResult, id: prompt.id });
      yield* session.takeEvent(isTerminal);
      assert.strictEqual(lastToolRowStatus(session.seen, job), "running");
      yield* fake.emit(frames.backgroundedCompletion);
      yield* fake.emit(frames.wakeAgentStart);
      const wake = yield* Queue.take(wakes);
      assert.isTrue(yield* session.runtime.hasPendingBackgroundWork!);
      yield* fake.emit(frames.asyncResultStart);
      yield* fake.emit(frames.sessionSettled);
      // The orchestrator's dispatch only queues the run; it starts later, outside the permit.
      const dispatched = yield* wake.dispatchIfCurrent!(Effect.void);
      assert.isTrue(Option.isSome(dispatched));
      yield* startTurn(session, { ordinal: 2, continuation: true });
      yield* session.takeEvent(isTerminal);

      assert.deepStrictEqual(
        session.seen.filter(isTerminal).map((event) => event.status),
        ["completed", "completed"],
      );
      assert.deepStrictEqual(
        session.seen.flatMap((event) =>
          event.type === "background_work.updated" ? [event.pending] : [],
        ),
        [true, false],
      );
      assert.strictEqual(lastToolRowStatus(session.seen, job), "completed");
      assert.lengthOf(commandsOf(fake, "prompt"), 1);
      assert.isFalse(yield* session.runtime.hasPendingBackgroundWork!);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("registers no host tools without an MCP session and refuses host calls", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess();
      const session = yield* openRuntime(fake);

      yield* fake.emit({
        type: "host_tool_call",
        id: "host-call-1",
        toolCallId: "toolu_host:cmd:0",
        toolName: "mcp__t3-code__thread_lookup",
        arguments: {},
      });
      const result = yield* fake.takeFrame("host_tool_result");

      assert.deepStrictEqual(
        commandsOf(fake, "set_host_tools").map((command) => command.tools),
        [[]],
      );
      assert.deepInclude(result, { id: "host-call-1" });
      assert.deepInclude(result.result, { isError: true });
      assert.strictEqual(session.runtime.providerSession.status, "ready");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("serves T3 tools over MCP and answers a failed tool call as an error result", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess();
      const host = yield* ProviderHost.ProviderHost;
      const path = yield* Path.Path;
      const fileSystem = yield* FileSystem.FileSystem;
      yield* (yield* McpProviderSessions.McpProviderSessions).set({
        environmentId: EnvironmentId.make("environment-omp-test"),
        threadId: THREAD_ID,
        providerSessionId: SESSION_ID,
        providerInstanceId: OMP_INSTANCE_ID,
        endpoint: "http://127.0.0.1:3773/mcp",
        authorizationHeader: "Bearer token-1",
        browserToolsAvailable: false,
      });
      yield* openRuntime(fake, { http: fakeMcpServer });

      yield* fake.emit({
        type: "host_tool_call",
        id: "host-call-1",
        toolCallId: "toolu_host:cmd:0",
        toolName: "mcp__t3-code__thread_lookup",
        arguments: { query: "omp" },
      });
      const result = yield* fake.takeFrame("host_tool_result");

      const registered = commandsOf(fake, "set_host_tools");
      assert.lengthOf(registered, 1);
      assert.deepStrictEqual(
        registered[0]?.tools.map((tool) => tool.name),
        ["mcp__t3-code__thread_lookup"],
      );
      assert.deepInclude(result.result, { isError: true });
      const overlay = yield* fileSystem.readFileString(
        ompOverlayPath(path, host.paths.stateDir, SESSION_ID),
      );
      assert.include(overlay, '"mcp__t3-code__thread_lookup": "allow"');
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("never sends the default model and skips thinking levels omp does not offer", () =>
    Effect.gen(function* () {
      const baseline = { provider: "openai", id: "gpt-test", contextWindow: 200_000 };
      const fake = yield* makeFakeOmpProcess({
        reply: (command) =>
          command.type === "get_state" ? fakeOmpSuccess(stateWith({ model: baseline })) : undefined,
      });
      const session = yield* openRuntime(fake);

      yield* runTurn(session, fake, { selection: modelSelection("default", "xhigh") });
      assert.lengthOf(commandsOf(fake, "set_model"), 0);
      assert.lengthOf(commandsOf(fake, "set_thinking_level"), 0);

      yield* runTurn(session, fake, {
        ordinal: 2,
        selection: modelSelection("anthropic/claude-test", "high"),
      });
      yield* runTurn(session, fake, { ordinal: 3, selection: modelSelection("default") });

      assert.deepStrictEqual(
        commandsOf(fake, "set_model").map(({ provider, modelId }) => ({ provider, modelId })),
        [
          { provider: "anthropic", modelId: "claude-test" },
          { provider: "openai", modelId: "gpt-test" },
        ],
      );
      assert.deepStrictEqual(
        commandsOf(fake, "set_thinking_level").map((command) => command.level),
        ["high"],
      );
      assert.strictEqual(
        session.runtime.getModelContextWindow?.(modelSelection("default")),
        200_000,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("restores omp's own thinking level when the default model names none", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess({
        reply: (command) =>
          command.type === "get_state"
            ? fakeOmpSuccess(stateWith({ thinkingLevel: "low" }))
            : undefined,
      });
      const session = yield* openRuntime(fake);

      yield* runTurn(session, fake, { selection: modelSelection("default", "high") });
      yield* runTurn(session, fake, { ordinal: 2, selection: modelSelection("default") });
      yield* runTurn(session, fake, { ordinal: 3, selection: modelSelection("default") });

      assert.deepStrictEqual(
        commandsOf(fake, "set_thinking_level").map((command) => command.level),
        ["high", "low"],
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("names the append-prompt file after the overlay's file-safe key", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const fake = yield* makeFakeOmpProcess();
      yield* openRuntime(fake, {
        providerSessionId: ProviderSessionId.make("provider-session:omp%3Atest:shared"),
      });

      const args = fake.spawns()[0]?.args ?? [];
      const appendFile = args[args.indexOf("--append-system-prompt") + 1] ?? "";
      const overlay = args[args.lastIndexOf("--config") + 1] ?? "";
      assert.strictEqual(
        path.basename(appendFile),
        `${path.basename(overlay, ".yml")}.append-system.md`,
      );
      // ':' opens an NTFS alternate data stream, so the write fails on Windows.
      assert.match(path.basename(appendFile), /^[A-Za-z0-9._-]+$/);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("resumes a session recorded under the real path of a symlinked cwd", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const host = yield* ProviderHost.ProviderHost;
      const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "omp-adapter-link-" });
      const realCwd = yield* fileSystem
        .realPath(root)
        .pipe(Effect.map((resolved) => path.join(resolved, "workspace")));
      yield* fileSystem.makeDirectory(realCwd);
      const linkCwd = path.join(root, "link");
      yield* fileSystem.symlink(realCwd, linkCwd);
      const sessionFile = yield* writeSessionFile(path.join(root, "session.jsonl"), realCwd);
      const fake = yield* makeFakeOmpProcess({
        reply: (command) =>
          command.type === "get_state" ? fakeOmpSuccess(stateWith({ sessionFile })) : undefined,
      });

      const session = yield* openRuntime(fake, {
        cwd: linkCwd,
        initialNativeThreadId: sessionFile,
      });
      const resumed = yield* session.runtime.resumeThread({
        providerThread: session.providerThread,
      });

      const args = fake.spawns()[0]?.args ?? [];
      assert.strictEqual(args[args.indexOf("--resume") + 1], sessionFile);
      assert.strictEqual(
        args[args.indexOf("--session-dir") + 1],
        ompSessionDirForCwd(path, host.paths.stateDir, realCwd),
      );
      assert.lengthOf(fake.spawns(), 1);
      assert.lengthOf(commandsOf(fake, "switch_session"), 0);
      assert.strictEqual(resumed.nativeThreadRef?.nativeId, sessionFile);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("records the turn's user entry and rolls back with branch at the next turn", () =>
    Effect.gen(function* () {
      const branched = "/fake/omp-sessions/--workspace--/branched.jsonl";
      let branch: ReadonlyArray<{ entryId: string; text: string }> = [];
      let sessionFile = FAKE_OMP_SESSION_FILE;
      const fake = yield* makeFakeOmpProcess({
        reply: (command) => {
          if (command.type === "get_branch_messages") return fakeOmpSuccess({ messages: branch });
          if (command.type === "get_state") return fakeOmpSuccess(stateWith({ sessionFile }));
          if (command.type !== "branch") return undefined;
          sessionFile = branched;
          return fakeOmpSuccess({ text: "Hello omp", cancelled: false });
        },
      });
      const session = yield* openRuntime(fake);

      yield* startTurn(session);
      const prompt = yield* fake.takeCommand("prompt");
      branch = [{ entryId: FIRST_USER_ENTRY, text: "Hello omp" }];
      yield* fake.emit(promptResult(prompt.id));
      yield* session.takeEvent(isTerminal);
      const completed = session.seen.flatMap((event) =>
        event.type === "provider_turn.updated" && event.providerTurn.completedAt !== null
          ? [event.providerTurn]
          : [],
      );
      assert.deepStrictEqual(completed.at(-1)?.nativeTurnRef, strongRef(FIRST_USER_ENTRY));

      const turns = [
        providerTurn(session.providerThread, 1, strongRef(FIRST_USER_ENTRY)),
        providerTurn(session.providerThread, 2, strongRef(SECOND_USER_ENTRY)),
      ];
      const uncaptured = yield* Effect.flip(
        session.runtime.rollbackThread({
          providerThread: session.providerThread,
          target: {
            type: "provider_turn",
            checkpointId: CheckpointId.make("cp-1"),
            appRunOrdinal: 1,
            providerTurn: turns[0]!,
          },
          providerThreadTurns: [
            turns[0]!,
            providerTurn(session.providerThread, 2, {
              ...strongRef("synthetic"),
              strength: "weak",
            }),
          ],
        }),
      );
      assert.strictEqual(uncaptured._tag, "ProviderAdapterRollbackThreadError");
      assert.lengthOf(commandsOf(fake, "branch"), 0);

      const snapshot = yield* session.runtime.rollbackThread({
        providerThread: session.providerThread,
        target: {
          type: "provider_turn",
          checkpointId: CheckpointId.make("cp-1"),
          appRunOrdinal: 1,
          providerTurn: turns[0]!,
        },
        providerThreadTurns: turns,
      });
      assert.deepStrictEqual(
        commandsOf(fake, "branch").map((command) => command.entryId),
        [SECOND_USER_ENTRY],
      );
      assert.strictEqual(snapshot.providerThread.nativeThreadRef?.nativeId, branched);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("forks through a turn in a separate process and binds the forked file", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspace = yield* fileSystem.makeTempDirectoryScoped({ prefix: "omp-adapter-fork-" });
      const cwd = yield* fileSystem.realPath(workspace);
      const forked = yield* writeSessionFile(path.join(cwd, "forked.jsonl"), cwd);
      // `omp --fork` writes the copy before `ready`; the fork point is read from its entries.
      const copy = yield* writeSessionFile(
        path.join(cwd, "copy.jsonl"),
        cwd,
        frames.forkEntries.entries,
      );
      let mainFile = FAKE_OMP_SESSION_FILE;
      const mainFake = yield* makeFakeOmpProcess({
        reply: (command) => {
          if (command.type === "switch_session") mainFile = command.sessionPath;
          return command.type === "get_state"
            ? fakeOmpSuccess(stateWith({ sessionFile: mainFile }))
            : undefined;
        },
      });
      let forkStates = 0;
      const forkScript: FakeOmpScript = {
        reply: (command) => {
          if (command.type === "fork") return fakeOmpSuccess({ cancelled: false });
          if (command.type !== "get_state") return undefined;
          forkStates += 1;
          return fakeOmpSuccess(stateWith({ sessionFile: forkStates === 1 ? copy : forked }));
        },
      };
      const forkFake = yield* makeFakeOmpProcess(forkScript);
      const session = yield* openRuntime(mainFake, {
        cwd,
        spawner: routeForkSpawns(mainFake, forkFake),
      });
      const source = session.providerThread;
      const turns = [
        providerTurn(source, 1, strongRef(FIRST_USER_ENTRY)),
        providerTurn(source, 2, strongRef(SECOND_USER_ENTRY)),
      ];

      const fork = yield* session.runtime.forkThread({
        sourceProviderThread: source,
        sourceProviderTurns: turns,
        providerTurnId: turns[0]!.id,
        targetThreadId: ThreadId.make("thread-omp-fork"),
      });

      const forkArgs = forkFake.spawns()[0]?.args ?? [];
      assert.strictEqual(forkArgs[forkArgs.indexOf("--fork") + 1], FAKE_OMP_SESSION_FILE);
      const unattendedOverlay = path.join(
        (yield* ProviderHost.ProviderHost).paths.stateDir,
        "omp-overlays",
        "unattended.yml",
      );
      assert.deepStrictEqual(forkArgs.slice(-2), ["--config", unattendedOverlay]);
      assert.strictEqual(
        yield* fileSystem.readFileString(unattendedOverlay),
        renderOmpUnattendedOverlay(),
      );
      assert.deepStrictEqual(
        commandsOf(forkFake, "fork").map((command) => command.entryId),
        [FORK_ENTRY],
      );
      assert.strictEqual(fork.nativeThreadRef?.nativeId, forked);
      assert.deepStrictEqual(fork.forkedFrom, {
        providerThreadId: source.id,
        providerTurnId: turns[0]!.id,
      });
      assert.deepStrictEqual(
        commandsOf(mainFake, "switch_session").map((command) => command.sessionPath),
        [forked],
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("rewrites the overlay to loosen and restarts on the same file to tighten", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const host = yield* ProviderHost.ProviderHost;
      const workspace = yield* fileSystem.makeTempDirectoryScoped({ prefix: "omp-adapter-mode-" });
      const cwd = yield* fileSystem.realPath(workspace);
      const sessionFile = yield* writeSessionFile(path.join(cwd, "session.jsonl"), cwd);
      const fake = yield* makeFakeOmpProcess({
        reply: (command) =>
          command.type === "get_state" ? fakeOmpSuccess(stateWith({ sessionFile })) : undefined,
      });
      const session = yield* openRuntime(fake, { cwd, runtimeMode: "auto-accept-edits" });
      const overlayPath = ompOverlayPath(path, host.paths.stateDir, SESSION_ID);
      assert.include(yield* fileSystem.readFileString(overlayPath), 'approvalMode: "write"');

      yield* runTurn(session, fake, { runtimeMode: "full-access" });
      assert.include(yield* fileSystem.readFileString(overlayPath), 'approvalMode: "yolo"');
      assert.lengthOf(fake.spawns(), 1);

      yield* runTurn(session, fake, { ordinal: 2, runtimeMode: "approval-required" });
      assert.include(yield* fileSystem.readFileString(overlayPath), 'approvalMode: "always-ask"');
      const spawns = fake.spawns();
      assert.lengthOf(spawns, 2);
      const restartArgs = spawns[1]?.args ?? [];
      assert.strictEqual(restartArgs[restartArgs.indexOf("--resume") + 1], sessionFile);
      assert.lengthOf(commandsOf(fake, "set_host_tools"), 2);
      assert.lengthOf(commandsOf(fake, "set_interrupt_mode"), 2);
      assert.lengthOf(commandsOf(fake, "prompt"), 2);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "starts the turn on the new session when tightening restarts before omp wrote one",
    () =>
      Effect.gen(function* () {
        const fresh = "/fake/omp-sessions/--workspace--/restarted.jsonl";
        const fake = yield* makeFakeOmpProcess();
        const restarted = yield* makeFakeOmpProcess({
          reply: (command) =>
            command.type === "get_state"
              ? fakeOmpSuccess(stateWith({ sessionFile: fresh }))
              : undefined,
        });
        const spawner = ChildProcessSpawner.make((command) =>
          (fake.spawns().length === 0 ? fake : restarted).spawner.spawn(command),
        );
        const session = yield* openRuntime(fake, { spawner });

        const terminal = yield* runTurn(session, restarted, { runtimeMode: "approval-required" });

        assert.notInclude(restarted.spawns()[0]?.args ?? [], "--resume");
        assert.strictEqual(terminal.status, "completed");
        const rebound = session.seen.flatMap((event) =>
          event.type === "provider_thread.updated"
            ? [event.providerThread.nativeThreadRef?.nativeId]
            : [],
        );
        assert.strictEqual(rebound.at(-1), fresh);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("declares a runtime-mode change as a session restart", () =>
    Effect.gen(function* () {
      const session = yield* openRuntime(yield* makeFakeOmpProcess());
      // The orchestrator then refuses a change that would end background jobs, and a wake never
      // runs under the mode the thread left.
      assert.isFalse(
        session.runtime.providerSession.capabilities.sessions.supportsRuntimeModeSwitchInSession,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "asks a dialog the restarted omp raises and runs the next turn once it is answered",
    () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const workspace = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "omp-adapter-mode-",
        });
        const cwd = yield* fileSystem.realPath(workspace);
        const sessionFile = yield* writeSessionFile(path.join(cwd, "session.jsonl"), cwd);
        const replyWithState = (command: OmpCommandFrame) =>
          command.type === "get_state" ? fakeOmpSuccess(stateWith({ sessionFile })) : undefined;
        const fake = yield* makeFakeOmpProcess({ reply: replyWithState });
        const restarted = yield* makeFakeOmpProcess({
          startup: [STARTUP_DIALOG],
          reply: (command) =>
            command.type === "negotiate_protocol" ? FAKE_OMP_UNANSWERED : replyWithState(command),
        });
        const spawner = ChildProcessSpawner.make((command) =>
          (fake.spawns().length === 0 ? fake : restarted).spawner.spawn(command),
        );
        const session = yield* openRuntime(fake, { cwd, spawner });

        const held = yield* Effect.flip(startTurn(session, { runtimeMode: "approval-required" }));
        const asked = yield* session.takeEvent(isRuntimeRequestUpdate);
        yield* session.runtime.respondToRuntimeRequest({
          requestId: asked.runtimeRequest.id,
          answers: { [STARTUP_DIALOG.id]: "yes" },
        });
        yield* restarted.respond(yield* restarted.takeCommand("negotiate_protocol"), {
          protocolVersion: 2,
        });
        yield* restarted.takeCommand("set_host_tools");
        const terminal = yield* runTurn(session, restarted, {
          ordinal: 2,
          runtimeMode: "approval-required",
        });

        assert.strictEqual(held._tag, "ProviderAdapterTurnStartError");
        assert.lengthOf(restarted.spawns(), 1);
        assert.strictEqual(asked.runtimeRequest.status, "pending");
        assert.deepStrictEqual(
          restarted
            .commands()
            .slice(1, 6)
            .map((command) => command.type),
          [
            "set_event_filter",
            "set_subagent_subscription",
            "set_ask_dialog",
            "set_interrupt_mode",
            "set_host_tools",
          ],
        );
        assert.lengthOf(commandsOf(restarted, "get_branch_messages"), 2);
        assert.strictEqual(terminal.status, "completed");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("ends the stopped process's background jobs and wake on a tightening restart", () =>
    Effect.gen(function* () {
      const wakes =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      const fake = yield* makeFakeOmpProcess();
      const session = yield* openRuntime(fake, {
        continuationRequests: {
          offer: (request) => Queue.offer(wakes, request).pipe(Effect.asVoid),
        },
      });
      const job = frames.backgroundedStart.toolCallId;

      yield* startTurn(session);
      const prompt = yield* fake.takeCommand("prompt");
      yield* fake.emit(frames.backgroundedStart);
      yield* fake.emit(frames.backgroundedEnd);
      yield* fake.emit({ ...frames.yieldedPromptResult, id: prompt.id });
      yield* session.takeEvent(isTerminal);
      yield* fake.emit(frames.wakeAgentStart);
      const wake = yield* Queue.take(wakes);
      // Buffered for the wake's turn; the restart kills the run that would have ended it.
      yield* fake.emit(frames.bashStart);
      yield* runTurn(session, fake, { ordinal: 2, runtimeMode: "approval-required" });

      assert.lengthOf(fake.spawns(), 2);
      assert.strictEqual(lastToolRowStatus(session.seen, job), "interrupted");
      assert.isUndefined(lastToolRowStatus(session.seen, frames.bashStart.toolCallId));
      assert.isTrue(Option.isNone(yield* wake.dispatchIfCurrent!(Effect.void)));
      assert.isFalse(yield* session.runtime.hasPendingBackgroundWork!);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});

type GoalCommandFrame = Extract<OmpCommandFrame, { readonly type: "goal" }>;

/** omp's reply to each `goal` op, with the frames omp writes before it. */
const goalReplies =
  (replies: Partial<Record<GoalCommandFrame["op"], FakeOmpReply>>) =>
  (command: OmpCommandFrame): FakeOmpReply | undefined =>
    command.type === "goal" ? replies[command.op] : undefined;

const isGoalUpdate = (
  event: AdapterEvent,
): event is Extract<AdapterEvent, { readonly type: "provider_thread.updated" }> =>
  event.type === "provider_thread.updated" && (event.providerThread.goal ?? null) !== null;

/** The goal the thread showed after each change: [status, tokens used, seconds used] or null. */
const shownGoals = (seen: ReadonlyArray<AdapterEvent>) => {
  const shown: Array<string> = [];
  for (const event of seen) {
    if (event.type !== "provider_thread.updated") continue;
    const goal = event.providerThread.goal ?? null;
    const entry = JSON.stringify(
      goal === null ? null : [goal.status, goal.tokensUsed, goal.timeUsedSeconds],
    );
    if (shown.at(-1) !== entry) shown.push(entry);
  }
  return shown.map((entry): unknown => JSON.parse(entry));
};

const goalOps = (fake: FakeOmpProcess) =>
  commandsOf(fake, "goal").map((command) => [command.op, command.objective]);

/** The replies control turns showed, as [message, level]. */
const goalNotices = (seen: ReadonlyArray<AdapterEvent>) =>
  seen.flatMap((event) =>
    event.type === "turn_item.updated" &&
    event.turnItem.type === "system_notice" &&
    nativeIdOf(event)?.startsWith("goal-reply:") === true
      ? [[event.turnItem.message, event.turnItem.level]]
      : [],
  );

describe("OmpAdapterV2 goals", () => {
  const { budgetLimited, lifecycle, complete, abortPauses } = goalFrames;
  const objective = budgetLimited.createResult.goal.objective;

  it.effect(
    "sets a goal, adopts the run omp starts for it and shows the goal run out of budget",
    () =>
      Effect.gen(function* () {
        const wakes =
          yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
        const fake = yield* makeFakeOmpProcess({
          reply: goalReplies({
            create: fakeOmpSuccess(budgetLimited.createResult, {
              before: [budgetLimited.createUpdated],
            }),
            drop: fakeOmpSuccess(budgetLimited.dropResult, { before: budgetLimited.dropUpdates }),
          }),
        });
        const session = yield* openRuntime(fake, {
          continuationRequests: {
            offer: (request) => Queue.offer(wakes, request).pipe(Effect.asVoid),
          },
        });
        const [agentStart, ...run] = budgetLimited.run;

        yield* startTurn(session, { text: `/goal ${objective}` });
        const control = yield* session.takeEvent(isTerminal);
        // omp starts working on the goal right after its reply (P2/goal-budget-limited.jsonl:12).
        yield* fake.emit(agentStart!);
        yield* Queue.take(wakes);
        yield* startTurn(session, { ordinal: 2, continuation: true });
        yield* emitAll(fake, run);
        const goalRun = yield* session.takeEvent(isTerminal);
        yield* startTurn(session, { ordinal: 3, text: "/goal clear" });
        yield* session.takeEvent(isTerminal);

        assert.deepStrictEqual([control.status, goalRun.status], ["completed", "completed"]);
        assert.deepStrictEqual(goalOps(fake), [
          ["create", objective],
          ["drop", undefined],
        ]);
        assert.lengthOf(commandsOf(fake, "prompt"), 0);
        assert.deepStrictEqual(shownGoals(session.seen), [
          null,
          ["active", 0, 0],
          ["budget_limited", 13_959, 1],
          ["budget_limited", 14_452, 2],
          ["budget_limited", 14_452, 4],
          null,
        ]);
        assert.deepStrictEqual(assistantTexts(session.seen, control.providerTurnId), []);
        assert.deepStrictEqual(assistantTexts(session.seen, goalRun.providerTurnId), [
          "I'll create the first file this turn.",
          "Progress: `one.txt` created with `ok` (2 bytes, confirmed by write tool).\n\nRemaining work: `two.txt`, `three.txt`, `four.txt`, `five.txt`, each containing `ok`, one per turn. The goal is not complete, so I have not marked it so. Next step: resume the goal to create `two.txt`.",
        ]);
        // omp's goal context, continuation and budget messages are hidden (`display: false`).
        const items = JSON.stringify(
          session.seen.filter((event) => event.type === "turn_item.updated"),
        );
        for (const hidden of [
          "<goal_context>",
          "Hidden continuation steer",
          "Active goal token budget reached",
        ]) {
          assert.notInclude(items, hidden);
        }
        assert.deepStrictEqual(goalNotices(session.seen), []);
        assert.isTrue(session.runtime.providerSession.capabilities.turns.supportsNativeGoals);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps a /goal control turn that took over a started run open until omp settles", () =>
    Effect.gen(function* () {
      const wakes =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      const fake = yield* makeFakeOmpProcess({
        reply: goalReplies({
          create: fakeOmpSuccess(budgetLimited.createResult, {
            before: [budgetLimited.createUpdated],
          }),
          get: FAKE_OMP_UNANSWERED,
        }),
      });
      const session = yield* openRuntime(fake, {
        continuationRequests: {
          offer: (request) => Queue.offer(wakes, request).pipe(Effect.asVoid),
        },
      });
      const [agentStart, ...run] = budgetLimited.run;

      yield* startTurn(session, { text: `/goal ${objective}` });
      yield* fake.takeCommand("goal");
      yield* session.takeEvent(isTerminal);
      // omp starts the goal's run right after its reply (P2/goal-budget-limited.jsonl:12).
      yield* fake.emit(agentStart!);
      const wake = yield* Queue.take(wakes);
      // A second /goal queued behind the first starts before the run's own turn.
      yield* startTurn(session, { ordinal: 2, text: "/goal" });
      yield* fake.respond(yield* fake.takeCommand("goal"), budgetLimited.getResult);
      yield* emitAll(fake, run);
      const check = yield* session.takeEvent(isTerminal);

      assert.strictEqual(check.status, "completed");
      assert.isTrue(Option.isNone(yield* wake.dispatchIfCurrent!(Effect.void)));
      assert.deepStrictEqual(goalNotices(session.seen), [
        [`Goal budget limited: ${objective}`, "info"],
      ]);
      const toolStart = run.find((frame) => frame.type === "tool_execution_start");
      assert.lengthOf(assistantTexts(session.seen, check.providerTurnId), 2);
      assert.strictEqual(lastToolRowStatus(session.seen, toolStart!.toolCallId!), "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([
    {
      name: "shows the goal",
      text: "/goal",
      op: "get",
      reply: fakeOmpSuccess(budgetLimited.getResult),
      notice: [`Goal budget limited: ${objective}`, "info"],
    },
    {
      name: "says no goal is set when pausing none",
      text: "/goal pause",
      op: "pause",
      reply: fakeOmpSuccess(lifecycle.noGoalResult),
      notice: ["No goal is set.", "info"],
    },
    {
      name: "explains why a goal out of budget does not resume",
      text: "/goal resume",
      op: "resume",
      reply: fakeOmpSuccess(budgetLimited.getResult),
      notice: [
        "This goal has used its token budget, so OMP cannot resume it. Clear it and set it again to keep working.",
        "info",
      ],
    },
    {
      name: "shows why omp refused a new goal",
      text: "/goal Ship it",
      op: "create",
      reply: fakeOmpFailure(lifecycle.createRefusedError),
      notice: [lifecycle.createRefusedError, "warning"],
    },
    {
      name: "pauses with no reply",
      text: "/goal pause",
      op: "pause",
      reply: fakeOmpSuccess(lifecycle.pauseResult, { before: lifecycle.pauseUpdates }),
      notice: undefined,
    },
  ] as const)("completes a /goal control turn that $name", ({ text, op, reply, notice }) =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess({ reply: goalReplies({ [op]: reply }) });
      const session = yield* openRuntime(fake);

      yield* startTurn(session, { text });
      const terminal = yield* session.takeEvent(isTerminal);

      assert.strictEqual(terminal.status, "completed");
      assert.deepStrictEqual(
        goalOps(fake).map(([sent]) => sent),
        [op],
      );
      assert.lengthOf(commandsOf(fake, "prompt"), 0);
      assert.deepStrictEqual<unknown>(
        goalNotices(session.seen),
        notice === undefined ? [] : [notice],
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps a completed goal omp forgot until the next goal command replaces it", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess({
        reply: goalReplies({
          get: fakeOmpSuccess(complete.getResult),
          drop: fakeOmpSuccess(lifecycle.noGoalResult),
          create: fakeOmpSuccess(budgetLimited.createResult, {
            before: [budgetLimited.createUpdated],
          }),
        }),
      });
      const session = yield* openRuntime(fake);
      const done = complete.completeUpdated.goal.objective;

      yield* fake.emit(complete.completeUpdated);
      yield* session.takeEvent(isGoalUpdate);
      yield* startTurn(session, { text: "/goal" });
      yield* session.takeEvent(isTerminal);
      yield* startTurn(session, { ordinal: 2, text: `/goal ${objective}` });
      yield* session.takeEvent(isTerminal);

      assert.deepStrictEqual(goalNotices(session.seen), [[`Goal complete: ${done}`, "info"]]);
      assert.deepStrictEqual(goalOps(fake), [
        ["get", undefined],
        ["drop", undefined],
        ["create", objective],
      ]);
      assert.deepStrictEqual(shownGoals(session.seen), [
        null,
        ["complete", 1_430, 5],
        ["active", 0, 0],
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("shows the goal omp restored when it opens a thread, and keeps a completed one", () =>
    Effect.gen(function* () {
      let goalState: unknown = lifecycle.pausedStateGoal;
      const fake = yield* makeFakeOmpProcess({
        reply: (command) =>
          command.type === "get_state" ? fakeOmpSuccess(stateWith({ goal: goalState })) : undefined,
      });
      const session = yield* openRuntime(fake);
      const completed = {
        objective: complete.completeUpdated.goal.objective,
        status: "complete",
        tokensUsed: 1_430,
        tokenBudget: 60_000,
        timeUsedSeconds: 5,
      } as const;

      goalState = null;
      const resumed = yield* session.runtime.resumeThread({
        providerThread: { ...session.providerThread, goal: completed },
      });
      const cleared = yield* session.runtime.resumeThread({
        providerThread: { ...session.providerThread },
      });

      assert.deepStrictEqual(session.providerThread.goal, {
        objective: "Create a file named goal.txt containing the word ok.",
        status: "paused",
        tokensUsed: 0,
        tokenBudget: 60_000,
        timeUsedSeconds: 4,
      });
      assert.deepStrictEqual(resumed.goal, completed);
      assert.isNull(cleared.goal);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("shows no goal on a new native session that replaced one with a completed goal", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess();
      const session = yield* openRuntime(fake);
      const completed = {
        objective: complete.completeUpdated.goal.objective,
        status: "complete",
        tokensUsed: 1_430,
        tokenBudget: 60_000,
        timeUsedSeconds: 5,
      } as const;

      const fresh = yield* session.runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy: session.policy,
        existingProviderThread: { ...session.providerThread, goal: completed },
        forceNewNativeThread: true,
      });

      assert.lengthOf(commandsOf(fake, "new_session"), 1);
      assert.isNull(fresh.goal);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("shows the goal paused once a tightening restart reopens the session", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspace = yield* fileSystem.makeTempDirectoryScoped({ prefix: "omp-adapter-goal-" });
      const cwd = yield* fileSystem.realPath(workspace);
      const sessionFile = yield* writeSessionFile(path.join(cwd, "session.jsonl"), cwd);
      let goalState: unknown = lifecycle.resumeUpdated.state;
      const fake = yield* makeFakeOmpProcess({
        reply: (command) =>
          command.type === "get_state"
            ? fakeOmpSuccess(stateWith({ sessionFile, goal: goalState }))
            : undefined,
      });
      const session = yield* openRuntime(fake, { cwd });

      // omp pauses an active goal it restores when a process opens it (rpc-goal.ts:337-368).
      goalState = lifecycle.pausedStateGoal;
      yield* runTurn(session, fake, { runtimeMode: "approval-required" });

      assert.lengthOf(fake.spawns(), 2);
      assert.deepStrictEqual(shownGoals(session.seen), [
        ["active", 0, 4],
        ["paused", 0, 4],
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("shows the goal omp keeps after a rollback rewinds the thread", () =>
    Effect.gen(function* () {
      const branched = "/fake/omp-sessions/--workspace--/branched.jsonl";
      let branch: ReadonlyArray<{ entryId: string; text: string }> = [];
      let state = stateWith({ goal: lifecycle.pausedStateGoal });
      const fake = yield* makeFakeOmpProcess({
        reply: (command) => {
          if (command.type === "get_branch_messages") return fakeOmpSuccess({ messages: branch });
          if (command.type === "get_state") return fakeOmpSuccess(state);
          if (command.type !== "branch") return undefined;
          // omp keeps only a goal the kept entries journaled, with no frame (rpc-goal.ts:337-347).
          state = stateWith({ sessionFile: branched, goal: null });
          return fakeOmpSuccess({ text: "Hello omp", cancelled: false });
        },
      });
      const session = yield* openRuntime(fake);
      yield* startTurn(session);
      const prompt = yield* fake.takeCommand("prompt");
      branch = [{ entryId: FIRST_USER_ENTRY, text: "Hello omp" }];
      yield* fake.emit(promptResult(prompt.id));
      yield* session.takeEvent(isTerminal);
      const turns = [
        providerTurn(session.providerThread, 1, strongRef(FIRST_USER_ENTRY)),
        providerTurn(session.providerThread, 2, strongRef(SECOND_USER_ENTRY)),
      ];

      const snapshot = yield* session.runtime.rollbackThread({
        providerThread: session.providerThread,
        target: {
          type: "provider_turn",
          checkpointId: CheckpointId.make("cp-1"),
          appRunOrdinal: 1,
          providerTurn: turns[0]!,
        },
        providerThreadTurns: turns,
      });

      const rebound = yield* session.takeEvent(
        (event): event is Extract<AdapterEvent, { readonly type: "provider_thread.updated" }> =>
          event.type === "provider_thread.updated" &&
          event.providerThread.nativeThreadRef?.nativeId === branched,
      );

      assert.strictEqual(session.providerThread.goal?.status, "paused");
      assert.isNull(snapshot.providerThread.goal);
      assert.isNull(rebound.providerThread.goal);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("publishes the goal only when omp reports a change", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess();
      const session = yield* openRuntime(fake);
      const budgetUpdate = budgetLimited.run.find((frame) => frame.type === "goal_updated");

      yield* emitAll(fake, [budgetLimited.createUpdated, budgetLimited.createUpdated]);
      yield* fake.emit(budgetUpdate!);
      const first = yield* session.takeEvent(isGoalUpdate);
      const second = yield* session.takeEvent(isGoalUpdate);

      assert.deepStrictEqual(
        [first.providerThread.goal?.status, second.providerThread.goal?.status],
        ["active", "budget_limited"],
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([
    { name: "pauses an active goal", goal: [abortPauses.activeUpdated], pauses: [["pause"]] },
    { name: "sends no goal command without a goal", goal: [], pauses: [] },
  ])("Stop $name", ({ goal, pauses }) =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess();
      const session = yield* openRuntime(fake);
      for (const frame of goal) {
        yield* fake.emit(frame);
        yield* session.takeEvent(isGoalUpdate);
      }

      yield* startTurn(session);
      const prompt = yield* fake.takeCommand("prompt");
      const started = yield* session.takeEvent(isProviderTurnUpdate);
      yield* session.runtime.interruptTurn({
        providerThread: session.providerThread,
        providerTurnId: started.providerTurn.id,
      });
      // omp pauses the goal itself when the abort ends a run (STG/10:41).
      if (goal.length > 0) yield* fake.emit(abortPauses.pausedUpdated);
      yield* fake.emit(promptResult(prompt.id, { status: "aborted" }));
      const terminal = yield* session.takeEvent(isTerminal);

      assert.strictEqual(terminal.status, "interrupted");
      assert.deepStrictEqual(
        goalOps(fake).map(([op]) => [op]),
        pauses,
      );
      assert.deepStrictEqual(
        shownGoals(session.seen).at(-1),
        goal.length > 0 ? ["paused", 1_245, 2] : null,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});

interface McpMessage {
  readonly id?: number;
  readonly method: string;
}

const decodeMcpBody = (request: HttpClientRequest.HttpClientRequest): McpMessage => {
  if (request.body._tag !== "Uint8Array") return { method: "" };
  return JSON.parse(new TextDecoder().decode(request.body.body)) as McpMessage;
};

const mcpResult = (id: number | undefined, result: unknown, headers?: Record<string, string>) =>
  Response.json({ jsonrpc: "2.0", id, result }, headers === undefined ? {} : { headers });

const LOOKUP_TOOL = {
  name: "thread_lookup",
  description: "Look up a thread.",
  inputSchema: { type: "object", properties: { query: { type: "string" } } },
  annotations: { readOnlyHint: true },
};

const MCP_REPLIES: Readonly<Record<string, (message: McpMessage) => Response>> = {
  initialize: (message) =>
    mcpResult(
      message.id,
      { protocolVersion: "2025-06-18", capabilities: { tools: {} } },
      { "mcp-session-id": "session-1" },
    ),
  "notifications/initialized": () => new Response(null, { status: 202 }),
  "tools/list": (message) => mcpResult(message.id, { tools: [LOOKUP_TOOL] }),
};

/** A T3 MCP endpoint that lists one read-only tool and fails every call with HTTP 500. */
const fakeMcpServer = HttpClient.make((request) => {
  const message = decodeMcpBody(request);
  const reply = MCP_REPLIES[message.method]?.(message) ?? new Response(null, { status: 500 });
  return Effect.succeed(HttpClientResponse.fromWeb(request, reply));
});

describe("OmpAdapterV2 advisor", () => {
  type RecordedFrame = Record<string, unknown>;
  const { sameRun, wake: wakeRun, midTurn, kept } = advisorFrames;

  const roleOf = (frame: RecordedFrame) => ompRecordString(frame["message"], "role");

  /** The recording answered the probe's prompt; replayed, it answers T3's. */
  const answering = (recorded: ReadonlyArray<RecordedFrame>, promptId: string) =>
    recorded.map((frame) =>
      frame["type"] === "prompt_result" ? { ...frame, id: promptId } : frame,
    );

  const recordedReplies = (recorded: ReadonlyArray<RecordedFrame>) =>
    recorded.flatMap((frame) =>
      frame["type"] === "message_end" && roleOf(frame) === "assistant"
        ? [ompContentText(ompRecordField(frame["message"], "content"))]
        : [],
    );

  /** What T3 shows for each recorded card: one single-note card per line. */
  const recordedNotices = (recorded: ReadonlyArray<RecordedFrame>) =>
    recorded.flatMap((frame) => {
      if (frame["type"] !== "message_end" || roleOf(frame) !== "custom") return [];
      const notes = ompRecordField(ompRecordField(frame["message"], "details"), "notes");
      const [note] = Array.isArray(notes) ? notes : [];
      const severity = ompRecordString(note, "severity");
      const label = severity === "blocker" ? "Blocker" : "Nit";
      return [
        [`${label}: ${ompRecordString(note, "note")}`, severity === "blocker" ? "warning" : "info"],
      ];
    });

  const indexOfFrame = (
    recorded: ReadonlyArray<RecordedFrame>,
    matches: (frame: RecordedFrame) => boolean,
    from = 0,
  ) => recorded.findIndex((frame, index) => index >= from && matches(frame));

  const isCardEnd = (frame: RecordedFrame) =>
    frame["type"] === "message_end" && roleOf(frame) === "custom";

  const advisorNotices = (seen: ReadonlyArray<AdapterEvent>, providerTurnId?: ProviderTurnId) =>
    seen.flatMap((event) =>
      event.type === "turn_item.updated" &&
      event.turnItem.type === "system_notice" &&
      event.turnItem.title === "Advisor review" &&
      (providerTurnId === undefined || event.turnItem.providerTurnId === providerTurnId)
        ? [[event.turnItem.message, event.turnItem.level]]
        : [],
    );

  const openWithWakes = Effect.fnUntraced(function* (fake: FakeOmpProcess) {
    const wakes =
      yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
    const session = yield* openRuntime(fake, {
      continuationRequests: { offer: (request) => Queue.offer(wakes, request).pipe(Effect.asVoid) },
    });
    return { session, wakes };
  });

  /** The orchestrator dispatches the continuation, which starts as a provider-created turn. */
  const startContinuation = Effect.fnUntraced(function* (
    session: OmpTestSession,
    wake: ProviderContinuationRequests.ProviderContinuationRequest,
    ordinal: number,
  ) {
    assert.isTrue(Option.isSome(yield* wake.dispatchIfCurrent!(Effect.void)));
    yield* startTurn(session, { ordinal, continuation: true });
  });

  it.effect(
    "ends the turn at its answer when omp continues the run for the advisor, and runs the rest as a continuation",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeOmpProcess();
        const { session, wakes } = yield* openWithWakes(fake);
        const card = indexOfFrame(sameRun, isCardEnd);

        yield* startTurn(session);
        const prompt = yield* fake.takeCommand("prompt");
        const run = answering(sameRun, prompt.id);
        // Without session_settled, only the prompt's late prompt_result can end the continuation.
        yield* emitAll(fake, run.slice(0, -1));
        const answered = yield* session.takeEvent(isTerminal);
        const wake = yield* Queue.take(wakes);
        yield* startContinuation(session, wake, 2);
        const followUp = yield* session.takeEvent(isTerminal);
        yield* fake.emit(run.at(-1)!);
        yield* runTurn(session, fake, { ordinal: 3 });

        assert.deepStrictEqual(
          session.seen.filter(isTerminal).map((event) => event.status),
          ["completed", "completed", "completed"],
        );
        assert.deepStrictEqual(
          assistantTexts(session.seen, answered.providerTurnId),
          recordedReplies(sameRun.slice(0, card)),
        );
        assert.deepStrictEqual(
          assistantTexts(session.seen, followUp.providerTurnId),
          recordedReplies(sameRun.slice(card)),
        );
        assert.deepStrictEqual(advisorNotices(session.seen, answered.providerTurnId), []);
        assert.deepStrictEqual(
          advisorNotices(session.seen, followUp.providerTurnId),
          recordedNotices(sameRun),
        );
        assert.strictEqual(wake.notification?.summary, "Advisor follow-up");
        assert.lengthOf(commandsOf(fake, "prompt"), 2);
        assert.deepStrictEqual(backgroundWorkPending(session.seen), []);
        assert.isFalse(yield* session.runtime.hasPendingBackgroundWork!);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "hands unread steers and an open dialog to the advisor continuation without losing any",
    () =>
      Effect.gen(function* () {
        const lateSteerText = "Mention the guard.";
        // omp acknowledges this steer only after the answer ended its turn.
        const fake = yield* makeFakeOmpProcess({
          reply: (command) =>
            command.type === "prompt" && command.message === lateSteerText
              ? FAKE_OMP_UNANSWERED
              : undefined,
        });
        const { session, wakes } = yield* openWithWakes(fake);
        const card = indexOfFrame(sameRun, isCardEnd);
        const agentEnd = indexOfFrame(sameRun, (frame) => frame["type"] === "agent_end");
        const dialogId = "15dbc451eb016398";
        const steerText = "Keep it to two lines.";

        yield* startTurn(session);
        const prompt = yield* fake.takeCommand("prompt");
        const firstTurn = (yield* session.takeEvent(isProviderTurnUpdate)).providerTurn.id;
        yield* fake.emit({
          type: "extension_ui_request",
          id: dialogId,
          method: "confirm",
          title: "Deploy",
          message: "Deploy to staging?",
          timeout: 60_000,
        });
        const asked = yield* session.takeEvent(isRuntimeRequestUpdate);
        yield* emitAll(fake, sameRun.slice(0, card + 1));
        yield* session.runtime.steerTurn(steerInput(session, firstTurn, steerText));
        const steer = yield* fake.takeCommand("prompt");
        yield* session.takeEvent(isSteerUpdate);
        yield* session.runtime.steerTurn(steerInput(session, firstTurn, lateSteerText));
        const lateSteer = yield* fake.takeCommand("prompt");
        yield* emitAll(fake, sameRun.slice(card + 1, agentEnd));
        const answered = yield* session.takeEvent(isTerminal);
        yield* fake.respond(lateSteer);
        // Taken before the continuation starts, so the ack arrived while it was pending.
        yield* session.takeEvent(isSteerUpdate);
        yield* session.runtime.respondToRuntimeRequest({
          requestId: asked.runtimeRequest.id,
          answers: { [dialogId]: "yes" },
        });
        // omp delivers both steers at the continuation's next step, then yields every prompt.
        yield* emitAll(fake, [
          userMessageStart(steerText, true),
          userMessageStart(lateSteerText, true),
          ...assistantReply("Kept it to two lines and mentioned the guard."),
          ...answering(sameRun.slice(agentEnd, -1), prompt.id),
          promptResult(steer.id),
          promptResult(lateSteer.id),
        ]);
        const wake = yield* Queue.take(wakes);
        yield* startContinuation(session, wake, 2);
        const followUp = yield* session.takeEvent(isTerminal);
        yield* fake.emit(sameRun.at(-1)!);
        yield* session.takeEvent(isBackgroundRelease);

        assert.deepStrictEqual([answered.status, followUp.status], ["completed", "completed"]);
        assert.deepStrictEqual(answered.retainedRuntimeRequestIds, [asked.runtimeRequest.id]);
        assert.deepStrictEqual(runtimeRequestStatuses(session.seen), ["pending", "resolved"]);
        assert.deepStrictEqual(
          fake.frames().filter((frame) => frame.type === "extension_ui_response"),
          [{ type: "extension_ui_response", id: dialogId, confirmed: true }],
        );
        const steerUpdates = session.seen.filter(isSteerUpdate);
        assert.deepStrictEqual(
          steerUpdates.map((event) => [event.messageId, event.status]),
          [
            [steerInput(session, firstTurn, steerText).message.messageId, "pending"],
            [steerInput(session, firstTurn, lateSteerText).message.messageId, "pending"],
            [steerInput(session, firstTurn, steerText).message.messageId, "completed"],
            [steerInput(session, firstTurn, lateSteerText).message.messageId, "completed"],
          ],
        );
        // The late steer's ack lands while the continuation is pending; both complete inside it.
        assert.isAbove(session.seen.indexOf(steerUpdates[1]!), session.seen.indexOf(answered));
        assert.isAbove(session.seen.indexOf(steerUpdates[2]!), session.seen.indexOf(answered));
        assert.deepStrictEqual(assistantTexts(session.seen, followUp.providerTurnId), [
          ...recordedReplies(sameRun.slice(card)),
          "Kept it to two lines and mentioned the guard.",
        ]);
        assert.deepStrictEqual(backgroundWorkPending(session.seen), [true, false]);
        assert.isFalse(yield* session.runtime.hasPendingBackgroundWork!);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "shows a card omp keeps after the answer in that turn when the run does not continue",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeOmpProcess();
        const { session, wakes } = yield* openWithWakes(fake);

        yield* startTurn(session);
        const prompt = yield* fake.takeCommand("prompt");
        yield* emitAll(fake, answering(kept, prompt.id));
        const terminal = yield* session.takeEvent(isTerminal);

        assert.strictEqual(terminal.status, "completed");
        assert.deepStrictEqual(
          assistantTexts(session.seen, terminal.providerTurnId),
          recordedReplies(kept),
        );
        assert.deepStrictEqual(
          advisorNotices(session.seen, terminal.providerTurnId),
          recordedNotices(kept),
        );
        assert.strictEqual(yield* Queue.size(wakes), 0);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps the turn whole when omp continues on a steer that follows a kept card", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess();
      const { session, wakes } = yield* openWithWakes(fake);
      const card = indexOfFrame(kept, isCardEnd);
      const steerText = "Also say how to call it.";

      yield* startTurn(session);
      const prompt = yield* fake.takeCommand("prompt");
      const firstTurn = (yield* session.takeEvent(isProviderTurnUpdate)).providerTurn.id;
      yield* emitAll(fake, kept.slice(0, card + 1));
      yield* session.runtime.steerTurn(steerInput(session, firstTurn, steerText));
      const steer = yield* fake.takeCommand("prompt");
      yield* emitAll(fake, [
        userMessageStart(steerText, true),
        ...assistantReply("Call it as divide(6, 3)."),
        ...answering(kept.slice(card + 1), prompt.id),
        promptResult(steer.id),
      ]);
      const terminal = yield* session.takeEvent(isTerminal);

      assert.strictEqual(terminal.status, "completed");
      assert.deepStrictEqual(assistantTexts(session.seen, terminal.providerTurnId), [
        ...recordedReplies(kept),
        "Call it as divide(6, 3).",
      ]);
      assert.deepStrictEqual(
        advisorNotices(session.seen, terminal.providerTurnId),
        recordedNotices(kept),
      );
      assert.strictEqual(yield* Queue.size(wakes), 0);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("shows the advisor's notes while the agent works as notices in one turn", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess();
      const { session, wakes } = yield* openWithWakes(fake);

      yield* startTurn(session);
      const prompt = yield* fake.takeCommand("prompt");
      yield* emitAll(fake, answering(midTurn, prompt.id));
      const terminal = yield* session.takeEvent(isTerminal);

      assert.lengthOf(session.seen.filter(isTerminal), 1);
      assert.deepStrictEqual(
        assistantTexts(session.seen, terminal.providerTurnId),
        recordedReplies(midTurn),
      );
      assert.deepStrictEqual(advisorNotices(session.seen), recordedNotices(midTurn));
      assert.deepStrictEqual(
        recordedNotices(midTurn).map(([, level]) => level),
        ["warning", "warning", "warning", "info"],
      );
      assert.strictEqual(yield* Queue.size(wakes), 0);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("shows the card in the run omp starts for it after the turn settled", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess();
      const { session, wakes } = yield* openWithWakes(fake);
      const wakeStart = indexOfFrame(wakeRun, (frame) => frame["type"] === "agent_start", 1);

      yield* startTurn(session);
      const prompt = yield* fake.takeCommand("prompt");
      yield* emitAll(fake, answering(wakeRun.slice(0, wakeStart), prompt.id));
      const answered = yield* session.takeEvent(isTerminal);
      yield* emitAll(fake, wakeRun.slice(wakeStart));
      const wake = yield* Queue.take(wakes);
      yield* startContinuation(session, wake, 2);
      const followUp = yield* session.takeEvent(isTerminal);

      assert.deepStrictEqual(
        assistantTexts(session.seen, answered.providerTurnId),
        recordedReplies(wakeRun.slice(0, wakeStart)),
      );
      assert.deepStrictEqual(advisorNotices(session.seen, answered.providerTurnId), []);
      assert.deepStrictEqual(
        advisorNotices(session.seen, followUp.providerTurnId),
        recordedNotices(wakeRun),
      );
      assert.deepStrictEqual(
        assistantTexts(session.seen, followUp.providerTurnId),
        recordedReplies(wakeRun.slice(wakeStart)),
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("shows a card omp keeps while no turn runs with the next message", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess();
      const session = yield* openRuntime(fake);
      const card = indexOfFrame(kept, isCardEnd);

      const first = yield* runTurn(session, fake);
      yield* emitAll(fake, kept.slice(card - 1, card + 1));
      // Handled after the card, so the card arrived while no turn ran.
      yield* fake.emit({
        type: "session_info_update",
        sessionId: FAKE_OMP_IDLE_STATE.sessionId,
        title: "Divide helper",
      });
      yield* session.takeEvent(
        (event): event is AdapterEvent => event.type === "provider_thread.updated",
      );
      const next = yield* runTurn(session, fake, { ordinal: 2 });

      assert.deepStrictEqual(advisorNotices(session.seen, first.providerTurnId), []);
      assert.deepStrictEqual(
        advisorNotices(session.seen, next.providerTurnId),
        recordedNotices(kept),
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});
