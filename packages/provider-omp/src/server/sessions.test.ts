import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CheckpointId,
  NodeId,
  ProviderDriverKind,
  ProviderThreadId,
  ProviderTurnId,
  type OrchestrationV2ProviderTurn,
} from "@t3tools/contracts";
import type { ProviderAdapterV2RollbackTarget } from "@t3tools/provider-core/server/ProviderAdapter";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import forkFixture from "./__fixtures__/sessions/fork-entries.json" with { type: "json" };
import { OmpRpcError } from "./rpc.ts";
import {
  FAKE_OMP_IDLE_STATE,
  FAKE_OMP_UNANSWERED,
  fakeOmpSuccess,
  makeFakeOmpProcess,
  type FakeOmpProcess,
} from "./rpc.testkit.ts";
import {
  deleteOmpSessionFiles,
  forkOmpSessionInSeparateProcess,
  ompForkBoundary,
  ompForkEntryId,
  ompRollbackEntryId,
  ompSessionDirForCwd,
  ompTurnUserEntryId,
  sessionFileMatchesCwd,
  type OmpForkProcessInput,
} from "./sessions.ts";
import type { OmpCommandFrame, OmpRecord } from "./wire.ts";

const twoTurnEntries: ReadonlyArray<OmpRecord> = forkFixture.twoTurnEntries.entries;
const FIRST_USER_ENTRY = "e1078788";
const SECOND_USER_ENTRY = "22d686f8";

describe("ompSessionDirForCwd", () => {
  it.effect("encodes the cwd into one directory under omp-sessions", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      assert.strictEqual(
        ompSessionDirForCwd(path, "/state", "/Users/me/my project/v2.0"),
        "/state/omp-sessions/--Users-me-my-project-v2.0--",
      );
      assert.strictEqual(
        ompSessionDirForCwd(path, "/state", "/workspace"),
        "/state/omp-sessions/--workspace--",
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});

describe("sessionFileMatchesCwd", () => {
  it.effect("compares realpaths and lets a not-yet-written file match", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fileSystem.makeTempDirectoryScoped();
      const workspace = path.join(root, "workspace");
      const linked = path.join(root, "linked");
      const other = path.join(root, "other");
      yield* fileSystem.makeDirectory(workspace);
      yield* fileSystem.makeDirectory(other);
      yield* fileSystem.symlink(workspace, linked);
      const sessionFile = path.join(root, "session.jsonl");
      yield* fileSystem.writeFileString(
        sessionFile,
        [
          { type: "title", v: 1, title: "", pad: " ".repeat(40) },
          { type: "session", version: 3, id: "s", cwd: linked },
          { type: "message", id: "a", parentId: null },
        ]
          .map((line) => JSON.stringify(line))
          .join("\n"),
      );

      assert.isTrue(yield* sessionFileMatchesCwd(sessionFile, linked));
      assert.isTrue(yield* sessionFileMatchesCwd(sessionFile, workspace));
      assert.isFalse(yield* sessionFileMatchesCwd(sessionFile, other));
      assert.isTrue(yield* sessionFileMatchesCwd(path.join(root, "lazy.jsonl"), other));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("ompTurnUserEntryId", () => {
  it("takes the first user entry the turn added to the branch", () => {
    const one = { entryId: "u1", text: "one" };
    const two = { entryId: "u2", text: "two" };
    const steer = { entryId: "s2", text: "also" };
    assert.strictEqual(ompTurnUserEntryId([one], [one, two, steer]), "u2");
    assert.isUndefined(ompTurnUserEntryId([one], [one]));
  });
});

describe("ompRollbackEntryId and ompForkBoundary", () => {
  const driver = ProviderDriverKind.make("omp");
  const ref = (nativeId: string | null, strength: "strong" | "weak" = "strong") => ({
    driver,
    nativeId,
    strength,
  });
  const turn = (
    ordinal: number,
    nativeTurnRef: OrchestrationV2ProviderTurn["nativeTurnRef"],
    extra: Partial<OrchestrationV2ProviderTurn> = {},
  ): OrchestrationV2ProviderTurn => ({
    id: ProviderTurnId.make(`turn-${ordinal}`),
    providerThreadId: ProviderThreadId.make("provider-thread"),
    nodeId: NodeId.make(`node-${ordinal}`),
    runAttemptId: null,
    nativeTurnRef,
    nativeThreadRef: ref("/state/omp-sessions/--w--/a.jsonl"),
    ordinal,
    status: "completed",
    startedAt: null,
    completedAt: null,
    ...extra,
  });
  const captured = [turn(1, ref("u1")), turn(2, ref("u2")), turn(3, ref("u3"))];
  const to = (
    ordinal: number,
    turns: ReadonlyArray<OrchestrationV2ProviderTurn>,
  ): ProviderAdapterV2RollbackTarget =>
    ordinal === 0
      ? { type: "thread_start", checkpointId: CheckpointId.make("c0"), appRunOrdinal: 0 }
      : {
          type: "provider_turn",
          checkpointId: CheckpointId.make(`c${ordinal}`),
          appRunOrdinal: ordinal,
          providerTurn: turns.find((candidate) => candidate.ordinal === ordinal)!,
        };
  const withoutSessionRef = (providerTurn: OrchestrationV2ProviderTurn) => {
    const { nativeThreadRef: _sessionRef, ...rest } = providerTurn;
    return rest;
  };
  const withTurn2 = (replacement: OrchestrationV2ProviderTurn) => [
    captured[0]!,
    replacement,
    captured[2]!,
  ];

  it.each([
    ["thread start", 0, captured, "u1"],
    ["an earlier checkpoint", 1, captured, "u2"],
    ["the previous checkpoint", 2, captured, "u3"],
    ["the last checkpoint", 3, captured, null],
    ["a command-only turn after the target", 1, withTurn2(turn(2, null)), "u3"],
    [
      "a dropped prompt after the target",
      1,
      withTurn2(turn(2, ref("d2", "weak"), { promptDropped: true })),
      "u3",
    ],
    [
      "an uncaptured first discarded turn",
      1,
      withTurn2(turn(2, ref("synthetic", "weak"))),
      undefined,
    ],
    ["a strong ref without an entry id", 1, withTurn2(turn(2, ref(null))), undefined],
    ["turns listed out of order", 0, [...captured].reverse(), "u1"],
    ["turns without a session ref", 1, captured.map(withoutSessionRef), "u2"],
  ] as const)("resolves %s", (_name, ordinal, turns, expected) => {
    assert.strictEqual(ompRollbackEntryId(to(ordinal, turns), turns), expected);
  });

  it.each([
    ["the whole session", undefined, captured, { through: undefined, next: undefined }],
    ["the first turn", 1, captured, { through: "u1", next: "u2" }],
    ["the last turn", 3, captured, { through: "u3", next: undefined }],
    ["past a command-only turn", 1, withTurn2(turn(2, null)), { through: "u1", next: "u3" }],
    [
      "past a dropped prompt",
      1,
      withTurn2(turn(2, ref("d2", "weak"), { promptDropped: true })),
      { through: "u1", next: "u3" },
    ],
    [
      "before an uncaptured turn",
      1,
      withTurn2(turn(2, ref("synthetic", "weak"))),
      { error: "omp turn 2 has no captured session entry to fork before" },
    ],
  ] as const)("bounds a fork through %s", (_name, ordinal, turns, expected) => {
    const target = turns.find((candidate) => candidate.ordinal === ordinal);
    assert.deepStrictEqual(ompForkBoundary(target, turns), expected);
  });
});

describe("ompForkEntryId", () => {
  it("walks from the next turn's user entry to the nearest message entry", () => {
    const entryId = ompForkEntryId(twoTurnEntries, SECOND_USER_ENTRY);
    assert.strictEqual(entryId, forkFixture.forkEntryId);
    assert.strictEqual(forkFixture.forkedEntries.leafId, entryId, "omp keeps the target entry");
    assert.isUndefined(ompForkEntryId(twoTurnEntries, FIRST_USER_ENTRY));
    assert.isUndefined(ompForkEntryId(twoTurnEntries, undefined));
    assert.isUndefined(ompForkEntryId(twoTurnEntries, "missing"));
  });

  it("skips hidden custom messages and ends on a parent cycle", () => {
    const entries = [
      { type: "message", id: "a1", parentId: null },
      { type: "custom_message", id: "notice", parentId: "a1", display: false },
      { type: "message", id: "u2", parentId: "notice" },
      { type: "custom", id: "loop-a", parentId: "loop-b" },
      { type: "custom", id: "loop-b", parentId: "loop-a" },
      { type: "message", id: "u3", parentId: "loop-a" },
    ];
    assert.strictEqual(ompForkEntryId(entries, "u2"), "a1");
    assert.isUndefined(ompForkEntryId(entries, "u3"));
  });
});

/** The two-turn session as omp writes it: title slot, header, then one entry per line. */
const twoTurnSessionLines = [
  { type: "title", v: 1, title: "", pad: " ".repeat(40) },
  { type: "session", version: 3, id: "01a212a6-1800-74b3-b874-65c76d6ce399", cwd: "/workspace" },
  ...twoTurnEntries,
]
  .map((line) => `${JSON.stringify(line)}\n`)
  .join("");

const sessionFileTree = (directory: string, name: string) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const file = path.join(directory, `${name}.jsonl`);
    yield* fileSystem.makeDirectory(path.join(directory, name, "spill"), { recursive: true });
    yield* fileSystem.writeFileString(path.join(directory, name, "spill", "1.log"), "out");
    yield* fileSystem.writeFileString(path.join(directory, `.${name}.jsonl.lock.os`), "");
    yield* fileSystem.writeFileString(file, twoTurnSessionLines);
    return file;
  });

const listDirectory = (directory: string) =>
  FileSystem.FileSystem.pipe(
    Effect.flatMap((fileSystem) => fileSystem.readDirectory(directory)),
    Effect.map((names) => [...names].sort()),
  );

describe("deleteOmpSessionFiles", () => {
  it.effect("removes the file, its lock and its artifacts, and nothing else", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fileSystem.makeTempDirectoryScoped();
      const doomed = yield* sessionFileTree(directory, "doomed");
      yield* sessionFileTree(directory, "kept");
      yield* fileSystem.writeFileString(path.join(directory, ".jsonl"), "");

      yield* deleteOmpSessionFiles(doomed);
      yield* deleteOmpSessionFiles(path.join(directory, ".jsonl"));
      yield* deleteOmpSessionFiles(path.join(directory, "never-written.jsonl"));

      assert.deepStrictEqual(yield* listDirectory(directory), [
        ".kept.jsonl.lock.os",
        "kept",
        "kept.jsonl",
      ]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("forkOmpSessionInSeparateProcess", () => {
  const sessionState = (sessionFile: string) => ({ ...FAKE_OMP_IDLE_STATE, sessionFile });

  /** Runs the fork against a fake omp; `finish` answers the held command, then omp exits. */
  const runFork = (
    fake: FakeOmpProcess,
    input: OmpForkProcessInput,
    finish: (fake: FakeOmpProcess) => Effect.Effect<void>,
  ) =>
    Effect.gen(function* () {
      const fork = yield* forkOmpSessionInSeparateProcess(input).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner),
        Effect.exit,
        Effect.forkChild,
      );
      yield* finish(fake);
      yield* fake.exit(0);
      return yield* Fiber.join(fork);
    });

  const answerSecond = (type: OmpCommandFrame["type"], data: unknown) => (fake: FakeOmpProcess) =>
    fake.takeCommand(type).pipe(
      Effect.andThen(fake.takeCommand(type)),
      Effect.flatMap((command) => fake.respond(command, data)),
    );

  const setup = Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const root = yield* fileSystem.makeTempDirectoryScoped();
    const sourceDir = path.join(root, "source");
    const sessionDir = path.join(root, "target");
    yield* fileSystem.makeDirectory(sourceDir);
    yield* fileSystem.makeDirectory(sessionDir);
    const source = yield* sessionFileTree(sourceDir, "source");
    const copy = yield* sessionFileTree(sessionDir, "copy");
    const input: OmpForkProcessInput = {
      binaryPath: "omp",
      launchArgs: ["--profile", "work", "--tools", "read"],
      environment: { PATH: "/usr/bin", T3CODE_THREAD_ID: "source-thread" },
      cwd: path.join(root, "next-worktree"),
      sessionDir,
      sourceSessionFile: source,
      unattendedOverlayPath: path.join(root, "omp-overlays", "unattended.yml"),
      throughTurnUserEntryId: FIRST_USER_ENTRY,
      nextTurnUserEntryId: SECOND_USER_ENTRY,
    };
    return {
      input,
      sourceDir,
      sessionDir,
      source,
      copy,
      forked: path.join(sessionDir, "forked.jsonl"),
    };
  });

  /** Answers the first `get_state` with the copy and holds the second, as `fork` succeeds. */
  const makeForkingOmp = (copy: string) => {
    let states = 0;
    return makeFakeOmpProcess({
      reply: (command) => {
        if (command.type === "get_state") {
          states += 1;
          return states === 1 ? fakeOmpSuccess(sessionState(copy)) : FAKE_OMP_UNANSWERED;
        }
        if (command.type === "fork") return fakeOmpSuccess({ cancelled: false });
        return undefined;
      },
    });
  };

  it.effect("forks a copy at the turn boundary and removes the copy", () =>
    Effect.gen(function* () {
      const { input, sessionDir, sourceDir, copy, forked } = yield* setup;
      const fake = yield* makeForkingOmp(copy);

      const result = yield* runFork(fake, input, answerSecond("get_state", sessionState(forked)));

      assert.strictEqual(yield* result, forked);
      const [spawn] = fake.spawns();
      assert.strictEqual(spawn?.cwd, input.cwd);
      assert.deepStrictEqual(spawn?.args, [
        "--mode",
        "rpc-ui",
        "--fork",
        input.sourceSessionFile,
        "--session-dir",
        sessionDir,
        "--no-title",
        "--profile",
        "work",
        "--no-extensions",
        "--no-tools",
        "--offline",
        "--config",
        input.unattendedOverlayPath,
      ]);
      assert.isUndefined(spawn?.env["T3CODE_THREAD_ID"]);
      assert.deepStrictEqual(
        fake.commands().map((command) => command.type),
        ["negotiate_protocol", "get_state", "fork", "get_state"],
        "the fork target comes from the copy on disk, not get_entries",
      );
      assert.deepInclude(fake.commands()[2], { type: "fork", entryId: forkFixture.forkEntryId });
      assert.deepStrictEqual(yield* listDirectory(sessionDir), []);
      assert.lengthOf(yield* listDirectory(sourceDir), 3);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("fails the fork and removes both files when omp is stopped before a clean exit", () =>
    Effect.gen(function* () {
      const { input, sessionDir, sourceDir, copy } = yield* setup;
      const forked = yield* sessionFileTree(sessionDir, "forked");
      const fake = yield* makeForkingOmp(copy);

      const result = yield* runFork(fake, input, (fake) =>
        answerSecond("get_state", sessionState(forked))(fake).pipe(Effect.andThen(fake.exit(143))),
      );

      assert.deepInclude(yield* Effect.flip(result), {
        detail: "omp stopped before it finished copying the session's artifacts",
      });
      assert.deepStrictEqual(yield* listDirectory(sessionDir), []);
      assert.lengthOf(yield* listDirectory(sourceDir), 3);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps the copy as the fork of a whole session", () =>
    Effect.gen(function* () {
      const { input, sessionDir, copy } = yield* setup;
      const fake = yield* makeFakeOmpProcess({
        reply: (command) => (command.type === "get_state" ? FAKE_OMP_UNANSWERED : undefined),
      });

      const result = yield* runFork(
        fake,
        { ...input, throughTurnUserEntryId: undefined, nextTurnUserEntryId: undefined },
        (fake) =>
          fake
            .takeCommand("get_state")
            .pipe(Effect.flatMap((command) => fake.respond(command, sessionState(copy)))),
      );

      assert.strictEqual(yield* result, copy);
      assert.deepStrictEqual(
        fake.commands().map((command) => command.type),
        ["negotiate_protocol", "get_state"],
      );
      assert.lengthOf(yield* listDirectory(sessionDir), 3);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("removes the copy when omp refuses the fork or the branch moved", () =>
    Effect.gen(function* () {
      const { input, sessionDir, copy } = yield* setup;
      const refusing = yield* makeFakeOmpProcess({
        reply: (command) => {
          if (command.type === "get_state") return fakeOmpSuccess(sessionState(copy));
          if (command.type === "fork") return FAKE_OMP_UNANSWERED;
          return undefined;
        },
      });

      const refused = yield* runFork(refusing, input, (fake) =>
        fake
          .takeCommand("fork")
          .pipe(
            Effect.flatMap((command) =>
              fake.respondFailure(command, "Invalid entry ID for forking: 00633b09"),
            ),
          ),
      );

      assert.deepInclude(yield* Effect.flip(refused), {
        operation: "fork",
        detail: "Invalid entry ID for forking: 00633b09",
      });
      assert.deepStrictEqual(yield* listDirectory(sessionDir), []);

      const moved = yield* sessionFileTree(sessionDir, "copy");
      const diverged = yield* makeFakeOmpProcess({
        reply: (command) => (command.type === "get_state" ? FAKE_OMP_UNANSWERED : undefined),
      });

      const result = yield* runFork(
        diverged,
        { ...input, throughTurnUserEntryId: "an-abandoned-branch" },
        (fake) =>
          fake
            .takeCommand("get_state")
            .pipe(Effect.flatMap((command) => fake.respond(command, sessionState(moved)))),
      );

      const error = yield* Effect.flip(result);
      assert.instanceOf(error, OmpRpcError);
      assert.strictEqual(error.detail, "the fork point is not on the branch of the turn it keeps");
      assert.notInclude(
        diverged.commands().map((command) => command.type),
        "fork",
      );
      assert.deepStrictEqual(yield* listDirectory(sessionDir), []);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("never removes the source when omp reports it as the new file", () =>
    Effect.gen(function* () {
      const { input, sourceDir, source } = yield* setup;
      const fake = yield* makeFakeOmpProcess({
        reply: (command) => (command.type === "get_state" ? FAKE_OMP_UNANSWERED : undefined),
      });

      const result = yield* runFork(fake, input, (fake) =>
        fake
          .takeCommand("get_state")
          .pipe(Effect.flatMap((command) => fake.respond(command, sessionState(source)))),
      );

      assert.deepInclude(yield* Effect.flip(result), {
        detail: "omp did not create a distinct session file",
      });
      assert.lengthOf(yield* listDirectory(sourceDir), 3);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
