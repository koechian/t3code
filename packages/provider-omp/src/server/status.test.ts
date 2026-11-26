import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as TestClock from "effect/testing/TestClock";
import { noCompatibilityPolicy } from "@t3tools/provider-core/server/compatibility";

import catalog from "./__fixtures__/commands/available-commands.json" with { type: "json" };
import picker from "./__fixtures__/modelCapabilities/model-picker.json" with { type: "json" };
import enabledModelsNoCredentials from "./__fixtures__/status/enabled-models-no-credentials.json" with { type: "json" };
import noDefaultModel from "./__fixtures__/status/no-default-model.json" with { type: "json" };
import {
  fakeOmpFailure,
  fakeOmpSuccess,
  makeFakeOmpProcess,
  type FakeOmpInvocation,
  type FakeOmpScript,
} from "./rpc.testkit.ts";
import type { OmpSettings } from "../settings.ts";
import type { OmpUnattendedOverlay } from "./launch.ts";
import { checkOmpProviderStatus, discoverOmpCommandsForCwd } from "./status.ts";

const settings = { enabled: true, binaryPath: "omp", launchArgs: "", customModels: [] } as const;
const PROBE_CWD = "/omp-status-probe";
const UNATTENDED_OVERLAY_PATH = "/state/omp-overlays/unattended.yml";
const UNATTENDED_OVERLAY: OmpUnattendedOverlay = {
  path: UNATTENDED_OVERLAY_PATH,
  ensure: Effect.void,
};

const BROKEN_ADVISORY = {
  status: "broken",
  message: null,
  recommendedVersion: null,
  recommendedRange: ">=19.1.1",
} as const;

const versionCli =
  (version = "19.1.1") =>
  (invocation: FakeOmpInvocation) =>
    invocation.args.includes("--version") ? { stdout: `omp/${version}\n`, exitCode: 0 } : undefined;

const probeScript = (script: Omit<FakeOmpScript, "cli"> = {}): FakeOmpScript => ({
  cli: versionCli(),
  reply: (command) => {
    switch (command.type) {
      case "get_model_picker":
        return fakeOmpSuccess(picker);
      case "get_available_commands":
        return fakeOmpSuccess(catalog);
      default:
        return undefined;
    }
  },
  ...script,
});

const isRpcSpawn = (invocation: FakeOmpInvocation) => invocation.args.includes("--mode");

const checkWith = (script: FakeOmpScript, overrides: Partial<OmpSettings> = {}) =>
  Effect.gen(function* () {
    const fake = yield* makeFakeOmpProcess(script);
    const snapshot = yield* checkOmpProviderStatus(
      { ...settings, ...overrides },
      {},
      PROBE_CWD,
      noCompatibilityPolicy,
      UNATTENDED_OVERLAY,
    ).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner));
    return { snapshot, spawns: fake.spawns() };
  });

const encoder = new TextEncoder();

/** A process that writes `stderr`, then exits with `exitCode` once `answered` completes. */
const exitingProcess = (input: {
  readonly stdout?: string;
  readonly stderr?: string;
  readonly exitCode: number;
  readonly answered?: Effect.Effect<void>;
}) => {
  const answered = input.answered ?? Effect.void;
  const bytes = (text: string | undefined) =>
    text === undefined ? Stream.empty : Stream.make(encoder.encode(text));
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(999_999_998),
    exitCode: Effect.as(answered, ChildProcessSpawner.ExitCode(input.exitCode)),
    isRunning: Effect.succeed(false),
    kill: () => Effect.void,
    unref: Effect.succeed(Effect.void),
    stdin: Sink.drain,
    stdout: Stream.unwrap(Effect.as(answered, bytes(input.stdout))),
    stderr: bytes(input.stderr),
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });
};

describe("checkOmpProviderStatus", () => {
  it.effect("reports a disabled instance without starting omp", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkOmpProviderStatus(
        { ...settings, enabled: false },
        {},
        PROBE_CWD,
        noCompatibilityPolicy,
        UNATTENDED_OVERLAY,
      ).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() => Effect.die("A disabled OMP must not spawn")),
        ),
      );
      assert.equal(snapshot.status, "disabled");
      assert.equal(snapshot.message, "OMP is disabled in T3 Code settings.");
      assert.deepEqual(
        snapshot.models.map((model) => model.slug),
        ["default"],
      );
    }),
  );

  it.effect("is ready with the picker's models and the workspace's commands", () =>
    Effect.gen(function* () {
      const { snapshot, spawns } = yield* checkWith(probeScript());
      assert.equal(snapshot.status, "ready");
      assert.equal(snapshot.version, "19.1.1");
      assert.deepEqual(snapshot.auth, { status: "authenticated", type: "omp" });
      assert.deepEqual(
        snapshot.models.map((model) => model.slug),
        [
          "default",
          "anthropic/claude-haiku-5-5",
          "anthropic/claude-opus-5-5",
          "openrouter/aion-labs/aion-2.0",
          "amazon-bedrock/us.meta.llama4-scout-17b-instruct-v1:0",
        ],
      );
      const commandNames = snapshot.slashCommands.map((command) => command.name);
      assert.include(commandNames, "compact");
      assert.notInclude(commandNames, "stats");
      assert.deepEqual(
        snapshot.skills.map((skill) => skill.name),
        ["handoff", "research"],
      );
      const probe = spawns.find(isRpcSpawn);
      assert.equal(probe?.cwd, PROBE_CWD);
      // Health checks stay passive: no session file, extensions, tools (MCP servers) or network.
      assert.includeMembers(
        [...(probe?.args ?? [])],
        ["--no-session", "--no-extensions", "--no-tools", "--offline"],
      );
    }),
  );

  it.effect("reports an omp without usable models as signed out", () =>
    Effect.gen(function* () {
      const { snapshot } = yield* checkWith(
        probeScript({
          reply: (command) =>
            command.type === "get_model_picker"
              ? fakeOmpSuccess({ rows: [], recent: 0, total: 0 })
              : undefined,
        }),
      );
      assert.equal(snapshot.status, "warning");
      assert.equal(snapshot.auth.status, "unauthenticated");
      assert.include(snapshot.message ?? "", "OMP has no usable models.");
    }),
  );

  it.effect("keeps the models when only the command catalog fails", () =>
    Effect.gen(function* () {
      const { snapshot } = yield* checkWith(
        probeScript({
          reply: (command) => {
            if (command.type === "get_model_picker") return fakeOmpSuccess(picker);
            if (command.type === "get_available_commands") return fakeOmpFailure("unavailable");
            return undefined;
          },
        }),
      );
      assert.equal(snapshot.status, "ready");
      assert.equal(snapshot.auth.status, "authenticated");
      assert.lengthOf(snapshot.models, 5);
      assert.deepEqual(snapshot.slashCommands, []);
    }),
  );

  // Ported from Pi: a malformed inventory must not restore stale models.
  it.effect("keeps omp selectable with unknown sign-in when the picker result is malformed", () =>
    Effect.gen(function* () {
      const { snapshot } = yield* checkWith(
        probeScript({
          reply: (command) =>
            command.type === "get_model_picker"
              ? fakeOmpSuccess({ rows: [{ provider: "anthropic" }], recent: 0, total: 1 })
              : undefined,
        }),
      );
      assert.equal(snapshot.status, "ready");
      assert.equal(snapshot.auth.status, "unknown");
      assert.deepEqual(
        snapshot.models.map((model) => model.slug),
        ["default"],
      );
      assert.include(snapshot.message ?? "", "could not refresh its models and commands");
    }),
  );

  it.effect("reports an omp without RPC protocol v2 as an error", () =>
    Effect.gen(function* () {
      const { snapshot } = yield* checkWith(
        probeScript({
          ready: { type: "ready", protocolVersion: 1, supportedProtocolVersions: [1] },
        }),
      );
      assert.equal(snapshot.status, "error");
      assert.equal(snapshot.version, "19.1.1");
      assert.include(snapshot.message ?? "", "does not support RPC protocol v2");
    }),
  );

  it.effect("stops at a version the policy rates broken", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess(probeScript({}));
      const snapshot = yield* checkOmpProviderStatus(
        { ...settings, binaryPath: "/opt/omp/bin/omp" },
        {},
        PROBE_CWD,
        (_driver, version) => Effect.succeed(version === "19.1.1" ? BROKEN_ADVISORY : undefined),
        UNATTENDED_OVERLAY,
      ).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner));
      assert.equal(snapshot.status, "error");
      assert.include(snapshot.message ?? "", "OMP 19.1.1 does not work");
      assert.deepEqual(
        fake.spawns().map((spawn) => [spawn.command, ...spawn.args]),
        [["/opt/omp/bin/omp", "--version"]],
      );
    }),
  );

  it.effect("writes the unattended overlay before each probe and passes it last", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpProcess(probeScript());
      const probesStartedAtWrite: Array<number> = [];
      const overlay: OmpUnattendedOverlay = {
        path: UNATTENDED_OVERLAY_PATH,
        ensure: Effect.sync(() => {
          probesStartedAtWrite.push(fake.spawns().filter(isRpcSpawn).length);
        }),
      };
      const withFake = Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner);
      yield* checkOmpProviderStatus(settings, {}, PROBE_CWD, noCompatibilityPolicy, overlay).pipe(
        withFake,
      );
      yield* discoverOmpCommandsForCwd(settings, {}, "/workspace", overlay).pipe(withFake);

      const probes = fake.spawns().filter(isRpcSpawn);
      assert.deepEqual(probesStartedAtWrite, [0, 1]);
      assert.deepEqual(
        probes.map((probe) => probe.args.slice(-2)),
        [
          ["--config", UNATTENDED_OVERLAY_PATH],
          ["--config", UNATTENDED_OVERLAY_PATH],
        ],
      );
    }),
  );

  it.effect("rejects launch arguments T3 controls before probing", () =>
    Effect.gen(function* () {
      const { snapshot, spawns } = yield* checkWith(probeScript(), { launchArgs: "--mode json" });
      assert.equal(snapshot.status, "error");
      assert.include(snapshot.message ?? "", "--mode");
      assert.isFalse(spawns.some(isRpcSpawn));
    }),
  );

  const exitsBeforeReady = (stderr: string) =>
    ChildProcessSpawner.make((command) => {
      const args = ChildProcess.isStandardCommand(command) ? command.args : [];
      return Effect.succeed(
        args.includes("--version")
          ? exitingProcess({ stdout: "omp/19.1.1\n", exitCode: 0 })
          : exitingProcess({ stderr, exitCode: 1 }),
      );
    });

  it.effect.each([
    {
      reason: "no default model",
      stderr: noDefaultModel.stderr,
      message: "OMP has no default model.",
    },
    {
      reason: "no credentials for enabledModels",
      stderr: enabledModelsNoCredentials.stderr,
      message: "OMP has no signed-in model that its enabledModels setting allows.",
    },
    {
      reason: "a model registry failure",
      stderr: "Failed to load models.yml\n\nNo models available.\n",
      message: "OMP has no usable models.",
    },
  ])("reports omp with $reason as signed out, not as an install error", ({ stderr, message }) =>
    Effect.gen(function* () {
      const snapshot = yield* checkOmpProviderStatus(
        settings,
        {},
        PROBE_CWD,
        noCompatibilityPolicy,
        UNATTENDED_OVERLAY,
      ).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, exitsBeforeReady(stderr)),
      );
      assert.equal(snapshot.status, "warning");
      assert.equal(snapshot.installed, true);
      assert.equal(snapshot.auth.status, "unauthenticated");
      assert.include(snapshot.message ?? "", message);
    }),
  );

  it.effect("keeps omp selectable when it exits before ready for another reason", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkOmpProviderStatus(
        settings,
        {},
        PROBE_CWD,
        noCompatibilityPolicy,
        UNATTENDED_OVERLAY,
      ).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          exitsBeforeReady("Error: something else\n"),
        ),
      );
      assert.equal(snapshot.status, "ready");
      assert.equal(snapshot.auth.status, "unknown");
      assert.include(snapshot.message ?? "", "could not refresh its models and commands");
    }),
  );

  it.effect("gives `omp --version` 10 s before reporting a timeout", () =>
    Effect.gen(function* () {
      const spawner = ChildProcessSpawner.make(() =>
        Effect.succeed(
          exitingProcess({ stdout: "omp/19.1.1\n", exitCode: 0, answered: Effect.never }),
        ),
      );
      const fiber = yield* checkOmpProviderStatus(
        settings,
        {},
        PROBE_CWD,
        noCompatibilityPolicy,
        UNATTENDED_OVERLAY,
      ).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.forkChild,
      );
      yield* TestClock.adjust("10 seconds");
      const snapshot = yield* Fiber.join(fiber);
      assert.equal(snapshot.status, "error");
      assert.equal(
        snapshot.message,
        "OMP CLI is installed but timed out while running `omp --version`.",
      );
    }),
  );
});
