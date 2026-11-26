import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { ProviderInstanceId } from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import {
  noCompatibilityPolicy,
  type ProviderCompatibilityLookup,
} from "@t3tools/provider-core/server/compatibility";

import picker from "./__fixtures__/modelCapabilities/model-picker.json" with { type: "json" };
import printRun from "./__fixtures__/textGeneration/print-thread-title.json" with { type: "json" };
import {
  makeOmpUnattendedOverlay,
  renderOmpTextGenerationOverlay,
  renderOmpUnattendedOverlay,
} from "./launch.ts";
import { fakeOmpSuccess, makeFakeOmpProcess, type FakeOmpInvocation } from "./rpc.testkit.ts";
import { makeOmpTextGeneration } from "./textGeneration.ts";

const settings = { enabled: true, binaryPath: "omp", launchArgs: "", customModels: [] } as const;
const COMMIT_REPLY = JSON.stringify({ subject: "Update README", body: "" });

interface AssistantEnd {
  readonly text?: string;
  readonly stopReason: string;
  readonly errorMessage?: string;
}

const assistantEnd = (end: AssistantEnd) => ({
  type: "message_end",
  message: {
    role: "assistant",
    content: end.text === undefined ? [] : [{ type: "text", text: end.text }],
    stopReason: end.stopReason,
    ...(end.errorMessage === undefined ? {} : { errorMessage: end.errorMessage }),
  },
});

/** The recorded `--mode json` run with its assistant `message_end` replaced by `ends`. */
const printStdout = (ends: ReadonlyArray<AssistantEnd>) =>
  printRun
    .map((record) => JSON.stringify(record))
    .flatMap((line) =>
      line.startsWith('{"type":"message_end"') && line.includes('"role":"assistant"')
        ? ends.map((end) => JSON.stringify(assistantEnd(end)))
        : [line],
    )
    .join("\n")
    .concat("\n");

const isPrintRun = (invocation: FakeOmpInvocation) => invocation.args[0] === "-p";

const setup = (input: {
  readonly stdout?: string;
  readonly exitCode?: number;
  readonly compatibility?: ProviderCompatibilityLookup;
}) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const stateDir = yield* fileSystem.makeTempDirectoryScoped();
    const overlayDir = path.join(stateDir, "omp-overlays");
    const fake = yield* makeFakeOmpProcess({
      cli: (invocation) => {
        if (invocation.args.includes("--version")) return { stdout: "omp/19.1.1\n", exitCode: 0 };
        return isPrintRun(invocation)
          ? {
              stdout: input.stdout ?? printStdout([{ text: COMMIT_REPLY, stopReason: "stop" }]),
              exitCode: input.exitCode ?? 0,
            }
          : undefined;
      },
      reply: (command) =>
        command.type === "get_model_picker" ? fakeOmpSuccess(picker) : undefined,
    });
    const textGeneration = yield* makeOmpTextGeneration({
      settings,
      environment: {},
      compatibility: input.compatibility ?? noCompatibilityPolicy,
      overlayDir,
      unattendedOverlay: yield* makeOmpUnattendedOverlay(stateDir),
    }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner));
    const generateCommitMessage = (model = "default") =>
      textGeneration.generateCommitMessage({
        cwd: "/repo",
        branch: "main",
        stagedSummary: "M README.md",
        stagedPatch: "diff --git a/README.md b/README.md",
        modelSelection: createModelSelection(ProviderInstanceId.make("omp"), model),
      });
    return { fake, fileSystem, overlayDir, path, generateCommitMessage };
  });

describe("makeOmpTextGeneration", () => {
  it.effect("runs omp's default model in print mode with the prompt on stdin", () =>
    Effect.gen(function* () {
      const { fake, fileSystem, overlayDir, path, generateCommitMessage } = yield* setup({});
      const result = yield* generateCommitMessage();
      yield* generateCommitMessage();

      assert.equal(result.subject, "Update README");
      const prints = fake.spawns().filter(isPrintRun);
      assert.lengthOf(prints, 2);
      const print = prints[0]!;
      assert.deepEqual(print.args.slice(0, 3), ["-p", "--mode", "json"]);
      assert.notInclude(print.args, "--model");
      // No session file in the user's omp directory, and no auto-resume of their last session.
      assert.equal(print.args.filter((arg) => arg === "--no-session").length, 1);
      assert.includeMembers([...print.args], ["--no-tools", "--no-extensions", "--system-prompt"]);
      assert.include(print.stdin, "M README.md");
      const overlayPath = path.join(overlayDir, "text-generation.yml");
      const unattendedPath = path.join(overlayDir, "unattended.yml");
      assert.deepEqual(print.args.slice(-4), ["--config", unattendedPath, "--config", overlayPath]);
      assert.equal(yield* fileSystem.readFileString(overlayPath), renderOmpTextGenerationOverlay());
      assert.equal(yield* fileSystem.readFileString(unattendedPath), renderOmpUnattendedOverlay());
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("selects a custom model through its exact picker row", () =>
    Effect.gen(function* () {
      const { fake, overlayDir, path, generateCommitMessage } = yield* setup({});
      yield* generateCommitMessage("openrouter/aion-labs/aion-2.0");

      const lookup = fake.commands().find((command) => command.type === "get_model_picker");
      assert.deepInclude(lookup, { query: "openrouter/aion-labs/aion-2.0", limit: 200 });
      const probe = fake.spawns().find((invocation) => invocation.args[0] === "--mode");
      assert.deepEqual(probe?.args.slice(-2), [
        "--config",
        path.join(overlayDir, "unattended.yml"),
      ]);
      const print = fake.spawns().find(isPrintRun);
      assert.equal(print?.args[print.args.indexOf("--model") + 1], "openrouter/aion-labs/aion-2.0");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("refuses a custom model omp does not list instead of running its default", () =>
    Effect.gen(function* () {
      const { fake, generateCommitMessage } = yield* setup({});
      const error = yield* generateCommitMessage("demo/missing-model").pipe(Effect.flip);

      assert.include(error.detail, "OMP model 'demo/missing-model' is not available");
      assert.isUndefined(fake.spawns().find(isPrintRun));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect.each([
    {
      label: "reports only the HTTP status of a provider error",
      ends: [{ stopReason: "error", errorMessage: '401: {"error":"Incorrect key sk-SECRET123"}' }],
      exitCode: 1,
      detail: "OMP could not generate text: the model provider returned HTTP 401.",
    },
    {
      label: "keeps raw provider text out of the error",
      ends: [{ stopReason: "error", errorMessage: "Invalid API key sk-SECRET123" }],
      exitCode: 1,
      detail: "OMP could not generate text: the model provider reported an error.",
    },
    {
      label: "reports a run the cost cap stopped",
      ends: [],
      exitCode: 3,
      detail: "OMP stopped text generation at its $0.25 cost cap.",
    },
    {
      label: "reports an empty reply",
      ends: [{ text: "  ", stopReason: "stop" }],
      exitCode: 0,
      detail: "OMP returned empty output.",
    },
  ])("$label", ({ ends, exitCode, detail }) =>
    Effect.gen(function* () {
      const { generateCommitMessage } = yield* setup({ stdout: printStdout(ends), exitCode });
      const error = yield* generateCommitMessage().pipe(Effect.flip);

      assert.equal(error.detail, detail);
      assert.notInclude(error.message, "SECRET123");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps the reply when omp answers after a failed attempt", () =>
    Effect.gen(function* () {
      const { generateCommitMessage } = yield* setup({
        stdout: printStdout([
          { stopReason: "error", errorMessage: "529 overloaded" },
          { text: COMMIT_REPLY, stopReason: "stop" },
        ]),
      });
      const result = yield* generateCommitMessage();

      assert.equal(result.subject, "Update README");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("stops at a version the policy rates broken", () =>
    Effect.gen(function* () {
      const { fake, generateCommitMessage } = yield* setup({
        compatibility: () =>
          Effect.succeed({
            status: "broken",
            message: null,
            recommendedVersion: null,
            recommendedRange: ">=19.1.2",
          }),
      });
      const error = yield* generateCommitMessage().pipe(Effect.flip);

      assert.include(error.detail, "OMP 19.1.1 does not work");
      assert.isUndefined(fake.spawns().find(isPrintRun));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
