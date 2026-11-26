import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { DEFAULT_SERVER_SETTINGS, ProviderInstanceId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpClient from "effect/http/HttpClient";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";

import type { OmpSettings } from "../settings.ts";
import { OmpDriver } from "./driver.ts";
import picker from "./__fixtures__/modelCapabilities/model-picker.json" with { type: "json" };
import {
  fakeOmpFailure,
  fakeOmpSuccess,
  makeFakeOmpProcess,
  type FakeOmpProcess,
} from "./rpc.testkit.ts";
import type { OmpAvailableSlashCommand } from "./wire.ts";

const layerTest = Layer.mergeAll(
  TestProviderHost.layer({
    cwd: "/machine",
    settings: { ...DEFAULT_SERVER_SETTINGS, enableProviderUpdateChecks: false },
    runBackgroundWork: false,
  }),
  IdAllocator.layer,
  McpProviderSessions.layer,
  ProviderLatestVersions.layer,
  Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make(() => Effect.die("Unexpected HTTP")),
  ),
).pipe(Layer.provideMerge(NodeServices.layer));

const PERSONAL_SKILL: OmpAvailableSlashCommand = { name: "skill:personal", source: "skill" };
const MACHINE_PROBE_DIR = "/omp-status-probe";

const workspaceCatalog = (cwd: string): ReadonlyArray<OmpAvailableSlashCommand> => [
  PERSONAL_SKILL,
  { name: `skill:${cwd.slice(1)}`, source: "skill" },
  { name: `prompt-${cwd.slice(1)}`, source: "file" },
];

const makeOmpFake = (catalog: ReadonlyArray<OmpAvailableSlashCommand> | "failed") =>
  makeFakeOmpProcess({
    cli: (invocation) =>
      invocation.args.includes("--version") ? { stdout: "omp/19.1.1\n", exitCode: 0 } : undefined,
    reply: (command) => {
      if (command.type === "get_model_picker") return fakeOmpSuccess(picker);
      if (command.type !== "get_available_commands") return undefined;
      return catalog === "failed"
        ? fakeOmpFailure("commands unavailable")
        : fakeOmpSuccess({ commands: catalog });
    },
  });

/** One fake per working directory, so each workspace probe answers with its own catalog. */
const makeOmpSpawner = Effect.fnUntraced(function* () {
  const machine = yield* makeOmpFake([PERSONAL_SKILL]);
  const workspaces = new Map<string, FakeOmpProcess>([
    ["/first", yield* makeOmpFake(workspaceCatalog("/first"))],
    ["/second", yield* makeOmpFake(workspaceCatalog("/second"))],
    ["/failed", yield* makeOmpFake("failed")],
  ]);
  const fakeFor = (cwd: string | undefined) => (cwd && workspaces.get(cwd)) || machine;
  const spawner = ChildProcessSpawner.make((command) =>
    ChildProcess.isStandardCommand(command)
      ? fakeFor(command.options.cwd).spawner.spawn(command)
      : Effect.die("Unexpected pipeline"),
  );
  const launches = () => [machine, ...workspaces.values()].flatMap((fake) => fake.spawns());
  return { spawner, launches };
});

const create = (config: Partial<OmpSettings> = {}, enabled = true) =>
  OmpDriver.create({
    instanceId: ProviderInstanceId.make("omp-workspace-test"),
    displayName: "My OMP",
    accentColor: "#abcdef",
    environment: [{ name: "PI_CODING_AGENT_DIR", value: "/isolated-omp", sensitive: false }],
    enabled,
    config: { ...OmpDriver.defaultConfig(), binaryPath: "custom-omp", ...config },
  });

it.layer(layerTest)("OmpDriver workspace discovery", (it) => {
  it.effect("keeps each workspace's skills and commands separate from the machine catalog", () =>
    Effect.gen(function* () {
      const { spawner, launches } = yield* makeOmpSpawner();
      const instance = yield* create({ launchArgs: '--profile work --skills "extra skill"' }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      );
      yield* instance.snapshot.refresh;
      assert.isDefined(instance.snapshotForCwd);
      const [first, second] = yield* Effect.all(
        [instance.snapshotForCwd!("/first"), instance.snapshotForCwd!("/second")],
        { concurrency: "unbounded" },
      );
      assert.deepEqual(
        first.skills.map((skill) => skill.name),
        ["personal", "first"],
      );
      assert.deepEqual(
        second.skills.map((skill) => skill.name),
        ["personal", "second"],
      );
      assert.deepEqual(
        first.slashCommands.map((command) => command.name),
        ["prompt-first", "goal"],
      );
      const machine = yield* instance.snapshot.getSnapshot;
      assert.equal(machine.status, "ready");
      assert.deepEqual(
        machine.skills.map((skill) => skill.name),
        ["personal"],
      );
      assert.equal(first.instanceId, instance.instanceId);
      assert.equal(first.displayName, "My OMP");
      assert.equal(first.accentColor, "#abcdef");
      assert.deepEqual(first.models, machine.models);
      assert.isFalse(first.slashCommandsPending);

      const rpcLaunches = launches().filter((launch) => launch.args.includes("--mode"));
      assert.isTrue(rpcLaunches.some((launch) => launch.cwd?.endsWith(MACHINE_PROBE_DIR)));
      const workspaceLaunch = rpcLaunches.find((launch) => launch.cwd === "/first");
      assert.equal(workspaceLaunch?.command, "custom-omp");
      assert.equal(workspaceLaunch?.env["PI_CODING_AGENT_DIR"], "/isolated-omp");
      assert.includeMembers(
        [...(workspaceLaunch?.args ?? [])],
        ["--profile", "work", "--skills", "extra skill", "--no-session", "--no-tools", "--offline"],
      );
    }),
  );

  it.effect("does not run a disabled provider's workspace probe", () =>
    Effect.gen(function* () {
      const instance = yield* create({}, false).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() => Effect.die("A disabled OMP must not spawn")),
        ),
      );
      assert.isDefined(instance.snapshotForCwd);
      const workspace = yield* instance.snapshotForCwd!("/first");
      assert.isFalse(workspace.enabled);
      assert.deepEqual(workspace.skills, []);
    }),
  );

  it.effect(
    "fails command discovery instead of returning an empty successful workspace catalog",
    () =>
      Effect.gen(function* () {
        const { spawner } = yield* makeOmpSpawner();
        const instance = yield* create().pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        );
        yield* instance.snapshot.refresh;
        assert.isDefined(instance.snapshotForCwd);
        const error = yield* Effect.flip(instance.snapshotForCwd!("/failed"));
        assert.equal(error._tag, "ProviderDriverError");
        assert.equal(error.instanceId, instance.instanceId);
        assert.deepEqual(
          (yield* instance.snapshot.getSnapshot).skills.map((skill) => skill.name),
          ["personal"],
        );
      }),
  );
});
