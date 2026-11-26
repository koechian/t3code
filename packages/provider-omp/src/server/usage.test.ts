import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { ProviderInstanceId } from "@t3tools/contracts";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import { OmpSettings } from "../settings.ts";
import session from "./__fixtures__/usage/compacted-session.json" with { type: "json" };
import { ompUsageFormat, ompUsageReader } from "./usage.ts";

const SESSION_ID = "01a211de-c591-7351-a44b-01a135831257";
const HOME = "/home/omp-user";
const DEFAULT_SETTINGS = Schema.decodeSync(OmpSettings)({});

const dedupeKey = (entryId: string, timestamp: string) =>
  JSON.stringify(["omp", entryId, timestamp, "anthropic/claude-haiku-5-5"]);

const scanSession = () => {
  const state = ompUsageFormat.state!.initial();
  const records = session.flatMap((entry) =>
    ompUsageFormat.parseLine(JSON.stringify(entry), state),
  );
  return { state, records };
};

describe("ompUsageFormat", () => {
  it("reads assistant messages and out-of-transcript model calls, attributed to the session", () => {
    const { records, state } = scanSession();
    assert.equal(state.sessionId, SESSION_ID);
    assert.deepEqual(
      records.map((record) => [
        record.provider,
        record.dedupeKey,
        record.totals.uncachedInputTokens,
        record.totals.outputTokens,
        record.reportedCostUsd,
      ]),
      [
        ["omp", dedupeKey("ae7d8357", "2026-11-25T08:42:23.651Z"), 4, 4, 0.00012891],
        ["omp", dedupeKey("8b62c854", "2026-11-25T08:42:24.410Z"), 4, 4, 0.0001291],
        ["omp", dedupeKey("b8fff9ed", "2026-11-25T08:42:26.545Z"), 487, 228, 0.0002894],
        ["omp", dedupeKey("f8f3fd43", "2026-11-25T08:42:28.684Z"), 4, 4, 0.00022820000000000002],
        ["omp", dedupeKey("fadd8092", "2026-11-25T08:42:30.813Z"), 4, 267, 0.00036185],
      ],
    );
  });

  it("keeps cache reads and writes apart and records the 1-hour cache writes", () => {
    const { records } = scanSession();
    assert.deepEqual(records[3], {
      provider: "omp",
      timestampMs: 1_795_596_148_684,
      model: "anthropic/claude-haiku-5-5",
      rateModel: "claude-haiku-5-5",
      sessionId: SESSION_ID,
      totals: {
        uncachedInputTokens: 4,
        cachedInputTokens: 12280,
        cacheCreationTokens: 515,
        outputTokens: 4,
        reasoningTokens: 0,
      },
      cacheCreation1hTokens: 515,
      reportedCostUsd: 0.00022820000000000002,
      speed: "standard",
      dedupeKey: dedupeKey("f8f3fd43", "2026-11-25T08:42:28.684Z"),
    });
  });

  it("caps reasoning at output and skips entries without tokens or cost", () => {
    const state = { sessionId: SESSION_ID };
    const message = (usage: Record<string, unknown>) => ({
      type: "message",
      id: "e1",
      timestamp: "2026-11-25T08:42:28.684Z",
      message: { role: "assistant", provider: "openai", model: "gpt-6", usage },
    });
    assert.deepEqual(
      ompUsageFormat
        .parseProjected(message({ output: 10, reasoningTokens: 25 }), state)
        .map((record) => record.totals.reasoningTokens),
      [10],
    );
    assert.deepEqual(
      ompUsageFormat.parseProjected(message({ output: 0, cost: { total: 0 } }), state),
      [],
    );
    assert.deepEqual(
      ompUsageFormat.parseProjected(
        { ...message({ output: 3 }), message: { role: "user", usage: { output: 3 } } },
        state,
      ),
      [],
    );
  });

  it("prices a model ID by the model after its provider prefix", () => {
    assert.equal(
      ompUsageReader.rateModel?.("openrouter/anthropic/claude-haiku-5-5"),
      "anthropic/claude-haiku-5-5",
    );
  });
});

const readDirectories = (environment: NodeJS.ProcessEnv, platform: NodeJS.Platform = "linux") =>
  Effect.gen(function* () {
    if (ompUsageReader.kind !== "transcripts") return assert.fail("expected a transcript reader");
    const directories = yield* ompUsageReader.directories(
      {
        instanceId: ProviderInstanceId.make("omp"),
        config: DEFAULT_SETTINGS,
        environment,
        configured: false,
      },
      { awaitRefresh: false },
    );
    return directories.map((directory) => directory.dir);
  }).pipe(
    Effect.provideService(HostProcess.HomeDirectory, HOME),
    Effect.provideService(HostProcess.Platform, platform),
  );

const layer = TestProviderHost.layer().pipe(Layer.provideMerge(NodeServices.layer));

describe("ompUsageReader.directories", () => {
  it.effect("scans T3's own session root, then omp's sessions directory", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const host = yield* ProviderHost.ProviderHost;
      assert.deepEqual(yield* readDirectories({}), [
        path.join(host.paths.stateDir, "omp-sessions"),
        path.join(HOME, ".omp", "agent", "sessions"),
      ]);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("resolves omp's sessions directory from the instance environment as omp does", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const cases: ReadonlyArray<readonly [NodeJS.ProcessEnv, string, NodeJS.Platform?]> = [
        [{ HOME: "/home/instance" }, "/home/instance/.omp/agent/sessions"],
        [{ USERPROFILE: "/users/instance" }, "/users/instance/.omp/agent/sessions", "win32"],
        [{ PI_CONFIG_DIR: ".omp-alt" }, `${HOME}/.omp-alt/agent/sessions`],
        [
          { OMP_PROFILE: "work", PI_CODING_AGENT_DIR: "~/ignored" },
          `${HOME}/.omp/profiles/work/agent/sessions`,
        ],
        [{ PI_PROFILE: "work" }, `${HOME}/.omp/profiles/work/agent/sessions`],
        [{ OMP_PROFILE: "", PI_PROFILE: "work" }, `${HOME}/.omp/agent/sessions`],
        [{ OMP_PROFILE: "default" }, `${HOME}/.omp/agent/sessions`],
        [{ OMP_PROFILE: "con" }, `${HOME}/.omp/agent/sessions`],
        [{ OMP_PROFILE: "Work!" }, `${HOME}/.omp/agent/sessions`],
      ];
      for (const [environment, expected, platform] of cases) {
        const directories = yield* readDirectories(environment, platform);
        assert.equal(directories[1], path.normalize(expected), JSON.stringify(environment));
      }
    }).pipe(Effect.provide(layer)),
  );

  it.effect("leaves a directory named by Pi's shared variables to Pi's reader", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const host = yield* ProviderHost.ProviderHost;
      const t3Only = [path.join(host.paths.stateDir, "omp-sessions")];
      assert.deepEqual(yield* readDirectories({ PI_CODING_AGENT_DIR: "~/custom" }), t3Only);
      assert.deepEqual(yield* readDirectories({ PI_CODING_AGENT_SESSION_DIR: "~/flat" }), t3Only);
      assert.deepEqual(
        yield* readDirectories({ OMP_PROFILE: "work", PI_CODING_AGENT_SESSION_DIR: "~/flat" }),
        t3Only,
      );
    }).pipe(Effect.provide(layer)),
  );

  it.effect("follows XDG_DATA_HOME only once omp's data has moved there", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const xdg = yield* fileSystem.makeTempDirectoryScoped({ prefix: "omp-xdg-" });
      const environment = { XDG_DATA_HOME: xdg };
      const defaultDir = path.join(HOME, ".omp", "agent", "sessions");
      assert.equal((yield* readDirectories(environment))[1], defaultDir);
      yield* fileSystem.makeDirectory(path.join(xdg, "omp"));
      assert.equal((yield* readDirectories(environment))[1], path.join(xdg, "omp", "sessions"));
      assert.equal((yield* readDirectories(environment, "win32"))[1], defaultDir);
      const profile = { ...environment, OMP_PROFILE: "work" };
      assert.equal(
        (yield* readDirectories(profile))[1],
        path.join(HOME, ".omp", "profiles", "work", "agent", "sessions"),
      );
      yield* fileSystem.makeDirectory(path.join(xdg, "omp", "profiles", "work"), {
        recursive: true,
      });
      assert.equal(
        (yield* readDirectories(profile))[1],
        path.join(xdg, "omp", "profiles", "work", "sessions"),
      );
    }).pipe(Effect.scoped, Effect.provide(layer)),
  );
});
