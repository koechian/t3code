import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import {
  buildOmpForkLaunch,
  buildOmpPrintLaunch,
  buildOmpProbeLaunch,
  buildOmpRpcLaunch,
  composeOmpAppendSystemPrompt,
  makeOmpUnattendedOverlay,
  ompApprovalModeForRuntimeMode,
  ompAppendSystemPromptPath,
  ompChildEnvironment,
  ompOverlayPath,
  renderOmpSessionOverlay,
  renderOmpTextGenerationOverlay,
  renderOmpUnattendedOverlay,
  resolveOmpLaunchArgs,
  writeOmpConfigOverlay,
} from "./launch.ts";

const UNATTENDED_OVERLAY = "/state/omp-overlays/unattended.yml";

const controlled = (flag: string) =>
  `OMP launch argument '${flag}' is controlled by T3 Code and cannot be overridden.`;

describe("resolveOmpLaunchArgs", () => {
  it.each([
    ["--mode text", controlled("--mode")],
    ["--session=old.jsonl", controlled("--session")],
    ["-r old.jsonl", controlled("-r")],
    ["--fork /tmp/a.jsonl", controlled("--fork")],
    ["--approval-mode=yolo", controlled("--approval-mode")],
    ["--offline --yolo", controlled("--yolo")],
    ["--append-system-prompt extra.md", controlled("--append-system-prompt")],
    ["--cwd /elsewhere", controlled("--cwd")],
    ["--model", "OMP launch argument '--model' requires a value."],
    ["--offline -- fix it", "OMP launch arguments cannot include positional prompts."],
    ["fix the bug", "OMP launch arguments cannot include positional prompt 'fix'."],
    ["--offline @notes.md", "OMP launch arguments cannot include positional prompt '@notes.md'."],
    ["--no-tools stray", "OMP launch arguments cannot include positional prompt 'stray'."],
    ["-x", "OMP launch argument '-x' is not supported by T3 Code."],
    ["-e=./ext.ts", "OMP launch argument '-e=./ext.ts' is not supported by T3 Code."],
  ])("rejects %s", (launchArgs, message) => {
    assert.deepStrictEqual(resolveOmpLaunchArgs(launchArgs), { ok: false, message });
  });

  it("keeps valid args and splits only built-in value flags", () => {
    assert.deepStrictEqual(
      resolveOmpLaunchArgs(
        "--profile=work --config ./mine.yml --model=anthropic/claude-haiku-5-5 " +
          "--system-prompt --resume --ext-flag value --ext-switch=on --offline -e ./ext.ts",
      ),
      {
        ok: true,
        args: [
          "--profile",
          "work",
          "--config",
          "./mine.yml",
          "--model",
          "anthropic/claude-haiku-5-5",
          "--system-prompt",
          "--resume",
          "--ext-flag",
          "value",
          "--ext-switch=on",
          "--offline",
          "-e",
          "./ext.ts",
        ],
      },
    );
  });
});

describe("overlays", () => {
  it.each([
    ["approval-required", "always-ask"],
    ["auto-accept-edits", "write"],
    ["auto", "always-ask"],
    ["full-access", "yolo"],
  ] as const)("maps %s to %s", (runtimeMode, approvalMode) => {
    assert.strictEqual(ompApprovalModeForRuntimeMode(runtimeMode), approvalMode);
  });

  it("writes the approval mode, quoted, deduplicated tool allows, no shell trace and RPC goals", () => {
    const goalContinuation = [
      "goal:",
      "  continuationModes:",
      '    - "interactive"',
      '    - "rpc"',
    ];
    assert.strictEqual(
      renderOmpSessionOverlay({
        approvalMode: "always-ask",
        allowedToolNames: [
          "mcp__t3-code__task_status",
          "mcp__t3-code__orchestrator_capabilities",
          "mcp__t3-code__task_status",
        ],
      }),
      [
        "display:",
        "  shellVisualization: false",
        ...goalContinuation,
        "tools:",
        '  approvalMode: "always-ask"',
        "  approval:",
        '    "mcp__t3-code__task_status": "allow"',
        '    "mcp__t3-code__orchestrator_capabilities": "allow"',
        "",
      ].join("\n"),
    );
    assert.strictEqual(
      renderOmpSessionOverlay({ approvalMode: "yolo", allowedToolNames: [] }),
      [
        "display:",
        "  shellVisualization: false",
        ...goalContinuation,
        "tools:",
        '  approvalMode: "yolo"',
        "",
      ].join("\n"),
    );
    assert.include(renderOmpTextGenerationOverlay(), 'memory:\n  backend: "off"\n');
  });

  it.effect("writes the unattended overlay once, with the marketplace refresh off", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const stateDir = yield* fileSystem.makeTempDirectoryScoped();
      const overlay = yield* makeOmpUnattendedOverlay(stateDir);
      const expected = 'marketplace:\n  autoUpdate: "off"\n';

      assert.strictEqual(overlay.path, path.join(stateDir, "omp-overlays", "unattended.yml"));
      assert.isFalse(yield* fileSystem.exists(overlay.path));
      yield* overlay.ensure;
      assert.strictEqual(yield* fileSystem.readFileString(overlay.path), expected);
      yield* fileSystem.writeFileString(overlay.path, "changed\n");
      yield* overlay.ensure;
      assert.strictEqual(yield* fileSystem.readFileString(overlay.path), "changed\n");
      assert.strictEqual(renderOmpUnattendedOverlay(), expected);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("rewrites an overlay in place without leaving staging files", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const stateDir = yield* fileSystem.makeTempDirectoryScoped();
      const overlay = ompOverlayPath(path, stateDir, "session/../1");

      yield* writeOmpConfigOverlay(overlay, "first\n");
      yield* writeOmpConfigOverlay(overlay, "second\n");

      assert.strictEqual(overlay, path.join(stateDir, "omp-overlays", "session-..-1.yml"));
      assert.strictEqual(yield* fileSystem.readFileString(overlay), "second\n");
      assert.deepStrictEqual(yield* fileSystem.readDirectory(path.dirname(overlay)), [
        "session-..-1.yml",
      ]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("writes overlays for session ids longer than a file name allows", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const stateDir = yield* fileSystem.makeTempDirectoryScoped();
      const sharedPrefix = `provider-session:provider-instance:omp:thread:${"a".repeat(240)}`;
      const overlays = ["1", "2"].map((suffix) =>
        ompOverlayPath(path, stateDir, `${sharedPrefix}:${suffix}`),
      );

      for (const overlay of overlays) {
        yield* writeOmpConfigOverlay(overlay, "tools: {}\n");
        yield* writeOmpConfigOverlay(ompAppendSystemPromptPath(path, overlay), "append\n");
      }

      assert.notStrictEqual(overlays[0], overlays[1]);
      assert.lengthOf(yield* fileSystem.readDirectory(path.join(stateDir, "omp-overlays")), 4);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("launch argv", () => {
  const environment = {
    PATH: "/usr/bin",
    PI_CODING_AGENT_DIR: "/home/user/.omp/agent",
    T3_MCP_URL: "http://127.0.0.1:9/mcp",
    T3_MCP_BEARER_TOKEN: "inherited-token",
    PI_RPC_EMIT_TITLE: "1",
    T3CODE_THREAD_ID: "parent-thread",
  };
  const userArgs = [
    "--profile",
    "work",
    "-e",
    "x",
    "--extension=x",
    "--hook",
    "h.ts",
    "--trusted-extension",
    "/abs/ext.ts",
    "--tools",
    "read,bash",
    "--tools=read",
    "--ext-flag",
    "value",
    "--ext-switch=on",
    "--ext-bool",
    "--system-prompt-template",
    "f",
    "--advisor",
    "--system-prompt",
    "--tools",
    "--no-lsp=1",
    "--offline",
  ];
  const keptByUnattended = [
    "--profile",
    "work",
    "--system-prompt-template",
    "f",
    "--advisor",
    "--system-prompt",
    "--tools",
    "--no-lsp=1",
    "--offline",
  ];

  it("passes extension flags only to the RPC launch", () => {
    const launch = buildOmpRpcLaunch({
      launchArgs: userArgs,
      environment,
      threadId: "thread-1",
      sessionDir: "/state/omp-sessions/--work--",
      overlayPath: "/state/omp-overlays/s.yml",
    });
    assert.deepStrictEqual(launch.args.slice(5, -2), userArgs);
  });

  it("orders the RPC argv with T3's overlay last", () => {
    const launch = buildOmpRpcLaunch({
      launchArgs: ["--config", "./mine.yml"],
      environment,
      threadId: "thread-1",
      sessionDir: "/state/omp-sessions/--work--",
      resumeSessionFile: "/state/omp-sessions/--work--/a.jsonl",
      overlayPath: "/state/omp-overlays/s.yml",
      appendSystemPromptPath: "/state/append.md",
    });
    assert.deepStrictEqual(launch.args, [
      "--mode",
      "rpc-ui",
      "--session-dir",
      "/state/omp-sessions/--work--",
      "--resume",
      "/state/omp-sessions/--work--/a.jsonl",
      "--no-title",
      "--append-system-prompt",
      "/state/append.md",
      "--config",
      "./mine.yml",
      "--config",
      "/state/omp-overlays/s.yml",
    ]);
    assert.deepStrictEqual(launch.env, {
      PATH: "/usr/bin",
      PI_CODING_AGENT_DIR: "/home/user/.omp/agent",
      T3CODE_THREAD_ID: "thread-1",
    });
  });

  it("drops tool-loading and extension flags from probe and fork launches", () => {
    const probe = buildOmpProbeLaunch({
      launchArgs: userArgs,
      environment,
      unattendedOverlayPath: UNATTENDED_OVERLAY,
    });
    assert.deepStrictEqual(probe.args, [
      "--mode",
      "rpc-ui",
      "--no-session",
      ...keptByUnattended,
      "--no-extensions",
      "--no-tools",
      "--offline",
      "--config",
      UNATTENDED_OVERLAY,
    ]);
    assert.deepStrictEqual(probe.env, ompChildEnvironment(environment, undefined));
    assert.isUndefined(probe.env["T3CODE_THREAD_ID"]);

    const fork = buildOmpForkLaunch({
      launchArgs: userArgs,
      environment,
      sessionDir: "/state/omp-sessions/--next--",
      sourceSessionFile: "/state/omp-sessions/--work--/a.jsonl",
      unattendedOverlayPath: UNATTENDED_OVERLAY,
    });
    assert.deepStrictEqual(fork.args, [
      "--mode",
      "rpc-ui",
      "--fork",
      "/state/omp-sessions/--work--/a.jsonl",
      "--session-dir",
      "/state/omp-sessions/--next--",
      "--no-title",
      ...keptByUnattended,
      "--no-extensions",
      "--no-tools",
      "--offline",
      "--config",
      UNATTENDED_OVERLAY,
    ]);
  });

  it("drops print-conflicting flags only from print launches", () => {
    const print = buildOmpPrintLaunch({
      launchArgs: userArgs,
      environment,
      model: { provider: "anthropic", modelId: "claude-haiku-5-5" },
      systemPrompt: "Write a title.\nOne line.",
      unattendedOverlayPath: UNATTENDED_OVERLAY,
      overlayPath: "/state/omp-overlays/tg.yml",
      maxTimeSeconds: 60,
      maxCostUsd: 0.05,
    });
    assert.deepStrictEqual(print.args, [
      "-p",
      "--mode",
      "json",
      "--no-session",
      "--profile",
      "work",
      "--system-prompt",
      "--tools",
      "--no-lsp=1",
      "--offline",
      "--model",
      "anthropic/claude-haiku-5-5",
      "--thinking",
      "off",
      "--no-tools",
      "--no-extensions",
      "--no-skills",
      "--no-rules",
      "--no-context-files",
      "--no-lsp",
      "--no-title",
      "--max-time",
      "60",
      "--max-cost",
      "0.05",
      "--system-prompt",
      "Write a title.\nOne line.",
      "--config",
      UNATTENDED_OVERLAY,
      "--config",
      "/state/omp-overlays/tg.yml",
    ]);
    const oneLine = buildOmpPrintLaunch({
      launchArgs: [],
      environment,
      model: undefined,
      systemPrompt: "README.md",
      unattendedOverlayPath: UNATTENDED_OVERLAY,
      overlayPath: "/state/omp-overlays/tg.yml",
      maxTimeSeconds: 60,
      maxCostUsd: 0.05,
    });
    assert.include(oneLine.args, "README.md\n", "a one-line prompt must not name a file");
    assert.notInclude(oneLine.args, "--model");
  });
});

describe("composeOmpAppendSystemPrompt", () => {
  it.effect("appends the first APPEND_SYSTEM.md omp would have found", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fileSystem.makeTempDirectoryScoped();
      const cwd = path.join(root, "project");
      const home = path.join(root, "home");
      const write = (relative: string, text: string) =>
        Effect.gen(function* () {
          const file = path.join(root, relative);
          yield* fileSystem.makeDirectory(path.dirname(file), { recursive: true });
          yield* fileSystem.writeFileString(file, text);
          return file;
        });
      const compose = (environment: NodeJS.ProcessEnv = {}) =>
        composeOmpAppendSystemPrompt({ cwd, home, environment, t3Instructions: "T3" });

      assert.strictEqual(yield* compose(), "T3");
      yield* write("home/.claude/APPEND_SYSTEM.md", "claude user");
      assert.strictEqual(yield* compose(), "T3", "Claude's home dir is opt-in");
      assert.strictEqual(
        yield* compose({ CLAUDE_CONFIG_DIR: path.join(home, ".claude") }),
        "T3\n\nclaude user",
      );
      yield* write("home/.omp/agent/APPEND_SYSTEM.md", "omp user");
      assert.strictEqual(yield* compose(), "T3\n\nomp user");
      yield* write("custom-agent/APPEND_SYSTEM.md", "custom agent");
      assert.strictEqual(
        yield* compose({ PI_CODING_AGENT_DIR: path.join(root, "custom-agent") }),
        "T3\n\ncustom agent",
      );
      const projectClaude = yield* write("project/.claude/APPEND_SYSTEM.md", "project claude");
      assert.strictEqual(yield* compose(), "T3\n\nproject claude");
      yield* write("project/.omp/APPEND_SYSTEM.md", "project omp");
      assert.strictEqual(yield* compose(), "T3\n\nproject omp");
      yield* fileSystem.remove(projectClaude);
      yield* fileSystem.remove(path.join(cwd, ".omp", "APPEND_SYSTEM.md"));
      assert.strictEqual(yield* compose(), "T3\n\nomp user");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
