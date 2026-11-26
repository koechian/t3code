// @effect-diagnostics-next-line nodeBuiltinImport:off - Pure tests read their recorded frames synchronously.
import * as NodeFS from "node:fs";

import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import {
  classifyOmpTool,
  isOmpBackgroundJobCompletion,
  ompCommandOutputProgress,
  ompEditPreview,
  ompParentToolCallId,
  ompToolCommandDelta,
  ompToolCommandNames,
  ompToolItemFromBackgroundCompletion,
  ompToolItemFromEnd,
  ompToolItemFromStart,
  ompToolTitle,
  type OmpToolKind,
} from "./toolItems.ts";
import { decodeOmpInboundFrame, type OmpInboundFrame, type OmpRecord } from "./wire.ts";

const FixtureRecord = Schema.Struct({
  line: Schema.Number,
  frame: Schema.Record(Schema.String, Schema.Unknown),
});
const decodeFixtureRecord = Schema.decodeUnknownSync(Schema.fromJsonString(FixtureRecord));
const decodeJsonRecord = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);

const fixtureCache = new Map<string, ReadonlyMap<number, OmpRecord>>();

/** Frames copied from the Phase 0 recordings, keyed by their line in the original fixture. */
function fixture(name: string): ReadonlyMap<number, OmpRecord> {
  const cached = fixtureCache.get(name);
  if (cached !== undefined) return cached;
  const text = NodeFS.readFileSync(
    new URL(`./__fixtures__/toolItems/${name}.jsonl`, import.meta.url),
    "utf8",
  );
  const frames = new Map(
    text
      .trim()
      .split("\n")
      .map((line) => decodeFixtureRecord(line))
      .map((record) => [record.line, record.frame] as const),
  );
  fixtureCache.set(name, frames);
  return frames;
}

function raw(name: string, line: number): OmpRecord {
  const frame = fixture(name).get(line);
  if (frame === undefined) throw new Error(`${name}:${line} is not in the fixture`);
  return frame;
}

type FrameOf<Type extends OmpInboundFrame["type"]> = Extract<OmpInboundFrame, { type: Type }>;

function isFrameOf<Type extends OmpInboundFrame["type"]>(
  frame: OmpInboundFrame,
  type: Type,
): frame is FrameOf<Type> {
  return frame.type === type;
}

function frame<Type extends OmpInboundFrame["type"]>(
  name: string,
  line: number,
  type: Type,
  rename: { readonly from: string; readonly to: string } = { from: "", to: "" },
): FrameOf<Type> {
  const record =
    rename.from.length === 0
      ? raw(name, line)
      : decodeJsonRecord(JSON.stringify(raw(name, line)).replaceAll(rename.from, rename.to));
  const decoded = Option.getOrThrow(decodeOmpInboundFrame(record));
  if (!isFrameOf(decoded, type)) throw new Error(`${name}:${line} is ${decoded.type}, not ${type}`);
  return decoded;
}

const start = (name: string, line: number) => frame(name, line, "tool_execution_start");
const update = (name: string, line: number) => frame(name, line, "tool_execution_update");
const end = (name: string, line: number) => frame(name, line, "tool_execution_end");
const bashUpdate = (text: string): FrameOf<"tool_execution_update"> => ({
  type: "tool_execution_update",
  toolCallId: "toolu_preview",
  toolName: "bash",
  partialResult: { content: [{ type: "text", text }] },
});
const preview = (name: string, line: number) =>
  ompEditPreview(frame(name, line, "tool_stream_update"));

/** What omp sends once a result passes its snapshot size cap: the numbered diff only. */
function withoutSnapshots(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(withoutSnapshots);
  if (!Predicate.isObject(value)) return value;
  const { oldText, newText, ...rest } = value;
  const entries = Object.entries(rest).map(([key, entry]) => [key, withoutSnapshots(entry)]);
  const pruned = oldText !== undefined || newText !== undefined;
  return { ...Object.fromEntries(entries), ...(pruned ? { snapshotsPruned: true } : {}) };
}

const TOOL_COMMANDS = ompToolCommandNames(["t3_discover"], []);

function completed(
  name: string,
  startLine: number,
  endLine: number,
  options: { readonly abortPending?: boolean; readonly previewLine?: number | undefined } = {},
) {
  const startFrame = start(name, startLine);
  const kind = classifyOmpTool(startFrame.toolName, startFrame.args, TOOL_COMMANDS);
  return ompToolItemFromEnd(
    startFrame,
    end(name, endLine),
    kind,
    options.abortPending ?? false,
    options.previewLine === undefined ? undefined : preview(name, options.previewLine),
  );
}

const SAMPLE_LINE_5_PATCH = [
  `diff --git "a/sample.txt" "b/sample.txt"`,
  `--- "a/sample.txt"`,
  `+++ "b/sample.txt"`,
  "@@ -2,7 +2,7 @@",
  " line 02: bravo",
  " line 03: charlie",
  " line 04: delta",
  "-line 05: echo",
  "+line 05: ECHO",
  " line 06: foxtrot",
  " line 07: golf",
  " line 08: hotel",
  "",
].join("\n");

describe("classifyOmpTool", () => {
  it.each<[string, number, OmpToolKind]>([
    ["02-turns-full-updates", 90, "command"],
    ["other-tools", 226, "tool_command"],
    ["other-tools", 288, "tool_command"],
    ["other-tools", 334, "tool_command"],
    ["02-roundtrip", 141, "tool_command"],
    ["readonly-tool-command", 32, "tool_command"],
    ["edit-hashline", 49, "file_change"],
    ["other-tools", 25, "file_change"],
    ["other-tools", 88, "dynamic"],
    ["other-tools", 316, "dynamic"],
    ["02-roundtrip", 23, "dynamic"],
  ])("%s:%i is %s", (name, line, kind) => {
    const frame = start(name, line);
    expect(classifyOmpTool(frame.toolName, frame.args, TOOL_COMMANDS)).toBe(kind);
  });

  it("treats a user's MCP command as a tool command before its mount notice arrives", () => {
    const args = { command: "mcp__rea_address_name --address 0x10" };
    expect(classifyOmpTool("bash", args, ompToolCommandNames([], []))).toBe("tool_command");
    expect(classifyOmpTool("bash", { command: "echo ast_grep" }, TOOL_COMMANDS)).toBe("command");
    expect(classifyOmpTool("ask", {}, TOOL_COMMANDS)).toBe("ask");
  });
});

describe("tool command names", () => {
  it("reads notices and hidden mount notices", () => {
    expect(ompToolCommandDelta(raw("01-registration", 10))).toEqual({
      added: ["t3_discover"],
      removed: [],
    });
    expect(ompToolCommandDelta(raw("01-registration", 33))).toEqual({
      added: [],
      removed: ["t3_discover"],
    });
    const mounted = {
      added: ["mcp__t3-code__thread_lookup", "mcp__t3-code__thread_rename"],
      removed: [],
    };
    expect(ompToolCommandDelta(raw("readonly-tool-command", 12))).toEqual(mounted);
    expect(ompToolCommandDelta(raw("readonly-tool-command", 20))).toEqual(mounted);
    expect(ompToolCommandDelta(raw("readonly-tool-command", 21))).toEqual(mounted);
    expect(ompToolCommandDelta(raw("readonly-tool-command", 13))).toBeUndefined();
    expect(
      ompToolCommandDelta({
        type: "notice",
        level: "info",
        source: "toolCommands",
        message: "Tool commands: added mcp__a_one, mcp__a_two; removed ast_grep",
      }),
    ).toEqual({ added: ["mcp__a_one", "mcp__a_two"], removed: ["ast_grep"] });
  });

  it("starts from the built-ins and registered host tools, then applies deltas in order", () => {
    const names = ompToolCommandNames(
      ["t3_discover"],
      [
        { added: ["mcp__docs_search"], removed: ["ast_grep"] },
        { added: [], removed: ["t3_discover"] },
      ],
    );
    expect(names.has("ast_edit")).toBe(true);
    expect(names.has("mcp__docs_search")).toBe(true);
    expect(names.has("ast_grep")).toBe(false);
    expect(names.has("t3_discover")).toBe(false);
  });
});

describe("bash rows", () => {
  it("streams cumulative output as appended suffixes", () => {
    let latest = "";
    const appended: Array<string> = [];
    for (let line = 91; line <= 98; line++) {
      const progress = ompCommandOutputProgress(latest, update("02-turns-full-updates", line));
      if (progress.kind === "replace") throw new Error(`line ${line} replaced the output`);
      if (progress.kind === "append") {
        appended.push(progress.delta);
        latest = progress.latest;
      }
    }
    expect(appended).toEqual(["line1\n", "line2\n", "line3\n", "\n\nWall time: 3.01 seconds"]);
    const result = completed("02-turns-full-updates", 90, 99);
    expect(result).toEqual({
      status: "completed",
      backgroundJobId: undefined,
      body: {
        type: "command_execution",
        title: "bash",
        input: "for i in 1 2 3; do echo line$i; sleep 1; done",
        output: latest,
        exitCode: 0,
      },
    });
  });

  it("streams the elision line and tail once truncation turns the head into head and tail", () => {
    const headOnly = ompCommandOutputProgress("", update("g12-large-output", 25));
    if (headOnly.kind !== "append") throw new Error("expected the first chunk to append");
    const headAndTail = ompCommandOutputProgress(headOnly.latest, update("g12-large-output", 26));
    expect(headAndTail.kind).toBe("replace");
    if (headAndTail.kind !== "replace") return;
    expect(headAndTail.delta.startsWith("[…30564ln elided…]\n34882\n")).toBe(true);
    expect(headAndTail.delta.endsWith("39999\n40000\n")).toBe(true);
    expect(headAndTail.latest.startsWith("1\n2\n3\n")).toBe(true);
    expect(ompCommandOutputProgress(headAndTail.latest, update("g12-large-output", 26))).toEqual({
      kind: "unchanged",
    });
  });

  it.each([
    [
      "the lines a moving tail adds",
      "1\n2\n[…5ln elided…]\n8\n9\n",
      "1\n2\n[…6ln elided…]\n9\n10\n",
      "10\n",
    ],
    [
      "the rest of a line the last preview cut",
      "1\n[…5ln elided…]\n7\nab",
      "1\n[…6ln elided…]\nabcd\n8\n",
      "cd\n8\n",
    ],
    [
      "the elision line when the tail moved past the last preview",
      "1\n[…5ln elided…]\n7\n",
      "1\n[…9ln elided…]\n11\n",
      "[…9ln elided…]\n11\n",
    ],
  ])("streams only %s past truncation", (_case, previous, latest, delta) => {
    const progress = ompCommandOutputProgress(previous, bashUpdate(latest));
    expect(progress).toEqual({ kind: "replace", delta, latest });
  });

  it("keeps output when an empty partial text arrives", () => {
    expect(ompCommandOutputProgress("slept\n", update("06-abort-variants", 67))).toEqual({
      kind: "unchanged",
    });
  });

  it("reports omp's non-zero exit code", () => {
    const result = completed("09-local-commands-and-compaction", 50, 55);
    expect(result.status).toBe("failed");
    expect(result.body).toMatchObject({ type: "command_execution", exitCode: 2 });
    expect(result.body).not.toHaveProperty("outputIndicatesFailure");
  });

  it("maps 'Command aborted' to interrupted only while an abort is pending", () => {
    const interrupted = completed("06-abort-variants", 64, 68, { abortPending: true });
    expect(interrupted.status).toBe("interrupted");
    expect(interrupted.body).toEqual({
      type: "command_execution",
      title: "bash",
      input: "sleep 10; echo slept",
      output: "Command aborted",
    });
    const failed = completed("06-abort-variants", 64, 68);
    expect(failed.status).toBe("failed");
    expect(failed.body).toMatchObject({ outputIndicatesFailure: true });
  });

  it("keeps a backgrounded command's job open until a late update completes it", () => {
    const backgrounded = completed("04-queue-followup-while-streaming", 22, 49);
    expect(backgrounded.status).toBe("completed");
    expect(backgrounded.backgroundJobId).toBe("bg_1");
    expect(backgrounded.body).not.toHaveProperty("exitCode");

    expect(isOmpBackgroundJobCompletion(update("04-queue-followup-while-streaming", 79))).toBe(
      false,
    );
    const finished = update("04-queue-followup-while-streaming", 81);
    expect(isOmpBackgroundJobCompletion(finished)).toBe(true);
    expect(
      ompToolItemFromBackgroundCompletion(
        start("04-queue-followup-while-streaming", 22),
        finished,
        "command",
      ),
    ).toEqual({
      status: "completed",
      backgroundJobId: undefined,
      body: {
        type: "command_execution",
        title: "bash",
        input: "sleep 12; echo slept",
        output: "slept\n\n\nWall time: 12.04 seconds",
        exitCode: 0,
      },
    });
  });

  it("closes a failed or cancelled background job", () => {
    const startFrame = start("04-queue-followup-while-streaming", 22);
    const finished = update("04-queue-followup-while-streaming", 81);
    const withState = (state: string, details: OmpRecord = {}) => ({
      ...finished,
      partialResult: {
        content: [{ type: "text", text: "boom" }],
        details: { ...details, async: { state, jobId: "bg_1", type: "bash" } },
      },
    });
    const failed = ompToolItemFromBackgroundCompletion(
      startFrame,
      withState("failed", { exitCode: 3 }),
      "command",
    );
    expect(failed.status).toBe("failed");
    expect(failed.body).toMatchObject({ exitCode: 3 });
    expect(isOmpBackgroundJobCompletion(withState("cancelled"))).toBe(true);
    expect(
      ompToolItemFromBackgroundCompletion(startFrame, withState("cancelled"), "command").status,
    ).toBe("interrupted");
  });
});

describe("tool command rows", () => {
  it("names the row after the dispatched tool and shows its arguments", () => {
    expect(ompToolItemFromStart(start("other-tools", 226), "tool_command")).toEqual({
      type: "dynamic_tool",
      title: "ast_grep",
      toolName: "ast_grep",
      input: { command: "ast_grep 'console.log($$$ARGS)' --path src/app.ts --lang ts" },
    });
    const result = completed("other-tools", 226, 233);
    expect(result.status).toBe("completed");
    expect(result.body).toMatchObject({
      type: "dynamic_tool",
      toolName: "ast_grep",
      input: { pat: "console.log($$$ARGS)", path: "src/app.ts", lang: "ts" },
      output: expect.stringContaining("*2:console.log"),
    });
  });

  it("keeps ast_edit and resolve as tool rows, never file changes", () => {
    const staged = completed("other-tools", 288, 292);
    expect(staged.backgroundJobId).toBe("bg_1");
    expect(staged.body).toMatchObject({ type: "dynamic_tool", toolName: "ast_edit" });

    expect(isOmpBackgroundJobCompletion(update("other-tools", 298))).toBe(false);
    const proposal = ompToolItemFromBackgroundCompletion(
      start("other-tools", 288),
      update("other-tools", 301),
      "tool_command",
    );
    expect(proposal.status).toBe("completed");
    expect(proposal.body).toMatchObject({
      type: "dynamic_tool",
      toolName: "ast_edit",
      input: {
        ops: [{ pat: "console.log($$$ARGS)", out: "console.debug($$$ARGS)" }],
        paths: ["src/app.ts"],
      },
      output: expect.stringContaining("Staged as a proposal"),
    });

    expect(completed("other-tools", 334, 339).body).toMatchObject({
      type: "dynamic_tool",
      toolName: "resolve",
      input: { reason: "Replace console.log with console.debug in src/app.ts as requested" },
      output: "Applied 2 replacements in 1 file.\n[src/app.ts#F65B]\n",
    });
  });

  it("renders T3 host tools mounted as commands, including one backgrounded for approval", () => {
    expect(completed("02-roundtrip", 141, 148).body).toMatchObject({
      toolName: "t3_discover",
      input: { query: "abc" },
      output: "record for $request.arguments.query: status=green\n",
    });
    expect(completed("readonly-tool-command", 32, 40).body).toMatchObject({
      type: "dynamic_tool",
      toolName: "mcp__t3-code__thread_lookup",
      input: { query: "alpha" },
      output: "ok from T3 for $request.toolName\n",
    });
    const rename = start("readonly-tool-command", 70);
    expect(completed("readonly-tool-command", 70, 72).backgroundJobId).toBe("bg_1");
    expect(isOmpBackgroundJobCompletion(update("readonly-tool-command", 82))).toBe(false);
    expect(
      ompToolItemFromBackgroundCompletion(
        rename,
        update("readonly-tool-command", 83),
        "tool_command",
      ),
    ).toMatchObject({
      status: "completed",
      body: { toolName: "mcp__t3-code__thread_rename", input: { title: "beta" } },
    });
    const hostCall = frame("readonly-tool-command", 33, "host_tool_call");
    const lookup = start("readonly-tool-command", 32);
    expect(ompParentToolCallId(hostCall.toolCallId)).toBe(lookup.toolCallId);
    expect(ompParentToolCallId(lookup.toolCallId)).toBeUndefined();
  });

  it("titles a bash row that runs an MCP tool after the MCP tool", () => {
    const presented = ompToolTitle("bash", { command: "mcp__docs__search_documentation --q x" });
    expect(presented).toBe("search documentation");
    expect(ompToolTitle("bash", { command: "ls" })).toBe("bash");
    expect(ompToolTitle(" ", {})).toBeNull();
  });
});

describe("edit rows", () => {
  it.each([
    ["edit-hashline", 49, 54, 53],
    ["edit-replace", 41, 44, undefined],
    ["edit-patch", 56, 59, 54],
    ["edit-apply_patch", 60, 63, 58],
  ])(
    "%s builds a unified diff from the file snapshots",
    (name, startLine, endLine, previewLine) => {
      const result = completed(name, startLine, endLine, { previewLine });
      expect(result).toEqual({
        status: "completed",
        backgroundJobId: undefined,
        body: {
          type: "file_change",
          title: "edit",
          fileName: "sample.txt",
          diffStr: SAMPLE_LINE_5_PATCH,
          additions: 1,
          deletions: 1,
          changes: [{ operation: "update", path: "sample.txt" }],
        },
      });
    },
  );

  it.each([
    [
      "edit-hashline",
      265,
      268,
      "Edit rejected for sample.txt: hash #0000 is not from this session.",
    ],
    ["edit-replace", 239, 244, "Could not find a close enough match in sample.txt."],
    ["edit-patch", 253, 256, "Failed to find context 'this line does not exist anywhere'"],
    ["edit-apply_patch", 251, 254, "Failed to find expected lines in sample.txt:"],
  ])("%s shows a failed edit's error as its diff", (name, startLine, endLine, error) => {
    const result = completed(name, startLine, endLine);
    expect(result.status).toBe("failed");
    expect(result.body).toMatchObject({ type: "file_change", fileName: "sample.txt" });
    expect(result.body).toHaveProperty("diffStr", expect.stringContaining(error));
    expect(result.body).not.toHaveProperty("changes");
  });

  it.each([
    ["edit-hashline", 109, 112],
    ["edit-patch", 104, 107],
  ])("%s keeps both hunks of a multi-hunk edit", (name, startLine, endLine) => {
    const body = completed(name, startLine, endLine).body;
    expect(body).toMatchObject({ fileName: "sample.txt", additions: 2, deletions: 2 });
    const diffStr = "diffStr" in body ? (body.diffStr ?? "") : "";
    expect(diffStr.match(/^@@ .* @@$/gm)).toEqual(["@@ -7,7 +7,7 @@", "@@ -32,7 +32,7 @@"]);
  });

  it.each([
    ["edit-apply_patch", 210, 213, "second.txt"],
    ["edit-extras-hashline", 63, 66, "second.txt"],
    ["edit-apply_patch", 210, 213, "pkg/sample.txt"],
    ["edit-extras-hashline", 63, 66, "pkg/sample.txt"],
  ])(
    "%s:%i-%i records one change per file of a multi-file edit, second file %s",
    (name, startLine, endLine, second) => {
      const rename = { from: "second.txt", to: second };
      const body = ompToolItemFromEnd(
        frame(name, startLine, "tool_execution_start", rename),
        frame(name, endLine, "tool_execution_end", rename),
        "file_change",
        false,
      ).body;
      expect(body).toMatchObject({
        fileName: "sample.txt",
        additions: 2,
        deletions: 2,
        changes: [
          { operation: "update", path: "sample.txt" },
          { operation: "update", path: second },
        ],
      });
      const diffStr = "diffStr" in body ? (body.diffStr ?? "") : "";
      expect(diffStr.match(/^diff --git .*$/gm)).toEqual([
        `diff --git "a/sample.txt" "b/sample.txt"`,
        `diff --git "a/${second}" "b/${second}"`,
      ]);
    },
  );

  it("records a created and a deleted file", () => {
    const body = completed("edit-extras-apply_patch", 38, 41).body;
    expect(body).toMatchObject({
      fileName: "created.txt",
      additions: 1,
      deletions: 10,
      changes: [
        { operation: "add", path: "created.txt" },
        { operation: "delete", path: "second.txt" },
      ],
    });
    const diffStr = "diffStr" in body ? (body.diffStr ?? "") : "";
    expect(diffStr).toContain(`--- /dev/null\n+++ "b/created.txt"\n@@ -0,0 +1,1 @@\n+created\n`);
    expect(diffStr).toContain(
      `--- "a/second.txt"\n+++ /dev/null\n@@ -1,10 +0,0 @@\n-row 1: value 1\n`,
    );
  });

  it.each([
    ["edit-patch", 104, 107],
    ["edit-apply_patch", 210, 213],
    ["edit-extras-apply_patch", 38, 41],
  ])("%s converts omp's numbered diff when snapshots are pruned", (name, startLine, endLine) => {
    const endFrame = end(name, endLine);
    const pruned = { ...endFrame, result: withoutSnapshots(endFrame.result) };
    const startFrame = start(name, startLine);
    const fromSnapshots = ompToolItemFromEnd(startFrame, endFrame, "file_change", false);
    const fromNumberedDiff = ompToolItemFromEnd(startFrame, pruned, "file_change", false);
    expect(JSON.stringify(pruned.result)).not.toContain("oldText");
    expect(fromNumberedDiff).toEqual(fromSnapshots);
  });

  it("splits a pruned hashline diff into hunks where its numbering jumps", () => {
    const endFrame = end("edit-hashline", 112);
    const pruned = { ...endFrame, result: withoutSnapshots(endFrame.result) };
    const body = ompToolItemFromEnd(start("edit-hashline", 109), pruned, "file_change", false).body;
    const diffStr = "diffStr" in body ? (body.diffStr ?? "") : "";
    expect(diffStr.match(/^@@ .* @@$/gm)).toEqual(["@@ -8,5 +8,5 @@", "@@ -33,5 +33,5 @@"]);
  });

  it.each([
    ["a lone carriage return", "x = 1;\r"],
    ["a line separator", "s = 'a b';"],
  ])("converts a numbered diff row containing %s", (_label, removed) => {
    const diff = ` 1|keep\n-2|${removed}\n+2|y = 2;`;
    const section = [
      `diff --git "a/f.js" "b/f.js"`,
      `--- "a/f.js"`,
      `+++ "b/f.js"`,
      "@@ -1,2 +1,2 @@",
      " keep",
      `-${removed}`,
      "+y = 2;",
      "",
    ].join("\n");
    const pruned = ompToolItemFromEnd(
      { type: "tool_execution_start", toolCallId: "t", toolName: "edit", args: { path: "f.js" } },
      {
        type: "tool_execution_end",
        toolCallId: "t",
        toolName: "edit",
        result: { details: { path: "/ws/f.js", op: "update", diff, snapshotsPruned: true } },
      },
      "file_change",
      false,
    );
    expect(pruned.body).toMatchObject({ diffStr: section, additions: 1, deletions: 1 });
    expect(
      ompEditPreview({
        type: "tool_stream_update",
        toolCallId: "t",
        toolName: "edit",
        update: { streaming: false, files: [{ path: "f.js", diff }] },
      }),
    ).toEqual({ final: true, files: [{ path: "f.js", diffStr: section }] });
  });

  it("names the file from the edit's arguments in every mode", () => {
    expect(ompToolItemFromStart(start("edit-hashline", 49), "file_change")).toEqual({
      type: "file_change",
      title: "edit",
      fileName: "sample.txt",
    });
    expect(ompToolItemFromStart(start("edit-apply_patch", 60), "file_change")).toMatchObject({
      fileName: "sample.txt",
    });
    expect(ompToolItemFromStart(start("edit-extras-apply_patch", 38), "file_change")).toMatchObject(
      {
        fileName: "created.txt",
      },
    );
    const sloppy = (input: string) =>
      ompToolItemFromStart(
        { type: "tool_execution_start", toolCallId: "t", toolName: "edit", args: { input } },
        "file_change",
      );
    const block = '*** Replace Block: [dependencies]\n[dependencies]\nserde = "1"\n';
    expect(sloppy(`*** Edit File: Cargo.toml\n${block}`)).toMatchObject({ fileName: "Cargo.toml" });
    expect(sloppy(`*** Edit File: Cargo.toml all\n${block}`)).toMatchObject({
      fileName: "Cargo.toml",
    });
    expect(
      ompToolItemFromStart(
        { type: "tool_execution_start", toolCallId: "t", toolName: "edit", args: { input: "?" } },
        "file_change",
        preview("edit-hashline", 53),
      ),
    ).toMatchObject({ type: "file_change", fileName: "sample.txt" });
    expect(
      ompToolItemFromStart(
        { type: "tool_execution_start", toolCallId: "t", toolName: "edit", args: { input: "?" } },
        "file_change",
      ),
    ).toMatchObject({ type: "dynamic_tool", toolName: "edit" });
  });
});

describe("edit previews", () => {
  it("converts the numbered preview diff and shows it on the running row", () => {
    const finalPreview = preview("edit-hashline", 53);
    const section = [
      `diff --git "a/sample.txt" "b/sample.txt"`,
      `--- "a/sample.txt"`,
      `+++ "b/sample.txt"`,
      "@@ -3,5 +3,5 @@",
      " line 03: charlie",
      " line 04: delta",
      "-line 05: echo",
      "+line 05: ECHO",
      " line 06: foxtrot",
      " line 07: golf",
      "",
    ].join("\n");
    expect(finalPreview).toEqual({
      final: true,
      files: [{ path: "sample.txt", diffStr: section }],
    });
    expect(
      ompToolItemFromStart(start("edit-hashline", 49), "file_change", finalPreview),
    ).toMatchObject({ fileName: "sample.txt", diffStr: section });
    expect(preview("edit-hashline", 43)).toMatchObject({
      final: false,
      files: [{ path: "sample.txt", diffStr: expect.stringContaining("@@ -5,1 +5,1 @@\n") }],
    });
  });

  it("carries a failing edit's error, even without a path", () => {
    expect(preview("edit-hashline", 263)).toEqual({
      final: true,
      files: [
        {
          path: "sample.txt",
          error: expect.stringContaining("hash #0000 is not from this session"),
        },
      ],
    });
    expect(preview("edit-apply_patch", 248)).toEqual({
      final: false,
      files: [{ path: "sample.txt" }],
    });
    expect(preview("edit-apply_patch", 249)).toEqual({
      final: true,
      files: [{ path: "", error: expect.stringContaining("Failed to find expected lines") }],
    });
  });

  it("previews a created and a deleted file", () => {
    const files = preview("edit-extras-apply_patch", 36)?.files ?? [];
    expect(files.map((file) => file.path)).toEqual(["created.txt", "second.txt"]);
    expect(files[0]?.diffStr).toContain(`--- /dev/null\n+++ "b/created.txt"\n@@ -0,0 +1,1 @@\n`);
    expect(files[1]?.diffStr).toContain(`--- "a/second.txt"\n+++ /dev/null\n@@ -1,10 +0,0 @@\n`);
  });
});

describe("other tools", () => {
  it("records a write's path and content", () => {
    expect(completed("other-tools", 25, 29)).toEqual({
      status: "completed",
      backgroundJobId: undefined,
      body: {
        type: "file_change",
        title: "write",
        fileName: "hello.txt",
        newStr: "hello\nworld\n",
      },
    });
  });

  it("shows a read's display text, and an image read's file instead of its data", () => {
    expect(completed("other-tools", 88, 89).body).toEqual({
      type: "dynamic_tool",
      title: "read",
      toolName: "read",
      input: { path: "src/app.ts" },
      output: expect.stringMatching(/^export function greet\(name: string\): string \{\n\tconsole/),
    });
    expect(completed("other-tools", 116, 117).body).toEqual({
      type: "dynamic_tool",
      title: "read",
      toolName: "read",
      input: { path: "pixel.png" },
      output: expect.stringMatching(/^Read image file \[image\/png\]/),
      viewedImagePath: "pixel.png",
    });
  });

  it.each([
    ["pixel.png", "pixel.png"],
    ["local://shot.png", undefined],
    ["shot.png:1-2", undefined],
    ["src/app.ts", undefined],
  ])("previews a read of %s only when it is a workspace image", (path, viewedImagePath) => {
    const body = ompToolItemFromStart(
      { type: "tool_execution_start", toolCallId: "t", toolName: "read", args: { path } },
      "dynamic",
    );
    expect("viewedImagePath" in body ? body.viewedImagePath : undefined).toBe(viewedImagePath);
  });

  it.each([
    ["grep", 146, 147, expect.stringContaining("*2│\tconsole.log")],
    ["glob", 175, 177, "# docs/\nguide.md\nreadme.md\n## nested/\nfaq.md"],
    ["eval", 367, 372, "42"],
    ["wait", 316, 317, expect.stringContaining("## Completed (1)")],
  ])("renders %s as a dynamic tool", (toolName, startLine, endLine, output) => {
    expect(completed("other-tools", startLine, endLine).body).toMatchObject({
      type: "dynamic_tool",
      toolName,
      output,
    });
  });

  it("renders a top-level host tool and its failure", () => {
    expect(completed("02-roundtrip", 23, 29).body).toMatchObject({
      type: "dynamic_tool",
      toolName: "t3_echo",
      input: { message: "hello" },
      output: "ECHO:$request.arguments.message",
    });
    const failed = completed("02-roundtrip", 63, 67);
    expect(failed.status).toBe("failed");
    expect(failed.body).toMatchObject({
      toolName: "t3_fail",
      output: "T3 refused: thread not found",
    });
  });
});
