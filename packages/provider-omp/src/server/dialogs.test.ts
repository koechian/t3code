// @effect-diagnostics-next-line nodeBuiltinImport:off -- Pure tests read their recorded frames synchronously.
import * as NodeFS from "node:fs";

import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import {
  classifyOmpUiRequest,
  isOmpToolApprovalSelect,
  ompApprovalResponse,
  ompQuestionResponse,
  type OmpApprovalLabels,
  type OmpUiRequestPlan,
} from "./dialogs.ts";
import { OmpExtensionUiRequest, type OmpSelectUiRequest } from "./wire.ts";

const FixtureLine = Schema.Struct({
  frame: Schema.Record(Schema.String, Schema.Unknown),
  source: Schema.String,
});
const decodeFixtureLine = Schema.decodeUnknownSync(Schema.fromJsonString(FixtureLine));
const decodeUiRequest = Schema.decodeUnknownSync(OmpExtensionUiRequest);

const fixtureLines = ["approvals.jsonl", "questions.jsonl"].flatMap((name) =>
  NodeFS.readFileSync(new URL(`./__fixtures__/dialogs/${name}`, import.meta.url), "utf8")
    .trim()
    .split("\n")
    .map((line) => decodeFixtureLine(line)),
);

function fixtureFrame(source: string) {
  const line = fixtureLines.find((candidate) => candidate.source.endsWith(source));
  if (line === undefined) throw new Error(`no fixture line ${source}`);
  return line.frame;
}

const fixtureRequest = (source: string) => decodeUiRequest(fixtureFrame(source));

const ALWAYS_ASK = { fullAccess: false };

function plan(source: string, context = ALWAYS_ASK): OmpUiRequestPlan {
  return classifyOmpUiRequest(fixtureRequest(source), context);
}

function selectRequest(title: string, options: ReadonlyArray<string>): OmpSelectUiRequest {
  return { type: "extension_ui_request", id: "select-1", method: "select", title, options };
}

function questionsOf(result: OmpUiRequestPlan) {
  if (result.kind !== "questions") throw new Error(`expected questions, got ${result.kind}`);
  return result;
}

function approvalOf(result: OmpUiRequestPlan) {
  if (result.kind !== "approval") throw new Error(`expected an approval, got ${result.kind}`);
  return result;
}

const BASH_LABELS = {
  accept: "Approve",
  acceptForSession: "Approve bash for this session",
  decline: "Deny",
};

describe("classifyOmpUiRequest approvals", () => {
  it.each<{
    source: string;
    toolName: string;
    requestKind: string;
    prompt: string;
    labels: OmpApprovalLabels;
  }>([
    {
      source: "replies-always-ask.jsonl:21",
      toolName: "bash",
      requestKind: "command",
      prompt: "Command: echo one > one.txt",
      labels: BASH_LABELS,
    },
    {
      source: "replies-always-ask.jsonl:265",
      toolName: "write",
      requestKind: "file-change",
      prompt: "Path: seven.txt\nContent:\nseven",
      labels: { ...BASH_LABELS, acceptForSession: "Approve write for this session" },
    },
    {
      source: "edit-apply_patch.jsonl:211",
      toolName: "edit",
      requestKind: "file-change",
      prompt: "File: sample.txt\nFile: second.txt",
      labels: { ...BASH_LABELS, acceptForSession: "Approve edit for this session" },
    },
    {
      // A tool command's own approval: the title names it, not the bash call it runs under.
      source: "other-tools.jsonl:291",
      toolName: "ast_edit",
      requestKind: "file-change",
      prompt:
        "Pattern: console.log($$$ARGS)\nReplacement: console.debug($$$ARGS)\nPaths: src/app.ts",
      labels: { ...BASH_LABELS, acceptForSession: "Approve ast_edit for this session" },
    },
    {
      source: "05-approval.jsonl:23",
      toolName: "t3_echo",
      requestKind: "command",
      prompt: "Allow tool: t3_echo",
      labels: { ...BASH_LABELS, acceptForSession: "Approve t3_echo for this session" },
    },
    {
      // always-ask-cached grants: "Approve Once" is the per-call answer, and no session option.
      source: "grants-always-ask-cached.jsonl:22",
      toolName: "read",
      requestKind: "command",
      prompt:
        "Reason: target not yet approved\nFolder: <workspace> — 2 entries (1 folder, 1 file)\n  - .git/\n  - NOTES.md",
      labels: { accept: "Approve Once", decline: "Deny" },
    },
  ])("classifies $source as a $toolName approval", ({ source, ...expected }) => {
    const approval = approvalOf(plan(source));
    expect(approval).toEqual({
      kind: "approval",
      requestId: fixtureFrame(source)["id"],
      ...expected,
      options: [
        { decision: "decline", label: "Deny" },
        ...(expected.labels.acceptForSession === undefined
          ? []
          : [{ decision: "acceptForSession", label: expected.labels.acceptForSession }]),
        { decision: "accept", label: expected.labels.accept },
      ],
    });
  });

  it.each([
    {
      request: "replies-always-ask.jsonl:21",
      decision: "decline",
      reply: "replies-always-ask.jsonl:22",
    },
    {
      request: "replies-always-ask.jsonl:58",
      decision: "cancel",
      reply: "replies-always-ask.jsonl:59",
    },
    {
      request: "replies-always-ask.jsonl:191",
      decision: "acceptForSession",
      reply: "replies-always-ask.jsonl:192",
    },
    {
      request: "replies-always-ask.jsonl:265",
      decision: "accept",
      reply: "replies-always-ask.jsonl:266",
    },
  ] as const)("answers $decision with the recorded reply", ({ request, decision, reply }) => {
    const approval = approvalOf(plan(request));
    expect(ompApprovalResponse(approval.requestId, approval.labels, decision)).toEqual(
      fixtureFrame(reply),
    );
  });

  it("approves once wherever omp offers no session option or a decision was never offered", () => {
    const cached = approvalOf(plan("grants-always-ask-cached.jsonl:22"));
    expect(ompApprovalResponse(cached.requestId, cached.labels, "accept")).toMatchObject({
      value: "Approve Once",
    });
    expect(ompApprovalResponse(cached.requestId, cached.labels, "acceptForSession")).toMatchObject({
      value: "Approve Once",
    });
    expect(ompApprovalResponse(cached.requestId, cached.labels, "acceptAlways")).toMatchObject({
      value: "Approve Once",
    });
  });

  it.each([
    { title: "Allow tool: bash\nCommand: ls", options: ["Approve", "Revise", "Abort"] },
    { title: "Allow tool: bash", options: ["Deny", "Approve"] },
    { title: "Allow tool: two words", options: ["Approve", "Deny"] },
    { title: "Pick one", options: ["Approve", "Deny"] },
  ])("keeps a select that only looks like an approval as a question: $options", (input) => {
    const request = selectRequest(input.title, input.options);
    expect(isOmpToolApprovalSelect(request)).toBe(false);
    expect(classifyOmpUiRequest(request, ALWAYS_ASK).kind).toBe("questions");
  });

  it.each([
    {
      fullAccess: false,
      title: "Allow tool: read\nReason: Restricted folder: ~/secrets",
      override: undefined,
    },
    {
      fullAccess: true,
      title: "Allow tool: read\nReason: Restricted folder: ~/secrets",
      override: "ask_rule",
    },
    {
      fullAccess: true,
      title:
        "Allow tool: computer\nAction: click\nProvider safety checks:\n1. Malicious instructions",
      override: "provider_check",
    },
  ] as const)(
    "explains a prompt under full access as $override",
    ({ fullAccess, title, override }) => {
      const approval = approvalOf(
        classifyOmpUiRequest(selectRequest(title, ["Approve", "Deny"]), { fullAccess }),
      );
      expect(approval.fullAccessOverride).toBe(override);
      expect(approval.options).toEqual([
        { decision: "decline", label: "Deny" },
        { decision: "accept", label: "Approve" },
      ]);
    },
  );

  it("closes the pending request omp cancels", () => {
    expect(plan("unanswered-abort-steer.jsonl:23")).toEqual({
      kind: "close",
      targetId: fixtureFrame("unanswered-abort-steer.jsonl:21")["id"],
    });
  });
});

describe("classifyOmpUiRequest dialogs", () => {
  it.each([
    {
      request: "ext-ui-methods.jsonl:8",
      answer: "Typecheck",
      reply: "ext-ui-methods.jsonl:10",
    },
    { request: "ext-ui-methods.jsonl:20", answer: "yes", reply: "ext-ui-methods.jsonl:22" },
    {
      request: "ext-ui-methods.jsonl:26",
      answer: "feature/rpc-host",
      reply: "ext-ui-methods.jsonl:28",
    },
    {
      request: "ext-ui-methods.jsonl:35",
      answer: "feat: edited\n\nbody",
      reply: "ext-ui-methods.jsonl:37",
    },
  ])("answers $request with the recorded reply", ({ request, answer, reply }) => {
    const questions = questionsOf(plan(request));
    expect(questions.questions.map((question) => question.id)).toEqual([questions.requestId]);
    expect(
      ompQuestionResponse(questions.requestId, questions.answer, { [questions.requestId]: answer }),
    ).toEqual({ ok: true, response: fixtureFrame(reply) });
  });

  it("maps each dialog method to the question T3 can render", () => {
    expect(questionsOf(plan("ext-ui-methods.jsonl:8")).questions[0]).toEqual({
      id: "15dbc44fefc16393",
      header: "Question",
      question: "Pick checks to run",
      options: [
        { label: "Lint", description: "Checked. oxlint over changed files", value: "Lint" },
        { label: "Typecheck", description: "Typecheck", value: "Typecheck" },
        { label: "Tests", description: "vitest related", value: "Tests" },
        { label: "Done", description: "Done", value: "Done" },
      ],
      allowCustomAnswer: false,
    });
    expect(questionsOf(plan("ext-ui-methods.jsonl:20")).questions[0]).toMatchObject({
      header: "Deploy",
      question: "Deploy to staging?",
      options: [
        { label: "Yes", value: "yes" },
        { label: "No", value: "no" },
      ],
      allowCustomAnswer: false,
    });
    expect(questionsOf(plan("ext-ui-methods.jsonl:26")).questions[0]).toMatchObject({
      question: "Branch name",
      options: [],
      required: false,
    });
    expect(questionsOf(plan("ext-ui-methods.jsonl:35")).questions[0]).toMatchObject({
      question: "Commit message",
      initialAnswer: "feat: draft\n\nbody",
      required: false,
    });
    expect(questionsOf(plan("ext-ui-methods.jsonl:39")).questions[0]).toMatchObject({
      initialAnswer: "",
    });
  });

  it.each([
    { method: "select", request: "ext-ui-methods.jsonl:8", answer: "Build" },
    { method: "confirm", request: "ext-ui-methods.jsonl:20", answer: "maybe" },
    { method: "input", request: "ext-ui-methods.jsonl:26", answer: undefined },
  ])("refuses a $method answer omp would not accept", ({ request, answer }) => {
    const questions = questionsOf(plan(request));
    const answers = answer === undefined ? {} : { [questions.requestId]: answer };
    expect(ompQuestionResponse(questions.requestId, questions.answer, answers).ok).toBe(false);
  });

  it("declines a secret input instead of carrying the secret through T3", () => {
    expect(plan("ext-ui-methods.jsonl:30")).toEqual({
      kind: "decline_secret",
      requestId: "15dbc451ec41639c",
      title: "API key",
    });
  });

  it.each([
    {
      source: "ext-ui-methods.jsonl:44",
      expected: { kind: "notice", message: "plain info", level: "info" },
    },
    {
      source: "ext-ui-methods.jsonl:45",
      expected: { kind: "notice", message: "careful", level: "warning" },
    },
    {
      source: "ext-ui-methods.jsonl:46",
      expected: { kind: "notice", message: "broken", level: "error" },
    },
    {
      source: "builtin-autoresearch-off-notify.jsonl:10",
      expected: { kind: "notice", message: "Autoresearch mode disabled", level: "info" },
    },
    {
      source: "ext-ui-methods.jsonl:47",
      expected: { kind: "status", key: "probe", text: "Indexing 3/10" },
    },
    {
      source: "ext-ui-methods.jsonl:48",
      expected: { kind: "status", key: "probe", text: undefined },
    },
    { source: "ext-ui-methods.jsonl:49", expected: { kind: "ignore" } },
    { source: "ext-ui-methods.jsonl:51", expected: { kind: "ignore" } },
    { source: "ext-ui-methods.jsonl:52", expected: { kind: "ignore" } },
  ])("presents $source", ({ source, expected }) => {
    expect(plan(source)).toEqual(expected);
  });
});

describe("classifyOmpUiRequest ask dialogs", () => {
  const askSource = "ext-ask-dialog-replies.jsonl:10";

  it("maps each ask question 1:1, answering with labels", () => {
    expect(questionsOf(plan(askSource)).questions).toEqual([
      {
        id: "db",
        header: "Storage",
        question: "Which database should the service use?",
        options: [
          { label: "Postgres", description: "Shared server, needs ops", value: "Postgres" },
          {
            label: "SQLite",
            description: "Recommended: One file, no new service",
            value: "SQLite",
            preview: "DATABASE_URL=file:app.db",
          },
          { label: "DuckDB", description: "DuckDB", value: "DuckDB" },
        ],
      },
      {
        id: "features",
        header: "Question",
        question: "Which features ship first?",
        options: [
          { label: "Auth", description: "Auth", value: "Auth" },
          { label: "Billing", description: "Billing", value: "Billing" },
          { label: "Search", description: "Search", value: "Search" },
        ],
        multiSelect: true,
      },
    ]);
  });

  it.each([
    {
      name: "options",
      answers: { db: "SQLite", features: ["Auth", "Search"] },
      response: fixtureFrame("ext-ask-dialog-replies.jsonl:12"),
    },
    {
      // omp trims customInput itself (ext-ask-dialog-replies.jsonl:19); T3 sends it trimmed.
      name: "custom text",
      answers: { db: "  CockroachDB  ", features: ["Billing", "Audit log"] },
      response: {
        type: "extension_ui_response",
        id: "15dbc3e8681ecd86",
        answers: [
          { id: "db", selectedOptions: [], customInput: "CockroachDB" },
          { id: "features", selectedOptions: ["Billing"], customInput: "Audit log" },
        ],
      },
    },
  ])("replies with $name in question order", ({ answers, response }) => {
    const questions = questionsOf(plan(askSource));
    expect(ompQuestionResponse(questions.requestId, questions.answer, answers)).toEqual({
      ok: true,
      response,
    });
  });

  it("refuses a single-select answer that carries an option and text (omp rejected it at :43)", () => {
    const questions = questionsOf(plan("ext-ask-dialog-replies.jsonl:40"));
    const result = ompQuestionResponse(questions.requestId, questions.answer, {
      db: ["SQLite", "also this"],
      features: [],
    });
    expect(result.ok).toBe(false);
  });
});

describe("classifyOmpUiRequest forms", () => {
  const formRequest = (html: string) =>
    classifyOmpUiRequest(
      { type: "extension_ui_request", id: "form-1", method: "form", html },
      ALWAYS_ASK,
    );

  it("asks a form's fields and replies with its data", () => {
    const questions = questionsOf(
      formRequest(
        '<form><label>City <input name="city" required></label><input type=number name=n></form>',
      ),
    );
    expect(questions.questions.map((question) => question.id)).toEqual(["city", "n"]);
    expect(ompQuestionResponse("form-1", questions.answer, { city: "Nairobi", n: "4" })).toEqual({
      ok: true,
      response: { type: "extension_ui_response", id: "form-1", data: { city: "Nairobi", n: 4 } },
    });
    expect(ompQuestionResponse("form-1", questions.answer, { city: "", n: "4" }).ok).toBe(false);
  });

  it.each([
    "<form><p>Read this and carry on.</p></form>",
    "<form><select name=s><option disabled>Only</option></select></form>",
  ])("submits the defaults of a form T3 cannot ask: %s", (html) => {
    expect(formRequest(html)).toMatchObject({ kind: "unrenderable_form", requestId: "form-1" });
  });

  it("submits the defaults of a form that asks for a password instead of asking it unmasked", () => {
    const plan = formRequest(
      '<form><input name="user"><label>Registry token <input type="password" name="token" required></label></form>',
    );
    expect(plan).toMatchObject({ kind: "unrenderable_form", requestId: "form-1" });
    expect(plan.kind === "unrenderable_form" ? plan.reason : "").toContain("password");
  });
});
