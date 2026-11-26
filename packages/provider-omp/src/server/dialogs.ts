/**
 * `extension_ui_request` → T3 runtime requests (approvals, questions) or presentation, and
 * T3 decisions/answers → `extension_ui_response`. Pure; the adapter owns pending state.
 */
import type {
  OrchestrationV2UserInputQuestion,
  ProviderApprovalDecision,
  ProviderApprovalFullAccessOverride,
  ProviderApprovalOption,
  ProviderUserInputAnswers,
} from "@t3tools/contracts";

import {
  ompFormData,
  ompFormQuestions,
  parseOmpFormHtml,
  type OmpFormField,
  type OmpParsedForm,
} from "./htmlForm.ts";
import type {
  OmpAskAnswer,
  OmpAskQuestion,
  OmpConfirmUiRequest,
  OmpEditorUiRequest,
  OmpExtensionUiRequest,
  OmpExtensionUiResponse,
  OmpFormUiRequest,
  OmpInputUiRequest,
  OmpSelectUiRequest,
} from "./wire.ts";

/** Exact option labels; omp treats any other value as Deny. */
export interface OmpApprovalLabels {
  readonly accept: string;
  /** Present only when omp offered `Approve <tool> for this session`. */
  readonly acceptForSession?: string;
  readonly decline: string;
}

export type OmpAnswerShape =
  | { readonly method: "select"; readonly options: ReadonlyArray<string> }
  | { readonly method: "confirm" }
  | { readonly method: "input" }
  | { readonly method: "editor" }
  | { readonly method: "ask"; readonly questions: ReadonlyArray<OmpAskQuestion> }
  | { readonly method: "form"; readonly form: OmpParsedForm };

export type OmpUiRequestPlan =
  | {
      readonly kind: "approval";
      readonly requestId: string;
      readonly toolName: string;
      readonly requestKind: "command" | "file-change";
      /** Title lines after `Allow tool: <name>`. */
      readonly prompt: string;
      readonly options: ReadonlyArray<ProviderApprovalOption>;
      readonly labels: OmpApprovalLabels;
      readonly fullAccessOverride?: ProviderApprovalFullAccessOverride;
    }
  | {
      readonly kind: "questions";
      readonly requestId: string;
      readonly questions: ReadonlyArray<OrchestrationV2UserInputQuestion>;
      readonly answer: OmpAnswerShape;
    }
  /** A form T3 cannot render: submit its defaults (`cancelled + timedOut`) and post a notice. */
  | { readonly kind: "unrenderable_form"; readonly requestId: string; readonly reason: string }
  /** `input {secret: true}`: cancel it and post a notice; v1 has no secret delivery path. */
  | { readonly kind: "decline_secret"; readonly requestId: string; readonly title: string }
  | { readonly kind: "close"; readonly targetId: string }
  | {
      readonly kind: "notice";
      readonly message: string;
      readonly level: "info" | "warning" | "error";
    }
  | { readonly kind: "status"; readonly key: string; readonly text: string | undefined }
  | { readonly kind: "ignore" };

export interface OmpUiRequestContext {
  /** The session runs with `tools.approvalMode: yolo`; residual prompts get a fullAccessOverride. */
  readonly fullAccess: boolean;
}

type OmpPresentationUiRequest = Exclude<
  OmpExtensionUiRequest,
  { readonly method: "select" | "confirm" | "input" | "editor" | "ask" | "form" }
>;

const APPROVAL_TITLE = /^Allow tool: (\S+)$/;
const SESSION_APPROVAL_OPTION = /^Approve \S+ for this session$/;
const FILE_CHANGE_TOOLS = new Set(["edit", "write", "ast_edit"]);
/** `extensibility/extensions/wrapper.ts` appends this line, then one row per pending check. */
const PROVIDER_SAFETY_APPENDIX = "Provider safety checks:";
const QUESTION_HEADER = "Question";
const PASSWORD_FORM_REASON =
  "omp asked for a password in a form, which T3 Code cannot collect yet. The form was submitted with its defaults.";
const CONFIRM_YES = "yes";
const CONFIRM_NO = "no";

export function classifyOmpUiRequest(
  request: OmpExtensionUiRequest,
  context: OmpUiRequestContext,
): OmpUiRequestPlan {
  switch (request.method) {
    case "select":
      return selectPlan(request, context);
    case "confirm":
      return questionsPlan(request.id, [confirmQuestion(request)], { method: "confirm" });
    case "input":
      return inputPlan(request);
    case "editor":
      return questionsPlan(request.id, [editorQuestion(request)], { method: "editor" });
    case "ask":
      return questionsPlan(request.id, request.questions.map(askQuestion), {
        method: "ask",
        questions: request.questions,
      });
    case "form":
      return formPlan(request);
    default:
      return presentationPlan(request);
  }
}

/**
 * A tool approval: the first title line matches `^Allow tool: (\S+)$`, the last option is
 * "Deny", and the options include "Approve" or "Approve Once". Every other select is a question.
 */
export function isOmpToolApprovalSelect(request: OmpSelectUiRequest): boolean {
  const firstLine = request.title.split("\n", 1)[0] ?? "";
  return (
    APPROVAL_TITLE.test(firstLine) &&
    request.options.at(-1) === "Deny" &&
    (request.options.includes("Approve") || request.options.includes("Approve Once"))
  );
}

/** accept → "Approve Once"/"Approve"; acceptForSession → session label; decline → "Deny"; cancel → cancelled. */
export function ompApprovalResponse(
  requestId: string,
  labels: OmpApprovalLabels,
  decision: ProviderApprovalDecision,
): OmpExtensionUiResponse {
  if (decision === "cancel") return ompCancelResponse(requestId);
  return { type: "extension_ui_response", id: requestId, value: approvalLabel(labels, decision) };
}

export type OmpAnswerResult =
  | { readonly ok: true; readonly response: OmpExtensionUiResponse }
  | { readonly ok: false; readonly reason: string };

/** Validates first; select, confirm, input and editor answers sit under the request id. */
export function ompQuestionResponse(
  requestId: string,
  shape: OmpAnswerShape,
  answers: ProviderUserInputAnswers,
): OmpAnswerResult {
  switch (shape.method) {
    case "select":
      return selectResponse(requestId, shape.options, answerFor(answers, requestId));
    case "confirm":
      return confirmResponse(requestId, answerFor(answers, requestId));
    case "input":
    case "editor":
      return textResponse(requestId, answerFor(answers, requestId));
    case "ask":
      return askResponse(requestId, shape.questions, answers);
    case "form":
      return formResponse(requestId, shape.form, answers);
  }
}

/** `{cancelled: true}`; with `timedOut` an ask form submits its defaults instead of aborting. */
export function ompCancelResponse(
  requestId: string,
  options?: { readonly timedOut?: boolean },
): OmpExtensionUiResponse {
  return options?.timedOut === true
    ? { type: "extension_ui_response", id: requestId, cancelled: true, timedOut: true }
    : { type: "extension_ui_response", id: requestId, cancelled: true };
}

function questionsPlan(
  requestId: string,
  questions: ReadonlyArray<OrchestrationV2UserInputQuestion>,
  answer: OmpAnswerShape,
): OmpUiRequestPlan {
  return { kind: "questions", requestId, questions, answer };
}

function selectPlan(request: OmpSelectUiRequest, context: OmpUiRequestContext): OmpUiRequestPlan {
  if (isOmpToolApprovalSelect(request)) return approvalPlan(request, context);
  return questionsPlan(request.id, [selectQuestion(request)], {
    method: "select",
    options: request.options,
  });
}

function inputPlan(request: OmpInputUiRequest): OmpUiRequestPlan {
  if (request.secret === true) {
    return { kind: "decline_secret", requestId: request.id, title: request.title };
  }
  return questionsPlan(request.id, [inputQuestion(request)], { method: "input" });
}

function formPlan(request: OmpFormUiRequest): OmpUiRequestPlan {
  const parsed = parseOmpFormHtml(request.html);
  if (!parsed.ok) return unrenderableForm(request.id, parsed.reason);
  // Like `input {secret: true}`: T3 questions are unmasked and their answers are kept.
  if (parsed.form.fields.some(isPasswordField)) {
    return unrenderableForm(request.id, PASSWORD_FORM_REASON);
  }
  const questions = ompFormQuestions(parsed.form);
  if (questions.length === 0) return unrenderableForm(request.id, "No field can be asked");
  return questionsPlan(request.id, questions, { method: "form", form: parsed.form });
}

function unrenderableForm(requestId: string, reason: string): OmpUiRequestPlan {
  return { kind: "unrenderable_form", requestId, reason };
}

function isPasswordField(field: OmpFormField): boolean {
  return field.kind === "text" && field.type === "password";
}

function presentationPlan(request: OmpPresentationUiRequest): OmpUiRequestPlan {
  switch (request.method) {
    case "cancel":
      return { kind: "close", targetId: request.targetId };
    case "notify":
      return { kind: "notice", message: request.message, level: request.notifyType ?? "info" };
    case "setStatus":
      return { kind: "status", key: request.statusKey, text: request.statusText };
    default:
      return { kind: "ignore" };
  }
}

function approvalPlan(request: OmpSelectUiRequest, context: OmpUiRequestContext): OmpUiRequestPlan {
  const [firstLine = "", ...details] = request.title.split("\n");
  const toolName = APPROVAL_TITLE.exec(firstLine)?.[1] ?? "";
  const labels = approvalLabels(request.options);
  return {
    kind: "approval",
    requestId: request.id,
    toolName,
    requestKind: FILE_CHANGE_TOOLS.has(toolName) ? "file-change" : "command",
    prompt: approvalPrompt(firstLine, details),
    options: approvalOptions(labels),
    labels,
    ...(context.fullAccess ? { fullAccessOverride: fullAccessOverride(details) } : {}),
  };
}

function approvalLabels(options: ReadonlyArray<string>): OmpApprovalLabels {
  const acceptForSession = options.find((option) => SESSION_APPROVAL_OPTION.test(option));
  return {
    accept: options.includes("Approve Once") ? "Approve Once" : "Approve",
    ...(acceptForSession === undefined ? {} : { acceptForSession }),
    decline: "Deny",
  };
}

function approvalOptions(labels: OmpApprovalLabels): ReadonlyArray<ProviderApprovalOption> {
  return [
    { decision: "decline", label: labels.decline },
    ...(labels.acceptForSession === undefined
      ? []
      : [{ decision: "acceptForSession" as const, label: labels.acceptForSession }]),
    { decision: "accept", label: labels.accept },
  ];
}

function approvalPrompt(firstLine: string, details: ReadonlyArray<string>): string {
  const prompt = details.join("\n").replace(/^\n+/, "").trimEnd();
  return prompt === "" ? firstLine : prompt;
}

/** Under yolo only a policy, a restricted folder or a provider safety check still asks. */
function fullAccessOverride(details: ReadonlyArray<string>): ProviderApprovalFullAccessOverride {
  return details.some((line) => line.trim() === PROVIDER_SAFETY_APPENDIX)
    ? "provider_check"
    : "ask_rule";
}

function approvalLabel(
  labels: OmpApprovalLabels,
  decision: Exclude<ProviderApprovalDecision, "cancel">,
): string {
  switch (decision) {
    case "decline":
      return labels.decline;
    case "acceptForSession":
      return labels.acceptForSession ?? labels.accept;
    default:
      // acceptAlways is never offered: omp's "Always Approve" writes a cache the user's TUI shares.
      return labels.accept;
  }
}

function selectQuestion(request: OmpSelectUiRequest): OrchestrationV2UserInputQuestion {
  return {
    id: request.id,
    header: QUESTION_HEADER,
    question: nonBlank(request.title) ?? "Choose an option",
    options: request.options.map((option, index) => selectOption(request, option, index)),
    allowCustomAnswer: false,
  };
}

/** A checkbox-marked select is one toggle of a loop, so the checked rows say so. */
function selectOption(request: OmpSelectUiRequest, option: string, index: number) {
  const label = nonBlank(option) ?? "Empty value";
  const detail = nonBlank(request.optionDetails?.[index]?.description);
  const checked =
    request.selectionMarker === "checkbox" && request.checkedIndices?.includes(index) === true;
  const description = checked ? (detail === undefined ? "Checked" : `Checked. ${detail}`) : detail;
  return { label, description: description ?? label, value: option };
}

function confirmQuestion(request: OmpConfirmUiRequest): OrchestrationV2UserInputQuestion {
  return {
    id: request.id,
    header: nonBlank(request.title) ?? "Confirm",
    question: nonBlank(request.message) ?? nonBlank(request.title) ?? "Confirm?",
    options: [
      { label: "Yes", description: "Yes", value: CONFIRM_YES },
      { label: "No", description: "No", value: CONFIRM_NO },
    ],
    allowCustomAnswer: false,
  };
}

function inputQuestion(request: OmpInputUiRequest): OrchestrationV2UserInputQuestion {
  return {
    id: request.id,
    header: QUESTION_HEADER,
    question: nonBlank(request.title) ?? "Enter a value",
    options: [],
    required: false,
  };
}

function editorQuestion(request: OmpEditorUiRequest): OrchestrationV2UserInputQuestion {
  return {
    id: request.id,
    header: QUESTION_HEADER,
    question: nonBlank(request.title) ?? "Edit the text",
    options: [],
    initialAnswer: request.prefill ?? "",
    required: false,
  };
}

function askQuestion(question: OmpAskQuestion, index: number): OrchestrationV2UserInputQuestion {
  return {
    id: askQuestionId(question, index),
    header: nonBlank(question.header) ?? QUESTION_HEADER,
    question: nonBlank(question.question) ?? nonBlank(question.header) ?? QUESTION_HEADER,
    options: question.options.map((option, optionIndex) => ({
      label: nonBlank(option.label) ?? `Option ${optionIndex + 1}`,
      description: askOptionDescription(option, optionIndex === question.recommended),
      value: option.label,
      ...(option.preview === undefined ? {} : { preview: option.preview }),
    })),
    ...(question.multi === true ? { multiSelect: true } : {}),
  };
}

function askOptionDescription(option: OmpAskQuestion["options"][number], recommended: boolean) {
  const description = nonBlank(option.description);
  if (!recommended) return description ?? nonBlank(option.label) ?? "Option";
  return description === undefined ? "Recommended" : `Recommended: ${description}`;
}

/** T3 needs trimmed, non-empty question ids; omp's own id still goes back in the reply. */
function askQuestionId(question: OmpAskQuestion, index: number): string {
  return question.id !== "" && question.id.trim() === question.id
    ? question.id
    : `question-${index + 1}`;
}

function selectResponse(
  requestId: string,
  options: ReadonlyArray<string>,
  answer: unknown,
): OmpAnswerResult {
  const value = singleText(answer);
  if (value === undefined || !options.includes(value)) {
    return { ok: false, reason: "Choose one of the offered options" };
  }
  return { ok: true, response: { type: "extension_ui_response", id: requestId, value } };
}

function confirmResponse(requestId: string, answer: unknown): OmpAnswerResult {
  const value = singleText(answer);
  if (value !== CONFIRM_YES && value !== CONFIRM_NO)
    return { ok: false, reason: "Answer yes or no" };
  return {
    ok: true,
    response: { type: "extension_ui_response", id: requestId, confirmed: value === CONFIRM_YES },
  };
}

function textResponse(requestId: string, answer: unknown): OmpAnswerResult {
  const value = singleText(answer);
  if (value === undefined) return { ok: false, reason: "Enter a text answer" };
  return { ok: true, response: { type: "extension_ui_response", id: requestId, value } };
}

function askResponse(
  requestId: string,
  questions: ReadonlyArray<OmpAskQuestion>,
  answers: ProviderUserInputAnswers,
): OmpAnswerResult {
  const replies: Array<OmpAskAnswer> = [];
  for (const [index, question] of questions.entries()) {
    const reply = askReply(question, answerFor(answers, askQuestionId(question, index)));
    if (!reply.ok) return reply;
    replies.push(reply.answer);
  }
  return {
    ok: true,
    response: { type: "extension_ui_response", id: requestId, answers: replies },
  };
}

/** omp rejects unknown or repeated labels, and a single-select carrying an option and text. */
function askReply(
  question: OmpAskQuestion,
  answer: unknown,
):
  | { readonly ok: true; readonly answer: OmpAskAnswer }
  | { readonly ok: false; readonly reason: string } {
  const entries = answerEntries(answer);
  const label = JSON.stringify(question.question);
  if (entries === undefined) return { ok: false, reason: `${label} needs text or options` };
  const labels = new Set(question.options.map((option) => option.label));
  const selectedOptions = [...new Set(entries.filter((entry) => labels.has(entry)))];
  const customInput = entries
    .filter((entry) => !labels.has(entry))
    .map((entry) => entry.trim())
    .filter((entry) => entry !== "")
    .join(", ");
  if (question.multi !== true && selectedOptions.length + (customInput === "" ? 0 : 1) > 1) {
    return { ok: false, reason: `${label} takes one answer` };
  }
  return {
    ok: true,
    answer: { id: question.id, selectedOptions, ...(customInput === "" ? {} : { customInput }) },
  };
}

function formResponse(
  requestId: string,
  form: OmpParsedForm,
  answers: ProviderUserInputAnswers,
): OmpAnswerResult {
  const result = ompFormData(form, answers);
  if (!result.ok) return result;
  return {
    ok: true,
    response: { type: "extension_ui_response", id: requestId, data: result.data },
  };
}

function answerFor(answers: ProviderUserInputAnswers, questionId: string): unknown {
  return Object.hasOwn(answers, questionId) ? answers[questionId] : undefined;
}

function answerEntries(answer: unknown): ReadonlyArray<string> | undefined {
  if (answer === undefined || answer === null) return [];
  if (typeof answer === "string") return [answer];
  if (!Array.isArray(answer)) return undefined;
  return answer.every((entry) => typeof entry === "string") ? answer : undefined;
}

function singleText(answer: unknown): string | undefined {
  if (typeof answer === "string") return answer;
  return Array.isArray(answer) && answer.length === 1 && typeof answer[0] === "string"
    ? answer[0]
    : undefined;
}

function nonBlank(text: string | undefined): string | undefined {
  const trimmed = text?.trim();
  return trimmed === undefined || trimmed === "" ? undefined : trimmed;
}
