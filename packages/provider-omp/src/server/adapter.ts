/**
 * OMP session runtime: one `omp --mode rpc-ui` process per provider session, one native thread
 * (session file) per process. Owns process lifecycle, the event pump, turn state, pending
 * runtime requests, and item emission; protocol shapes live in the sibling modules.
 * Copy-and-adapt of packages/provider-pi/src/server/adapter.ts (see MODULES.md).
 */
import {
  ProviderDriverKind,
  type ChatAttachment,
  type MessageId,
  type ModelSelection,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderFailure,
  type OrchestrationV2ProviderRef,
  type OrchestrationV2ProviderRetry,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2ProviderTurnTokenUsage,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2TurnItem,
  type ProviderApprovalDecision,
  type ProviderInstanceId,
  type ProviderTurnId,
  type ProviderUserInputAnswers,
  type RuntimeMode,
} from "@t3tools/contracts";
import type * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as HttpClient from "effect/http/HttpClient";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { AgentScope } from "@t3tools/shared/AgentScope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import {
  ProviderAdapterDriverCreateError,
  type ProviderAdapterDriver,
  type ProviderAdapterDriverCreateInput,
} from "@t3tools/provider-core/server/adapterDriver";
import { providerMessageTextWithAttachmentPaths } from "@t3tools/provider-core/server/attachmentPrompt";
import {
  makeProviderFailure,
  makeProviderRetryTurnItem,
} from "@t3tools/provider-core/server/failure";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import { T3_CODE_ORCHESTRATION_INSTRUCTIONS } from "@t3tools/provider-core/server/orchestrationInstructions";
import { PULL_REQUEST_LINKING_INSTRUCTIONS } from "@t3tools/provider-core/server/runtimeInstructions";
import { turnScopedSelectionTransition } from "@t3tools/provider-core/server/selectionTransition";

import { OmpSettings } from "../settings.ts";
import { OmpProviderCapabilitiesV2 } from "./capabilities.ts";
import {
  classifyOmpSlashInput,
  isOmpCommandInvocation,
  stripAnsi,
  type OmpSlashDisposition,
} from "./commands.ts";
import {
  classifyOmpUiRequest,
  ompApprovalResponse,
  ompCancelResponse,
  ompQuestionResponse,
  type OmpAnswerShape,
  type OmpApprovalLabels,
  type OmpUiRequestPlan,
} from "./dialogs.ts";
import {
  EMPTY_OMP_HOST_TOOL_SET,
  OMP_HOST_TOOLS_UNAVAILABLE_NOTICE,
  loadOmpHostTools,
  makeOmpHostToolBridge,
  makeT3McpClient,
  type OmpHostToolBridge,
  type OmpHostToolSet,
  type T3McpClient,
} from "./hostTools.ts";
import {
  buildOmpRpcLaunch,
  composeOmpAppendSystemPrompt,
  makeOmpUnattendedOverlay,
  ompAppendSystemPromptPath,
  ompApprovalModeForRuntimeMode,
  ompOverlayPath,
  renderOmpSessionOverlay,
  resolveOmpLaunchArgs,
  writeOmpConfigOverlay,
  type OmpApprovalMode,
} from "./launch.ts";
import {
  OMP_DEFAULT_MODEL_SLUG,
  OMP_MODEL_PICKER_LIMIT,
  ompModelSlug,
  resolveOmpModelSlug,
  validateOmpThinkingLevel,
} from "./modelCapabilities.ts";
import {
  makeOmpRpcConnection,
  OMP_DIALOG_METHODS,
  type OmpRpcConnection,
  type OmpRpcError,
  type OmpRpcTimeoutError,
} from "./rpc.ts";
import {
  deleteOmpSessionFiles,
  forkOmpSessionInSeparateProcess,
  ompForkBoundary,
  ompRollbackEntryId,
  ompSessionDirForCwd,
  ompTurnUserEntryId,
  sessionFileMatchesCwd,
} from "./sessions.ts";
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
  type OmpEditPreview,
  type OmpToolCommandDelta,
  type OmpToolItemBody,
  type OmpToolKind,
} from "./toolItems.ts";
import {
  decodeOmpInboundFrame,
  OmpBranchMessage,
  OmpBranchResult,
  OmpModelInfo,
  OmpModelPickerResult,
  OmpPromptAck,
  OmpSessionState,
  OmpSubagentList,
  ompRecordBoolean,
  ompRecordField,
  ompRecordNumber,
  ompRecordString,
  type OmpAssistantMessageEvent,
  type OmpCommand,
  type OmpCommandFrame,
  type OmpImageContent,
  type OmpInboundFrame,
  type OmpModelPickerRow,
  type OmpPromptError,
  type OmpPromptResultFrame,
  type OmpRecord,
  type OmpResponseFrame,
  type OmpStreamingBehavior,
  type OmpThinkingSelector,
  type OmpToolExecutionEndFrame,
  type OmpToolExecutionStartFrame,
  type OmpToolExecutionUpdateFrame,
  type OmpUsageLimit,
} from "./wire.ts";

export const OMP_PROVIDER = ProviderDriverKind.make("omp");
const DEFAULT_OMP_SETTINGS = Schema.decodeSync(OmpSettings)({});

const STREAM_FLUSH_MS = 50;
const OMP_SHORT_REQUEST_TIMEOUT_MS = 2_000;
/** `cancel_subagent` answers once the child's abort settled, about 1.3 s after it was sent. */
const OMP_SUBAGENT_CANCEL_TIMEOUT_MS = 5_000;
/** What Stop asks of omp before ending it; with the 3 s end grace it stays inside 10 s. */
const OMP_STOP_BUDGET_MS = 7_000;
const OMP_LIFECYCLE_TIMEOUT_MS = 60_000;
const MAX_NOTICE_LENGTH = 4_000;
const MAX_NOTICE_TITLE_LENGTH = 200;
const OMP_UNSOLICITED_ACTIVITY_ERROR =
  "omp started agent work outside an active T3 turn. The session was stopped to prevent invisible tool execution.";
const OMP_T3_INSTRUCTIONS = `${T3_CODE_ORCHESTRATION_INSTRUCTIONS.trim()}\n\n${PULL_REQUEST_LINKING_INSTRUCTIONS}`;
const OMP_NO_HOST_TOOL_RESULT = "T3 Code tools are unavailable in this omp session.";
const OMP_HOST_TOOLS_REFUSED_NOTICE =
  "T3 Code tools are unavailable in this session because omp did not accept them. The agent keeps working without them.";
const OMP_STARTUP_DIALOG_PENDING_DETAIL =
  "omp is waiting for an answer to a dialog it opened while starting. Answer it, then try again";
const APPROVAL_MODE_RANK: Readonly<Record<OmpApprovalMode, number>> = {
  "always-ask": 0,
  write: 1,
  yolo: 2,
};
const INTERACTIVE_UI_METHODS: ReadonlySet<string> = new Set([...OMP_DIALOG_METHODS, "cancel"]);

type OmpPreparationError =
  | OmpRpcError
  | OmpRpcTimeoutError
  | ProviderAdapter.ProviderAdapterProtocolError;

const decodeSessionState = Schema.decodeUnknownOption(OmpSessionState);
const decodeBranchResult = Schema.decodeUnknownOption(OmpBranchResult);
const decodeBranchMessages = Schema.decodeUnknownOption(
  Schema.Struct({ messages: Schema.Array(OmpBranchMessage) }),
);
const decodeModelPicker = Schema.decodeUnknownOption(OmpModelPickerResult);
const decodeThinkingLevels = Schema.decodeUnknownOption(
  Schema.Struct({ levels: Schema.Array(Schema.String) }),
);
const decodePromptAck = Schema.decodeUnknownOption(OmpPromptAck);
const decodeModelInfo = Schema.decodeUnknownOption(OmpModelInfo);
const decodeSubagentList = Schema.decodeUnknownOption(OmpSubagentList);
const decodeHostToolsReply = Schema.decodeUnknownOption(
  Schema.Struct({ toolNames: Schema.Array(Schema.String) }),
);

export interface OmpAdapterV2Options {
  readonly instanceId: ProviderInstanceId;
  readonly settings: OmpSettings;
  readonly environment: NodeJS.ProcessEnv;
  readonly continuationRequests?: {
    readonly offer: (
      request: ProviderContinuationRequests.ProviderContinuationRequest,
    ) => Effect.Effect<void>;
  };
}

function contentText(content: unknown): string {
  if (!Array.isArray(content)) {
    return typeof content === "string" ? content : "";
  }
  return content
    .map((block) =>
      ompRecordField(block, "type") === "text" ? (ompRecordString(block, "text") ?? "") : "",
    )
    .join("");
}

function providerRef(
  nativeId: string,
  strength: "strong" | "weak" = "strong",
): OrchestrationV2ProviderRef {
  return { driver: OMP_PROVIDER, nativeId, strength };
}

function nonNegativeInteger(input: unknown, key: string): number | undefined {
  const value = ompRecordNumber(input, key);
  return value === undefined ? undefined : Math.max(0, Math.trunc(value));
}

function boundedText(text: string, limit: number): string {
  return text.length > limit ? `${text.slice(0, limit)}…` : text;
}

function noticeTitle(message: string): string {
  return boundedText(message.split("\n", 1)[0] ?? message, MAX_NOTICE_TITLE_LENGTH);
}

type OmpMessageTokenUsage = Pick<
  OrchestrationV2ProviderTurnTokenUsage,
  "inputTokens" | "cachedInputTokens" | "outputTokens"
>;

/** omp counts uncached input, cache reads and cache writes separately; input includes all three. */
function ompMessageTokenUsage(usage: unknown): OmpMessageTokenUsage {
  const uncachedInputTokens = nonNegativeInteger(usage, "input");
  const cachedInputTokens = nonNegativeInteger(usage, "cacheRead");
  const outputTokens = nonNegativeInteger(usage, "output");
  return {
    ...(uncachedInputTokens === undefined
      ? {}
      : {
          inputTokens:
            uncachedInputTokens +
            (cachedInputTokens ?? 0) +
            (nonNegativeInteger(usage, "cacheWrite") ?? 0),
        }),
    ...(cachedInputTokens === undefined ? {} : { cachedInputTokens }),
    ...(outputTokens === undefined ? {} : { outputTokens }),
  };
}

interface OmpStreamDeltaStep {
  readonly kind: "assistant_message" | "reasoning";
  readonly phase: "start" | "delta" | "end";
}

const STREAM_DELTA_STEPS: ReadonlyMap<string, OmpStreamDeltaStep> = new Map([
  ["text_start", { kind: "assistant_message", phase: "start" }],
  ["text_delta", { kind: "assistant_message", phase: "delta" }],
  ["text_end", { kind: "assistant_message", phase: "end" }],
  ["thinking_start", { kind: "reasoning", phase: "start" }],
  ["thinking_delta", { kind: "reasoning", phase: "delta" }],
  ["thinking_end", { kind: "reasoning", phase: "end" }],
]);

type OmpCompactionStatus = "running" | "completed" | "failed" | "cancelled";

function terminalCompactionStatus(interrupted: boolean, failed: boolean): OmpCompactionStatus {
  if (interrupted) return "cancelled";
  return failed ? "failed" : "completed";
}

const COMPACTION_TITLES: Readonly<Record<OmpCompactionStatus, string>> = {
  running: "Compacting context...",
  completed: "Context compacted",
  failed: "Context compaction failed",
  cancelled: "Context compaction stopped",
};

interface OmpModelRef {
  readonly provider: string;
  readonly modelId: string;
}

function ompModelRefOf(model: unknown): OmpModelRef | undefined {
  const provider = ompRecordString(model, "provider");
  const modelId = ompRecordString(model, "id");
  return provider === undefined || modelId === undefined ? undefined : { provider, modelId };
}

function ompModelSlugOf(model: unknown): string | undefined {
  const ref = ompModelRefOf(model);
  return ref === undefined ? undefined : ompModelSlug({ provider: ref.provider, id: ref.modelId });
}

function ompPromptFailure(
  error: OmpPromptError,
  usageLimit: OmpUsageLimit | undefined,
): OrchestrationV2ProviderFailure {
  const limited = error.httpStatus === 429 || usageLimit !== undefined;
  const resetsAtSec = usageLimit?.resetsAtSec;
  return makeProviderFailure({
    message: error.message,
    code: error.code ?? null,
    class: limited ? "usage_limit" : "provider_error",
    retryable: error.retryable,
    resetAt:
      limited && resetsAtSec !== undefined
        ? DateTime.formatIso(DateTime.makeUnsafe(resetsAtSec * 1_000))
        : null,
  });
}

const WAKE_BUFFER_BYPASS_TYPES: ReadonlySet<string> = new Set([
  "response",
  "host_tool_call",
  "host_tool_cancel",
  "available_commands_update",
]);

/** Session-scoped frames a pending wake must not buffer: they answer requests, need replies now, or replace session state. */
function bypassesWakeBuffer(record: OmpRecord): boolean {
  const type = String(record["type"]);
  if (WAKE_BUFFER_BYPASS_TYPES.has(type)) return true;
  return type === "extension_ui_request" && INTERACTIVE_UI_METHODS.has(String(record["method"]));
}

interface OmpStreamItemState {
  readonly nativeItemId: string;
  readonly kind: "assistant_message" | "reasoning";
  text: string;
  completed: boolean;
  flushScheduled: boolean;
  published: boolean;
  readonly startedAt: DateTime.Utc;
}

interface OmpCompactionState {
  readonly nativeItemId: string;
  readonly startedAt: DateTime.Utc;
}

interface OmpProviderRetryState {
  readonly retry: OrchestrationV2ProviderRetry;
  readonly failure: OrchestrationV2ProviderFailure;
  readonly startedAt: DateTime.Utc;
  readonly itemOrdinal: number;
}

type OmpToolRowStatus = "completed" | "failed" | "interrupted";

interface OmpToolRowState {
  readonly start: OmpToolExecutionStartFrame;
  readonly kind: OmpToolKind;
  readonly startedAt: DateTime.Utc;
  latestOutput: string;
  ended: boolean;
  backgroundJobId: string | undefined;
  lastBody: OmpToolItemBody | undefined;
}

/** How the turn's own prompt (or its stand-in) ended. Undefined while it is still running. */
type OmpTurnSettlement =
  | {
      readonly kind: "prompt_result";
      readonly status: OmpPromptResultFrame["status"];
      readonly error: OmpPromptError | undefined;
      readonly agentInvoked: boolean;
    }
  | { readonly kind: "local_command" }
  | { readonly kind: "rejected"; readonly message: string }
  | { readonly kind: "compact"; readonly error: string | undefined }
  | { readonly kind: "session_settled" }
  | { readonly kind: "intercepted" };

interface PendingOmpPrompt {
  readonly id: string;
  readonly turn: ActiveOmpTurn;
  readonly kind: "turn_start" | "steer" | "compact";
  readonly messageId: MessageId | undefined;
  readonly acknowledged: Deferred.Deferred<void>;
  responded: boolean;
  /** False once the first response failed or ran a local command: no `prompt_result` follows. */
  resultExpected: boolean;
}

/** A prompt the turn sent, and the omp queue that holds it while a run streams. */
interface OmpSentPrompt {
  readonly text: string;
  readonly queue: "steering" | "followUp";
}

interface ActiveOmpTurn {
  readonly turnInput: ProviderAdapter.ProviderAdapterV2TurnInput;
  readonly providerTurn: OrchestrationV2ProviderTurn;
  readonly itemOrdinals: Map<string, number>;
  nextItemOrdinal: number;
  messageOrdinal: number;
  readonly streamItems: Map<string, OmpStreamItemState>;
  readonly tools: Map<string, OmpToolRowState>;
  /** Started tools no approval was linked to yet, in start order (R§25). */
  readonly unlinkedToolCallIds: Array<string>;
  readonly editPreviews: Map<string, OmpEditPreview>;
  readonly slashCommand: boolean;
  readonly continuation: boolean;
  readonly beforeBranch: ReadonlyArray<OmpBranchMessage> | undefined;
  mainPrompt: PendingOmpPrompt | undefined;
  settlement: OmpTurnSettlement | undefined;
  interrupted: boolean;
  abortRequested: boolean;
  adoptedWake: boolean;
  awaitingSettle: boolean;
  activeCompaction: OmpCompactionState | null;
  compactionSkipped: boolean;
  compactionCancelled: boolean;
  activeProviderRetry: OmpProviderRetryState | null;
  failure: OrchestrationV2ProviderFailure | null;
  readonly unreadSteers: Array<MessageId>;
  readonly sentPrompts: Array<OmpSentPrompt>;
  lastMessageUsage: OmpMessageTokenUsage | undefined;
  lastLiveUsedTokens: number | null;
  reportedModel: string | undefined;
  /** Read before Stop ended omp; the turn finalizes after the process can no longer answer. */
  stopEndState: OmpTurnEndState | undefined;
}

interface OmpTurnEndState {
  readonly after: ReadonlyArray<OmpBranchMessage> | undefined;
  readonly live: Option.Option<OmpSessionState>;
}

interface OmpThreadState {
  providerThread: OrchestrationV2ProviderThread;
  activeTurn: ActiveOmpTurn | null;
}

interface OmpWake {
  readonly state: OmpThreadState;
  readonly records: Array<OmpRecord>;
}

type PendingOmpResponse =
  | {
      readonly kind: "approval";
      readonly labels: OmpApprovalLabels;
      readonly requestKind: "command" | "file-change";
    }
  | { readonly kind: "questions"; readonly shape: OmpAnswerShape };

interface PendingOmpRequest {
  readonly nativeRequestId: string;
  readonly response: PendingOmpResponse;
  readonly toolCallId: string | undefined;
  readonly basePrompt: string;
  runtimeRequest: OrchestrationV2RuntimeRequest;
  readonly node: OrchestrationV2ExecutionNode;
  turnItem: OrchestrationV2TurnItem;
}

type OmpRequestItemBase = Pick<
  Extract<OrchestrationV2TurnItem, { readonly type: "approval_request" }>,
  | "id"
  | "threadId"
  | "runId"
  | "nodeId"
  | "providerThreadId"
  | "providerTurnId"
  | "nativeItemRef"
  | "parentItemId"
  | "ordinal"
  | "status"
  | "title"
  | "startedAt"
  | "completedAt"
  | "updatedAt"
>;

interface OmpRequestContext {
  readonly turn: ActiveOmpTurn | null;
  readonly threadId: OrchestrationV2ExecutionNode["threadId"];
  readonly runId: OrchestrationV2ExecutionNode["runId"];
  readonly rootNodeId: OrchestrationV2ExecutionNode["parentNodeId"];
  readonly providerThreadId: OrchestrationV2ExecutionNode["providerThreadId"];
  readonly providerTurnId: OrchestrationV2ExecutionNode["providerTurnId"];
}

interface OmpRequestIds {
  readonly requestId: OrchestrationV2RuntimeRequest["id"];
  readonly nodeId: OrchestrationV2ExecutionNode["id"];
  readonly nativeRequestId: string;
  readonly createdAt: DateTime.Utc;
}

function pendingRuntimeRequest(
  ids: OmpRequestIds,
  context: OmpRequestContext,
  providerSessionId: OrchestrationV2ProviderSession["id"],
  kind: OrchestrationV2RuntimeRequest["kind"],
): OrchestrationV2RuntimeRequest {
  return {
    id: ids.requestId,
    nodeId: ids.nodeId,
    providerTurnId: context.providerTurnId,
    nativeRequestRef: providerRef(ids.nativeRequestId),
    kind,
    status: "pending",
    // A denial's message is steered to the agent after "Deny" (R§28).
    responseCapability: {
      type: "live",
      providerSessionId,
      ...(kind === "user_input" ? {} : { supportsFeedback: true }),
    },
    createdAt: ids.createdAt,
    resolvedAt: null,
  };
}

function waitingRequestNode(
  ids: OmpRequestIds,
  context: OmpRequestContext,
  isApproval: boolean,
): OrchestrationV2ExecutionNode {
  return {
    id: ids.nodeId,
    threadId: context.threadId,
    runId: context.runId,
    parentNodeId: context.rootNodeId,
    rootNodeId: context.rootNodeId ?? ids.nodeId,
    kind: isApproval ? "approval_request" : "user_input_request",
    status: "waiting",
    countsForRun: false,
    providerThreadId: context.providerThreadId,
    providerTurnId: context.providerTurnId,
    nativeItemRef: providerRef(ids.nativeRequestId),
    runtimeRequestId: ids.requestId,
    checkpointScopeId: null,
    startedAt: ids.createdAt,
    completedAt: null,
  };
}

function requestItemBase(
  ids: OmpRequestIds,
  context: OmpRequestContext,
  id: OmpRequestItemBase["id"],
  ordinal: number,
): OmpRequestItemBase {
  return {
    id,
    threadId: context.threadId,
    runId: context.runId,
    nodeId: ids.nodeId,
    providerThreadId: context.providerThreadId,
    providerTurnId: context.providerTurnId,
    nativeItemRef: providerRef(ids.nativeRequestId),
    parentItemId: null,
    ordinal,
    status: "waiting",
    title: null,
    startedAt: ids.createdAt,
    completedAt: null,
    updatedAt: ids.createdAt,
  };
}

/** A turn that ended while omp still had work for it: a yielded prompt or a backgrounded job. */
interface OmpBackgroundHold {
  readonly turn: ActiveOmpTurn;
  awaitingSettle: boolean;
  /** A Stop cancelled this work, so its jobs end interrupted whatever omp reports next. */
  stopped: boolean;
  readonly jobs: Map<string, OmpToolRowState>;
  /** Runtime requests kept open past the turn, cancelled when the hold is released. */
  readonly requests: Set<string>;
}

interface OmpHostToolState {
  readonly client: T3McpClient | undefined;
  readonly toolSet: OmpHostToolSet;
  /** Posted once, in the first turn: nothing emitted during openSession is visible. */
  readonly notice: string | undefined;
}

interface OmpPromptPayload {
  readonly message: string;
  readonly images: ReadonlyArray<OmpImageContent>;
}

interface OmpTurnOutcome {
  readonly status: "completed" | "interrupted" | "failed";
  readonly failure: OrchestrationV2ProviderFailure | null;
  readonly promptDropped: boolean;
}

function settlementFailure(
  settlement: OmpTurnSettlement | undefined,
  usageLimit: OmpUsageLimit | undefined,
): OrchestrationV2ProviderFailure | null {
  if (settlement?.kind === "rejected") {
    return makeProviderFailure({ message: settlement.message, class: "provider_error" });
  }
  if (settlement?.kind === "compact" && settlement.error !== undefined) {
    return makeProviderFailure({ message: settlement.error, class: "provider_error" });
  }
  if (settlement?.kind === "prompt_result" && settlement.error !== undefined) {
    return ompPromptFailure(settlement.error, usageLimit);
  }
  return null;
}

function retainedRequestsField(retained: ReadonlyArray<OrchestrationV2RuntimeRequest["id"]>) {
  return retained.length === 0 ? {} : { retainedRuntimeRequestIds: retained };
}

/** `compact` and `abort` share omp's serial command queue, so an abort waits out the compaction. */
function compactCommandRunning(turn: ActiveOmpTurn): boolean {
  return turn.mainPrompt?.kind === "compact" && !turn.mainPrompt.responded;
}

function settlementDroppedPrompt(settlement: OmpTurnSettlement | undefined): boolean {
  if (settlement?.kind === "rejected") return true;
  return settlement?.kind === "prompt_result" && !settlement.agentInvoked;
}

/** `/fresh` and `/session delete` report `aborted` without being stopped (R§12). */
function settlementWasStopped(turn: ActiveOmpTurn): boolean {
  const settlement = turn.settlement;
  return (
    settlement?.kind === "prompt_result" && settlement.status === "aborted" && !turn.slashCommand
  );
}

function ompTurnOutcome(
  turn: ActiveOmpTurn,
  usageLimit: OmpUsageLimit | undefined,
): OmpTurnOutcome {
  if (turn.interrupted) return { status: "interrupted", failure: null, promptDropped: false };
  const failure = settlementFailure(turn.settlement, usageLimit) ?? turn.failure;
  if (failure !== null) {
    return {
      status: "failed",
      failure,
      promptDropped: settlementDroppedPrompt(turn.settlement),
    };
  }
  if (settlementWasStopped(turn)) {
    return { status: "interrupted", failure: null, promptDropped: false };
  }
  return { status: "completed", failure: null, promptDropped: false };
}

function editPreviewDiff(preview: OmpEditPreview | undefined): string | undefined {
  const parts = (preview?.files ?? []).flatMap((file) =>
    file.diffStr === undefined ? [] : [file.diffStr],
  );
  return parts.length === 0 ? undefined : parts.join("\n");
}

function editPreviewErrors(preview: OmpEditPreview | undefined): ReadonlyArray<string> {
  return (preview?.files ?? []).flatMap((file) =>
    file.error === undefined ? [] : [`${file.path}: ${file.error}`],
  );
}

function linkedCommand(basePrompt: string, row: OmpToolRowState | undefined): string | undefined {
  if (row?.start.toolName !== "bash") return undefined;
  const command = ompRecordString(row.start.args, "command");
  return command === undefined || basePrompt.includes(command) ? undefined : `$ ${command}`;
}

/** omp's titles carry no diff, so the card shows the linked tool's edit preview (TSR§5). */
function approvalPrompt(
  basePrompt: string,
  row: OmpToolRowState | undefined,
  preview: OmpEditPreview | undefined,
): string {
  const diff = editPreviewDiff(preview);
  const errors = editPreviewErrors(preview);
  return [
    basePrompt.trim(),
    linkedCommand(basePrompt, row),
    diff === undefined ? undefined : `\`\`\`diff\n${diff}\n\`\`\``,
    errors.length === 0 ? undefined : `This edit will fail:\n${errors.join("\n")}`,
  ]
    .filter((part): part is string => part !== undefined && part.length > 0)
    .join("\n\n");
}

function snapshotThread(
  providerThread: OrchestrationV2ProviderThread,
): ProviderAdapter.ProviderAdapterV2ThreadSnapshot {
  return { providerThread, messages: [], runtimeRequests: [] };
}

/**
 * omp rejects a bare prompt while any run streams, including runs it started itself (R§15). A
 * turn's prompt goes as `followUp`, which joins such a run after its work and acts as a bare
 * prompt when omp is idle (P2/queue-prompt-during-autonomous-run.jsonl).
 */
function promptCommand(
  payload: OmpPromptPayload,
  streamingBehavior: OmpStreamingBehavior,
): Extract<OmpCommand, { readonly type: "prompt" }> {
  return {
    type: "prompt",
    message: payload.message,
    streamingBehavior,
    ...(payload.images.length === 0 ? {} : { images: payload.images }),
  };
}

function sentPromptOf(command: OmpCommand): OmpSentPrompt | undefined {
  if (command.type !== "prompt") return undefined;
  const queue = command.streamingBehavior === "steer" ? "steering" : "followUp";
  return { text: command.message, queue };
}

/** `effect` until `deadline` (epoch ms); what it has not done by then is skipped. */
function untilDeadline(deadline: number, effect: Effect.Effect<void>): Effect.Effect<void> {
  return Clock.currentTimeMillis.pipe(
    Effect.flatMap((now) => effect.pipe(Effect.timeoutOption(Math.max(0, deadline - now)))),
    Effect.asVoid,
  );
}

function compactCommand(
  disposition: Extract<OmpSlashDisposition, { readonly kind: "compact" }>,
): Extract<OmpCommand, { readonly type: "compact" }> {
  return disposition.customInstructions === undefined
    ? { type: "compact" }
    : { type: "compact", customInstructions: disposition.customInstructions };
}

function backgroundJobIds(message: OmpRecord): ReadonlySet<string> {
  const jobs = ompRecordField(ompRecordField(message, "details"), "jobs");
  return new Set(
    (Array.isArray(jobs) ? jobs : []).flatMap((job) => {
      const jobId = ompRecordString(job, "jobId");
      return jobId === undefined ? [] : [jobId];
    }),
  );
}

function snapshotRole(message: unknown): "user" | "assistant" | undefined {
  const role = ompRecordString(message, "role");
  return role === "user" || role === "assistant" ? role : undefined;
}

type OmpFrameType = OmpInboundFrame["type"];
type OmpFrameOf<Type extends OmpFrameType> = Extract<OmpInboundFrame, { readonly type: Type }>;
type OmpFrameError = OmpRpcError | OmpRpcTimeoutError | IdAllocator.IdAllocatorV2Error;
type OmpFrameHandler = (
  frame: OmpInboundFrame,
  record: OmpRecord,
) => Effect.Effect<void, OmpFrameError>;
type OmpFrameHandlers = {
  readonly [Type in OmpFrameType]?: (
    frame: OmpFrameOf<Type>,
    record: OmpRecord,
  ) => Effect.Effect<void, OmpFrameError>;
};

export const makeOmpAdapterV2 = Effect.fn("makeOmpAdapterV2")(function* (
  options: OmpAdapterV2Options,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const agentScope = yield* AgentScope;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const httpClient = yield* HttpClient.HttpClient;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const host = yield* ProviderHost.ProviderHost;
  const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
  const { continuationRequests } = options;
  const binaryPath = options.settings.binaryPath || "omp";
  const unattendedOverlay = yield* makeOmpUnattendedOverlay(host.paths.stateDir);

  const protocolError = (detail: string, payload?: unknown) =>
    new ProviderAdapter.ProviderAdapterProtocolError({
      driver: OMP_PROVIDER,
      detail,
      ...(payload === undefined ? {} : { payload }),
    });

  const withPlatform = <A, E>(
    effect: Effect.Effect<A, E, FileSystem.FileSystem | Path.Path>,
  ): Effect.Effect<A, E> =>
    effect.pipe(
      Effect.provideService(FileSystem.FileSystem, fileSystem),
      Effect.provideService(Path.Path, path),
    );

  return ProviderAdapter.ProviderAdapterV2.of({
    instanceId: options.instanceId,
    driver: OMP_PROVIDER,
    getCapabilities: Effect.fn("OmpAdapter.getCapabilities")(function () {
      return Effect.succeed(OmpProviderCapabilitiesV2);
    }),
    planSelectionTransition: Effect.fn("OmpAdapter.planSelectionTransition")(function () {
      return Effect.succeed(turnScopedSelectionTransition());
    }),
    openSession: Effect.fn("OmpAdapterV2.openSession")(function* (
      input: ProviderAdapter.ProviderAdapterV2OpenSessionInput,
    ) {
      const scope = yield* Effect.scope;
      const cwd = input.runtimePolicy.cwd ?? host.paths.cwd;
      const openError = (cause: unknown) =>
        new ProviderAdapter.ProviderAdapterOpenSessionError({
          driver: OMP_PROVIDER,
          providerSessionId: input.providerSessionId,
          cause,
        });
      const resolvedLaunchArgs = resolveOmpLaunchArgs(options.settings.launchArgs);
      if (!resolvedLaunchArgs.ok) {
        return yield* openError(protocolError(resolvedLaunchArgs.message));
      }
      const realCwd = yield* fileSystem.realPath(cwd).pipe(Effect.orElseSucceed(() => cwd));
      const sessionDir = ompSessionDirForCwd(path, host.paths.stateDir, realCwd);
      const overlayPath = ompOverlayPath(path, host.paths.stateDir, input.providerSessionId);
      const appendSystemPromptPath = ompAppendSystemPromptPath(path, overlayPath);

      const readMcpCredential = mcpSessions
        .read(input.threadId)
        .pipe(
          Effect.map((config) =>
            config === undefined
              ? undefined
              : { endpoint: config.endpoint, authorizationHeader: config.authorizationHeader },
          ),
        );

      // Turns keep working when T3's MCP server is unreachable (docs/internals/03_providers.md).
      const prepareHostTools = Effect.fnUntraced(function* () {
        const none: OmpHostToolState = {
          client: undefined,
          toolSet: EMPTY_OMP_HOST_TOOL_SET,
          notice: undefined,
        };
        if ((yield* readMcpCredential) === undefined) return none;
        const client = yield* makeT3McpClient(readMcpCredential).pipe(Effect.option);
        if (Option.isNone(client)) return { ...none, notice: OMP_HOST_TOOLS_UNAVAILABLE_NOTICE };
        const load = yield* loadOmpHostTools(client.value);
        const usable = load.notice === undefined;
        return {
          client: usable ? client.value : undefined,
          toolSet: load.toolSet,
          notice: load.notice,
        };
      });
      const hostTools = yield* prepareHostTools().pipe(
        Effect.provideService(HttpClient.HttpClient, httpClient),
      );

      let approvalMode = ompApprovalModeForRuntimeMode(input.runtimePolicy.runtimeMode);
      const writeOverlay = (mode: OmpApprovalMode) =>
        withPlatform(
          writeOmpConfigOverlay(
            overlayPath,
            renderOmpSessionOverlay({
              approvalMode: mode,
              allowedToolNames: hostTools.toolSet.readOnlyHostNames,
            }),
          ),
        );
      yield* fileSystem
        .makeDirectory(path.dirname(overlayPath), { recursive: true })
        .pipe(Effect.ignore);
      yield* fileSystem.makeDirectory(sessionDir, { recursive: true }).pipe(Effect.ignore);
      yield* Effect.addFinalizer(() =>
        Effect.forEach(
          [overlayPath, appendSystemPromptPath],
          (file) => fileSystem.remove(file).pipe(Effect.ignore),
          { discard: true },
        ),
      );
      yield* writeOverlay(approvalMode).pipe(Effect.mapError(openError));
      const appendSystemPrompt = yield* withPlatform(
        composeOmpAppendSystemPrompt({
          cwd: realCwd,
          home: options.environment["HOME"] ?? options.environment["USERPROFILE"] ?? "",
          environment: options.environment,
          t3Instructions: OMP_T3_INSTRUCTIONS,
        }),
      );
      yield* withPlatform(writeOmpConfigOverlay(appendSystemPromptPath, appendSystemPrompt)).pipe(
        Effect.mapError(openError),
      );

      const sessionFileExists = (sessionFile: string) =>
        fileSystem.exists(sessionFile).pipe(Effect.orElseSucceed(() => false));
      const sessionFileInCwd = (sessionFile: string) =>
        sessionFileMatchesCwd(sessionFile, cwd).pipe(
          Effect.provideService(FileSystem.FileSystem, fileSystem),
        );

      // `--resume <missing>` exits before `ready` (R§38), so only a checked file is resumed.
      const resumableSessionFile = Effect.fnUntraced(function* (sessionFile: string | undefined) {
        if (sessionFile === undefined || !(yield* sessionFileExists(sessionFile))) return undefined;
        return (yield* sessionFileInCwd(sessionFile)) ? sessionFile : undefined;
      });

      const startConnection = Effect.fnUntraced(function* (resumeSessionFile: string | undefined) {
        const connectionScope = yield* Scope.fork(scope, "sequential");
        return yield* Effect.gen(function* () {
          const launch = buildOmpRpcLaunch({
            launchArgs: resolvedLaunchArgs.args,
            environment: options.environment,
            threadId: input.threadId,
            sessionDir,
            ...(resumeSessionFile === undefined ? {} : { resumeSessionFile }),
            overlayPath,
            appendSystemPromptPath,
          });
          const scopedLaunch = yield* agentScope.wrap({
            command: binaryPath,
            args: launch.args,
            name: "omp",
            threadId: input.threadId,
            env: launch.env,
          });
          // Finalizers run in reverse order: reap detached work after terminating omp.
          const scopeUnit = scopedLaunch.unit;
          const stopScope =
            scopeUnit === undefined
              ? Effect.void
              : yield* Effect.cached(agentScope.stop(scopeUnit));
          yield* Effect.addFinalizer(() => stopScope);
          const connection = yield* makeOmpRpcConnection({
            command: scopedLaunch.command,
            args: scopedLaunch.args,
            cwd,
            env: launch.env,
          });
          // Its tools carry T3CODE_THREAD_ID, so port discovery must scan while omp runs.
          const trackAgent = host.trackAgent;
          if (trackAgent !== undefined) {
            yield* Effect.acquireRelease(Effect.sync(trackAgent), (release) =>
              Effect.sync(release),
            );
          }
          // Detached children can keep stdout open after omp exits.
          if (scopeUnit !== undefined) {
            yield* connection.exited.pipe(
              Effect.matchEffect({ onFailure: () => stopScope, onSuccess: () => stopScope }),
              Effect.forkIn(connectionScope),
            );
          }
          return { connection, scope: connectionScope };
        }).pipe(
          Effect.provideService(Scope.Scope, connectionScope),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.onExit((exit) =>
            Exit.isFailure(exit) ? Scope.close(connectionScope, exit) : Effect.void,
          ),
        );
      });

      const initialResumeFile = yield* resumableSessionFile(input.initialNativeThreadId);
      let started = yield* startConnection(initialResumeFile).pipe(Effect.mapError(openError));
      let connection: OmpRpcConnection = started.connection;

      const request = (command: OmpCommand, timeoutMs?: number) =>
        Effect.suspend(() =>
          connection.request(command, timeoutMs === undefined ? undefined : { timeoutMs }),
        ).pipe(Effect.map((response) => response.data));

      const readState = (timeoutMs?: number) =>
        request({ type: "get_state" }, timeoutMs).pipe(
          Effect.map(decodeSessionState),
          Effect.orElseSucceed(() => Option.none<OmpSessionState>()),
        );

      const readBranchMessages = (timeoutMs?: number) =>
        request({ type: "get_branch_messages" }, timeoutMs).pipe(
          Effect.map((data) =>
            Option.getOrUndefined(
              Option.map(decodeBranchMessages(data), (value) => value.messages),
            ),
          ),
          Effect.orElseSucceed((): ReadonlyArray<OmpBranchMessage> | undefined => undefined),
        );

      const readThinkingLevels = request({ type: "get_available_thinking_levels" }).pipe(
        Effect.map((data) =>
          Option.match(decodeThinkingLevels(data), {
            onNone: (): ReadonlyArray<string> => [],
            onSome: (value) => value.levels,
          }),
        ),
        Effect.orElseSucceed((): ReadonlyArray<string> => []),
      );

      let toolCommandBase: ReadonlyArray<string> = [];
      const toolCommandDeltas: Array<OmpToolCommandDelta> = [];
      let toolCommands: ReadonlySet<string> = new Set();
      // Names in the latest catalog (a full replacement per `available_commands_update`).
      let commandCatalogNames: ReadonlySet<string> = new Set();
      const pendingSessionNotices: Array<string> = [];

      const applyToolCommandDelta = (delta: OmpToolCommandDelta | undefined) => {
        if (delta === undefined) return;
        toolCommandDeltas.push(delta);
        toolCommands = ompToolCommandNames(toolCommandBase, toolCommandDeltas);
      };

      // Process-scoped: sent once per spawn, never per turn (R§52).
      const registerHostTools = Effect.fnUntraced(function* () {
        const definitions = hostTools.toolSet.definitions;
        const reply = yield* request({ type: "set_host_tools", tools: definitions }).pipe(
          Effect.map(decodeHostToolsReply),
          Effect.option,
          Effect.map(Option.flatten),
        );
        const registered = new Set(Option.isSome(reply) ? reply.value.toolNames : []);
        toolCommandBase = definitions
          .filter((tool) => tool.loadMode !== "essential" && registered.has(tool.name))
          .map((tool) => tool.name);
        // The pump may already have applied this process's notices (see prepareProcess).
        toolCommands = ompToolCommandNames(toolCommandBase, toolCommandDeltas);
        if (Option.isSome(reply) || definitions.length === 0) return;
        yield* Effect.logWarning("omp refused T3 Code's host tools", {
          providerSessionId: input.providerSessionId,
          toolCount: definitions.length,
        });
        pendingSessionNotices.push(OMP_HOST_TOOLS_REFUSED_NOTICE);
      });

      // After every spawn and before the first prompt (DECISIONS: Process and transport).
      const configureProcess = Effect.fnUntraced(function* () {
        yield* request({ type: "set_event_filter", events: null, messageUpdates: "delta" });
        yield* request({ type: "set_ask_dialog", enabled: true });
        yield* request({ type: "set_interrupt_mode", mode: "wait" });
        yield* registerHostTools();
      });

      /**
       * Configures the current process once omp negotiated, then runs `registration`. omp holds
       * every command while a dialog it raised at startup waits, so nothing is written earlier and
       * the pump may handle frames first. A process T3 could not prepare is stopped.
       */
      const prepareProcess = Effect.fnUntraced(function* (
        registration: Effect.Effect<void, OmpPreparationError>,
      ) {
        const active = connection;
        const prepared = yield* Deferred.make<void, OmpPreparationError>();
        yield* active.negotiated.pipe(
          Effect.andThen(configureProcess()),
          Effect.andThen(registration),
          Effect.tapError(() => active.terminate),
          Deferred.into(prepared),
          Effect.forkIn(started.scope),
        );
        return prepared;
      });
      let processPreparation = yield* prepareProcess(Effect.void);
      // Only an open session can deliver the answer to a dialog omp raised while starting.
      if (yield* connection.isNegotiated) {
        yield* Deferred.await(processPreparation).pipe(Effect.mapError(openError));
      }
      if (hostTools.notice !== undefined) pendingSessionNotices.push(hostTools.notice);

      /**
       * Thread work needs a prepared process. While a startup dialog holds omp this fails at once,
       * because the orchestrator delivers the dialog's answer only after the calling effect ends.
       */
      const awaitProcessPreparation = Effect.gen(function* () {
        const preparation = processPreparation;
        if (!(yield* Deferred.isDone(preparation)) && !(yield* connection.isNegotiated)) {
          return yield* protocolError(OMP_STARTUP_DIALOG_PENDING_DETAIL);
        }
        yield* Deferred.await(preparation);
      });

      const bridge: OmpHostToolBridge | undefined =
        hostTools.client === undefined
          ? undefined
          : yield* makeOmpHostToolBridge({
              client: hostTools.client,
              toolSet: hostTools.toolSet,
              send: (frame) => Effect.suspend(() => connection.send(frame)),
            });

      const now = yield* DateTime.now;
      let sessionEntity: OrchestrationV2ProviderSession = {
        id: input.providerSessionId,
        driver: OMP_PROVIDER,
        providerInstanceId: options.instanceId,
        status: "ready",
        cwd,
        model: input.modelSelection.model,
        capabilities: OmpProviderCapabilitiesV2,
        extensionStatuses: [],
        createdAt: now,
        updatedAt: now,
        lastError: null,
      };
      const events = yield* Queue.unbounded<
        ProviderAdapter.ProviderAdapterV2Event,
        ProviderAdapter.ProviderAdapterV2Error | Cause.Done
      >();
      // omp can settle right after a dialog response; serialize so terminal cannot overtake it.
      const sessionEventPermit = yield* Semaphore.make(1);
      const pendingPrompts = new Map<string, PendingOmpPrompt>();
      const pendingRequests = new Map<string, PendingOmpRequest>();
      const requestIdByNativeId = new Map<string, string>();
      const backgroundHolds = new Map<ProviderTurnId, OmpBackgroundHold>();
      let threadState: OmpThreadState | null = null;
      let pendingWake: OmpWake | null = null;
      let registrationAttempted = initialResumeFile !== undefined;
      let registrationInProgress = false;
      let rollbackInProgress = false;
      let restartGate: Deferred.Deferred<void> | null = null;
      let stopRequested = false;
      let unsolicitedActivityDetected = false;
      let closed = false;
      let nextPromptId = 0;
      let appliedModel: string | null = null;
      let appliedThinking: string | null = null;
      let appliedSessionName: string | null = null;
      let baselineModel: OmpModelRef | null = null;
      /** Undefined until the first get_state; null when omp reported no level. */
      let baselineThinking: string | null | undefined;
      let availableThinkingLevels: ReadonlyArray<string> = [];
      let pickerRows: ReadonlyArray<OmpModelPickerRow> | null = null;
      let lastBranchMessages: ReadonlyArray<OmpBranchMessage> | undefined;
      let contextWindow: number | null = null;
      const modelContextWindows = new Map<string, number>();

      const emit = (event: ProviderAdapter.ProviderAdapterV2Event) =>
        Queue.offer(events, event).pipe(Effect.asVoid);

      /* Provider events include streaming deltas; avoid a span for each update. */
      const updateProviderSession = Effect.fnUntraced(function* (
        status: OrchestrationV2ProviderSession["status"],
        lastError: string | null = sessionEntity.lastError,
      ) {
        sessionEntity = { ...sessionEntity, status, lastError, updatedAt: yield* DateTime.now };
        yield* emit({
          type: "provider_session.updated",
          driver: OMP_PROVIDER,
          providerSession: sessionEntity,
        });
      });

      const reportStartupPhase = Effect.fn("OmpAdapter.reportStartupPhase")(function* (
        phase: string | undefined,
      ) {
        if (sessionEntity.startupPhase === phase) return;
        const { startupPhase: _reported, ...current } = sessionEntity;
        sessionEntity = {
          ...current,
          ...(phase === undefined ? {} : { startupPhase: phase }),
          updatedAt: yield* DateTime.now,
        };
        yield* emit({
          type: "provider_session.updated",
          driver: OMP_PROVIDER,
          providerSession: sessionEntity,
        });
      });
      const reportingResume = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        reportStartupPhase("resume").pipe(
          Effect.andThen(effect),
          Effect.ensuring(Effect.suspend(() => reportStartupPhase(undefined))),
        );

      /* Provider events include streaming deltas; avoid a span for each update. */
      const updateProviderThread = Effect.fnUntraced(function* (
        state: OmpThreadState,
        patch: Partial<OrchestrationV2ProviderThread>,
      ) {
        state.providerThread = {
          ...state.providerThread,
          ...patch,
          updatedAt: yield* DateTime.now,
        };
        yield* emit({
          type: "provider_thread.updated",
          driver: OMP_PROVIDER,
          providerThread: state.providerThread,
        });
      });

      const sameSessionFile = (a: string | null | undefined, b: string | null | undefined) =>
        a != null && b != null && path.resolve(cwd, a) === path.resolve(cwd, b);

      const rememberModelContextWindow = (model: unknown): number | null => {
        const slug = ompModelSlugOf(model);
        const capacity = nonNegativeInteger(model, "contextWindow");
        if (slug === undefined || capacity === undefined || capacity === 0) return null;
        modelContextWindows.set(slug, capacity);
        return capacity;
      };

      const rememberState = (state: OmpSessionState) => {
        const capacity = state.contextUsage?.contextWindow;
        const modelWindow = rememberModelContextWindow(state.model);
        contextWindow = capacity !== undefined && capacity > 0 ? capacity : modelWindow;
        const slug = ompModelSlugOf(state.model);
        if (slug !== undefined && contextWindow !== null) {
          modelContextWindows.set(slug, contextWindow);
        }
      };

      const forgetSessionSelections = () => {
        appliedModel = null;
        appliedThinking = null;
        appliedSessionName = null;
        lastBranchMessages = undefined;
      };

      const rebindSessionFile = Effect.fnUntraced(function* (
        state: OmpThreadState,
        sessionFile: string | undefined,
      ) {
        if (sessionFile === undefined) return;
        if (sameSessionFile(state.providerThread.nativeThreadRef?.nativeId, sessionFile)) return;
        yield* updateProviderThread(state, { nativeThreadRef: providerRef(sessionFile) });
      });

      // omp moves a writer to a sibling file when another omp holds the lease (R§37).
      const adoptLiveSessionFile = (state: OmpThreadState, live: Option.Option<OmpSessionState>) =>
        rebindSessionFile(state, Option.getOrUndefined(live)?.sessionFile);

      const itemOrdinal = (turn: ActiveOmpTurn, nativeItemId: string): number => {
        const existing = turn.itemOrdinals.get(nativeItemId);
        if (existing !== undefined) return existing;
        const ordinal = turn.nextItemOrdinal++;
        turn.itemOrdinals.set(nativeItemId, ordinal);
        return ordinal;
      };

      const baseItemFields = (
        turn: ActiveOmpTurn,
        nativeItemId: string,
        startedAt: DateTime.Utc,
        updatedAt: DateTime.Utc,
        parentNativeItemId?: string,
      ) => ({
        id: idAllocator.derive.turnItemFromProviderItem({ driver: OMP_PROVIDER, nativeItemId }),
        threadId: turn.turnInput.threadId,
        runId: turn.turnInput.runId,
        nodeId: idAllocator.derive.nodeFromProviderItem({ driver: OMP_PROVIDER, nativeItemId }),
        providerThreadId: turn.turnInput.providerThread.id,
        providerTurnId: turn.providerTurn.id,
        nativeItemRef: providerRef(nativeItemId),
        parentItemId:
          parentNativeItemId === undefined
            ? null
            : idAllocator.derive.turnItemFromProviderItem({
                driver: OMP_PROVIDER,
                nativeItemId: parentNativeItemId,
              }),
        ordinal: itemOrdinal(turn, nativeItemId),
        startedAt,
        updatedAt,
      });

      const emitItemNode = (
        turn: ActiveOmpTurn,
        nativeItemId: string,
        kind: OrchestrationV2ExecutionNode["kind"],
        status: OrchestrationV2ExecutionNode["status"],
        startedAt: DateTime.Utc,
        completedAt: DateTime.Utc | null,
        parentNativeItemId?: string,
      ) =>
        emit({
          type: "node.updated",
          driver: OMP_PROVIDER,
          node: {
            id: idAllocator.derive.nodeFromProviderItem({ driver: OMP_PROVIDER, nativeItemId }),
            threadId: turn.turnInput.threadId,
            runId: turn.turnInput.runId,
            parentNodeId:
              parentNativeItemId === undefined
                ? turn.turnInput.rootNodeId
                : idAllocator.derive.nodeFromProviderItem({
                    driver: OMP_PROVIDER,
                    nativeItemId: parentNativeItemId,
                  }),
            rootNodeId: turn.turnInput.rootNodeId,
            kind,
            status,
            countsForRun: false,
            providerThreadId: turn.turnInput.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            nativeItemRef: providerRef(nativeItemId),
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt,
            completedAt,
          },
        });

      const emitSystemNotice = Effect.fnUntraced(function* (
        turn: ActiveOmpTurn,
        nativeItemId: string,
        message: string,
        level?: "info" | "warning" | "error",
      ) {
        const emittedAt = yield* DateTime.now;
        yield* emitItemNode(turn, nativeItemId, "system", "completed", emittedAt, emittedAt);
        yield* emit({
          type: "turn_item.updated",
          driver: OMP_PROVIDER,
          turnItem: {
            ...baseItemFields(turn, nativeItemId, emittedAt, emittedAt),
            status: "completed",
            completedAt: emittedAt,
            title: noticeTitle(message),
            type: "system_notice",
            message,
            ...(level === undefined ? {} : { level }),
          },
        });
      });

      const noticeInActiveTurn = (message: string, level?: "info" | "warning" | "error") =>
        Effect.suspend(() => {
          const turn = threadState?.activeTurn ?? null;
          const text = boundedText(message.trim(), MAX_NOTICE_LENGTH);
          if (turn === null || text.length === 0) return Effect.void;
          return emitSystemNotice(
            turn,
            `notice:${turn.providerTurn.id}:${turn.nextItemOrdinal}`,
            text,
            level,
          );
        });

      const emitSteerStatus = (
        turn: ActiveOmpTurn,
        messageId: MessageId,
        status: "pending" | "completed" | "cancelled",
      ) =>
        emit({
          type: "steer.updated",
          driver: OMP_PROVIDER,
          threadId: turn.turnInput.threadId,
          runId: turn.turnInput.runId,
          messageId,
          status,
        });

      const emitProviderRetry = Effect.fnUntraced(function* (
        turn: ActiveOmpTurn,
        providerRetry: OmpProviderRetryState,
        status: "running" | "completed" | "failed" | "interrupted" | "cancelled",
        updatedAt: DateTime.Utc,
      ) {
        yield* emit({
          type: "turn_item.updated",
          driver: OMP_PROVIDER,
          turnItem: makeProviderRetryTurnItem({
            driver: OMP_PROVIDER,
            threadId: turn.turnInput.threadId,
            runId: turn.turnInput.runId,
            nodeId: turn.turnInput.rootNodeId,
            providerThreadId: turn.turnInput.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            itemOrdinal: providerRetry.itemOrdinal,
            failure: providerRetry.failure,
            retry: providerRetry.retry,
            status,
            startedAt: providerRetry.startedAt,
            updatedAt,
          }),
        });
      });

      const compactionNativeItemId = (turn: ActiveOmpTurn): string =>
        `compaction:${turn.providerTurn.id}:${turn.nextItemOrdinal}`;

      const emitCompaction = Effect.fnUntraced(function* (
        turn: ActiveOmpTurn,
        compaction: OmpCompactionState,
        status: OmpCompactionStatus,
        details: {
          readonly summary?: string;
          readonly beforeTokenCount?: number;
          readonly afterTokenCount?: number;
        } = {},
      ) {
        const emittedAt = yield* DateTime.now;
        const completedAt = status === "running" ? null : emittedAt;
        yield* emitItemNode(
          turn,
          compaction.nativeItemId,
          "system",
          status,
          compaction.startedAt,
          completedAt,
        );
        yield* emit({
          type: "turn_item.updated",
          driver: OMP_PROVIDER,
          turnItem: {
            ...baseItemFields(turn, compaction.nativeItemId, compaction.startedAt, emittedAt),
            status,
            title: COMPACTION_TITLES[status],
            completedAt,
            type: "compaction",
            driver: OMP_PROVIDER,
            ...details,
          },
        });
      });

      /* Provider events include streaming deltas; avoid a span for each update. */
      const emitStreamItem = Effect.fnUntraced(function* (
        turn: ActiveOmpTurn,
        item: OmpStreamItemState,
        streaming: boolean,
      ) {
        item.published = true;
        const emittedAt = yield* DateTime.now;
        const status = streaming ? "running" : "completed";
        const completedAt = streaming ? null : emittedAt;
        const base = baseItemFields(turn, item.nativeItemId, item.startedAt, emittedAt);
        yield* emitItemNode(
          turn,
          item.nativeItemId,
          item.kind,
          status,
          item.startedAt,
          completedAt,
        );
        if (item.kind === "reasoning") {
          yield* emit({
            type: "turn_item.updated",
            driver: OMP_PROVIDER,
            turnItem: {
              ...base,
              status,
              title: null,
              completedAt,
              type: "reasoning",
              text: item.text,
              streaming,
            },
          });
          return;
        }
        const messageId = idAllocator.derive.messageFromProviderItem({
          driver: OMP_PROVIDER,
          nativeItemId: item.nativeItemId,
        });
        yield* emit({
          type: "turn_item.updated",
          driver: OMP_PROVIDER,
          turnItem: {
            ...base,
            status,
            title: null,
            completedAt,
            type: "assistant_message",
            messageId,
            text: item.text,
            streaming,
          },
        });
        yield* emit({
          type: "message.updated",
          driver: OMP_PROVIDER,
          message: {
            id: messageId,
            threadId: turn.turnInput.threadId,
            runId: turn.turnInput.runId,
            nodeId: base.nodeId,
            role: "assistant",
            text: item.text,
            attachments: [],
            streaming,
            createdBy: "agent",
            creationSource: "provider",
            createdAt: item.startedAt,
            updatedAt: emittedAt,
          },
        });
      });

      /* Provider events include streaming deltas; avoid a span for each update. */
      const scheduleStreamFlush = Effect.fnUntraced(function* (
        turn: ActiveOmpTurn,
        item: OmpStreamItemState,
      ) {
        if (item.flushScheduled || item.completed) return;
        item.flushScheduled = true;
        yield* Effect.sleep(Duration.millis(STREAM_FLUSH_MS)).pipe(
          Effect.andThen(
            Effect.suspend(() => {
              item.flushScheduled = false;
              return item.completed ? Effect.void : emitStreamItem(turn, item, true);
            }),
          ),
          Effect.forkIn(scope),
        );
      });

      const streamItemId = (turn: ActiveOmpTurn, contentIndex: number) =>
        `${turn.providerTurn.id}:m${turn.messageOrdinal}:c${contentIndex}`;

      const streamItemFor = Effect.fnUntraced(function* (
        turn: ActiveOmpTurn,
        kind: OmpStreamItemState["kind"],
        contentIndex: number,
      ) {
        const nativeItemId = streamItemId(turn, contentIndex);
        const existing = turn.streamItems.get(nativeItemId);
        if (existing !== undefined) return existing;
        const item: OmpStreamItemState = {
          nativeItemId,
          kind,
          text: "",
          completed: false,
          flushScheduled: false,
          published: false,
          startedAt: yield* DateTime.now,
        };
        turn.streamItems.set(nativeItemId, item);
        itemOrdinal(turn, nativeItemId);
        return item;
      });

      const completeStreamItem = (turn: ActiveOmpTurn, item: OmpStreamItemState, text?: string) =>
        Effect.suspend(() => {
          if (item.completed) return Effect.void;
          item.completed = true;
          if (text !== undefined && text.length > 0) item.text = text;
          // A block that was shown and then started again empty still replaces what it showed.
          return item.text.length === 0 && !item.published
            ? Effect.void
            : emitStreamItem(turn, item, false);
        });

      const completeOpenStreamItems = (turn: ActiveOmpTurn) =>
        Effect.forEach(
          Array.from(turn.streamItems.values()).filter((item) => !item.completed),
          (item) => completeStreamItem(turn, item),
          { discard: true },
        );

      /* In delta mode only an assistant message's start and end carry usage (TL/03). */
      const reportLiveUsage = Effect.fnUntraced(function* (turn: ActiveOmpTurn, usage: unknown) {
        const usedTokens = nonNegativeInteger(usage, "totalTokens");
        if (usedTokens === undefined || usedTokens === 0) return;
        turn.lastMessageUsage = ompMessageTokenUsage(usage);
        if (usedTokens === turn.lastLiveUsedTokens || contextWindow === null) return;
        turn.lastLiveUsedTokens = usedTokens;
        yield* emit({
          type: "provider_turn.updated",
          driver: OMP_PROVIDER,
          threadId: turn.turnInput.threadId,
          providerTurn: {
            ...turn.providerTurn,
            tokenUsage: {
              usedTokens,
              maxTokens: contextWindow,
              ...turn.lastMessageUsage,
              updatedAt: DateTime.formatIso(yield* DateTime.now),
            },
          },
        });
      });

      const emitToolRow = Effect.fnUntraced(function* (
        turn: ActiveOmpTurn,
        toolCallId: string,
        row: OmpToolRowState,
        body: OmpToolItemBody,
        status: OmpToolRowStatus | "running",
      ) {
        const emittedAt = yield* DateTime.now;
        const completedAt = status === "running" ? null : emittedAt;
        const parent = ompParentToolCallId(toolCallId);
        yield* emitItemNode(
          turn,
          toolCallId,
          "tool_call",
          status,
          row.startedAt,
          completedAt,
          parent,
        );
        row.lastBody = body;
        const turnItem: OrchestrationV2TurnItem = {
          ...baseItemFields(turn, toolCallId, row.startedAt, emittedAt, parent),
          status,
          completedAt,
          ...body,
        };
        yield* emit({ type: "turn_item.updated", driver: OMP_PROVIDER, turnItem });
      });

      const emitCommandOutputDelta = (turn: ActiveOmpTurn, toolCallId: string, chunk: string) =>
        chunk.length === 0
          ? Effect.void
          : emit({
              type: "command_output.delta",
              driver: OMP_PROVIDER,
              threadId: turn.turnInput.threadId,
              itemId: idAllocator.derive.turnItemFromProviderItem({
                driver: OMP_PROVIDER,
                nativeItemId: toolCallId,
              }),
              chunk,
            });

      const holdBackgroundWork = Effect.fnUntraced(function* (turn: ActiveOmpTurn) {
        const existing = backgroundHolds.get(turn.providerTurn.id);
        if (existing !== undefined) return existing;
        const hold: OmpBackgroundHold = {
          turn,
          awaitingSettle: false,
          stopped: false,
          jobs: new Map(),
          requests: new Set(),
        };
        backgroundHolds.set(turn.providerTurn.id, hold);
        yield* emit({
          type: "background_work.updated",
          driver: OMP_PROVIDER,
          providerTurnId: turn.providerTurn.id,
          pending: true,
        });
        return hold;
      });

      /** Once omp has nothing left for the turn, nothing can still be waiting on these requests. */
      const cancelHeldRequests = Effect.fnUntraced(function* (hold: OmpBackgroundHold) {
        const resolvedAt = yield* DateTime.now;
        for (const requestId of hold.requests) {
          const pending = pendingRequests.get(requestId);
          if (pending === undefined) continue;
          forgetRequest(pending);
          yield* cancelRequest(pending, resolvedAt, !closed);
        }
        hold.requests.clear();
      });

      const releaseHoldIfIdle = Effect.fnUntraced(function* (hold: OmpBackgroundHold) {
        if (hold.awaitingSettle || hold.jobs.size > 0) return;
        if (backgroundHolds.get(hold.turn.providerTurn.id) !== hold) return;
        backgroundHolds.delete(hold.turn.providerTurn.id);
        yield* cancelHeldRequests(hold);
        yield* emit({
          type: "background_work.updated",
          driver: OMP_PROVIDER,
          providerTurnId: hold.turn.providerTurn.id,
          pending: false,
        });
      });

      /** Without the job's final update the row keeps the last body it showed. */
      const closeBackgroundJob = Effect.fnUntraced(function* (
        hold: OmpBackgroundHold,
        toolCallId: string,
        status: OmpToolRowStatus,
        finalUpdate: OmpToolExecutionUpdateFrame | undefined,
      ) {
        const row = hold.jobs.get(toolCallId);
        if (row === undefined) return;
        hold.jobs.delete(toolCallId);
        const completion =
          finalUpdate === undefined
            ? undefined
            : ompToolItemFromBackgroundCompletion(row.start, finalUpdate, row.kind);
        const body = completion?.body ?? row.lastBody ?? ompToolItemFromStart(row.start, row.kind);
        yield* emitToolRow(hold.turn, toolCallId, row, body, completion?.status ?? status);
        yield* releaseHoldIfIdle(hold);
      });

      /** `session_settled` covers every job omp reported: abort only detaches them (TSR§0.6). */
      const releaseBackgroundHolds = Effect.fnUntraced(function* (jobsLost: boolean) {
        for (const hold of Array.from(backgroundHolds.values())) {
          hold.awaitingSettle = false;
          const status =
            jobsLost || hold.stopped || hold.turn.interrupted ? "interrupted" : "completed";
          for (const toolCallId of Array.from(hold.jobs.keys())) {
            yield* closeBackgroundJob(hold, toolCallId, status, undefined);
          }
          yield* releaseHoldIfIdle(hold);
        }
      });

      const findBackgroundJob = (toolCallId: string) => {
        for (const hold of backgroundHolds.values()) {
          if (hold.jobs.has(toolCallId)) return hold;
        }
        return undefined;
      };

      const cancelRequest = Effect.fnUntraced(function* (
        pending: PendingOmpRequest,
        resolvedAt: DateTime.Utc,
        notifyOmp: boolean,
      ) {
        if (notifyOmp) {
          yield* connection.send(ompCancelResponse(pending.nativeRequestId)).pipe(Effect.ignore);
        }
        pending.runtimeRequest = { ...pending.runtimeRequest, status: "cancelled", resolvedAt };
        yield* emit({
          type: "runtime_request.updated",
          driver: OMP_PROVIDER,
          threadId: pending.node.threadId,
          runtimeRequest: pending.runtimeRequest,
        });
        yield* emit({
          type: "node.updated",
          driver: OMP_PROVIDER,
          node: { ...pending.node, status: "cancelled", completedAt: resolvedAt },
        });
        yield* emit({
          type: "turn_item.updated",
          driver: OMP_PROVIDER,
          turnItem: {
            ...pending.turnItem,
            status: "cancelled",
            completedAt: resolvedAt,
            updatedAt: resolvedAt,
          },
        });
      });

      const forgetRequest = (pending: PendingOmpRequest) => {
        const requestId = requestIdByNativeId.get(pending.nativeRequestId);
        requestIdByNativeId.delete(pending.nativeRequestId);
        if (requestId !== undefined) pendingRequests.delete(requestId);
      };

      const cancelPendingRequests = Effect.fn("OmpAdapter.cancelPendingRequests")(function* (
        resolvedAt: DateTime.Utc,
        notifyOmp: boolean,
      ) {
        const pending = Array.from(pendingRequests.values());
        pendingRequests.clear();
        requestIdByNativeId.clear();
        yield* Effect.forEach(pending, (request) => cancelRequest(request, resolvedAt, notifyOmp), {
          discard: true,
        });
      });

      const settleOpenTools = Effect.fnUntraced(function* (turn: ActiveOmpTurn, stopped: boolean) {
        for (const [toolCallId, row] of turn.tools) {
          if (row.ended || row.kind === "ask") continue;
          row.ended = true;
          const body =
            row.lastBody ??
            ompToolItemFromStart(row.start, row.kind, turn.editPreviews.get(toolCallId));
          yield* emitToolRow(turn, toolCallId, row, body, stopped ? "interrupted" : "completed");
        }
      });

      const releasePromptsOf = Effect.fnUntraced(function* (turn: ActiveOmpTurn) {
        for (const [id, pending] of Array.from(pendingPrompts)) {
          if (pending.turn !== turn) continue;
          pendingPrompts.delete(id);
          yield* Deferred.succeed(pending.acknowledged, undefined);
        }
      });

      const turnTokenUsage = (
        turn: ActiveOmpTurn,
        live: Option.Option<OmpSessionState>,
        updatedAt: DateTime.Utc,
      ): OrchestrationV2ProviderTurnTokenUsage | undefined => {
        const usage = Option.getOrUndefined(live)?.contextUsage;
        if (usage === undefined || usage.contextWindow === 0) return undefined;
        return {
          usedTokens: usage.tokens,
          maxTokens: usage.contextWindow,
          ...turn.lastMessageUsage,
          updatedAt: DateTime.formatIso(updatedAt),
        };
      };

      /** Undefined keeps the synthetic ref; null means the turn left nothing on the branch. */
      const turnEntryRef = (
        turn: ActiveOmpTurn,
        after: ReadonlyArray<OmpBranchMessage> | undefined,
      ): OrchestrationV2ProviderRef | null | undefined => {
        if (turn.beforeBranch === undefined || after === undefined) return undefined;
        const entryId = ompTurnUserEntryId(turn.beforeBranch, after);
        return entryId === undefined ? null : providerRef(entryId);
      };

      const settleTurnCompaction = Effect.fnUntraced(function* (
        turn: ActiveOmpTurn,
        status: OmpCompactionStatus,
      ) {
        if (turn.activeCompaction === null) return;
        yield* emitCompaction(turn, turn.activeCompaction, status);
        turn.activeCompaction = null;
      });

      const settleTurnRetry = Effect.fnUntraced(function* (
        turn: ActiveOmpTurn,
        interrupted: boolean,
        completedAt: DateTime.Utc,
      ) {
        if (turn.activeProviderRetry === null) return;
        const status = interrupted ? "interrupted" : "completed";
        yield* emitProviderRetry(turn, turn.activeProviderRetry, status, completedAt);
        turn.activeProviderRetry = null;
      });

      // Every terminal path settles open items and requests (docs/internals/03_providers.md).
      const settleTurnItems = Effect.fnUntraced(function* (
        turn: ActiveOmpTurn,
        outcome: OmpTurnOutcome,
        completedAt: DateTime.Utc,
        keepRequests: boolean,
      ) {
        const interrupted = outcome.status === "interrupted";
        const failed = outcome.failure !== null;
        yield* completeOpenStreamItems(turn);
        yield* settleTurnCompaction(turn, terminalCompactionStatus(interrupted, failed));
        // A failed retry is reported with the turn's failure (emitTurnTerminal).
        if (!failed) yield* settleTurnRetry(turn, interrupted, completedAt);
        yield* settleOpenTools(turn, interrupted || failed);
        if (!keepRequests) yield* cancelPendingRequests(completedAt, true);
        for (const messageId of turn.unreadSteers.splice(0)) {
          yield* emitSteerStatus(turn, messageId, interrupted ? "cancelled" : "completed");
        }
      });

      const emitTurnTerminal = Effect.fnUntraced(function* (
        state: OmpThreadState,
        turn: ActiveOmpTurn,
        outcome: OmpTurnOutcome,
        completedAt: DateTime.Utc,
        retained: ReadonlyArray<OrchestrationV2RuntimeRequest["id"]>,
      ) {
        const base = {
          type: "turn.terminal",
          driver: OMP_PROVIDER,
          providerThreadId: state.providerThread.id,
          providerTurnId: turn.providerTurn.id,
          runOrdinal: turn.turnInput.runOrdinal,
          threadDisposition: "reusable",
          ...retainedRequestsField(retained),
        } as const;
        const failure = outcome.failure;
        if (failure === null) {
          yield* emit({
            ...base,
            status: outcome.status === "interrupted" ? "interrupted" : "completed",
            failure: null,
          });
          return;
        }
        const failureItemId = `terminal-failure:${turn.providerTurn.id}`;
        const retry = turn.activeProviderRetry;
        if (retry !== null) {
          yield* emitProviderRetry(turn, { ...retry, failure }, "failed", completedAt);
        } else {
          yield* emit({
            type: "turn_item.updated",
            driver: OMP_PROVIDER,
            turnItem: {
              ...baseItemFields(turn, failureItemId, completedAt, completedAt),
              status: "failed",
              title: null,
              completedAt,
              type: "error",
              failure,
            },
          });
        }
        yield* emit({
          ...base,
          failureItemOrdinal: itemOrdinal(turn, failureItemId),
          status: "failed",
          failure,
          ...(outcome.promptDropped ? { promptDropped: true } : {}),
          ...(retry === null ? {} : { retry: retry.retry, retryStartedAt: retry.startedAt }),
        });
      });

      /** The branch and state after a turn; a dead transport cannot answer, so nothing is read. */
      const readTurnEndState = Effect.fnUntraced(function* (
        turn: ActiveOmpTurn,
        processAlive: boolean,
      ) {
        if (turn.stopEndState !== undefined) return turn.stopEndState;
        if (!processAlive) return { after: undefined, live: Option.none<OmpSessionState>() };
        const after = yield* readBranchMessages();
        if (after !== undefined) lastBranchMessages = after;
        const live = yield* readState(OMP_SHORT_REQUEST_TIMEOUT_MS);
        if (Option.isSome(live)) rememberState(live.value);
        return { after, live };
      });

      const completedProviderTurn = (
        state: OmpThreadState,
        turn: ActiveOmpTurn,
        outcome: OmpTurnOutcome,
        completedAt: DateTime.Utc,
        end: OmpTurnEndState,
      ): OrchestrationV2ProviderTurn => {
        const nativeTurnRef = turnEntryRef(turn, end.after);
        const tokenUsage = turnTokenUsage(turn, end.live, completedAt);
        const nativeThreadRef = state.providerThread.nativeThreadRef;
        return {
          ...turn.providerTurn,
          ...(nativeTurnRef === undefined ? {} : { nativeTurnRef }),
          ...(nativeThreadRef === null ? {} : { nativeThreadRef }),
          status: outcome.status,
          completedAt,
          ...(tokenUsage === undefined ? {} : { tokenUsage }),
          ...(turn.reportedModel === undefined ? {} : { model: turn.reportedModel }),
        };
      };

      const openRuntimeRequestIds = () =>
        Array.from(pendingRequests.values(), (pending) => pending.runtimeRequest.id);

      const markIdleAfterTurn = Effect.fnUntraced(function* (
        state: OmpThreadState,
        failure: OrchestrationV2ProviderFailure | null,
      ) {
        yield* updateProviderThread(state, { status: "idle" });
        yield* updateProviderSession(
          failure === null ? "ready" : "error",
          failure?.message ?? null,
        );
      });

      const holdUntilSettled = Effect.fnUntraced(function* (
        turn: ActiveOmpTurn,
        retained: ReadonlyArray<OrchestrationV2RuntimeRequest["id"]>,
      ) {
        const hold = yield* holdBackgroundWork(turn);
        hold.awaitingSettle = true;
        for (const requestId of retained) hold.requests.add(String(requestId));
      });

      const finalizeTurn = Effect.fnUntraced(function* (
        state: OmpThreadState,
        processAlive = true,
      ) {
        const turn = state.activeTurn;
        if (turn === null) return;
        state.activeTurn = null;
        yield* releasePromptsOf(turn);
        const end = yield* readTurnEndState(turn, processAlive);
        yield* adoptLiveSessionFile(state, end.live);
        const outcome = ompTurnOutcome(turn, Option.getOrUndefined(end.live)?.usageLimit);
        const completedAt = yield* DateTime.now;
        const awaitingWork = turn.awaitingSettle && processAlive && !closed;
        // A nested tool-command approval backgrounds its row while it waits (TSR§5).
        const keepRequests = awaitingWork && outcome.status === "completed";
        yield* settleTurnItems(turn, outcome, completedAt, keepRequests);
        const retained = keepRequests ? openRuntimeRequestIds() : [];
        yield* emit({
          type: "provider_turn.updated",
          driver: OMP_PROVIDER,
          threadId: turn.turnInput.threadId,
          providerTurn: completedProviderTurn(state, turn, outcome, completedAt, end),
          ...retainedRequestsField(retained),
        });
        // Background work is marked before the root terminal (ProviderAdapter.ts).
        if (awaitingWork) yield* holdUntilSettled(turn, retained);
        yield* markIdleAfterTurn(state, outcome.failure);
        yield* emitTurnTerminal(state, turn, outcome, completedAt, retained);
      });

      /** Completes once the turn's own prompt settled and every steer it sent got its result. */
      const completeTurnIfSettled = Effect.fnUntraced(function* (turn: ActiveOmpTurn) {
        const state = threadState;
        if (state === null || state.activeTurn !== turn || turn.settlement === undefined) return;
        for (const pending of pendingPrompts.values()) {
          if (pending.turn === turn && pending.resultExpected) return;
        }
        yield* finalizeTurn(state);
      });

      const sendPrompt = Effect.fnUntraced(function* (
        turn: ActiveOmpTurn,
        kind: PendingOmpPrompt["kind"],
        command: Extract<OmpCommand, { readonly type: "prompt" | "compact" }>,
        messageId?: MessageId,
      ) {
        const id = `t3-prompt-${nextPromptId++}`;
        const pending: PendingOmpPrompt = {
          id,
          turn,
          kind,
          messageId,
          acknowledged: yield* Deferred.make<void>(),
          responded: false,
          resultExpected: true,
        };
        // Registered before the write so a fast ack or prompt_result always finds its owner.
        pendingPrompts.set(id, pending);
        if (kind !== "steer") turn.mainPrompt = pending;
        const frame: OmpCommandFrame = { ...command, id };
        yield* connection
          .send(frame)
          .pipe(Effect.tapError(() => Effect.sync(() => pendingPrompts.delete(id))));
        const sent = sentPromptOf(command);
        if (sent !== undefined) turn.sentPrompts.push(sent);
      });

      const onCompactResponse = (pending: PendingOmpPrompt, frame: OmpResponseFrame) => {
        const turn = pending.turn;
        pending.resultExpected = false;
        // A skipped or stopped compaction is not a turn failure.
        const quiet = frame.success || turn.compactionSkipped || turn.compactionCancelled;
        turn.settlement = {
          kind: "compact",
          error: quiet ? undefined : (frame.error ?? "omp context compaction failed."),
        };
        return completeTurnIfSettled(turn);
      };

      const onMainPromptResponse = Effect.fnUntraced(function* (
        pending: PendingOmpPrompt,
        frame: OmpResponseFrame,
      ) {
        const turn = pending.turn;
        if (pending.kind === "compact") return yield* onCompactResponse(pending, frame);
        if (!frame.success) {
          pending.resultExpected = false;
          turn.settlement = {
            kind: "rejected",
            message: frame.error ?? "omp rejected the prompt.",
          };
          return yield* completeTurnIfSettled(turn);
        }
        if (Option.getOrUndefined(decodePromptAck(frame.data))?.agentInvoked !== false) return;
        // A local command's command_output frames precede this ack on the same queue.
        pending.resultExpected = false;
        turn.settlement = { kind: "local_command" };
        yield* completeTurnIfSettled(turn);
      });

      const onSteerResponse = Effect.fnUntraced(function* (
        pending: PendingOmpPrompt,
        frame: OmpResponseFrame,
      ) {
        const localCommand =
          Option.getOrUndefined(decodePromptAck(frame.data))?.agentInvoked === false;
        if (!frame.success || localCommand) {
          pending.resultExpected = false;
          if (!frame.success) {
            yield* Effect.logWarning("omp rejected a steer message.", {
              errorLength: frame.error?.length,
            });
          }
          return yield* completeTurnIfSettled(pending.turn);
        }
        if (pending.messageId === undefined) return;
        pending.turn.unreadSteers.push(pending.messageId);
        yield* emitSteerStatus(pending.turn, pending.messageId, "pending");
      });

      const onResponse = Effect.fnUntraced(function* (frame: OmpResponseFrame) {
        const pending = frame.id === undefined ? undefined : pendingPrompts.get(frame.id);
        // The first response for an id is the ack; omp can repeat it (TL/04:24-27).
        if (pending === undefined || pending.responded) return;
        pending.responded = true;
        yield* Deferred.succeed(pending.acknowledged, undefined);
        if (threadState?.activeTurn !== pending.turn) return;
        if (pending.kind === "steer") return yield* onSteerResponse(pending, frame);
        yield* onMainPromptResponse(pending, frame);
      });

      const onPromptResult = Effect.fnUntraced(function* (frame: OmpPromptResultFrame) {
        const pending = frame.id === undefined ? undefined : pendingPrompts.get(frame.id);
        if (pending === undefined) return;
        pendingPrompts.delete(pending.id);
        yield* Deferred.succeed(pending.acknowledged, undefined);
        const turn = pending.turn;
        if (threadState?.activeTurn !== turn) return;
        if (!frame.sessionSettled && frame.agentInvoked) turn.awaitingSettle = true;
        if (pending.kind !== "steer") {
          turn.settlement = {
            kind: "prompt_result",
            status: frame.status,
            error: frame.error,
            agentInvoked: frame.agentInvoked,
          };
        } else if (frame.error !== undefined) {
          turn.failure ??= ompPromptFailure(frame.error, undefined);
        }
        yield* completeTurnIfSettled(turn);
      });

      const stopPendingWake = Effect.fnUntraced(function* (wake: OmpWake) {
        if (pendingWake !== wake || threadState !== wake.state || closed) return;
        pendingWake = null;
        stopRequested = true;
        yield* cancelPendingRequests(yield* DateTime.now, false);
        yield* connection.terminate;
      });

      const wakeTarget = (state: OmpThreadState | null) => {
        const appThreadId = state?.providerThread.appThreadId ?? null;
        if (state === null || appThreadId === null || continuationRequests === undefined) {
          return undefined;
        }
        return closed ? undefined : { state, appThreadId, offer: continuationRequests.offer };
      };

      const offerWake = Effect.fnUntraced(function* (
        target: NonNullable<ReturnType<typeof wakeTarget>>,
        record: OmpRecord,
      ) {
        const { state, appThreadId, offer } = target;
        const wake: OmpWake = { state, records: [record] };
        pendingWake = wake;
        if (rollbackInProgress) return yield* stopPendingWake(wake);
        yield* updateProviderSession("running", null);
        yield* offer({
          threadId: appThreadId,
          providerThreadId: state.providerThread.id,
          driver: OMP_PROVIDER,
          detail: null,
          delivery: "adapter_buffered",
          dispatchIfCurrent: (dispatch) =>
            sessionEventPermit.withPermits(1)(
              Effect.suspend(() =>
                pendingWake === wake && threadState === state && !closed
                  ? dispatch.pipe(
                      Effect.asSome,
                      Effect.tapError(() => stopPendingWake(wake)),
                    )
                  : Effect.succeedNone,
              ),
            ),
          clearIfCurrent: () => sessionEventPermit.withPermits(1)(stopPendingWake(wake)),
        }).pipe(Effect.tapError(() => stopPendingWake(wake)));
      });

      /**
       * A plain abort hands input a run had already taken back to omp's queue, and omp resumes the
       * run to deliver it. The resumed run has no tool running yet, so withdrawing the queue now
       * cannot move work to the background.
       */
      const sendAbort = (type: "abort" | "abort_and_restore_queue") =>
        Effect.suspend(() =>
          connection.send({ type, id: `t3-abort-${nextPromptId++}` }).pipe(Effect.ignore),
        );

      const continueActiveTurn = (turn: ActiveOmpTurn) => {
        if (!turn.interrupted || !turn.abortRequested) {
          turn.abortRequested = false;
          return Effect.void;
        }
        return sendAbort("abort_and_restore_queue");
      };

      // An unprompted run continues the active turn, or wakes an idle thread (R§9).
      const onAgentStart = Effect.fnUntraced(function* (_frame: unknown, record: OmpRecord) {
        const turn = threadState?.activeTurn ?? null;
        if (turn !== null) return yield* continueActiveTurn(turn);
        // An idle omp starts a run to deliver the result of a subagent Stop cancelled (STG/03:137).
        if (stopRequested) return yield* sendAbort("abort");
        const target = wakeTarget(threadState);
        if (target !== undefined) return yield* offerWake(target, record);
        unsolicitedActivityDetected = true;
        yield* updateProviderSession("error", OMP_UNSOLICITED_ACTIVITY_ERROR);
        yield* connection.terminate;
      });

      const onSessionSettled = Effect.fnUntraced(function* () {
        yield* releaseBackgroundHolds(false);
        const turn = threadState?.activeTurn ?? null;
        if (turn === null || turn.mainPrompt !== undefined || !turn.adoptedWake) return;
        turn.settlement ??= { kind: "session_settled" };
        yield* completeTurnIfSettled(turn);
      });

      const onCustomMessageEnd = Effect.fnUntraced(function* (
        message: OmpRecord,
        record: OmpRecord,
      ) {
        applyToolCommandDelta(ompToolCommandDelta(record));
        if (ompRecordString(message, "customType") !== "async-result") return;
        const jobIds = backgroundJobIds(message);
        for (const hold of Array.from(backgroundHolds.values())) {
          for (const [toolCallId, row] of Array.from(hold.jobs)) {
            if (row.backgroundJobId === undefined || !jobIds.has(row.backgroundJobId)) continue;
            const status = hold.stopped ? "interrupted" : "completed";
            yield* closeBackgroundJob(hold, toolCallId, status, undefined);
          }
        }
      });

      const onMessageStart = Effect.fnUntraced(function* (frame: OmpFrameOf<"message_start">) {
        const turn = threadState?.activeTurn ?? null;
        if (turn === null) return;
        const role = ompRecordString(frame.message, "role");
        if (role === "user" && ompRecordBoolean(frame.message, "steering") === true) {
          const steer = turn.unreadSteers.shift();
          if (steer !== undefined) yield* emitSteerStatus(turn, steer, "completed");
          return;
        }
        if (role !== "assistant") return;
        turn.messageOrdinal += 1;
        yield* reportLiveUsage(turn, ompRecordField(frame.message, "usage"));
      });

      const recoverProviderRetry = Effect.fnUntraced(function* (turn: ActiveOmpTurn) {
        const retry = turn.activeProviderRetry;
        if (retry === null) return;
        turn.activeProviderRetry = null;
        yield* emitProviderRetry(turn, retry, "completed", yield* DateTime.now);
      });

      /* Per-delta handler; shares the pump's span. */
      const applyAssistantDelta = Effect.fnUntraced(function* (
        turn: ActiveOmpTurn,
        delta: OmpAssistantMessageEvent,
      ) {
        const step = STREAM_DELTA_STEPS.get(delta.type);
        if (step === undefined) return;
        const contentIndex = delta.contentIndex ?? 0;
        if (step.phase === "start") {
          // A provider that reconnects mid-block starts it again; its earlier text is abandoned.
          const restarted = turn.streamItems.get(streamItemId(turn, contentIndex));
          if (restarted !== undefined && !restarted.completed) restarted.text = "";
          return;
        }
        const item = yield* streamItemFor(turn, step.kind, contentIndex);
        if (step.phase === "delta") {
          item.text += delta.delta ?? "";
          return yield* scheduleStreamFlush(turn, item);
        }
        const content = typeof delta.content === "string" ? delta.content : undefined;
        yield* completeStreamItem(turn, item, content);
      });

      /* Per-delta handler; shares the pump's span. */
      const onMessageUpdate = Effect.fnUntraced(function* (frame: OmpFrameOf<"message_update">) {
        const turn = threadState?.activeTurn ?? null;
        // Late deltas after an abort are dropped; message_end keeps the partial text (R§14).
        if (turn === null || turn.abortRequested) return;
        yield* recoverProviderRetry(turn);
        yield* applyAssistantDelta(turn, frame.assistantMessageEvent);
      });

      const onAssistantMessageEnd = Effect.fnUntraced(function* (
        turn: ActiveOmpTurn,
        message: OmpRecord,
      ) {
        yield* completeOpenStreamItems(turn);
        const reported = ompModelSlugOf({
          provider: ompRecordString(message, "provider"),
          id: ompRecordString(message, "model"),
        });
        if (reported !== undefined) turn.reportedModel = reported;
        yield* reportLiveUsage(turn, ompRecordField(message, "usage"));
        if (ompRecordString(message, "stopReason") === "error" && turn.failure === null) {
          turn.failure = makeProviderFailure({
            message: ompRecordString(message, "errorMessage") ?? "omp reported a model error.",
            class: "provider_error",
          });
        }
      });

      const onMessageEnd = Effect.fnUntraced(function* (
        frame: OmpFrameOf<"message_end">,
        record: OmpRecord,
      ) {
        const role = ompRecordString(frame.message, "role");
        if (role === "custom") return yield* onCustomMessageEnd(frame.message, record);
        const turn = threadState?.activeTurn ?? null;
        if (turn === null || role !== "assistant") return;
        yield* onAssistantMessageEnd(turn, frame.message);
      });

      const removeUnlinked = (turn: ActiveOmpTurn, toolCallId: string) => {
        const index = turn.unlinkedToolCallIds.indexOf(toolCallId);
        if (index >= 0) turn.unlinkedToolCallIds.splice(index, 1);
      };

      const onToolStart = Effect.fnUntraced(function* (frame: OmpFrameOf<"tool_execution_start">) {
        const turn = threadState?.activeTurn ?? null;
        if (turn === null) return;
        const kind = classifyOmpTool(frame.toolName, frame.args, toolCommands);
        const row: OmpToolRowState = {
          start: frame,
          kind,
          startedAt: yield* DateTime.now,
          latestOutput: "",
          ended: false,
          backgroundJobId: undefined,
          lastBody: undefined,
        };
        turn.tools.set(frame.toolCallId, row);
        turn.unlinkedToolCallIds.push(frame.toolCallId);
        // An ask tool renders as its form dialog, not as a row.
        if (kind === "ask") return;
        // An edit's preview often arrives before its start (TSR§1.2).
        const body = ompToolItemFromStart(frame, kind, turn.editPreviews.get(frame.toolCallId));
        yield* emitToolRow(turn, frame.toolCallId, row, body, "running");
      });

      const onBackgroundJobUpdate = Effect.fnUntraced(function* (
        frame: OmpToolExecutionUpdateFrame,
      ) {
        const hold = findBackgroundJob(frame.toolCallId);
        if (hold === undefined || !isOmpBackgroundJobCompletion(frame)) return;
        yield* closeBackgroundJob(hold, frame.toolCallId, "completed", frame);
      });

      /* Per-update handler; shares the pump's span. */
      const streamCommandOutput = Effect.fnUntraced(function* (
        turn: ActiveOmpTurn,
        row: OmpToolRowState,
        frame: OmpToolExecutionUpdateFrame,
      ) {
        const progress = ompCommandOutputProgress(row.latestOutput, frame);
        if (progress.kind === "unchanged") return;
        row.latestOutput = progress.latest;
        // Output only ever streams as deltas; the row's own output arrives with the tool's end.
        yield* emitCommandOutputDelta(turn, frame.toolCallId, progress.delta);
      });

      /* Per-update handler; shares the pump's span. */
      const onToolUpdate = Effect.fnUntraced(function* (
        frame: OmpFrameOf<"tool_execution_update">,
      ) {
        const turn = threadState?.activeTurn ?? null;
        const row = turn?.tools.get(frame.toolCallId);
        if (turn === null || row === undefined || row.ended) {
          return yield* onBackgroundJobUpdate(frame);
        }
        if (row.kind === "command") yield* streamCommandOutput(turn, row, frame);
      });

      const onToolEnd = Effect.fnUntraced(function* (frame: OmpToolExecutionEndFrame) {
        const turn = threadState?.activeTurn ?? null;
        const row = turn?.tools.get(frame.toolCallId);
        if (turn === null || row === undefined) return;
        row.ended = true;
        removeUnlinked(turn, frame.toolCallId);
        const preview = turn.editPreviews.get(frame.toolCallId);
        turn.editPreviews.delete(frame.toolCallId);
        if (row.kind === "ask") return;
        const completion = ompToolItemFromEnd(
          row.start,
          frame,
          row.kind,
          turn.abortRequested,
          preview,
        );
        if (completion.backgroundJobId === undefined) {
          return yield* emitToolRow(
            turn,
            frame.toolCallId,
            row,
            completion.body,
            completion.status,
          );
        }
        row.backgroundJobId = completion.backgroundJobId;
        (yield* holdBackgroundWork(turn)).jobs.set(frame.toolCallId, row);
        yield* emitToolRow(turn, frame.toolCallId, row, completion.body, "running");
      });

      const refreshLinkedApproval = Effect.fnUntraced(function* (
        turn: ActiveOmpTurn,
        toolCallId: string,
      ) {
        for (const pending of pendingRequests.values()) {
          if (pending.toolCallId !== toolCallId || pending.turnItem.type !== "approval_request") {
            continue;
          }
          pending.turnItem = {
            ...pending.turnItem,
            prompt: approvalPrompt(
              pending.basePrompt,
              turn.tools.get(toolCallId),
              turn.editPreviews.get(toolCallId),
            ),
            updatedAt: yield* DateTime.now,
          };
          yield* emit({
            type: "turn_item.updated",
            driver: OMP_PROVIDER,
            turnItem: pending.turnItem,
          });
        }
      });

      /** Keeps the newest preview and reports whether it is final, the only one shown. */
      const rememberEditPreview = (
        turn: ActiveOmpTurn,
        toolCallId: string,
        preview: OmpEditPreview,
      ) => {
        if (turn.editPreviews.get(toolCallId)?.final === true && !preview.final) return false;
        turn.editPreviews.set(toolCallId, preview);
        return preview.final;
      };

      // Each generation re-sends the whole preview; only the final one is shown, as a throttle.
      const onToolStreamUpdate = Effect.fnUntraced(function* (
        frame: OmpFrameOf<"tool_stream_update">,
      ) {
        const turn = threadState?.activeTurn ?? null;
        const preview = ompEditPreview(frame);
        if (turn === null || preview === undefined) return;
        if (!rememberEditPreview(turn, frame.toolCallId, preview)) return;
        const row = turn.tools.get(frame.toolCallId);
        if (row === undefined || row.ended) return;
        const body = ompToolItemFromStart(row.start, row.kind, preview);
        yield* emitToolRow(turn, frame.toolCallId, row, body, "running");
        yield* refreshLinkedApproval(turn, frame.toolCallId);
      });

      /** Parallel calls keep start and approval order, so the oldest unlinked same-name start wins. */
      const linkedToolCallId = (turn: ActiveOmpTurn | null, toolName: string) => {
        if (turn === null) return undefined;
        const byName = turn.unlinkedToolCallIds.find(
          (toolCallId) => turn.tools.get(toolCallId)?.start.toolName === toolName,
        );
        const toolCallId = byName ?? turn.unlinkedToolCallIds.at(-1);
        if (toolCallId !== undefined) removeUnlinked(turn, toolCallId);
        return toolCallId;
      };

      /** Where a dialog belongs: the active turn, else the thread (startup dialogs have no turn). */
      const requestContext = (): OmpRequestContext => {
        const state = threadState;
        const turn = state?.activeTurn ?? null;
        const providerThreadId = state?.providerThread.id ?? null;
        if (turn === null) {
          return {
            turn,
            threadId: state?.providerThread.appThreadId ?? input.threadId,
            runId: null,
            rootNodeId: null,
            providerThreadId,
            providerTurnId: null,
          };
        }
        return {
          turn,
          threadId: turn.turnInput.threadId,
          runId: turn.turnInput.runId,
          rootNodeId: turn.turnInput.rootNodeId,
          providerThreadId,
          providerTurnId: turn.providerTurn.id,
        };
      };

      const openRuntimeRequest = Effect.fnUntraced(function* (
        nativeRequestId: string,
        response: PendingOmpResponse,
        toolCallId: string | undefined,
        basePrompt: string,
        makeItem: (
          base: OmpRequestItemBase,
          requestId: OrchestrationV2RuntimeRequest["id"],
        ) => OrchestrationV2TurnItem,
      ) {
        const context = requestContext();
        const requestId = yield* idAllocator.allocate.runtimeRequest({
          driver: OMP_PROVIDER,
          ...(context.providerTurnId === null ? {} : { providerTurnId: context.providerTurnId }),
          nativeRequestId,
        });
        const ids: OmpRequestIds = {
          requestId,
          nodeId: idAllocator.derive.approvalNode({ requestId }),
          nativeRequestId,
          createdAt: yield* DateTime.now,
        };
        const isApproval = response.kind === "approval";
        const runtimeRequest = pendingRuntimeRequest(
          ids,
          context,
          input.providerSessionId,
          response.kind === "approval" ? response.requestKind : "user_input",
        );
        const node = waitingRequestNode(ids, context, isApproval);
        const ordinal = context.turn === null ? 0 : itemOrdinal(context.turn, nativeRequestId);
        const itemId = idAllocator.derive.approvalTurnItem({ requestId });
        const turnItem = makeItem(requestItemBase(ids, context, itemId, ordinal), requestId);
        const pending: PendingOmpRequest = {
          nativeRequestId,
          response,
          toolCallId,
          basePrompt,
          runtimeRequest,
          node,
          turnItem,
        };
        pendingRequests.set(String(requestId), pending);
        requestIdByNativeId.set(nativeRequestId, String(requestId));
        const threadId = context.threadId;
        yield* emit({
          type: "runtime_request.updated",
          driver: OMP_PROVIDER,
          threadId,
          runtimeRequest,
        });
        yield* emit({ type: "node.updated", driver: OMP_PROVIDER, node });
        yield* emit({ type: "turn_item.updated", driver: OMP_PROVIDER, turnItem });
      });

      const openApproval = (plan: Extract<OmpUiRequestPlan, { readonly kind: "approval" }>) =>
        Effect.suspend(() => {
          const turn = threadState?.activeTurn ?? null;
          const toolCallId = linkedToolCallId(turn, plan.toolName);
          const linked = toolCallId === undefined ? undefined : turn?.tools.get(toolCallId);
          const preview = toolCallId === undefined ? undefined : turn?.editPreviews.get(toolCallId);
          const prompt = approvalPrompt(plan.prompt, linked, preview);
          return openRuntimeRequest(
            plan.requestId,
            { kind: "approval", labels: plan.labels, requestKind: plan.requestKind },
            toolCallId,
            plan.prompt,
            (base, requestId) => ({
              ...base,
              title: `Allow ${plan.toolName}`,
              type: "approval_request",
              requestId,
              requestKind: plan.requestKind,
              ...(prompt.length === 0 ? {} : { prompt }),
              options: plan.options,
              ...(plan.fullAccessOverride === undefined
                ? {}
                : { fullAccessOverride: plan.fullAccessOverride }),
            }),
          );
        });

      const openQuestions = (plan: Extract<OmpUiRequestPlan, { readonly kind: "questions" }>) =>
        openRuntimeRequest(
          plan.requestId,
          { kind: "questions", shape: plan.answer },
          undefined,
          "",
          (base, requestId) => ({
            ...base,
            title: plan.questions[0]?.header ?? "Question",
            type: "user_input_request",
            requestId,
            questions: plan.questions,
          }),
        );

      // omp already cancelled the dialog itself (R§27), so nothing is sent back.
      const closeRequestByNativeId = Effect.fnUntraced(function* (targetId: string) {
        const requestId = requestIdByNativeId.get(targetId);
        const pending = requestId === undefined ? undefined : pendingRequests.get(requestId);
        if (pending === undefined) return;
        forgetRequest(pending);
        yield* cancelRequest(pending, yield* DateTime.now, false);
      });

      const uiPlanHandlers: {
        readonly [Kind in OmpUiRequestPlan["kind"]]: (
          plan: Extract<OmpUiRequestPlan, { readonly kind: Kind }>,
        ) => Effect.Effect<void, OmpFrameError>;
      } = {
        approval: openApproval,
        questions: openQuestions,
        unrenderable_form: (plan) =>
          connection
            .send(ompCancelResponse(plan.requestId, { timedOut: true }))
            .pipe(Effect.andThen(noticeInActiveTurn(plan.reason, "warning"))),
        decline_secret: (plan) =>
          connection
            .send(ompCancelResponse(plan.requestId))
            .pipe(
              Effect.andThen(
                noticeInActiveTurn(
                  `omp asked for secret input ("${plan.title}"), which T3 Code cannot provide yet. The request was cancelled.`,
                  "warning",
                ),
              ),
            ),
        close: (plan) => closeRequestByNativeId(plan.targetId),
        notice: (plan) => noticeInActiveTurn(plan.message, plan.level),
        status: () => Effect.void,
        ignore: () => Effect.void,
      };

      const onUiRequest = (frame: OmpFrameOf<"extension_ui_request">) =>
        Effect.suspend(() => {
          const plan = classifyOmpUiRequest(frame, { fullAccess: approvalMode === "yolo" });
          const handler = uiPlanHandlers[plan.kind] as (
            plan: OmpUiRequestPlan,
          ) => Effect.Effect<void, OmpFrameError>;
          return handler(plan);
        });

      const onHostToolCall = (frame: OmpFrameOf<"host_tool_call">) =>
        bridge !== undefined
          ? bridge.handleCall(frame)
          : connection.send({
              type: "host_tool_result",
              id: frame.id,
              result: {
                content: [{ type: "text", text: OMP_NO_HOST_TOOL_RESULT }],
                isError: true,
              },
            });

      const onCompactionStart = Effect.fnUntraced(function* () {
        const turn = threadState?.activeTurn ?? null;
        if (turn === null) return;
        if (turn.activeCompaction !== null) {
          yield* emitCompaction(turn, turn.activeCompaction, "cancelled");
        }
        const compaction: OmpCompactionState = {
          nativeItemId: compactionNativeItemId(turn),
          startedAt: yield* DateTime.now,
        };
        turn.activeCompaction = compaction;
        yield* emitCompaction(turn, compaction, "running");
      });

      const completeCompaction = Effect.fnUntraced(function* (
        turn: ActiveOmpTurn,
        compaction: OmpCompactionState,
        summary: string,
        tokensBefore: number,
      ) {
        // No frame carries the size after compaction; get_state does (R§62).
        const live = yield* readState(OMP_SHORT_REQUEST_TIMEOUT_MS);
        if (Option.isSome(live)) rememberState(live.value);
        const afterTokenCount = Option.getOrUndefined(live)?.contextUsage?.tokens;
        yield* emitCompaction(turn, compaction, "completed", {
          summary,
          beforeTokenCount: Math.max(0, tokensBefore),
          ...(afterTokenCount === undefined ? {} : { afterTokenCount }),
        });
      });

      const takeCompaction = Effect.fnUntraced(function* (turn: ActiveOmpTurn) {
        const compaction = turn.activeCompaction ?? {
          nativeItemId: compactionNativeItemId(turn),
          startedAt: yield* DateTime.now,
        };
        turn.activeCompaction = null;
        return compaction;
      });

      /** A stopped or skipped ("Nothing to compact") compaction does not fail its turn. */
      const stopCompaction = (
        turn: ActiveOmpTurn,
        compaction: OmpCompactionState,
        frame: OmpFrameOf<"manual_compaction_end"> | OmpFrameOf<"auto_compaction_end">,
      ) => {
        turn.compactionCancelled ||= frame.aborted;
        turn.compactionSkipped ||= frame.skipped === true;
        const reason = frame.errorMessage;
        return emitCompaction(
          turn,
          compaction,
          "cancelled",
          reason === undefined ? {} : { summary: reason },
        );
      };

      const onCompactionEnd = Effect.fnUntraced(function* (
        frame: OmpFrameOf<"manual_compaction_end"> | OmpFrameOf<"auto_compaction_end">,
      ) {
        const turn = threadState?.activeTurn ?? null;
        if (turn === null) return;
        const compaction = yield* takeCompaction(turn);
        if (frame.aborted || frame.skipped === true) {
          return yield* stopCompaction(turn, compaction, frame);
        }
        if (frame.result === undefined) {
          return yield* emitCompaction(turn, compaction, "failed", {
            summary: (frame.errorMessage ?? "omp context compaction failed.").slice(0, 1_000),
          });
        }
        if (frame.type === "auto_compaction_end" && frame.willRetry) turn.failure = null;
        yield* completeCompaction(
          turn,
          compaction,
          frame.result.summary,
          frame.result.tokensBefore,
        );
      });

      const providerRetryState = (
        turn: ActiveOmpTurn,
        retry: OrchestrationV2ProviderRetry,
        failure: OrchestrationV2ProviderFailure,
        emittedAt: DateTime.Utc,
      ): OmpProviderRetryState => ({
        retry,
        failure,
        startedAt: turn.activeProviderRetry?.startedAt ?? emittedAt,
        itemOrdinal:
          turn.activeProviderRetry?.itemOrdinal ??
          itemOrdinal(turn, `terminal-failure:${turn.providerTurn.id}`),
      });

      const onRetryStart = Effect.fnUntraced(function* (frame: OmpFrameOf<"auto_retry_start">) {
        const turn = threadState?.activeTurn ?? null;
        if (turn === null) return;
        const emittedAt = yield* DateTime.now;
        const attempt = Math.max(1, Math.trunc(frame.attempt));
        const retry = {
          attempt,
          maxAttempts: Math.max(attempt, Math.trunc(frame.maxAttempts)),
          retryDelayMs: Math.max(0, Math.trunc(frame.delayMs)),
        };
        const failure = makeProviderFailure({
          message: frame.errorMessage,
          class: "provider_error",
          retryable: true,
        });
        turn.activeProviderRetry = providerRetryState(turn, retry, failure, emittedAt);
        yield* emitProviderRetry(turn, turn.activeProviderRetry, "running", emittedAt);
      });

      const onRetryRecovered = Effect.fnUntraced(function* (
        turn: ActiveOmpTurn,
        attempt: number,
        emittedAt: DateTime.Utc,
      ) {
        turn.failure = null;
        const current = turn.activeProviderRetry;
        if (current === null) return;
        turn.activeProviderRetry = null;
        yield* emitProviderRetry(
          turn,
          { ...current, retry: { ...current.retry, attempt } },
          "completed",
          emittedAt,
        );
      });

      const onRetryEnd = Effect.fnUntraced(function* (frame: OmpFrameOf<"auto_retry_end">) {
        const turn = threadState?.activeTurn ?? null;
        if (turn === null) return;
        const emittedAt = yield* DateTime.now;
        const attempt = Math.max(1, Math.trunc(frame.attempt));
        if (frame.success) return yield* onRetryRecovered(turn, attempt, emittedAt);
        const failure = makeProviderFailure({
          message: frame.finalError ?? "omp auto-retry failed.",
          class: "provider_error",
          retryable: false,
        });
        const retry = {
          attempt,
          maxAttempts: turn.activeProviderRetry?.retry.maxAttempts ?? attempt,
          retryDelayMs: turn.activeProviderRetry?.retry.retryDelayMs ?? null,
        };
        turn.activeProviderRetry = providerRetryState(turn, retry, failure, emittedAt);
        turn.failure = failure;
        yield* emitProviderRetry(turn, turn.activeProviderRetry, "failed", emittedAt);
      });

      const onNotice = Effect.fnUntraced(function* (
        frame: OmpFrameOf<"notice">,
        record: OmpRecord,
      ) {
        // Tool-command inventories can list hundreds of names; they update state, not the transcript.
        if (frame.source === "toolCommands") {
          return applyToolCommandDelta(ompToolCommandDelta(record));
        }
        const state = threadState;
        if (frame.source === "session-persistence" && state !== null) {
          yield* adoptLiveSessionFile(state, yield* readState(OMP_SHORT_REQUEST_TIMEOUT_MS));
        }
        yield* noticeInActiveTurn(frame.message, frame.level);
      });

      const onSessionInfoUpdate = Effect.fnUntraced(function* (
        frame: OmpFrameOf<"session_info_update">,
      ) {
        const state = threadState;
        const title = frame.title?.trim() ?? "";
        if (state === null || title.length === 0) return;
        appliedSessionName = frame.title ?? null;
        yield* updateProviderThread(state, {
          nativeMetadata: { ...state.providerThread.nativeMetadata, title },
        });
      });

      const frameHandlers: OmpFrameHandlers = {
        response: onResponse,
        prompt_result: onPromptResult,
        session_settled: onSessionSettled,
        agent_start: onAgentStart,
        message_start: onMessageStart,
        message_update: onMessageUpdate,
        message_end: onMessageEnd,
        tool_execution_start: onToolStart,
        tool_execution_update: onToolUpdate,
        tool_execution_end: onToolEnd,
        tool_stream_update: onToolStreamUpdate,
        extension_ui_request: onUiRequest,
        host_tool_call: onHostToolCall,
        host_tool_cancel: (frame) =>
          bridge === undefined ? Effect.void : bridge.handleCancel(frame),
        manual_compaction_start: onCompactionStart,
        auto_compaction_start: onCompactionStart,
        manual_compaction_end: onCompactionEnd,
        auto_compaction_end: onCompactionEnd,
        auto_retry_start: onRetryStart,
        auto_retry_end: onRetryEnd,
        retry_fallback_applied: (frame) =>
          noticeInActiveTurn(`omp retried with ${frame.to} after ${frame.from} failed.`, "info"),
        extension_error: (frame) =>
          Effect.logWarning("omp extension failed", {
            extensionPath: frame.extensionPath,
            event: frame.event,
            errorLength: frame.error.length,
          }),
        advisor_yielded: () => Effect.logDebug("omp advisor yielded"),
        notice: onNotice,
        command_output: (frame) => noticeInActiveTurn(stripAnsi(frame.text)),
        session_info_update: onSessionInfoUpdate,
        available_commands_update: (frame) =>
          Effect.sync(() => {
            commandCatalogNames = new Set(
              frame.commands.flatMap((command) => [command.name, ...(command.aliases ?? [])]),
            );
          }),
        thinking_level_changed: (frame) =>
          Effect.sync(() => {
            appliedThinking = frame.thinkingLevel ?? null;
          }),
        model_changed: () =>
          Effect.sync(() => {
            appliedModel = null;
          }),
        config_update: (frame) =>
          Effect.sync(() => {
            appliedModel = ompModelSlugOf(frame.model) ?? null;
            if (frame.thinkingLevel !== undefined) appliedThinking = frame.thinkingLevel;
          }),
        rpc_frame_error: (frame) =>
          Effect.logWarning("omp could not encode a frame", {
            originalType: frame.originalType,
            errorLength: frame.error.length,
          }),
      };

      const dispatchFrame = (frame: OmpInboundFrame, record: OmpRecord) => {
        const handler = frameHandlers[frame.type] as OmpFrameHandler | undefined;
        return handler === undefined ? Effect.void : handler(frame, record);
      };

      // A wake's output waits for the turn that adopts it (A:2286-2301).
      const bufferForWake = Effect.fnUntraced(function* (record: OmpRecord) {
        const wake = pendingWake;
        if (wake === null || threadState?.activeTurn !== null || bypassesWakeBuffer(record)) {
          return false;
        }
        wake.records.push(record);
        if (record["type"] === "session_settled") yield* updateProviderSession("ready", null);
        return true;
      });

      const handleRecord = Effect.fnUntraced(function* (record: OmpRecord) {
        if (yield* bufferForWake(record)) return;
        const frame = decodeOmpInboundFrame(record);
        if (Option.isNone(frame)) return;
        yield* dispatchFrame(frame.value, record);
      });

      const runtimeIsIdle = () => {
        const state = threadState;
        return (
          state !== null &&
          state.activeTurn === null &&
          pendingWake === null &&
          pendingRequests.size === 0 &&
          backgroundHolds.size === 0 &&
          !registrationInProgress &&
          !rollbackInProgress
        );
      };

      const endEvents = Effect.fnUntraced(function* (
        cause: Cause.Cause<unknown>,
        idle: boolean,
        interrupted: boolean,
      ) {
        if (unsolicitedActivityDetected) {
          yield* updateProviderSession("error", OMP_UNSOLICITED_ACTIVITY_ERROR);
          return yield* Queue.end(events);
        }
        if (stopRequested || idle) {
          yield* updateProviderSession("stopped", null);
          return yield* Queue.end(events);
        }
        yield* updateProviderSession(
          "error",
          interrupted ? "omp process was stopped." : "omp process exited unexpectedly.",
        );
        yield* Queue.fail(
          events,
          new ProviderAdapter.ProviderAdapterEventStreamError({
            driver: OMP_PROVIDER,
            providerSessionId: input.providerSessionId,
            cause,
          }),
        );
      });

      // No prompt_result arrives once omp is gone (R§6), so the open turn ends here.
      const retireRuntime = Effect.fnUntraced(function* (cause: Cause.Cause<unknown>) {
        const idle = runtimeIsIdle();
        closed = true;
        pendingWake = null;
        const state = threadState;
        const turn = state?.activeTurn ?? null;
        const interrupted = turn?.interrupted === true;
        for (const pending of pendingPrompts.values()) {
          yield* Deferred.succeed(pending.acknowledged, undefined);
        }
        if (state !== null && turn !== null) {
          if (!interrupted) {
            turn.failure = makeProviderFailure({
              cause,
              message: "omp process exited unexpectedly.",
              class: "transport_error",
            });
          }
          yield* finalizeTurn(state, false);
        } else {
          yield* cancelPendingRequests(yield* DateTime.now, false);
        }
        yield* releaseBackgroundHolds(true);
        if (bridge !== undefined) yield* bridge.cancelAll;
        yield* endEvents(cause, idle, interrupted);
      });

      /* Runs inside the per-record pump loop. */
      const takeRecord = Effect.fnUntraced(function* () {
        const activeConnection = connection;
        return yield* Queue.take(activeConnection.events).pipe(
          Effect.catchTags({
            OmpRpcError: Effect.fnUntraced(function* (error: OmpRpcError) {
              // A restart swaps the connection after the old transport goes down.
              const gate = restartGate;
              if (gate !== null) yield* Deferred.await(gate);
              if (activeConnection !== connection) return undefined;
              return yield* error;
            }),
          }),
        );
      });

      yield* Effect.gen(function* () {
        while (true) {
          const record = yield* takeRecord();
          if (record === undefined) continue;
          yield* sessionEventPermit.withPermits(1)(handleRecord(record));
        }
      }).pipe(
        Effect.catchCause((cause) => sessionEventPermit.withPermits(1)(retireRuntime(cause))),
        Effect.forkIn(scope),
      );

      const lifecycleRequest = Effect.fn("OmpAdapter.lifecycleRequest")(
        function* (command: OmpCommand) {
          const data = yield* request(command, OMP_LIFECYCLE_TIMEOUT_MS);
          if (ompRecordBoolean(data, "cancelled") === true) {
            return yield* protocolError(`An omp extension cancelled ${command.type}`);
          }
          return data;
        },
        (effect, command) =>
          effect.pipe(
            Effect.tapError((error) =>
              Effect.logWarning("omp session lifecycle request failed", {
                providerSessionId: input.providerSessionId,
                operation: command.type,
                errorTag: error._tag,
              }),
            ),
          ),
      );

      const runForkProcess = (
        sourceSessionFile: string,
        throughTurnUserEntryId: string | undefined,
        nextTurnUserEntryId: string | undefined,
      ) =>
        unattendedOverlay.ensure.pipe(
          Effect.mapError(() => protocolError("Could not write omp's unattended settings overlay")),
          Effect.andThen(
            withPlatform(
              forkOmpSessionInSeparateProcess({
                binaryPath,
                launchArgs: resolvedLaunchArgs.args,
                environment: options.environment,
                cwd,
                sessionDir,
                sourceSessionFile,
                unattendedOverlayPath: unattendedOverlay.path,
                throughTurnUserEntryId,
                nextTurnUserEntryId,
              }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner)),
            ),
          ),
        );

      /** A workspace handoff reopens a session whose header names another cwd (A:3129-3135). */
      const attachSessionFile = Effect.fnUntraced(function* (
        sessionFile: string,
        existing: OrchestrationV2ProviderThread,
        importedNativeSession: boolean,
      ) {
        if (!(yield* sessionFileExists(sessionFile))) {
          // Files appear with the first assistant message (R§39); a thread with turns lost one.
          if (existing.lastRunOrdinal !== null || importedNativeSession) {
            return yield* protocolError("omp session file for this thread is missing");
          }
          return yield* lifecycleRequest({ type: "new_session" });
        }
        const target = (yield* sessionFileInCwd(sessionFile))
          ? sessionFile
          : yield* runForkProcess(sessionFile, undefined, undefined);
        // Never switch_session to an unchecked path: a missing one is created empty (R§38).
        return yield* reportingResume(
          lifecycleRequest({ type: "switch_session", sessionPath: target }),
        );
      });

      const readRegisteredState = Effect.fnUntraced(function* () {
        const data = yield* request({ type: "get_state" }, OMP_LIFECYCLE_TIMEOUT_MS);
        const state = Option.getOrUndefined(decodeSessionState(data));
        if (state?.sessionFile === undefined) {
          return yield* protocolError("get_state returned no persisted sessionFile");
        }
        rememberState(state);
        if (baselineModel === null && appliedModel === null) {
          baselineModel = ompModelRefOf(state.model) ?? null;
        }
        if (baselineThinking === undefined) baselineThinking = state.thinkingLevel ?? null;
        appliedThinking ??= state.thinkingLevel ?? null;
        availableThinkingLevels = yield* readThinkingLevels;
        lastBranchMessages = yield* readBranchMessages();
        return state.sessionFile;
      });

      const switchToExistingSession = Effect.fnUntraced(function* (
        resumeId: string,
        existing: OrchestrationV2ProviderThread,
        importedNativeSession: boolean,
      ) {
        const live = Option.getOrUndefined(yield* readState(OMP_LIFECYCLE_TIMEOUT_MS));
        if (sameSessionFile(live?.sessionFile, resumeId)) return;
        threadState = null;
        forgetSessionSelections();
        yield* attachSessionFile(resumeId, existing, importedNativeSession);
      });

      /** Returns whether a new native session replaced the one the process had open. */
      const ensureNativeSession = Effect.fnUntraced(function* (
        threadInput: ProviderAdapter.ProviderAdapterV2EnsureThreadInput,
        importedNativeSession: boolean,
      ) {
        const existing = threadInput.existingProviderThread;
        const resumeId = existing?.nativeThreadRef?.nativeId ?? undefined;
        const forceNew = threadInput.forceNewNativeThread === true;
        const needsNewSession = forceNew || (resumeId === undefined && registrationAttempted);
        registrationAttempted = true;
        if (resumeId !== undefined && existing !== undefined && !forceNew) {
          yield* switchToExistingSession(resumeId, existing, importedNativeSession);
          return false;
        }
        if (!needsNewSession) return false;
        threadState = null;
        forgetSessionSelections();
        yield* lifecycleRequest({ type: "new_session" });
        return true;
      });

      const buildProviderThread = (
        threadInput: ProviderAdapter.ProviderAdapterV2EnsureThreadInput,
        nativeId: string,
        createdAt: DateTime.Utc,
        fresh: boolean,
      ): OrchestrationV2ProviderThread => {
        const existing = threadInput.existingProviderThread;
        if (existing !== undefined) {
          return {
            ...existing,
            providerSessionId: input.providerSessionId,
            nativeThreadRef: providerRef(nativeId),
            ...(fresh ? { nativeConversationHeadRef: null, contextUsage: null } : {}),
            status: "idle",
            updatedAt: createdAt,
          };
        }
        return {
          id: idAllocator.derive.providerThread({ driver: OMP_PROVIDER, nativeThreadId: nativeId }),
          driver: OMP_PROVIDER,
          providerInstanceId: options.instanceId,
          providerSessionId: input.providerSessionId,
          appThreadId: threadInput.threadId,
          ownerNodeId: null,
          nativeThreadRef: providerRef(nativeId),
          nativeConversationHeadRef: null,
          status: "idle",
          firstRunOrdinal: null,
          lastRunOrdinal: null,
          handoffIds: [],
          forkedFrom: null,
          pendingBackgroundTasks: [],
          createdAt,
          updatedAt: createdAt,
        };
      };

      /** A wake that arrived on this same binding keeps it; any other switch must wait. */
      const wakeOwnsThread = (threadInput: ProviderAdapter.ProviderAdapterV2EnsureThreadInput) => {
        const state = threadState;
        const existing = threadInput.existingProviderThread;
        return (
          state !== null &&
          state === pendingWake?.state &&
          state.providerThread.appThreadId === threadInput.threadId &&
          (existing === undefined ||
            sameSessionFile(
              existing.nativeThreadRef?.nativeId,
              state.providerThread.nativeThreadRef?.nativeId,
            ))
        );
      };

      const registerThread = Effect.fnUntraced(
        function* (
          threadInput: ProviderAdapter.ProviderAdapterV2EnsureThreadInput,
          publish = true,
          importedNativeSession = false,
        ) {
          if (rollbackInProgress) {
            return yield* protocolError("Cannot register an omp thread during rollback");
          }
          if (threadState?.activeTurn != null) {
            return yield* protocolError("Cannot register an omp thread while a turn is active");
          }
          if (pendingWake !== null) {
            if (threadState !== null && wakeOwnsThread(threadInput)) {
              return threadState.providerThread;
            }
            return yield* protocolError(
              "Cannot switch omp threads while native work awaits a turn",
            );
          }
          yield* awaitProcessPreparation;
          const fresh = yield* ensureNativeSession(threadInput, importedNativeSession);
          const nativeId = yield* readRegisteredState();
          const providerThread = buildProviderThread(
            threadInput,
            nativeId,
            yield* DateTime.now,
            fresh,
          );
          threadState = { providerThread, activeTurn: null };
          if (publish) {
            yield* emit({ type: "provider_thread.updated", driver: OMP_PROVIDER, providerThread });
          }
          return providerThread;
        },
        (effect) =>
          Effect.acquireUseRelease(
            Effect.suspend(() => {
              if (registrationInProgress) {
                return protocolError("Cannot register an omp thread during registration");
              }
              registrationInProgress = true;
              return Effect.void;
            }),
            () => effect,
            () =>
              Effect.sync(() => {
                registrationInProgress = false;
              }),
          ),
      );

      /** The stopped process took its background jobs and any run it had not handed to a turn. */
      const forgetStoppedProcessWork = Effect.suspend(() => {
        pendingWake = null;
        return releaseBackgroundHolds(true);
      });

      const reregisterThread = (state: OmpThreadState | null) =>
        readRegisteredState().pipe(
          Effect.flatMap((sessionFile) =>
            state !== null && threadState === state
              ? rebindSessionFile(state, sessionFile)
              : Effect.void,
          ),
        );

      const replaceConnection = Effect.fnUntraced(function* (
        resume: string | undefined,
        state: OmpThreadState | null,
      ) {
        yield* sessionEventPermit.withPermits(1)(cancelPendingRequests(yield* DateTime.now, false));
        if (bridge !== undefined) yield* bridge.cancelAll;
        yield* Scope.close(started.scope, Exit.void);
        yield* sessionEventPermit.withPermits(1)(forgetStoppedProcessWork);
        started = yield* startConnection(resume);
        connection = started.connection;
        commandCatalogNames = new Set();
        toolCommandDeltas.length = 0;
        forgetSessionSelections();
        processPreparation = yield* prepareProcess(reregisterThread(state));
        yield* awaitProcessPreparation;
      });

      /** omp reads the overlay only at spawn, so tightening restarts on the same file (§1 item 5). */
      const restartProcess = Effect.fn("OmpAdapter.restartProcess")(function* () {
        const state = threadState;
        const resume = yield* resumableSessionFile(
          state?.providerThread.nativeThreadRef?.nativeId ?? undefined,
        );
        const gate = yield* Deferred.make<void>();
        restartGate = gate;
        yield* replaceConnection(resume, state).pipe(
          Effect.ensuring(
            Effect.suspend(() => {
              if (restartGate === gate) restartGate = null;
              return Deferred.succeed(gate, undefined);
            }),
          ),
        );
      });

      /** True when it restarted omp, which can move the thread to another session file. */
      const applyRuntimeMode = Effect.fnUntraced(function* (runtimeMode: RuntimeMode) {
        const next = ompApprovalModeForRuntimeMode(runtimeMode);
        if (next === approvalMode) return false;
        const tightened = APPROVAL_MODE_RANK[next] < APPROVAL_MODE_RANK[approvalMode];
        yield* writeOverlay(next);
        approvalMode = next;
        // A looser mode applies in place: omp polls the overlay (R§23).
        if (!tightened) return false;
        yield* reportingResume(restartProcess());
        return true;
      });

      const pickerRowsFor = Effect.fnUntraced(function* (slug: string) {
        if (pickerRows !== null && resolveOmpModelSlug(slug, pickerRows) !== null) {
          return pickerRows;
        }
        // Extensions and models.json edits can add providers after the first listing.
        const data = yield* request({ type: "get_model_picker", limit: OMP_MODEL_PICKER_LIMIT });
        const rows = Option.match(decodeModelPicker(data), {
          onNone: (): ReadonlyArray<OmpModelPickerRow> => [],
          onSome: (picker) => picker.rows,
        });
        pickerRows = rows;
        return rows;
      });

      const setModel = Effect.fnUntraced(function* (model: OmpModelRef) {
        const selected = yield* request({
          type: "set_model",
          provider: model.provider,
          modelId: model.modelId,
        });
        contextWindow =
          rememberModelContextWindow(Option.getOrUndefined(decodeModelInfo(selected))) ??
          contextWindow;
        // set_model resets thinking to the model's default and changes the offered levels.
        appliedThinking = null;
        availableThinkingLevels = yield* readThinkingLevels;
      });

      const publishSessionModel = Effect.fnUntraced(function* (model: string) {
        sessionEntity = { ...sessionEntity, model, updatedAt: yield* DateTime.now };
        yield* emit({
          type: "provider_session.updated",
          driver: OMP_PROVIDER,
          providerSession: sessionEntity,
        });
      });

      // `default` is a sentinel and never sent: it restores the model omp started with.
      const applyDefaultModel = Effect.fnUntraced(function* () {
        const baseline = baselineModel;
        if (appliedModel === null || baseline === null) return;
        yield* setModel(baseline);
        appliedModel = null;
        yield* publishSessionModel(OMP_DEFAULT_MODEL_SLUG);
      });

      const applyModel = Effect.fnUntraced(function* (slug: string) {
        if (slug === OMP_DEFAULT_MODEL_SLUG) return yield* applyDefaultModel();
        if (slug === appliedModel) return;
        const parsed = resolveOmpModelSlug(slug, yield* pickerRowsFor(slug));
        if (parsed === null) {
          return yield* protocolError(`omp model '${slug}' must use provider/model format`);
        }
        yield* setModel(parsed);
        appliedModel = slug;
        yield* publishSessionModel(slug);
      });

      /** `default` with no level restores the level omp started with, as it restores the model. */
      const selectedThinkingLevel = (modelSelection: ModelSelection) => {
        const requested = getModelSelectionStringOptionValue(modelSelection, "thinking");
        const restoresBaseline =
          requested === undefined && modelSelection.model === OMP_DEFAULT_MODEL_SLUG;
        // omp accepts any level and silently clears it, so an unoffered one is skipped (R§47).
        return validateOmpThinkingLevel(
          restoresBaseline ? (baselineThinking ?? undefined) : requested,
          availableThinkingLevels,
        );
      };

      const applySelection = Effect.fnUntraced(function* (modelSelection: ModelSelection) {
        yield* applyModel(modelSelection.model);
        const level = selectedThinkingLevel(modelSelection);
        if (level === undefined || level === appliedThinking) return;
        const selector: OmpThinkingSelector = level;
        yield* request({ type: "set_thinking_level", level: selector });
        appliedThinking = level;
      });

      const applySessionName = (title: string) =>
        title === appliedSessionName
          ? Effect.void
          : request({ type: "set_session_name", name: title }).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  appliedSessionName = title;
                }),
              ),
              Effect.ignore,
            );

      const resolvePromptPayload = Effect.fnUntraced(function* (
        text: string,
        attachments: ReadonlyArray<ChatAttachment>,
      ) {
        const images: Array<OmpImageContent> = [];
        const message = yield* providerMessageTextWithAttachmentPaths({
          text,
          attachments,
          resolveAttachmentPath: host.resolveAttachmentPath,
          resolveExtractedTextPath: host.resolveExtractedTextPath,
        });
        for (const attachment of attachments) {
          const attachmentPath = host.resolveAttachmentPath(attachment);
          if (attachmentPath === null || !attachment.mimeType.startsWith("image/")) continue;
          const bytes = yield* fileSystem.readFile(attachmentPath);
          images.push({
            type: "image",
            data: Buffer.from(bytes).toString("base64"),
            mimeType: attachment.mimeType,
          });
        }
        const payload: OmpPromptPayload = { message, images };
        return payload;
      });

      const isLiveSessionOf = (state: OmpThreadState, nativeId: string | null | undefined) =>
        sameSessionFile(state.providerThread.nativeThreadRef?.nativeId, nativeId);

      const makeActiveTurn = Effect.fnUntraced(function* (
        state: OmpThreadState,
        turnInput: ProviderAdapter.ProviderAdapterV2TurnInput,
        disposition: OmpSlashDisposition | undefined,
        beforeBranch: ReadonlyArray<OmpBranchMessage> | undefined,
      ) {
        const startedAt = yield* DateTime.now;
        const syntheticNativeTurnId = `${state.providerThread.id}:attempt:${turnInput.attemptId}`;
        const providerTurn: OrchestrationV2ProviderTurn = {
          id: idAllocator.derive.providerTurn({
            driver: OMP_PROVIDER,
            nativeTurnId: syntheticNativeTurnId,
          }),
          providerThreadId: turnInput.providerThread.id,
          nodeId: turnInput.rootNodeId,
          runAttemptId: turnInput.attemptId,
          nativeTurnRef: providerRef(syntheticNativeTurnId, "weak"),
          ...(state.providerThread.nativeThreadRef === null
            ? {}
            : { nativeThreadRef: state.providerThread.nativeThreadRef }),
          ordinal: turnInput.providerTurnOrdinal,
          status: "running",
          startedAt,
          completedAt: null,
        };
        const turn: ActiveOmpTurn = {
          turnInput,
          providerTurn,
          itemOrdinals: new Map(),
          nextItemOrdinal: turnInput.providerTurnOrdinal * 100 + 1,
          messageOrdinal: 0,
          streamItems: new Map(),
          tools: new Map(),
          unlinkedToolCallIds: [],
          editPreviews: new Map(),
          slashCommand:
            disposition !== undefined && turnInput.message.text.trimStart().startsWith("/"),
          continuation: disposition === undefined,
          beforeBranch,
          mainPrompt: undefined,
          settlement: undefined,
          interrupted: false,
          abortRequested: false,
          adoptedWake: false,
          awaitingSettle: false,
          activeCompaction: null,
          compactionSkipped: false,
          compactionCancelled: false,
          activeProviderRetry: null,
          failure: null,
          unreadSteers: [],
          sentPrompts: [],
          lastMessageUsage: undefined,
          lastLiveUsedTokens: null,
          reportedModel: undefined,
          stopEndState: undefined,
        };
        return turn;
      });

      const dispatchTurn = Effect.fnUntraced(function* (
        turn: ActiveOmpTurn,
        disposition: OmpSlashDisposition | undefined,
        payload: OmpPromptPayload | null,
      ) {
        if (disposition?.kind === "compact") {
          return yield* sendPrompt(turn, "compact", compactCommand(disposition));
        }
        if (payload !== null) {
          yield* sendPrompt(turn, "turn_start", promptCommand(payload, "followUp"));
        }
      });

      const flushSessionNotices = Effect.fnUntraced(function* (turn: ActiveOmpTurn) {
        for (const notice of pendingSessionNotices.splice(0)) {
          yield* emitSystemNotice(
            turn,
            `session-notice:${turn.providerTurn.id}:${turn.nextItemOrdinal}`,
            notice,
            "warning",
          );
        }
      });

      const settleWithoutPrompt = Effect.fnUntraced(function* (
        turn: ActiveOmpTurn,
        disposition: OmpSlashDisposition | undefined,
        wake: OmpWake | null,
      ) {
        for (const record of wake?.records ?? []) yield* handleRecord(record);
        if (threadState?.activeTurn !== turn) return;
        if (disposition?.kind === "intercept") {
          yield* emitSystemNotice(
            turn,
            `intercepted:${turn.providerTurn.id}`,
            disposition.notice,
            "info",
          );
          turn.settlement = { kind: "intercepted" };
          return yield* completeTurnIfSettled(turn);
        }
        if (turn.continuation && wake === null) {
          turn.settlement = { kind: "session_settled" };
          yield* completeTurnIfSettled(turn);
        }
      });

      const turnOwnershipChanged = (
        state: OmpThreadState,
        sessionFile: string | null | undefined,
      ) =>
        threadState !== state ||
        registrationInProgress ||
        rollbackInProgress ||
        state.activeTurn !== null ||
        closed ||
        stopRequested ||
        !isLiveSessionOf(state, sessionFile);

      const installTurn = Effect.fnUntraced(function* (
        state: OmpThreadState,
        turn: ActiveOmpTurn,
        disposition: OmpSlashDisposition | undefined,
        payload: OmpPromptPayload | null,
        sessionFile: string | null | undefined,
      ) {
        const turnInput = turn.turnInput;
        if (turnOwnershipChanged(state, sessionFile)) {
          return yield* protocolError("omp turn ownership changed before it could start");
        }
        const wake = pendingWake?.state === state ? pendingWake : null;
        if (wake !== null && state.providerThread.appThreadId !== turnInput.threadId) {
          return yield* protocolError("omp native wake belongs to a different app thread");
        }
        pendingWake = null;
        state.providerThread = {
          ...turnInput.providerThread,
          nativeThreadRef: state.providerThread.nativeThreadRef,
        };
        state.activeTurn = turn;
        turn.adoptedWake = wake !== null;
        yield* dispatchTurn(turn, disposition, payload).pipe(
          Effect.tapError(() =>
            Effect.sync(() => {
              if (state.activeTurn === turn) state.activeTurn = null;
            }),
          ),
        );
        yield* emit({
          type: "provider_turn.updated",
          driver: OMP_PROVIDER,
          threadId: turnInput.threadId,
          providerTurn: turn.providerTurn,
        });
        yield* updateProviderThread(state, {
          status: "active",
          firstRunOrdinal: state.providerThread.firstRunOrdinal ?? turnInput.runOrdinal,
          lastRunOrdinal: turnInput.runOrdinal,
        });
        yield* updateProviderSession("running", null);
        yield* flushSessionNotices(turn);
        yield* settleWithoutPrompt(turn, disposition, wake);
      });

      /** Mode and model for a user turn; returns the session file the turn must start on. */
      const applyTurnSettings = Effect.fnUntraced(function* (
        turnInput: ProviderAdapter.ProviderAdapterV2TurnInput,
      ) {
        const restarted = yield* applyRuntimeMode(turnInput.runtimePolicy.runtimeMode);
        yield* applySelection(turnInput.modelSelection);
        // A restart resumes only a file omp wrote; before the first reply, omp starts a new one.
        const thread = restarted ? threadState?.providerThread : turnInput.providerThread;
        return thread?.nativeThreadRef?.nativeId;
      });

      const prepareTurn = Effect.fnUntraced(function* (
        turnInput: ProviderAdapter.ProviderAdapterV2TurnInput,
        continuation: boolean,
      ) {
        const sessionFile = continuation
          ? turnInput.providerThread.nativeThreadRef?.nativeId
          : yield* applyTurnSettings(turnInput);
        yield* applySessionName(turnInput.appThread.title);
        const disposition = continuation
          ? undefined
          : classifyOmpSlashInput(turnInput.message.text, commandCatalogNames);
        // Resolve attachments before turn installation so failures cannot strand activeTurn.
        const payload =
          disposition?.kind === "send"
            ? yield* resolvePromptPayload(turnInput.message.text, turnInput.message.attachments)
            : null;
        const beforeBranch = lastBranchMessages ?? (yield* readBranchMessages());
        return { disposition, payload, beforeBranch, sessionFile };
      });

      const checkTurnCanStart = Effect.fnUntraced(function* (
        turnInput: ProviderAdapter.ProviderAdapterV2TurnInput,
      ) {
        if (registrationInProgress || rollbackInProgress) {
          return yield* protocolError("Cannot start an omp turn while its thread is changing");
        }
        const state = threadState;
        if (state === null) return yield* protocolError("omp session has no registered thread");
        if (state.activeTurn !== null) {
          return yield* new ProviderAdapter.ProviderAdapterProtocolError({
            driver: OMP_PROVIDER,
            detail: `omp provider thread ${turnInput.providerThread.id} already has an active turn`,
            reason: "previous_turn_active",
          });
        }
        if (closed || stopRequested) {
          return yield* protocolError("Cannot start an omp turn after session closure");
        }
        if (!isLiveSessionOf(state, turnInput.providerThread.nativeThreadRef?.nativeId)) {
          return yield* protocolError("omp turn requested for a different native session");
        }
      });

      const startTurn = Effect.fn("OmpAdapter.startTurn")(
        function* (turnInput: ProviderAdapter.ProviderAdapterV2TurnInput) {
          yield* checkTurnCanStart(turnInput);
          yield* awaitProcessPreparation;
          const continuation =
            turnInput.message.createdBy === "agent" &&
            turnInput.message.creationSource === "provider";
          const prepared = yield* prepareTurn(turnInput, continuation);
          // Only install and send hold the permit; setup must leave the pump free for dialogs.
          yield* sessionEventPermit.withPermits(1)(
            Effect.gen(function* () {
              const state = threadState;
              if (state === null) {
                return yield* protocolError("omp session has no registered thread");
              }
              const turn = yield* makeActiveTurn(
                state,
                turnInput,
                prepared.disposition,
                prepared.beforeBranch,
              );
              yield* installTurn(
                state,
                turn,
                prepared.disposition,
                prepared.payload,
                prepared.sessionFile,
              );
            }),
          );
        },
        (effect, turnInput) =>
          effect.pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterTurnStartError({
                  driver: OMP_PROVIDER,
                  threadId: turnInput.threadId,
                  providerThreadId: turnInput.providerThread.id,
                  runId: turnInput.runId,
                  cause,
                }),
            ),
          ),
      );

      const steerTurn = Effect.fn("OmpAdapter.steerTurn")(
        function* (steerInput: ProviderAdapter.ProviderAdapterV2SteerInput) {
          const turn = threadState?.activeTurn ?? null;
          if (turn === null || turn.providerTurn.id !== steerInput.providerTurnId) {
            return yield* protocolError(`omp turn ${steerInput.providerTurnId} is not active`);
          }
          // A steer never dispatches slash commands (R§59); the orchestrator queues it as a turn.
          if (isOmpCommandInvocation(steerInput.message.text, commandCatalogNames)) {
            return yield* new ProviderAdapter.ProviderAdapterSteerDeclinedError({
              driver: OMP_PROVIDER,
              providerThreadId: steerInput.providerThread.id,
              providerTurnId: steerInput.providerTurnId,
            });
          }
          const payload = yield* resolvePromptPayload(
            steerInput.message.text,
            steerInput.message.attachments,
          );
          yield* sessionEventPermit.withPermits(1)(
            Effect.gen(function* () {
              if (threadState?.activeTurn !== turn) {
                return yield* protocolError(`omp turn ${steerInput.providerTurnId} is not active`);
              }
              yield* sendPrompt(
                turn,
                "steer",
                promptCommand(payload, "steer"),
                steerInput.message.messageId,
              );
            }),
          );
        },
        (effect, steerInput) =>
          effect.pipe(
            Effect.mapError((cause) =>
              cause._tag === "ProviderAdapterSteerDeclinedError"
                ? cause
                : new ProviderAdapter.ProviderAdapterSteerRunError({
                    driver: OMP_PROVIDER,
                    providerThreadId: steerInput.providerThread.id,
                    providerTurnId: steerInput.providerTurnId,
                    cause,
                  }),
            ),
          ),
      );

      /**
       * A plain abort keeps queued user input, and omp resumes the run on a queued steer.
       * `abort_and_restore_queue` withdraws it, but from omp 19.2.0 on it backgrounds running bash
       * and eval. Removing a prompt omp already delivered is a no-op, so every prompt is removed.
       */
      const withdrawSentPrompts = Effect.fnUntraced(function* (turn: ActiveOmpTurn) {
        const admissions = Array.from(pendingPrompts.values()).flatMap((pending) =>
          pending.turn === turn ? [Deferred.await(pending.acknowledged)] : [],
        );
        // omp acknowledges a prompt once it is queued, so a removal sent after the ack finds it.
        yield* Effect.all(admissions, { discard: true }).pipe(
          Effect.timeoutOption(OMP_SHORT_REQUEST_TIMEOUT_MS),
        );
        for (const sent of turn.sentPrompts.splice(0)) {
          yield* request(
            { type: "remove_queued_message", message: sent.text, queue: sent.queue },
            OMP_SHORT_REQUEST_TIMEOUT_MS,
          ).pipe(Effect.ignore);
        }
      });

      /** Stop cancels every subagent, so the jobs held for any turn are stopped too. */
      const markBackgroundWorkStopped = () => {
        for (const hold of backgroundHolds.values()) hold.stopped = true;
      };

      /**
       * abort leaves detached subagents running, and their commands run in sessions of their own,
       * outside the process group T3 signals when it ends omp. omp answers commands one at a time
       * and a cancel only once its child stopped, so each waits for the one before.
       */
      const cancelSubagents = Effect.gen(function* () {
        const listed = yield* request({ type: "get_subagents" }, OMP_SHORT_REQUEST_TIMEOUT_MS).pipe(
          Effect.map(decodeSubagentList),
          Effect.orElseSucceed(() => Option.none<typeof OmpSubagentList.Type>()),
        );
        if (Option.isNone(listed)) return;
        for (const child of listed.value.subagents) {
          yield* request(
            { type: "cancel_subagent", subagentId: child.id },
            OMP_SUBAGENT_CANCEL_TIMEOUT_MS,
          ).pipe(Effect.ignore);
        }
      });

      const stopDeadline = Clock.currentTimeMillis.pipe(
        Effect.map((now) => now + OMP_STOP_BUDGET_MS),
      );

      /** The finalize read, taken while omp can still answer; rollback and fork need the entry. */
      const captureStopEndState = Effect.fnUntraced(function* (
        state: OmpThreadState,
        turn: ActiveOmpTurn,
      ) {
        // A second Stop before the turn finalizes would read from the process the first one ended.
        if (turn.stopEndState !== undefined) return;
        const live = yield* readState(OMP_SHORT_REQUEST_TIMEOUT_MS);
        yield* adoptLiveSessionFile(state, live);
        const after = yield* readBranchMessages(OMP_SHORT_REQUEST_TIMEOUT_MS);
        turn.stopEndState = { after, live };
      });

      const abortForRestart = Effect.fnUntraced(function* (turn: ActiveOmpTurn) {
        yield* withdrawSentPrompts(turn);
        turn.abortRequested = true;
        yield* request({ type: "abort" }, OMP_SHORT_REQUEST_TIMEOUT_MS).pipe(Effect.ignore);
      });

      /** Input the abort handed back resumes the run, and the pump cannot see it under the permit. */
      const resumedAfterAbort = (live: Option.Option<OmpSessionState>) =>
        Option.exists(
          live,
          (state) => state.isStreaming === true || (state.queuedMessages?.steering.length ?? 0) > 0,
        );

      const settleBeforeRestart = Effect.fnUntraced(function* (turn: ActiveOmpTurn) {
        const state = threadState;
        if (state?.activeTurn === turn) yield* captureStopEndState(state, turn);
        const live = turn.stopEndState?.live ?? Option.none();
        if (resumedAfterAbort(live)) yield* sendAbort("abort_and_restore_queue");
        // Ending omp parks its subagents and reaps their commands only inside the stdin grace.
        yield* cancelSubagents;
      });

      const stopForRestart = Effect.fnUntraced(function* (turn: ActiveOmpTurn) {
        stopRequested = true;
        markBackgroundWorkStopped();
        const deadline = yield* stopDeadline;
        // Behind a running compact, abort and get_state could only time out.
        const answers = !compactCommandRunning(turn);
        if (answers) yield* untilDeadline(deadline, abortForRestart(turn));
        yield* sessionEventPermit.withPermits(1)(
          Effect.gen(function* () {
            if (answers) yield* untilDeadline(deadline, settleBeforeRestart(turn));
            yield* connection.terminate;
          }),
        );
      });

      const idleWorkRemains = () => pendingWake !== null || (backgroundHolds.size > 0 && !closed);

      const beginIdleStop = () => {
        if (!idleWorkRemains()) return false;
        stopRequested = true;
        markBackgroundWorkStopped();
        return true;
      };

      /** Work a settled turn left behind; omp's abort only detaches background jobs (TSR§0.6). */
      const stopIdleProcess = Effect.suspend(() => {
        const wake = pendingWake;
        if (wake !== null) return stopPendingWake(wake);
        return closed ? Effect.void : connection.terminate;
      });

      /**
       * The abort stops a wake run omp already started. Cancels run outside the permit so the pump
       * can abort the run omp starts to deliver a cancelled child's result (`onAgentStart`).
       */
      const stopIdleWork = Effect.gen(function* () {
        if (!(yield* sessionEventPermit.withPermits(1)(Effect.sync(beginIdleStop)))) return;
        const deadline = yield* stopDeadline;
        const abort = request({ type: "abort" }, OMP_SHORT_REQUEST_TIMEOUT_MS).pipe(Effect.ignore);
        yield* untilDeadline(deadline, Effect.andThen(abort, cancelSubagents));
        yield* sessionEventPermit.withPermits(1)(stopIdleProcess);
      });

      const abortTurn = Effect.fnUntraced(function* (turn: ActiveOmpTurn) {
        // Early Stop: an abort sent before omp registered the prompt can be lost.
        if (turn.mainPrompt !== undefined) yield* Deferred.await(turn.mainPrompt.acknowledged);
        if (threadState?.activeTurn !== turn) return;
        yield* withdrawSentPrompts(turn);
        turn.abortRequested = true;
        // omp settles right after the abort, before the cancels below have stopped any child.
        markBackgroundWorkStopped();
        yield* request({ type: "abort" }).pipe(
          Effect.tapError(() =>
            Effect.sync(() => {
              turn.interrupted = false;
              turn.abortRequested = false;
            }),
          ),
        );
        yield* untilDeadline(yield* stopDeadline, cancelSubagents);
      });

      const interruptTurn = Effect.fn("OmpAdapter.interruptTurn")(
        function* (interruptInput: ProviderAdapter.ProviderAdapterV2InterruptInput) {
          const turn = threadState?.activeTurn ?? null;
          const restart = interruptInput.requestRuntimeRestart === true;
          if (turn === null && restart) return yield* stopIdleWork;
          if (turn === null || turn.providerTurn.id !== interruptInput.providerTurnId) {
            return yield* protocolError(`omp turn ${interruptInput.providerTurnId} is not active`);
          }
          turn.interrupted = true;
          if (restart || compactCommandRunning(turn)) return yield* stopForRestart(turn);
          yield* abortTurn(turn);
        },
        (effect, interruptInput) =>
          effect.pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterInterruptError({
                  driver: OMP_PROVIDER,
                  providerThreadId: interruptInput.providerThread.id,
                  providerTurnId: interruptInput.providerTurnId,
                  cause,
                }),
            ),
          ),
      );

      const questionResponse = (
        nativeRequestId: string,
        shape: OmpAnswerShape,
        decision: ProviderApprovalDecision | undefined,
        answers: ProviderUserInputAnswers | undefined,
      ) => {
        if (answers === undefined || decision === "cancel" || decision === "decline") {
          // Skipping a form submits its defaults; only Stop cancels the turn (R§33).
          return {
            ok: true,
            response: ompCancelResponse(nativeRequestId, { timedOut: shape.method === "form" }),
          } as const;
        }
        return ompQuestionResponse(nativeRequestId, shape, answers);
      };

      const requestAnswer = (
        pending: PendingOmpRequest,
        requestInput: ProviderAdapter.ProviderAdapterV2RuntimeRequestResponseInput,
      ) =>
        pending.response.kind === "approval"
          ? ({
              ok: true,
              response: ompApprovalResponse(
                pending.nativeRequestId,
                pending.response.labels,
                requestInput.decision ?? "cancel",
              ),
            } as const)
          : questionResponse(
              pending.nativeRequestId,
              pending.response.shape,
              requestInput.decision,
              requestInput.answers,
            );

      const resolveRequest = Effect.fnUntraced(function* (pending: PendingOmpRequest) {
        const resolvedAt = yield* DateTime.now;
        pending.runtimeRequest = { ...pending.runtimeRequest, status: "resolved", resolvedAt };
        yield* emit({
          type: "runtime_request.updated",
          driver: OMP_PROVIDER,
          threadId: pending.node.threadId,
          runtimeRequest: pending.runtimeRequest,
        });
        yield* emit({
          type: "node.updated",
          driver: OMP_PROVIDER,
          node: { ...pending.node, status: "completed", completedAt: resolvedAt },
        });
        yield* emit({
          type: "turn_item.updated",
          driver: OMP_PROVIDER,
          turnItem: {
            ...pending.turnItem,
            status: "completed",
            completedAt: resolvedAt,
            updatedAt: resolvedAt,
          },
        });
      });

      /** Deny with a message: "Deny", then steer the text; a steer alone leaves it blocked (R§28). */
      const steerFeedback = Effect.fnUntraced(function* (feedback: string | undefined) {
        const turn = threadState?.activeTurn ?? null;
        const text = feedback?.trim() ?? "";
        if (turn === null || text.length === 0) return;
        yield* sendPrompt(turn, "steer", promptCommand({ message: text, images: [] }, "steer"));
      });

      const respondToRuntimeRequest = Effect.fn("OmpAdapter.respondToRuntimeRequest")(
        function* (requestInput: ProviderAdapter.ProviderAdapterV2RuntimeRequestResponseInput) {
          const pending = pendingRequests.get(String(requestInput.requestId));
          if (pending === undefined) {
            return yield* protocolError(`No pending omp request ${requestInput.requestId}`);
          }
          const answer = requestAnswer(pending, requestInput);
          if (!answer.ok) return yield* protocolError(answer.reason);
          yield* connection.send(answer.response);
          forgetRequest(pending);
          yield* resolveRequest(pending);
          if (pending.response.kind === "approval" && requestInput.decision === "decline") {
            yield* steerFeedback(requestInput.feedback);
          }
        },
        (effect, requestInput) =>
          effect.pipe(
            sessionEventPermit.withPermits(1),
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
                  driver: OMP_PROVIDER,
                  requestId: requestInput.requestId,
                  cause,
                }),
            ),
          ),
      );

      const snapshotMessages = (state: OmpThreadState, nativeId: string, data: unknown) => {
        const messages = ompRecordField(data, "messages");
        const threadId = state.providerThread.appThreadId ?? input.threadId;
        return (Array.isArray(messages) ? messages : []).flatMap((message, index) => {
          const role = snapshotRole(message);
          const text = contentText(ompRecordField(message, "content"));
          if (role === undefined || text.length === 0) return [];
          const at = Option.getOrElse(
            DateTime.make(ompRecordNumber(message, "timestamp") ?? Number.NaN),
            () => state.providerThread.createdAt,
          );
          return [
            {
              id: idAllocator.derive.messageFromProviderItem({
                driver: OMP_PROVIDER,
                nativeItemId: `${nativeId}:snapshot-message:${index}`,
              }),
              threadId,
              runId: null,
              nodeId: null,
              role,
              text,
              attachments: [],
              streaming: false,
              createdBy: role === "user" ? ("user" as const) : ("agent" as const),
              creationSource: "provider" as const,
              createdAt: at,
              updatedAt: at,
            },
          ];
        });
      };

      /** Holds a request until a running `/compact` answers, since omp queues it behind the compaction. */
      const afterRunningCompaction = <A, E>(effect: Effect.Effect<A, E>) =>
        Effect.suspend(() => {
          const turn = threadState?.activeTurn ?? null;
          const compact =
            turn !== null && compactCommandRunning(turn) ? turn.mainPrompt : undefined;
          return compact === undefined
            ? effect
            : Deferred.await(compact.acknowledged).pipe(Effect.andThen(effect));
        });

      const readThreadSnapshot = Effect.fn("OmpAdapter.readThreadSnapshot")(
        function* (snapshotInput: ProviderAdapter.ProviderAdapterV2ReadThreadSnapshotInput) {
          const state = threadState;
          const wantedNativeId = snapshotInput.providerThread.nativeThreadRef?.nativeId;
          if (state === null || wantedNativeId == null || !isLiveSessionOf(state, wantedNativeId)) {
            return yield* protocolError(
              "omp snapshot requested for a thread this session does not host",
            );
          }
          // get_messages holds only the active branch, so handoffs never see abandoned ones.
          const data = yield* afterRunningCompaction(
            request({ type: "get_messages" }, OMP_LIFECYCLE_TIMEOUT_MS),
          );
          return {
            providerThread: state.providerThread,
            messages: snapshotMessages(state, wantedNativeId, data),
            runtimeRequests: [],
          };
        },
        (effect, snapshotInput) =>
          effect.pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterReadThreadSnapshotError({
                  driver: OMP_PROVIDER,
                  providerThreadId: snapshotInput.providerThread.id,
                  cause,
                }),
            ),
          ),
      );

      /** `branch` writes a new file and leaves the original untouched (R§40). */
      const branchSession = Effect.fnUntraced(function* (state: OmpThreadState, entryId: string) {
        const result = yield* lifecycleRequest({ type: "branch", entryId });
        if (Option.getOrUndefined(decodeBranchResult(result))?.cancelled === true) {
          return yield* protocolError("An omp extension cancelled the rollback branch");
        }
        const live = Option.getOrUndefined(yield* readState(OMP_LIFECYCLE_TIMEOUT_MS));
        if (live?.sessionFile === undefined) {
          return yield* protocolError("omp rollback did not report its session file");
        }
        forgetSessionSelections();
        lastBranchMessages = yield* readBranchMessages();
        yield* sessionEventPermit.withPermits(1)(
          updateProviderThread(state, {
            nativeThreadRef: providerRef(live.sessionFile),
            nativeConversationHeadRef: null,
          }),
        );
      });

      const loadRollbackThread = Effect.fnUntraced(function* (
        rollbackInput: ProviderAdapter.ProviderAdapterV2RollbackThreadInput,
      ) {
        // A fresh runtime must load the persisted session before branching.
        if (
          threadState !== null ||
          rollbackInput.providerThread.nativeThreadRef?.nativeId == null
        ) {
          return;
        }
        yield* registerThread(
          {
            threadId: rollbackInput.providerThread.appThreadId ?? input.threadId,
            modelSelection: input.modelSelection,
            runtimePolicy: input.runtimePolicy,
            existingProviderThread: rollbackInput.providerThread,
          },
          false,
        );
      });

      const rollbackThread = Effect.fn("OmpAdapter.rollbackThread")(
        function* (rollbackInput: ProviderAdapter.ProviderAdapterV2RollbackThreadInput) {
          if (registrationInProgress || rollbackInProgress) {
            return yield* protocolError("Cannot roll back an omp thread while it is changing");
          }
          yield* loadRollbackThread(rollbackInput);
          const state = threadState;
          if (state === null) return yield* protocolError("omp session has no registered thread");
          if (state.providerThread.id !== rollbackInput.providerThread.id) {
            return yield* protocolError(
              "omp rollback requested for a thread this session does not host",
            );
          }
          if (state.activeTurn !== null || pendingWake !== null) {
            return yield* protocolError("Cannot roll back while omp work is running");
          }
          const entryId = ompRollbackEntryId(
            rollbackInput.target,
            rollbackInput.providerThreadTurns,
          );
          if (entryId === null) return snapshotThread(state.providerThread);
          // Keeping a turn whose entry cannot be located would roll back too little.
          if (entryId === undefined) {
            return yield* protocolError("omp rollback boundary turn has no captured session entry");
          }
          rollbackInProgress = true;
          yield* branchSession(state, entryId).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                rollbackInProgress = false;
              }),
            ),
          );
          return snapshotThread(state.providerThread);
        },
        (effect, rollbackInput) =>
          effect.pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterRollbackThreadError({
                  driver: OMP_PROVIDER,
                  providerThreadId: rollbackInput.providerThread.id,
                  checkpointId: rollbackInput.target.checkpointId,
                  cause,
                }),
            ),
          ),
      );

      const forkedProviderThread = (
        forkInput: ProviderAdapter.ProviderAdapterV2ForkThreadInput,
        nativeId: string,
        createdAt: DateTime.Utc,
      ): OrchestrationV2ProviderThread => ({
        ...forkInput.sourceProviderThread,
        id: idAllocator.derive.providerThread({ driver: OMP_PROVIDER, nativeThreadId: nativeId }),
        appThreadId: forkInput.targetThreadId,
        providerSessionId: input.providerSessionId,
        providerInstanceId: options.instanceId,
        ownerNodeId: forkInput.ownerNodeId ?? null,
        nativeThreadRef: providerRef(nativeId),
        nativeConversationHeadRef: null,
        firstRunOrdinal: null,
        lastRunOrdinal: null,
        handoffIds: [],
        pendingBackgroundTasks: [],
        forkedFrom: {
          providerThreadId: forkInput.sourceProviderThread.id,
          ...(forkInput.providerTurnId === undefined
            ? {}
            : { providerTurnId: forkInput.providerTurnId }),
        },
        createdAt,
        updatedAt: createdAt,
      });

      const resolveForkSource = Effect.fnUntraced(function* (
        forkInput: ProviderAdapter.ProviderAdapterV2ForkThreadInput,
      ) {
        const source = forkInput.sourceProviderThread;
        const sourceFile = source.nativeThreadRef?.nativeId;
        if (sourceFile == null) {
          return yield* protocolError("omp fork source has no session file");
        }
        const sourceTurns = (forkInput.sourceProviderTurns ?? []).filter(
          (turn) => turn.providerThreadId === source.id,
        );
        const target = sourceTurns.find((turn) => turn.id === forkInput.providerTurnId);
        if (forkInput.providerTurnId !== undefined && target === undefined) {
          return yield* protocolError("omp fork target turn is missing");
        }
        const boundary = ompForkBoundary(target, sourceTurns);
        if ("error" in boundary) return yield* protocolError(boundary.error);
        return { sourceFile, boundary };
      });

      const forkThread = Effect.fn("OmpAdapter.forkThread")(
        function* (forkInput: ProviderAdapter.ProviderAdapterV2ForkThreadInput) {
          if (threadState?.activeTurn != null) {
            return yield* protocolError("Cannot fork while an omp turn is active");
          }
          const { sourceFile, boundary } = yield* resolveForkSource(forkInput);
          // `fork` switches its process to the new file, so it runs in a separate one (R§41).
          const nativeId = yield* runForkProcess(sourceFile, boundary.through, boundary.next);
          const providerThread = forkedProviderThread(forkInput, nativeId, yield* DateTime.now);
          return yield* registerThread(
            {
              threadId: forkInput.targetThreadId,
              modelSelection: forkInput.modelSelection ?? input.modelSelection,
              runtimePolicy: forkInput.runtimePolicy ?? input.runtimePolicy,
              existingProviderThread: providerThread,
            },
            false,
          ).pipe(
            Effect.tapError(() =>
              withPlatform(deleteOmpSessionFiles(nativeId)).pipe(Effect.ignore),
            ),
          );
        },
        (effect, forkInput) =>
          effect.pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterForkThreadError({
                  driver: OMP_PROVIDER,
                  providerThreadId: forkInput.sourceProviderThread.id,
                  cause,
                }),
            ),
          ),
      );

      // `set_session_name` makes no model call and emits no event (R§61).
      const applyThreadTitle = Effect.fn("OmpAdapter.applyThreadTitle")(
        function* (titleInput: {
          readonly providerThread: OrchestrationV2ProviderThread;
          readonly title: string;
        }) {
          yield* afterRunningCompaction(
            request({ type: "set_session_name", name: titleInput.title }),
          );
          appliedSessionName = titleInput.title;
        },
        (effect) =>
          effect.pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterProtocolError({
                  driver: OMP_PROVIDER,
                  detail: "omp could not rename the session",
                  cause,
                }),
            ),
          ),
      );

      const hasPendingBackgroundWorkFor = (providerThread: OrchestrationV2ProviderThread) =>
        (pendingWake !== null && pendingWake.state.providerThread.id === providerThread.id) ||
        (backgroundHolds.size > 0 && threadState?.providerThread.id === providerThread.id);

      const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
        instanceId: options.instanceId,
        driver: OMP_PROVIDER,
        providerSessionId: input.providerSessionId,
        get providerSession() {
          return sessionEntity;
        },
        events: Stream.fromQueue(events),
        // Background jobs and wakes die with the omp process, which this session owns.
        backgroundTasksEndWithProcess: true,
        hasPendingBackgroundWork: Effect.sync(
          () => pendingWake !== null || backgroundHolds.size > 0,
        ),
        hasPendingBackgroundWorkForThread: (providerThread) =>
          Effect.sync(() => hasPendingBackgroundWorkFor(providerThread)),
        getModelContextWindow: (selection) => {
          if (selection.instanceId !== options.instanceId) return undefined;
          const slug =
            selection.model === OMP_DEFAULT_MODEL_SLUG
              ? ompModelSlugOf(
                  baselineModel === null
                    ? undefined
                    : { provider: baselineModel.provider, id: baselineModel.modelId },
                )
              : selection.model;
          return slug === undefined ? undefined : modelContextWindows.get(slug);
        },
        ensureThread: (threadInput) =>
          registerThread(threadInput).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterEnsureThreadError({
                  driver: OMP_PROVIDER,
                  threadId: threadInput.threadId,
                  cause,
                }),
            ),
          ),
        resumeThread: (threadInput) =>
          registerThread(
            {
              threadId:
                threadInput.threadId ?? threadInput.providerThread.appThreadId ?? input.threadId,
              modelSelection: threadInput.modelSelection ?? input.modelSelection,
              runtimePolicy: threadInput.runtimePolicy ?? input.runtimePolicy,
              existingProviderThread: threadInput.providerThread,
            },
            true,
            threadInput.importedNativeSession === true,
          ).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterResumeThreadError({
                  driver: OMP_PROVIDER,
                  providerSessionId: input.providerSessionId,
                  providerThreadId: threadInput.providerThread.id,
                  cause,
                }),
            ),
          ),
        startTurn,
        compactThread: (turnInput) =>
          startTurn({ ...turnInput, message: { ...turnInput.message, text: "/compact" } }),
        steerTurn,
        applyThreadTitle,
        interruptTurn,
        respondToRuntimeRequest,
        readThreadSnapshot,
        rollbackThread,
        forkThread,
      };
      return runtime;
    }),
  });
});

export type OmpAdapterV2DriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | FileSystem.FileSystem
  | Path.Path
  | HttpClient.HttpClient
  | IdAllocator.IdAllocatorV2
  | McpProviderSessions.McpProviderSessions
  | ProviderHost.ProviderHost;

export const OmpAdapterV2Driver: ProviderAdapterDriver<OmpSettings, OmpAdapterV2DriverEnv> = {
  driverKind: OMP_PROVIDER,
  configSchema: OmpSettings,
  defaultConfig: (): OmpSettings => DEFAULT_OMP_SETTINGS,
  create: Effect.fn("OmpAdapterV2Driver.create")(
    function* (input: ProviderAdapterDriverCreateInput<OmpSettings>) {
      const hostEnvironment = yield* HostProcess.Environment;
      const continuationRequests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
      return yield* makeOmpAdapterV2({
        instanceId: input.instanceId,
        settings: { ...input.config, enabled: input.enabled },
        environment: yield* mergeProviderInstanceEnvironment(input.environment, hostEnvironment),
        continuationRequests,
      });
    },
    (effect, input) =>
      effect.pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterDriverCreateError({
              driver: OMP_PROVIDER,
              instanceId: input.instanceId,
              detail: "Failed to create OMP adapter.",
              cause,
            }),
        ),
      ),
  ),
};
