/**
 * The subset of omp's RPC protocol (v2, `--mode rpc-ui`) that T3 uses.
 * Source of truth: oh-my-pi packages/coding-agent/src/modes/rpc/wire/{frames,events,state,commands}.ts.
 * Inbound schemas declare only the fields T3 reads. A decoded value drops every other key, so read
 * an undeclared field from the raw record; an unknown frame type decodes to None.
 */
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";

export const OMP_RPC_PROTOCOL_VERSION = 2;
export const OMP_MAX_FRAME_BYTES = 1_048_576;
export const OMP_MAX_REASSEMBLED_FRAME_BYTES = 67_108_864;
export const OMP_RPC_CHUNK_PAYLOAD_BYTES = 262_144;

export type OmpRecord = Readonly<Record<string, unknown>>;

export function ompRecordField(input: unknown, key: string): unknown {
  return Predicate.isObject(input) ? (input as OmpRecord)[key] : undefined;
}

export function ompRecordString(input: unknown, key: string): string | undefined {
  const value = ompRecordField(input, key);
  return Predicate.isString(value) ? value : undefined;
}

export function ompRecordNumber(input: unknown, key: string): number | undefined {
  const value = ompRecordField(input, key);
  return Predicate.isNumber(value) && Number.isFinite(value) ? value : undefined;
}

export function ompRecordBoolean(input: unknown, key: string): boolean | undefined {
  const value = ompRecordField(input, key);
  return Predicate.isBoolean(value) ? value : undefined;
}

const OpenRecord = Schema.Record(Schema.String, Schema.Unknown);

export const OmpReadyFrame = Schema.Struct({
  type: Schema.Literal("ready"),
  protocolVersion: Schema.optional(Schema.Int),
  supportedProtocolVersions: Schema.optional(Schema.Array(Schema.Int)),
  maxFrameBytes: Schema.optional(Schema.Int),
  maxReassembledFrameBytes: Schema.optional(Schema.Int),
});
export type OmpReadyFrame = typeof OmpReadyFrame.Type;

/** `id` is absent for failures omp could not correlate (e.g. `command: "parse"`). */
export const OmpResponseFrame = Schema.Struct({
  type: Schema.Literal("response"),
  id: Schema.optional(Schema.String),
  command: Schema.String,
  success: Schema.Boolean,
  data: Schema.optional(Schema.Unknown),
  error: Schema.optional(Schema.String),
  code: Schema.optional(Schema.String),
});
export type OmpResponseFrame = typeof OmpResponseFrame.Type;

export const OmpRpcChunkFrame = Schema.Struct({
  type: Schema.Literal("rpc_chunk"),
  chunkId: Schema.String,
  index: Schema.Int,
  count: Schema.Int,
  byteLength: Schema.Int,
  data: Schema.String,
});
export type OmpRpcChunkFrame = typeof OmpRpcChunkFrame.Type;

export const OmpRpcFrameErrorFrame = Schema.Struct({
  type: Schema.Literal("rpc_frame_error"),
  originalType: Schema.optional(Schema.String),
  error: Schema.String,
});

export const OmpPromptStatus = Schema.Literals(["completed", "aborted", "error"]);
export type OmpPromptStatus = typeof OmpPromptStatus.Type;

export const OmpPromptError = Schema.Struct({
  message: Schema.String,
  provider: Schema.optional(Schema.String),
  model: Schema.optional(Schema.String),
  httpStatus: Schema.optional(Schema.Int),
  retryable: Schema.Boolean,
  code: Schema.optional(Schema.String),
});
export type OmpPromptError = typeof OmpPromptError.Type;

export const OmpPromptResultFrame = Schema.Struct({
  type: Schema.Literal("prompt_result"),
  id: Schema.optional(Schema.String),
  agentInvoked: Schema.Boolean,
  status: OmpPromptStatus,
  error: Schema.optional(OmpPromptError),
  sessionSettled: Schema.Boolean,
});
export type OmpPromptResultFrame = typeof OmpPromptResultFrame.Type;

export const OmpSessionSettledFrame = Schema.Struct({ type: Schema.Literal("session_settled") });

export const OmpAgentStartFrame = Schema.Struct({
  type: Schema.Literal("agent_start"),
  resumedToolCallIds: Schema.optional(Schema.Array(Schema.String)),
});

export const OmpAgentEndFrame = Schema.Struct({
  type: Schema.Literal("agent_end"),
  isTerminal: Schema.optional(Schema.Boolean),
  yielded: Schema.optional(Schema.Boolean),
  awaitingAsyncWork: Schema.optional(Schema.Boolean),
});
export type OmpAgentEndFrame = typeof OmpAgentEndFrame.Type;

export const OmpTurnStartFrame = Schema.Struct({ type: Schema.Literal("turn_start") });
export const OmpTurnEndFrame = Schema.Struct({ type: Schema.Literal("turn_end") });

/** Messages are open records owned by omp; read `role` and pass everything else through. */
export const OmpMessageStartFrame = Schema.Struct({
  type: Schema.Literal("message_start"),
  message: OpenRecord,
  messageId: Schema.optional(Schema.String),
});

/** `type` is text_start/_delta/_end, thinking_start/_delta/_end, toolcall_*, image_end, done, error. */
export const OmpAssistantMessageEvent = Schema.Struct({
  type: Schema.String,
  contentIndex: Schema.optional(Schema.Int),
  delta: Schema.optional(Schema.String),
  content: Schema.optional(Schema.Unknown),
  partial: Schema.optional(OpenRecord),
});
export type OmpAssistantMessageEvent = typeof OmpAssistantMessageEvent.Type;

export const OmpMessageUpdateFrame = Schema.Struct({
  type: Schema.Literal("message_update"),
  message: OpenRecord,
  assistantMessageEvent: OmpAssistantMessageEvent,
  messageId: Schema.optional(Schema.String),
});
export type OmpMessageUpdateFrame = typeof OmpMessageUpdateFrame.Type;

export const OmpMessageEndFrame = Schema.Struct({
  type: Schema.Literal("message_end"),
  message: OpenRecord,
  messageId: Schema.optional(Schema.String),
});
export type OmpMessageEndFrame = typeof OmpMessageEndFrame.Type;

export const OmpToolExecutionStartFrame = Schema.Struct({
  type: Schema.Literal("tool_execution_start"),
  toolCallId: Schema.String,
  toolName: Schema.String,
  args: Schema.optional(Schema.Unknown),
  intent: Schema.optional(Schema.String),
});
export type OmpToolExecutionStartFrame = typeof OmpToolExecutionStartFrame.Type;

export const OmpToolExecutionUpdateFrame = Schema.Struct({
  type: Schema.Literal("tool_execution_update"),
  toolCallId: Schema.String,
  toolName: Schema.String,
  args: Schema.optional(Schema.Unknown),
  partialResult: Schema.optional(Schema.Unknown),
});
export type OmpToolExecutionUpdateFrame = typeof OmpToolExecutionUpdateFrame.Type;

export const OmpToolExecutionEndFrame = Schema.Struct({
  type: Schema.Literal("tool_execution_end"),
  toolCallId: Schema.String,
  toolName: Schema.String,
  result: Schema.optional(Schema.Unknown),
  isError: Schema.optional(Schema.Boolean),
});
export type OmpToolExecutionEndFrame = typeof OmpToolExecutionEndFrame.Type;

/**
 * Edit-only diff preview, sent while the model streams the arguments (often before
 * `tool_execution_start`). Each generation re-sends the whole preview; trust the latest.
 */
export const OmpToolStreamUpdateFrame = Schema.Struct({
  type: Schema.Literal("tool_stream_update"),
  toolCallId: Schema.String,
  toolName: Schema.String,
  update: Schema.optional(
    Schema.Struct({
      generation: Schema.optional(Schema.Int),
      streaming: Schema.optional(Schema.Boolean),
      files: Schema.optional(
        Schema.Array(
          Schema.Struct({
            path: Schema.String,
            diff: Schema.optional(Schema.String),
            firstChangedLine: Schema.optional(Schema.Int),
            op: Schema.optional(Schema.String),
            error: Schema.optional(Schema.String),
          }),
        ),
      ),
    }),
  ),
});
export type OmpToolStreamUpdateFrame = typeof OmpToolStreamUpdateFrame.Type;

export const OmpCompactionResult = Schema.Struct({
  summary: Schema.String,
  shortSummary: Schema.optional(Schema.String),
  firstKeptEntryId: Schema.String,
  tokensBefore: Schema.Int,
});
export type OmpCompactionResult = typeof OmpCompactionResult.Type;

export const OmpManualCompactionStartFrame = Schema.Struct({
  type: Schema.Literal("manual_compaction_start"),
  mode: Schema.optional(Schema.String),
});

export const OmpManualCompactionEndFrame = Schema.Struct({
  type: Schema.Literal("manual_compaction_end"),
  result: Schema.optional(OmpCompactionResult),
  aborted: Schema.Boolean,
  skipped: Schema.optional(Schema.Boolean),
  errorMessage: Schema.optional(Schema.String),
});
export type OmpManualCompactionEndFrame = typeof OmpManualCompactionEndFrame.Type;

export const OmpAutoCompactionStartFrame = Schema.Struct({
  type: Schema.Literal("auto_compaction_start"),
  reason: Schema.String,
  action: Schema.String,
});

export const OmpAutoCompactionEndFrame = Schema.Struct({
  type: Schema.Literal("auto_compaction_end"),
  action: Schema.String,
  result: Schema.optional(OmpCompactionResult),
  aborted: Schema.Boolean,
  willRetry: Schema.Boolean,
  errorMessage: Schema.optional(Schema.String),
  skipped: Schema.optional(Schema.Boolean),
});

/** Upstream declares `delayMs` an integer, but omp 19.1.1 sends a fraction (TL/07b:18). */
export const OmpAutoRetryStartFrame = Schema.Struct({
  type: Schema.Literal("auto_retry_start"),
  attempt: Schema.Int,
  maxAttempts: Schema.Int,
  delayMs: Schema.Number,
  errorMessage: Schema.String,
  errorId: Schema.optional(Schema.Int),
});
export type OmpAutoRetryStartFrame = typeof OmpAutoRetryStartFrame.Type;

export const OmpAutoRetryEndFrame = Schema.Struct({
  type: Schema.Literal("auto_retry_end"),
  success: Schema.Boolean,
  attempt: Schema.Int,
  finalError: Schema.optional(Schema.String),
});

export const OmpRetryFallbackAppliedFrame = Schema.Struct({
  type: Schema.Literal("retry_fallback_applied"),
  from: Schema.String,
  to: Schema.String,
  role: Schema.String,
  reason: Schema.optional(Schema.String),
});

export const OmpAdvisorYieldedFrame = Schema.Struct({ type: Schema.Literal("advisor_yielded") });

export const OmpExtensionErrorFrame = Schema.Struct({
  type: Schema.Literal("extension_error"),
  extensionPath: Schema.String,
  event: Schema.String,
  error: Schema.String,
});

export const OmpQueueUpdateFrame = Schema.Struct({
  type: Schema.Literal("queue_update"),
  steering: Schema.Array(Schema.String),
  followUp: Schema.Array(Schema.String),
  liveSteered: Schema.optional(Schema.Int),
});
export type OmpQueueUpdateFrame = typeof OmpQueueUpdateFrame.Type;

export const OmpNotifyType = Schema.Literals(["info", "warning", "error"]);
export type OmpNotifyType = typeof OmpNotifyType.Type;

export const OmpNoticeFrame = Schema.Struct({
  type: Schema.Literal("notice"),
  level: OmpNotifyType,
  message: Schema.String,
  source: Schema.optional(Schema.String),
});
export type OmpNoticeFrame = typeof OmpNoticeFrame.Type;

/** Output of a builtin slash command; carries no id, so it is correlated by order. */
export const OmpCommandOutputFrame = Schema.Struct({
  type: Schema.Literal("command_output"),
  text: Schema.String,
});

export const OmpSessionInfoUpdateFrame = Schema.Struct({
  type: Schema.Literal("session_info_update"),
  title: Schema.optional(Schema.String),
  sessionId: Schema.String,
});

export const OmpThinkingLevelChangedFrame = Schema.Struct({
  type: Schema.Literal("thinking_level_changed"),
  thinkingLevel: Schema.optional(Schema.String),
  configured: Schema.optional(Schema.String),
  resolved: Schema.optional(Schema.String),
});

export const OmpModelChangedFrame = Schema.Struct({ type: Schema.Literal("model_changed") });

export const OmpConfigUpdateFrame = Schema.Struct({
  type: Schema.Literal("config_update"),
  model: Schema.optional(OpenRecord),
  thinkingLevel: Schema.optional(Schema.String),
});

export const OmpSlashCommandSource = Schema.Literals([
  "builtin",
  "skill",
  "extension",
  "custom",
  "mcp_prompt",
  "file",
]);

export const OmpAvailableSlashCommand = Schema.Struct({
  name: Schema.String,
  aliases: Schema.optional(Schema.Array(Schema.String)),
  description: Schema.optional(Schema.String),
  input: Schema.optional(Schema.Struct({ hint: Schema.optional(Schema.String) })),
  source: Schema.String,
});
export type OmpAvailableSlashCommand = typeof OmpAvailableSlashCommand.Type;

export const OmpAvailableCommandsUpdateFrame = Schema.Struct({
  type: Schema.Literal("available_commands_update"),
  commands: Schema.Array(OmpAvailableSlashCommand),
});

const uiRequest = <Method extends string, Fields extends Schema.Struct.Fields>(
  method: Method,
  fields: Fields,
) =>
  Schema.Struct({
    type: Schema.Literal("extension_ui_request"),
    id: Schema.String,
    method: Schema.Literal(method),
    ...fields,
  });

export const OmpSelectUiRequest = uiRequest("select", {
  title: Schema.String,
  options: Schema.Array(Schema.String),
  optionDetails: Schema.optional(
    Schema.Array(Schema.Struct({ description: Schema.optional(Schema.String) })),
  ),
  initialIndex: Schema.optional(Schema.Int),
  selectionMarker: Schema.optional(Schema.Literals(["radio", "checkbox"])),
  checkedIndices: Schema.optional(Schema.Array(Schema.Int)),
  markableCount: Schema.optional(Schema.Int),
  timeout: Schema.optional(Schema.Int),
});
export type OmpSelectUiRequest = typeof OmpSelectUiRequest.Type;

export const OmpConfirmUiRequest = uiRequest("confirm", {
  title: Schema.String,
  message: Schema.String,
  timeout: Schema.optional(Schema.Int),
});
export type OmpConfirmUiRequest = typeof OmpConfirmUiRequest.Type;

export const OmpInputUiRequest = uiRequest("input", {
  title: Schema.String,
  placeholder: Schema.optional(Schema.String),
  timeout: Schema.optional(Schema.Int),
  secret: Schema.optional(Schema.Boolean),
});
export type OmpInputUiRequest = typeof OmpInputUiRequest.Type;

export const OmpEditorUiRequest = uiRequest("editor", {
  title: Schema.String,
  prefill: Schema.optional(Schema.String),
  promptStyle: Schema.optional(Schema.Boolean),
});
export type OmpEditorUiRequest = typeof OmpEditorUiRequest.Type;

export const OmpAskOption = Schema.Struct({
  label: Schema.String,
  description: Schema.optional(Schema.String),
  preview: Schema.optional(Schema.String),
});

export const OmpAskQuestion = Schema.Struct({
  id: Schema.String,
  question: Schema.String,
  header: Schema.optional(Schema.String),
  options: Schema.Array(OmpAskOption),
  multi: Schema.optional(Schema.Boolean),
  recommended: Schema.optional(Schema.Int),
});
export type OmpAskQuestion = typeof OmpAskQuestion.Type;

export const OmpAskUiRequest = uiRequest("ask", {
  questions: Schema.Array(OmpAskQuestion),
  timeout: Schema.optional(Schema.Int),
});
export type OmpAskUiRequest = typeof OmpAskUiRequest.Type;

/** The `ask` tool always sends an HTML form, with or without `set_ask_dialog`. */
export const OmpFormUiRequest = uiRequest("form", {
  html: Schema.String,
  timeout: Schema.optional(Schema.Int),
});
export type OmpFormUiRequest = typeof OmpFormUiRequest.Type;

export const OmpCancelUiRequest = uiRequest("cancel", { targetId: Schema.String });
export type OmpCancelUiRequest = typeof OmpCancelUiRequest.Type;

export const OmpNotifyUiRequest = uiRequest("notify", {
  message: Schema.String,
  notifyType: Schema.optional(OmpNotifyType),
});
export type OmpNotifyUiRequest = typeof OmpNotifyUiRequest.Type;

export const OmpSetStatusUiRequest = uiRequest("setStatus", {
  statusKey: Schema.String,
  statusText: Schema.optional(Schema.String),
});

export const OmpSetWidgetUiRequest = uiRequest("setWidget", {
  widgetKey: Schema.String,
  widgetLines: Schema.optional(Schema.Array(Schema.String)),
  widgetPlacement: Schema.optional(Schema.String),
});

export const OmpSetTitleUiRequest = uiRequest("setTitle", { title: Schema.String });

export const OmpSetEditorTextUiRequest = uiRequest("set_editor_text", { text: Schema.String });

export const OmpOpenUrlUiRequest = uiRequest("open_url", {
  url: Schema.String,
  launchUrl: Schema.optional(Schema.String),
  instructions: Schema.optional(Schema.String),
});

export const OmpExtensionUiRequest = Schema.Union([
  OmpSelectUiRequest,
  OmpConfirmUiRequest,
  OmpInputUiRequest,
  OmpEditorUiRequest,
  OmpAskUiRequest,
  OmpFormUiRequest,
  OmpCancelUiRequest,
  OmpNotifyUiRequest,
  OmpSetStatusUiRequest,
  OmpSetWidgetUiRequest,
  OmpSetTitleUiRequest,
  OmpSetEditorTextUiRequest,
  OmpOpenUrlUiRequest,
]);
export type OmpExtensionUiRequest = typeof OmpExtensionUiRequest.Type;

export interface OmpAskAnswer {
  readonly id: string;
  readonly selectedOptions: ReadonlyArray<string>;
  readonly customInput?: string;
}

export type OmpFormValue = string | number | ReadonlyArray<string>;

export type OmpExtensionUiResponse =
  | { readonly type: "extension_ui_response"; readonly id: string; readonly value: string }
  | { readonly type: "extension_ui_response"; readonly id: string; readonly confirmed: boolean }
  | {
      readonly type: "extension_ui_response";
      readonly id: string;
      readonly cancelled: true;
      readonly timedOut?: boolean;
    }
  | {
      readonly type: "extension_ui_response";
      readonly id: string;
      readonly answers: ReadonlyArray<OmpAskAnswer>;
    }
  | {
      readonly type: "extension_ui_response";
      readonly id: string;
      readonly data: Readonly<Record<string, OmpFormValue>>;
    };

export const OmpHostToolCallFrame = Schema.Struct({
  type: Schema.Literal("host_tool_call"),
  id: Schema.String,
  /** `<bash toolCallId>:cmd:<n>` when a discoverable tool runs as a bash tool command. */
  toolCallId: Schema.String,
  toolName: Schema.String,
  arguments: OpenRecord,
});
export type OmpHostToolCallFrame = typeof OmpHostToolCallFrame.Type;

export const OmpHostToolCancelFrame = Schema.Struct({
  type: Schema.Literal("host_tool_cancel"),
  id: Schema.String,
  targetId: Schema.String,
});
export type OmpHostToolCancelFrame = typeof OmpHostToolCancelFrame.Type;

export type OmpToolLoadMode = "essential" | "discoverable";

export interface OmpHostToolDefinition {
  readonly name: string;
  readonly label?: string;
  readonly description: string;
  /** A JSON Schema (draft 2020-12) object schema. */
  readonly parameters: Readonly<Record<string, unknown>>;
  readonly hidden?: boolean;
  readonly loadMode?: OmpToolLoadMode;
}

export type OmpUserContent =
  | { readonly type: "text"; readonly text: string }
  | { readonly type: "image"; readonly data: string; readonly mimeType: string };

/** `isError` belongs inside the payload; a top-level flag drops details and non-text content. */
export interface OmpHostToolResultPayload {
  readonly content: ReadonlyArray<OmpUserContent>;
  readonly details?: unknown;
  readonly isError?: boolean;
}

export interface OmpHostToolUpdateFrame {
  readonly type: "host_tool_update";
  readonly id: string;
  readonly partialResult: OmpHostToolResultPayload;
}

export interface OmpHostToolResultFrame {
  readonly type: "host_tool_result";
  readonly id: string;
  readonly result: OmpHostToolResultPayload;
}

export const OmpContextUsage = Schema.Struct({
  tokens: Schema.Int,
  contextWindow: Schema.Int,
  percent: Schema.Number,
});
export type OmpContextUsage = typeof OmpContextUsage.Type;

export const OmpQueuedMessages = Schema.Struct({
  steering: Schema.Array(Schema.String),
  followUp: Schema.Array(Schema.String),
  liveSteered: Schema.optional(Schema.Int),
});

export const OmpModelInfo = Schema.Struct({
  id: Schema.String,
  name: Schema.optional(Schema.String),
  provider: Schema.String,
  reasoning: Schema.optional(Schema.Boolean),
  contextWindow: Schema.optional(Schema.NullOr(Schema.Int)),
});
export type OmpModelInfo = typeof OmpModelInfo.Type;

/** Upstream `UsageLimitState`: present only while the account is past its limit. */
export const OmpUsageLimit = Schema.Union([
  Schema.Struct({
    stage: Schema.Literal("low_priority"),
    resetsAtSec: Schema.Number,
    allowanceLeftPercent: Schema.optional(Schema.Int),
  }),
  Schema.Struct({
    stage: Schema.Literal("wrap_up"),
    resetsAtSec: Schema.optional(Schema.Number),
    extraUsage: Schema.Boolean,
  }),
]);
export type OmpUsageLimit = typeof OmpUsageLimit.Type;

/** `get_state` (about 32 KB): keep it out of hot loops. */
export const OmpSessionState = Schema.Struct({
  model: Schema.optional(OmpModelInfo),
  thinkingLevel: Schema.optional(Schema.String),
  configuredThinkingLevel: Schema.optional(Schema.String),
  thinkingLevels: Schema.optional(Schema.Array(Schema.String)),
  isStreaming: Schema.optional(Schema.Boolean),
  isCompacting: Schema.optional(Schema.Boolean),
  interruptMode: Schema.optional(Schema.String),
  sessionFile: Schema.optional(Schema.String),
  sessionId: Schema.String,
  sessionName: Schema.optional(Schema.String),
  hasPendingAsyncWork: Schema.optional(Schema.Boolean),
  isSettled: Schema.optional(Schema.Boolean),
  queuedMessages: Schema.optional(OmpQueuedMessages),
  contextUsage: Schema.optional(OmpContextUsage),
  usageLimit: Schema.optional(OmpUsageLimit),
});
export type OmpSessionState = typeof OmpSessionState.Type;

export const OmpModelPickerRow = Schema.Struct({
  provider: Schema.String,
  id: Schema.String,
  name: Schema.String,
  role: Schema.optional(Schema.String),
  thinking: Schema.optional(Schema.Array(Schema.String)),
  thinkingLevel: Schema.optional(Schema.String),
  current: Schema.optional(Schema.Boolean),
});
export type OmpModelPickerRow = typeof OmpModelPickerRow.Type;

export const OmpModelPickerResult = Schema.Struct({
  rows: Schema.Array(OmpModelPickerRow),
  recent: Schema.Int,
  total: Schema.Int,
});
export type OmpModelPickerResult = typeof OmpModelPickerResult.Type;

export const OmpBranchMessage = Schema.Struct({ entryId: Schema.String, text: Schema.String });
export type OmpBranchMessage = typeof OmpBranchMessage.Type;

export const OmpBranchResult = Schema.Struct({ text: Schema.String, cancelled: Schema.Boolean });
export type OmpBranchResult = typeof OmpBranchResult.Type;

export const OmpCancellationResult = Schema.Struct({ cancelled: Schema.Boolean });

/** One live child in `get_subagents` (rpc-types.ts:418-431); finished children are not listed. */
export const OmpSubagentSummary = Schema.Struct({
  id: Schema.String,
  parentToolCallId: Schema.optional(Schema.String),
});
export type OmpSubagentSummary = typeof OmpSubagentSummary.Type;

export const OmpSubagentList = Schema.Struct({ subagents: Schema.Array(OmpSubagentSummary) });

/** `agentInvoked: false` means a local slash command completed and no `prompt_result` follows. */
export const OmpPromptAck = Schema.Struct({ agentInvoked: Schema.optional(Schema.Boolean) });
export type OmpPromptAck = typeof OmpPromptAck.Type;

export type OmpStreamingBehavior = "steer" | "followUp";
export type OmpThinkingSelector =
  | "auto"
  | "inherit"
  | "off"
  | "minimal"
  | "low"
  | "medium"
  | "high"
  | "xhigh"
  | "max";

export interface OmpImageContent {
  readonly type: "image";
  readonly data: string;
  readonly mimeType: string;
}

export type OmpCommand =
  | { readonly type: "negotiate_protocol"; readonly protocolVersion: number }
  | {
      readonly type: "set_event_filter";
      readonly events: ReadonlyArray<string> | null;
      readonly messageUpdates?: "full" | "delta";
    }
  | { readonly type: "set_ask_dialog"; readonly enabled: boolean }
  | { readonly type: "set_interrupt_mode"; readonly mode: "immediate" | "wait" }
  | { readonly type: "set_host_tools"; readonly tools: ReadonlyArray<OmpHostToolDefinition> }
  | {
      readonly type: "prompt";
      readonly message: string;
      readonly images?: ReadonlyArray<OmpImageContent>;
      readonly streamingBehavior?: OmpStreamingBehavior;
    }
  | { readonly type: "abort" }
  | {
      readonly type: "abort_and_prompt";
      readonly message: string;
      readonly images?: ReadonlyArray<OmpImageContent>;
    }
  | { readonly type: "abort_and_restore_queue" }
  | { readonly type: "abort_and_deliver_queue" }
  | { readonly type: "abort_retry" }
  | {
      readonly type: "remove_queued_message";
      readonly message: string;
      readonly queue: "steering" | "followUp";
    }
  | { readonly type: "promote_queued_message"; readonly message: string }
  | { readonly type: "compact"; readonly customInstructions?: string }
  | { readonly type: "get_state" }
  | { readonly type: "get_model_picker"; readonly query?: string; readonly limit?: number }
  | { readonly type: "get_available_thinking_levels" }
  | { readonly type: "get_available_commands" }
  | {
      readonly type: "set_model";
      readonly provider: string;
      readonly modelId: string;
      readonly thinkingLevel?: OmpThinkingSelector;
    }
  | { readonly type: "set_thinking_level"; readonly level: OmpThinkingSelector }
  | { readonly type: "set_session_name"; readonly name: string }
  | { readonly type: "get_branch_messages" }
  | { readonly type: "get_entries"; readonly since?: string }
  | { readonly type: "get_tree" }
  | { readonly type: "get_messages" }
  | { readonly type: "branch"; readonly entryId: string }
  | {
      readonly type: "navigate_tree";
      readonly entryId: string;
      readonly summarize?: boolean;
      readonly customInstructions?: string;
    }
  | { readonly type: "fork"; readonly entryId?: string }
  | {
      readonly type: "switch_session";
      readonly sessionPath: string;
      readonly provider?: string;
      readonly modelId?: string;
    }
  | {
      readonly type: "open_session";
      readonly sessionDir: string;
      readonly provider?: string;
      readonly modelId?: string;
    }
  | { readonly type: "new_session"; readonly parentSession?: string }
  | { readonly type: "get_session_lease"; readonly sessionIds: ReadonlyArray<string> }
  | { readonly type: "get_last_assistant_text" }
  | { readonly type: "get_subagents" }
  | { readonly type: "cancel_subagent"; readonly subagentId: string };

export type OmpCommandType = OmpCommand["type"];

/**
 * A command written with a caller-chosen id through `OmpRpcConnection.send`. Its response is
 * not awaited: it arrives on `events`, in order with the frames around it.
 */
export type OmpCommandFrame = OmpCommand & { readonly id: string };

/** Everything T3 writes to omp's stdin. */
export type OmpOutboundFrame =
  | OmpCommandFrame
  | OmpExtensionUiResponse
  | OmpHostToolUpdateFrame
  | OmpHostToolResultFrame;

export const OmpInboundFrame = Schema.Union([
  OmpReadyFrame,
  OmpResponseFrame,
  OmpRpcFrameErrorFrame,
  OmpPromptResultFrame,
  OmpSessionSettledFrame,
  OmpAgentStartFrame,
  OmpAgentEndFrame,
  OmpTurnStartFrame,
  OmpTurnEndFrame,
  OmpMessageStartFrame,
  OmpMessageUpdateFrame,
  OmpMessageEndFrame,
  OmpToolExecutionStartFrame,
  OmpToolExecutionUpdateFrame,
  OmpToolExecutionEndFrame,
  OmpToolStreamUpdateFrame,
  OmpManualCompactionStartFrame,
  OmpManualCompactionEndFrame,
  OmpAutoCompactionStartFrame,
  OmpAutoCompactionEndFrame,
  OmpAutoRetryStartFrame,
  OmpAutoRetryEndFrame,
  OmpRetryFallbackAppliedFrame,
  OmpAdvisorYieldedFrame,
  OmpExtensionErrorFrame,
  OmpQueueUpdateFrame,
  OmpNoticeFrame,
  OmpCommandOutputFrame,
  OmpSessionInfoUpdateFrame,
  OmpThinkingLevelChangedFrame,
  OmpModelChangedFrame,
  OmpConfigUpdateFrame,
  OmpAvailableCommandsUpdateFrame,
  OmpExtensionUiRequest,
  OmpHostToolCallFrame,
  OmpHostToolCancelFrame,
]);
export type OmpInboundFrame = typeof OmpInboundFrame.Type;

/** None for frame types T3 does not use (advisor_cost_changed, subagent_*, goal_updated, ...). */
export const decodeOmpInboundFrame = Schema.decodeUnknownOption(OmpInboundFrame);
