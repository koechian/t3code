/**
 * T3's own tools as omp host tools. The adapter is an MCP client of the thread's T3 MCP endpoint
 * (the same URL and bearer token Pi's injected extension uses): `tools/list` feeds
 * `set_host_tools`, and each `host_tool_call` becomes a `tools/call`.
 */
import * as Effect from "effect/Effect";
import * as FiberMap from "effect/FiberMap";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as Sse from "effect/encoding/Sse";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import type * as HttpClientResponse from "effect/http/HttpClientResponse";
import type { McpProviderSessionConfig } from "@t3tools/provider-core/server/mcpSession";

import type { OmpRpcError } from "./rpc.ts";
import {
  ompRecordField,
  ompRecordNumber,
  ompRecordString,
  type OmpHostToolCallFrame,
  type OmpHostToolCancelFrame,
  type OmpHostToolDefinition,
  type OmpHostToolResultFrame,
  type OmpHostToolResultPayload,
  type OmpHostToolUpdateFrame,
  type OmpUserContent,
} from "./wire.ts";

const T3McpRpcError = Schema.Struct({ code: Schema.Number, message: Schema.String });

export class OmpHostToolsError extends Schema.TaggedError<OmpHostToolsError>()(
  "OmpHostToolsError",
  {
    operation: Schema.Literals(["initialize", "tools/list", "tools/call"]),
    reason: Schema.Literals([
      "no_session",
      "unreachable",
      "http_status",
      "rpc_error",
      "bad_response",
      "timeout",
    ]),
    status: Schema.optional(Schema.Int),
    /** The server's JSON-RPC error, its message capped at RPC_ERROR_MESSAGE_MAX_CHARS. */
    rpcError: Schema.optional(T3McpRpcError),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    switch (this.reason) {
      case "no_session":
        return "This thread has no T3 MCP session.";
      case "unreachable":
        return `T3 MCP ${this.operation} could not reach the server.`;
      case "http_status":
        return `T3 MCP ${this.operation} failed (HTTP ${this.status ?? "error"}).`;
      case "rpc_error":
        return `T3 MCP ${this.operation} failed: ${this.rpcError?.message ?? "error"}`;
      case "timeout":
        return `T3 MCP ${this.operation} timed out.`;
      case "bad_response":
        return `T3 MCP ${this.operation} failed.`;
    }
  }
}

/** Pi's public names for T3 tools; `mcpToolPresentation` renders them as T3 tool rows. */
const OMP_T3_TOOL_PREFIX = "mcp__t3-code__";

/** Sent with `loadMode: "essential"`; mirrors DIRECT_TOOLS in apps/server/src/mcp/McpToolExposure.ts. */
const OMP_ESSENTIAL_T3_TOOLS: ReadonlySet<string> = new Set([
  "orchestrator_capabilities",
  "delegate_task",
  "task_status",
]);

/** omp accepts any name, and one bad definition poisons the session until `new_session`. */
const OMP_HOST_TOOL_NAME_PATTERN = /^[a-zA-Z0-9_-]{1,128}$/;

export const T3_MCP_CONNECT_TIMEOUT_MS = 10_000;

/** Posted once when `tools/list` fails; turns go on without T3's tools. */
export const OMP_HOST_TOOLS_UNAVAILABLE_NOTICE =
  "T3 Code tools are unavailable in this session because the T3 Code MCP server did not answer. The agent keeps working without them.";

const MCP_PROTOCOL_VERSION = "2025-06-18";
const MCP_CLIENT_INFO = { name: "t3-code-omp", version: "1.0.0" };
const RPC_ERROR_MESSAGE_MAX_CHARS = 500;
const T3_MCP_SERVER_NAME = "t3-code";

export interface T3McpTool {
  readonly name: string;
  readonly description?: string;
  readonly inputSchema?: Readonly<Record<string, unknown>>;
  readonly annotations?: { readonly readOnlyHint?: boolean };
}

export interface T3McpCallToolResult {
  readonly content: ReadonlyArray<unknown>;
  readonly structuredContent?: unknown;
  readonly isError?: boolean;
}

/** An MCP `notifications/progress` for one `tools/call`. */
export interface T3McpProgress {
  readonly progress: number;
  readonly total?: number;
  readonly message?: string;
}

export interface T3McpCallOptions {
  readonly onProgress?: (progress: T3McpProgress) => Effect.Effect<void>;
}

export interface T3McpClient {
  /** Follows `nextCursor` until the list ends. Lists nothing when the thread has no MCP session. */
  readonly listTools: Effect.Effect<ReadonlyArray<T3McpTool>, OmpHostToolsError>;
  /** Interrupting it aborts the HTTP request. No timeout: T3 tools such as waits can block. */
  readonly callTool: (
    name: string,
    args: Readonly<Record<string, unknown>>,
    options?: T3McpCallOptions,
  ) => Effect.Effect<T3McpCallToolResult, OmpHostToolsError>;
}

/** The thread's current T3 MCP credential (`McpProviderSessions.read`); undefined when it has none. */
export type T3McpConfigReader = Effect.Effect<
  Pick<McpProviderSessionConfig, "endpoint" | "authorizationHeader"> | undefined
>;

type T3McpCredential = Pick<McpProviderSessionConfig, "endpoint" | "authorizationHeader">;
type T3McpOperation = OmpHostToolsError["operation"];

interface T3McpSession {
  readonly credential: T3McpCredential;
  readonly sessionId: string | undefined;
}

interface T3McpExchange {
  readonly operation: T3McpOperation;
  readonly method: string;
  readonly params: Readonly<Record<string, unknown>>;
  readonly onProgress?: ((progress: T3McpProgress) => Effect.Effect<void>) | undefined;
}

interface T3McpReply {
  readonly result: unknown;
  readonly sessionId: string | undefined;
}

const T3McpToolEntry = Schema.Struct({
  name: Schema.String,
  description: Schema.optionalKey(Schema.String),
  inputSchema: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
  annotations: Schema.optionalKey(
    Schema.Struct({ readOnlyHint: Schema.optionalKey(Schema.Boolean) }),
  ),
});
const decodeT3McpTool = Schema.decodeUnknownOption(T3McpToolEntry);

const T3McpCallToolResultEntry = Schema.Struct({
  content: Schema.optionalKey(Schema.Array(Schema.Unknown)),
  structuredContent: Schema.optionalKey(Schema.Unknown),
  isError: Schema.optionalKey(Schema.Boolean),
});
const decodeT3McpCallToolResult = Schema.decodeUnknownOption(T3McpCallToolResultEntry);

const decodeJsonMessage = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Unknown));

function mcpHeaders(session: T3McpSession, method: string): Record<string, string> {
  return {
    accept: "application/json, text/event-stream",
    authorization: session.credential.authorizationHeader,
    ...(method === "initialize" ? {} : { "mcp-protocol-version": MCP_PROTOCOL_VERSION }),
    ...(session.sessionId === undefined ? {} : { "mcp-session-id": session.sessionId }),
  };
}

function isEventStream(response: HttpClientResponse.HttpClientResponse): boolean {
  return (response.headers["content-type"] ?? "").toLowerCase().startsWith("text/event-stream");
}

function decodedMessage<A>(text: string, fallback: A): Result.Result<unknown, A> {
  return Result.fromOption(decodeJsonMessage(text), () => fallback);
}

/** JSON-RPC messages in a JSON body or in SSE `data:` events; undecodable messages are skipped. */
function responseMessages(response: HttpClientResponse.HttpClientResponse) {
  if (!isEventStream(response)) {
    return Stream.fromEffect(response.text).pipe(
      Stream.filterMap((text) => decodedMessage(text, text)),
    );
  }
  return response.stream.pipe(
    Stream.decodeText,
    Stream.pipeThroughChannel(Sse.decode()),
    Stream.filterMap((event) => decodedMessage(event.data, event)),
  );
}

function isReplyTo(id: number) {
  return (message: unknown) =>
    ompRecordField(message, "id") === id && ompRecordField(message, "method") === undefined;
}

function progressNotification(message: unknown, token: string): T3McpProgress | undefined {
  const params = ompRecordField(message, "params");
  if (ompRecordString(message, "method") !== "notifications/progress") return undefined;
  if (ompRecordField(params, "progressToken") !== token) return undefined;
  const progress = ompRecordNumber(params, "progress");
  if (progress === undefined) return undefined;
  const total = ompRecordNumber(params, "total");
  const text = ompRecordString(params, "message");
  return {
    progress,
    ...(total === undefined ? {} : { total }),
    ...(text === undefined ? {} : { message: text }),
  };
}

function boundedRpcError(error: unknown): typeof T3McpRpcError.Type {
  return {
    code: ompRecordNumber(error, "code") ?? 0,
    message: (ompRecordString(error, "message") ?? "error").slice(0, RPC_ERROR_MESSAGE_MAX_CHARS),
  };
}

function replyResult(
  reply: Option.Option<unknown>,
  operation: T3McpOperation,
): Result.Result<unknown, OmpHostToolsError> {
  if (Option.isNone(reply)) {
    return Result.fail(new OmpHostToolsError({ operation, reason: "bad_response" }));
  }
  const error = ompRecordField(reply.value, "error");
  if (error !== undefined) {
    return Result.fail(
      new OmpHostToolsError({ operation, reason: "rpc_error", rpcError: boundedRpcError(error) }),
    );
  }
  return Result.succeed(ompRecordField(reply.value, "result"));
}

function forwardProgress(
  onProgress: T3McpExchange["onProgress"],
  token: string,
): (message: unknown) => Effect.Effect<void> {
  return (message) => {
    const progress = progressNotification(message, token);
    return progress === undefined || onProgress === undefined ? Effect.void : onProgress(progress);
  };
}

function isSessionRejection(error: OmpHostToolsError, session: T3McpSession): boolean {
  return error.status === 401 || (error.status === 404 && session.sessionId !== undefined);
}

function sameCredential(left: T3McpCredential, right: T3McpCredential): boolean {
  return left.endpoint === right.endpoint && left.authorizationHeader === right.authorizationHeader;
}

interface T3McpToolsPage {
  readonly tools: ReadonlyArray<T3McpTool>;
  readonly nextCursor: string | undefined;
}

function toolsPage(result: unknown): T3McpToolsPage | undefined {
  const tools = ompRecordField(result, "tools");
  if (!Array.isArray(tools)) return undefined;
  const nextCursor = ompRecordString(result, "nextCursor");
  return {
    tools: tools.flatMap((entry) => Option.toArray(decodeT3McpTool(entry))),
    nextCursor: nextCursor === "" ? undefined : nextCursor,
  };
}

function repeatsCursor(page: T3McpToolsPage, seen: ReadonlySet<string>): boolean {
  return page.nextCursor !== undefined && seen.has(page.nextCursor);
}

function callToolResult(result: unknown): Result.Result<T3McpCallToolResult, OmpHostToolsError> {
  return Result.fromOption(
    Option.map(decodeT3McpCallToolResult(result), (decoded) => ({
      ...decoded,
      content: decoded.content ?? [],
    })),
    () => new OmpHostToolsError({ operation: "tools/call", reason: "bad_response" }),
  );
}

/**
 * Streamable-HTTP JSON-RPC: `initialize`, `notifications/initialized`, then list/call. Reads the
 * credential before each call and initializes again when it changed or a call got HTTP 401 (or 404
 * for a session the server forgot): re-attaching a thread can revoke and reissue the token while
 * omp keeps running.
 */
export const makeT3McpClient = Effect.fn("OmpHostTools.makeT3McpClient")(function* (
  readConfig: T3McpConfigReader,
): Effect.fn.Return<T3McpClient, OmpHostToolsError, HttpClient.HttpClient> {
  const http = yield* HttpClient.HttpClient;
  const sessionLock = yield* Semaphore.make(1);
  let current: T3McpSession | undefined;
  let lastRequestId = 0;

  const post = (
    session: T3McpSession,
    method: string,
    message: Readonly<Record<string, unknown>>,
  ) =>
    http.execute(
      HttpClientRequest.post(session.credential.endpoint).pipe(
        HttpClientRequest.setHeaders(mcpHeaders(session, method)),
        HttpClientRequest.bodyJsonUnsafe(message),
      ),
    );

  // exchange, acquireSession and request are untraced: they run inside the initialize, list or
  // call span that needs them.
  const exchange = Effect.fnUntraced(function* (session: T3McpSession, request: T3McpExchange) {
    const { operation } = request;
    const id = ++lastRequestId;
    const progressToken = `t3-omp-${id}`;
    const params =
      request.onProgress === undefined
        ? request.params
        : { ...request.params, _meta: { progressToken } };
    const response = yield* post(session, request.method, {
      jsonrpc: "2.0",
      id,
      method: request.method,
      params,
    }).pipe(
      Effect.mapError(
        (cause) => new OmpHostToolsError({ operation, reason: "unreachable", cause }),
      ),
    );
    if (response.status < 200 || response.status >= 300) {
      return yield* new OmpHostToolsError({
        operation,
        reason: "http_status",
        status: response.status,
      });
    }
    const reply = yield* responseMessages(response).pipe(
      Stream.tap(forwardProgress(request.onProgress, progressToken)),
      Stream.filter(isReplyTo(id)),
      Stream.runHead,
      Effect.mapError(
        (cause) => new OmpHostToolsError({ operation, reason: "bad_response", cause }),
      ),
    );
    const result = yield* Effect.fromResult(replyResult(reply, operation));
    return { result, sessionId: response.headers["mcp-session-id"] } satisfies T3McpReply;
  });

  const initialize = Effect.fn("OmpHostTools.initialize")(
    function* (credential: T3McpCredential) {
      const opened = yield* exchange(
        { credential, sessionId: undefined },
        {
          operation: "initialize",
          method: "initialize",
          params: {
            protocolVersion: MCP_PROTOCOL_VERSION,
            capabilities: {},
            clientInfo: MCP_CLIENT_INFO,
          },
        },
      );
      const session: T3McpSession = { credential, sessionId: opened.sessionId };
      yield* Effect.ignore(
        post(session, "notifications/initialized", {
          jsonrpc: "2.0",
          method: "notifications/initialized",
        }),
      );
      current = session;
      return session;
    },
    Effect.timeoutOrElse({
      duration: T3_MCP_CONNECT_TIMEOUT_MS,
      orElse: () =>
        Effect.fail(new OmpHostToolsError({ operation: "initialize", reason: "timeout" })),
    }),
  );

  const reusableSession = (credential: T3McpCredential) =>
    current !== undefined && sameCredential(current.credential, credential) ? current : undefined;

  const acquireSession = Effect.fnUntraced(function* (operation: T3McpOperation) {
    const credential = yield* readConfig;
    if (credential === undefined) {
      return yield* new OmpHostToolsError({ operation, reason: "no_session" });
    }
    return yield* sessionLock.withPermit(
      Effect.suspend(() => {
        const reusable = reusableSession(credential);
        return reusable === undefined ? initialize(credential) : Effect.succeed(reusable);
      }),
    );
  });

  const renewSession = (operation: T3McpOperation, stale: T3McpSession) =>
    sessionLock
      .withPermit(
        Effect.sync(() => {
          if (current === stale) current = undefined;
        }),
      )
      .pipe(Effect.andThen(acquireSession(operation)));

  const request = Effect.fnUntraced(function* (call: T3McpExchange) {
    const session = yield* acquireSession(call.operation);
    return yield* exchange(session, call).pipe(
      Effect.catchIf(
        (error) => isSessionRejection(error, session),
        () =>
          renewSession(call.operation, session).pipe(
            Effect.flatMap((renewed) => exchange(renewed, call)),
          ),
      ),
    );
  });

  const listTools = Effect.gen(function* () {
    if ((yield* readConfig) === undefined) return [];
    const tools: Array<T3McpTool> = [];
    const seenCursors = new Set<string>();
    let cursor: string | undefined;
    do {
      const reply = yield* request({
        operation: "tools/list",
        method: "tools/list",
        params: cursor === undefined ? {} : { cursor },
      });
      const page = toolsPage(reply.result);
      if (page === undefined || repeatsCursor(page, seenCursors)) {
        return yield* new OmpHostToolsError({ operation: "tools/list", reason: "bad_response" });
      }
      tools.push(...page.tools);
      cursor = page.nextCursor;
      if (cursor !== undefined) seenCursors.add(cursor);
    } while (cursor !== undefined);
    return tools;
  }).pipe(Effect.withSpan("OmpHostTools.listTools"));

  const callTool = (
    name: string,
    args: Readonly<Record<string, unknown>>,
    options?: T3McpCallOptions,
  ) =>
    request({
      operation: "tools/call",
      method: "tools/call",
      params: { name, arguments: args },
      onProgress: options?.onProgress,
    }).pipe(
      Effect.flatMap((reply) => Effect.fromResult(callToolResult(reply.result))),
      Effect.withSpan("OmpHostTools.callTool"),
    );

  return { listTools, callTool } satisfies T3McpClient;
});

export interface OmpHostToolSet {
  readonly definitions: ReadonlyArray<OmpHostToolDefinition>;
  /** omp host tool name → T3 MCP tool name. */
  readonly mcpNameByHostName: ReadonlyMap<string, string>;
  /** Host names of tools annotated `readOnlyHint: true`; the overlay allows them without a prompt. */
  readonly readOnlyHostNames: ReadonlyArray<string>;
  readonly rejected: ReadonlyArray<{ readonly name: string; readonly reason: string }>;
}

export const EMPTY_OMP_HOST_TOOL_SET: OmpHostToolSet = {
  definitions: [],
  mcpNameByHostName: new Map(),
  readOnlyHostNames: [],
  rejected: [],
};

const JSON_SCHEMA_TYPES: ReadonlySet<string> = new Set([
  "array",
  "boolean",
  "integer",
  "null",
  "number",
  "object",
  "string",
]);
const SUBSCHEMA_KEYWORDS = [
  "items",
  "additionalProperties",
  "unevaluatedItems",
  "unevaluatedProperties",
  "contains",
  "propertyNames",
  "not",
  "if",
  "then",
  "else",
] as const;
const SUBSCHEMA_LIST_KEYWORDS = ["allOf", "anyOf", "oneOf", "prefixItems"] as const;
const SUBSCHEMA_MAP_KEYWORDS = [
  "properties",
  "patternProperties",
  "dependentSchemas",
  "$defs",
  "definitions",
] as const;

type SchemaEntry = readonly [path: string, schema: unknown];

function isSchemaRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return Predicate.isObject(value) && !Array.isArray(value);
}

/** A present keyword that is not a list yields one entry whose value is not a schema. */
function listEntries(path: string, value: unknown): ReadonlyArray<SchemaEntry> {
  if (value === undefined) return [];
  if (!Array.isArray(value)) return [[path, undefined]];
  return value.map((schema, index) => [`${path}[${index}]`, schema] as const);
}

function mapEntries(path: string, value: unknown): ReadonlyArray<SchemaEntry> {
  if (value === undefined) return [];
  if (!isSchemaRecord(value)) return [[path, undefined]];
  return Object.entries(value).map(([key, schema]) => [`${path}.${key}`, schema] as const);
}

function subschemaEntries(
  path: string,
  schema: Readonly<Record<string, unknown>>,
): ReadonlyArray<SchemaEntry> {
  return [
    ...SUBSCHEMA_KEYWORDS.filter((key) => key in schema).map(
      (key) => [`${path}.${key}`, schema[key]] as const,
    ),
    ...SUBSCHEMA_LIST_KEYWORDS.flatMap((key) => listEntries(`${path}.${key}`, schema[key])),
    ...SUBSCHEMA_MAP_KEYWORDS.flatMap((key) => mapEntries(`${path}.${key}`, schema[key])),
  ];
}

function isJsonSchemaType(value: unknown): boolean {
  const names = Array.isArray(value) ? value : [value];
  return (
    names.length > 0 &&
    names.every((name) => Predicate.isString(name) && JSON_SCHEMA_TYPES.has(name))
  );
}

function keywordProblem(path: string, schema: Readonly<Record<string, unknown>>) {
  if (schema.type !== undefined && !isJsonSchemaType(schema.type)) {
    return `${path}.type is not a JSON Schema type`;
  }
  const required = schema.required;
  if (required !== undefined && !(Array.isArray(required) && required.every(Predicate.isString))) {
    return `${path}.required must list property names`;
  }
  if (schema.enum !== undefined && !Array.isArray(schema.enum)) {
    return `${path}.enum must be a list`;
  }
  return undefined;
}

/**
 * Structural draft 2020-12 checks for what a model API rejects: a bad `type` name, a non-schema in
 * a schema position (including draft-07 tuple `items`), malformed `required` or `enum`
 * (`FX/host-tools/06d-bogus-schema.jsonl:20`).
 */
function jsonSchemaProblem(path: string, schema: unknown): string | undefined {
  if (Predicate.isBoolean(schema)) return undefined;
  if (!isSchemaRecord(schema)) return `${path} is not a schema`;
  const problem = keywordProblem(path, schema);
  if (problem !== undefined) return problem;
  for (const [entryPath, entry] of subschemaEntries(path, schema)) {
    const nested = jsonSchemaProblem(entryPath, entry);
    if (nested !== undefined) return nested;
  }
  return undefined;
}

/** Zero-argument MCP tools may omit `properties`, which some providers reject (MX:97-108). */
function hostToolParameters(tool: T3McpTool): Readonly<Record<string, unknown>> {
  const schema = tool.inputSchema ?? {};
  return {
    ...schema,
    type: schema.type ?? "object",
    ...(schema.properties === undefined ? { properties: {} } : {}),
  };
}

function hostToolRejection(
  hostName: string,
  parameters: Readonly<Record<string, unknown>>,
  taken: ReadonlyMap<string, string>,
): string | undefined {
  if (!OMP_HOST_TOOL_NAME_PATTERN.test(hostName)) {
    return `name must match ${OMP_HOST_TOOL_NAME_PATTERN.source}`;
  }
  if (taken.has(hostName)) return "duplicate name";
  if (parameters.type !== "object") return "parameters must be an object schema";
  return jsonSchemaProblem("parameters", parameters);
}

function hostToolDefinition(
  tool: T3McpTool,
  hostName: string,
  parameters: Readonly<Record<string, unknown>>,
): OmpHostToolDefinition {
  return {
    name: hostName,
    label: tool.name,
    // omp refuses the whole set_host_tools command on a blank description (rpc-mode.ts:1024-1027).
    description: tool.description?.trim() || tool.name,
    parameters,
    loadMode: OMP_ESSENTIAL_T3_TOOLS.has(tool.name) ? "essential" : "discoverable",
  };
}

/**
 * Prefixes, validates names and object schemas, and picks `loadMode`. Every name carries the
 * `mcp__t3-code__` prefix, so none can equal an omp built-in.
 */
export function ompHostToolSet(tools: ReadonlyArray<T3McpTool>): OmpHostToolSet {
  const definitions: Array<OmpHostToolDefinition> = [];
  const mcpNameByHostName = new Map<string, string>();
  const readOnlyHostNames: Array<string> = [];
  const rejected: Array<{ readonly name: string; readonly reason: string }> = [];
  for (const tool of tools) {
    const hostName = `${OMP_T3_TOOL_PREFIX}${tool.name}`;
    const parameters = hostToolParameters(tool);
    const reason = hostToolRejection(hostName, parameters, mcpNameByHostName);
    if (reason !== undefined) {
      rejected.push({ name: tool.name, reason });
      continue;
    }
    definitions.push(hostToolDefinition(tool, hostName, parameters));
    mcpNameByHostName.set(hostName, tool.name);
    if (tool.annotations?.readOnlyHint === true) readOnlyHostNames.push(hostName);
  }
  return { definitions, mcpNameByHostName, readOnlyHostNames, rejected };
}

function modelContentBlock(part: unknown): ReadonlyArray<OmpUserContent> {
  const type = ompRecordString(part, "type");
  const text = ompRecordString(part, "text");
  if (type === "text" && text !== undefined) return [{ type: "text", text }];
  const data = ompRecordString(part, "data");
  const mimeType = ompRecordString(part, "mimeType");
  if (type === "image" && data !== undefined && mimeType !== undefined) {
    return [{ type: "image", data, mimeType }];
  }
  return [];
}

/** Structured output is model text only when the server sent no text or image blocks (MX:115-143). */
function modelContent(result: T3McpCallToolResult): ReadonlyArray<OmpUserContent> {
  const blocks = result.content.flatMap(modelContentBlock);
  if (blocks.length > 0) return blocks;
  return [{ type: "text", text: JSON.stringify(result.structuredContent ?? result) }];
}

function hasText(content: ReadonlyArray<OmpUserContent>): boolean {
  return content.some((part) => part.type === "text" && part.text.length > 0);
}

function t3ToolDetails(mcpToolName: string) {
  return { server: T3_MCP_SERVER_NAME, tool: mcpToolName };
}

/** MCP `CallToolResult` → host tool payload, with `isError` inside the payload. */
export function ompHostToolResult(
  result: T3McpCallToolResult,
  mcpToolName: string,
): OmpHostToolResultPayload {
  const content = modelContent(result);
  if (result.isError !== true) return { content, details: t3ToolDetails(mcpToolName) };
  return {
    content: hasText(content)
      ? content
      : [...content, { type: "text", text: `MCP tool t3-code/${mcpToolName} returned an error` }],
    details: t3ToolDetails(mcpToolName),
    isError: true,
  };
}

function hostToolProgress(progress: T3McpProgress, mcpToolName: string): OmpHostToolResultPayload {
  const fraction =
    progress.total === undefined
      ? `${progress.progress}`
      : `${progress.progress}/${progress.total}`;
  return {
    content: [{ type: "text", text: progress.message ?? fraction }],
    details: t3ToolDetails(mcpToolName),
  };
}

function hostToolFailure(error: OmpHostToolsError, mcpToolName: string): OmpHostToolResultPayload {
  return {
    content: [{ type: "text", text: `T3 Code tool ${mcpToolName} failed. ${error.message}` }],
    details: t3ToolDetails(mcpToolName),
    isError: true,
  };
}

function unregisteredHostTool(hostName: string): OmpHostToolResultPayload {
  return {
    content: [{ type: "text", text: `Host tool "${hostName}" is not registered` }],
    details: {},
    isError: true,
  };
}

export interface OmpHostToolsLoad {
  readonly toolSet: OmpHostToolSet;
  /** `OMP_HOST_TOOLS_UNAVAILABLE_NOTICE` when the listing failed; post it once. */
  readonly notice: string | undefined;
}

/**
 * The tool set to send as `set_host_tools`. An unreachable server, an error or a listing slower
 * than T3_MCP_CONNECT_TIMEOUT_MS yields the empty set and a notice; a thread with no MCP session
 * yields the empty set and no notice.
 */
export const loadOmpHostTools = Effect.fn("OmpHostTools.load")(function* (client: T3McpClient) {
  return yield* client.listTools.pipe(
    Effect.timeoutOrElse({
      duration: T3_MCP_CONNECT_TIMEOUT_MS,
      orElse: () =>
        Effect.fail(new OmpHostToolsError({ operation: "tools/list", reason: "timeout" })),
    }),
    Effect.map((tools): OmpHostToolsLoad => ({
      toolSet: ompHostToolSet(tools),
      notice: undefined,
    })),
    Effect.tap(({ toolSet }) =>
      toolSet.rejected.length === 0
        ? Effect.void
        : Effect.logWarning("Some T3 MCP tools were not registered with omp.", {
            rejected: toolSet.rejected,
          }),
    ),
    Effect.catchTags({
      OmpHostToolsError: (error) =>
        Effect.logWarning("T3 MCP tools are unavailable for this omp session.", {
          operation: error.operation,
          reason: error.reason,
          status: error.status,
        }).pipe(
          Effect.as<OmpHostToolsLoad>({
            toolSet: EMPTY_OMP_HOST_TOOL_SET,
            notice: OMP_HOST_TOOLS_UNAVAILABLE_NOTICE,
          }),
        ),
    }),
  );
});

export interface OmpHostToolBridge {
  /** Runs the call in its own fiber and always answers with a `host_tool_result`. */
  readonly handleCall: (frame: OmpHostToolCallFrame) => Effect.Effect<void>;
  /** Interrupts the in-flight call for `targetId`; omp ignores late results. */
  readonly handleCancel: (frame: OmpHostToolCancelFrame) => Effect.Effect<void>;
  /** On process exit or restart. */
  readonly cancelAll: Effect.Effect<void>;
}

export interface OmpHostToolBridgeInput {
  readonly client: T3McpClient;
  readonly toolSet: OmpHostToolSet;
  readonly send: (
    frame: OmpHostToolResultFrame | OmpHostToolUpdateFrame,
  ) => Effect.Effect<void, OmpRpcError>;
}

const callHostTool = (
  input: OmpHostToolBridgeInput,
  frame: OmpHostToolCallFrame,
  mcpToolName: string,
) =>
  input.client
    .callTool(mcpToolName, frame.arguments, {
      onProgress: (progress) =>
        Effect.ignore(
          input.send({
            type: "host_tool_update",
            id: frame.id,
            partialResult: hostToolProgress(progress, mcpToolName),
          }),
        ),
    })
    .pipe(
      Effect.map((result) => ompHostToolResult(result, mcpToolName)),
      Effect.catchTags({
        OmpHostToolsError: (error) =>
          Effect.logWarning("T3 MCP tool call failed.", {
            tool: mcpToolName,
            operation: error.operation,
            reason: error.reason,
            status: error.status,
          }).pipe(Effect.as(hostToolFailure(error, mcpToolName))),
      }),
    );

const serveHostToolCall = (input: OmpHostToolBridgeInput, frame: OmpHostToolCallFrame) => {
  const mcpToolName = input.toolSet.mcpNameByHostName.get(frame.toolName);
  const result =
    mcpToolName === undefined
      ? Effect.succeed(unregisteredHostTool(frame.toolName))
      : callHostTool(input, frame, mcpToolName);
  return result.pipe(
    Effect.flatMap((payload) =>
      input.send({ type: "host_tool_result", id: frame.id, result: payload }),
    ),
    Effect.catchTags({
      OmpRpcError: (error) =>
        Effect.logWarning("Could not send a host tool result to omp.", {
          operation: error.operation,
        }),
    }),
  );
};

export const makeOmpHostToolBridge = Effect.fn("OmpHostTools.makeBridge")(function* (
  input: OmpHostToolBridgeInput,
): Effect.fn.Return<OmpHostToolBridge, never, Scope.Scope> {
  const calls = yield* FiberMap.make<string>();
  return {
    handleCall: (frame) =>
      FiberMap.run(calls, frame.id, serveHostToolCall(input, frame)).pipe(Effect.asVoid),
    handleCancel: (frame) => FiberMap.remove(calls, frame.targetId),
    cancelAll: FiberMap.clear(calls),
  } satisfies OmpHostToolBridge;
});
