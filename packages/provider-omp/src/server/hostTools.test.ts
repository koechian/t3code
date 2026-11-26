import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientError from "effect/http/HttpClientError";
import type * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as TestClock from "effect/testing/TestClock";

import frames from "./__fixtures__/hostTools/frames.json" with { type: "json" };
import {
  EMPTY_OMP_HOST_TOOL_SET,
  OMP_HOST_TOOLS_UNAVAILABLE_NOTICE,
  T3_MCP_CONNECT_TIMEOUT_MS,
  loadOmpHostTools,
  makeOmpHostToolBridge,
  makeT3McpClient,
  ompHostToolResult,
  ompHostToolSet,
  type T3McpClient,
  type T3McpConfigReader,
  type T3McpProgress,
  type T3McpTool,
} from "./hostTools.ts";
import {
  OmpHostToolCallFrame,
  OmpHostToolCancelFrame,
  type OmpHostToolResultFrame,
  type OmpHostToolUpdateFrame,
} from "./wire.ts";

const decodeCall = Schema.decodeUnknownSync(OmpHostToolCallFrame);
const decodeCancel = Schema.decodeUnknownSync(OmpHostToolCancelFrame);
const lookupCall = decodeCall(frames.readOnlyToolCommandCall);
const unregisteredCall = decodeCall(frames.toolCommandCall);
const slowCall = decodeCall(frames.slowCall);
const slowCancel = decodeCancel(frames.slowCancel);

const ENDPOINT = "http://127.0.0.1:3773/mcp";

const credential = (token: string) => ({
  endpoint: ENDPOINT,
  authorizationHeader: `Bearer ${token}`,
});

interface McpMessage {
  readonly id?: number;
  readonly method: string;
  readonly params?: Readonly<Record<string, unknown>>;
}

interface RecordedRequest {
  readonly message: McpMessage;
  readonly headers: Readonly<Record<string, string | undefined>>;
}

const jsonReply = (id: number | undefined, result: unknown, headers?: Record<string, string>) =>
  Response.json({ jsonrpc: "2.0", id, result }, headers === undefined ? {} : { headers });

const sseReply = (messages: ReadonlyArray<unknown>) =>
  new Response(messages.map((message) => `data: ${JSON.stringify(message)}\n\n`).join(""), {
    headers: { "content-type": "text/event-stream" },
  });

interface FakeMcpServerOptions {
  readonly pages?: ReadonlyArray<{
    readonly tools: ReadonlyArray<unknown>;
    readonly nextCursor?: string;
  }>;
  /** The authorization the server accepts right now. */
  readonly accepts?: () => string;
  readonly knowsSession?: (sessionId: string) => boolean;
  readonly call?: (message: McpMessage) => Effect.Effect<Response>;
}

/** An in-memory T3 MCP endpoint: Streamable HTTP, JSON replies, one session id per initialize. */
function makeFakeMcpServer(options: FakeMcpServerOptions = {}) {
  const requests: Array<RecordedRequest> = [];
  let sessions = 0;
  const rejection = (headers: RecordedRequest["headers"]): Response | undefined => {
    const accepted = options.accepts?.() ?? "Bearer token-1";
    if (headers.authorization !== accepted) return new Response(null, { status: 401 });
    const sessionId = headers["mcp-session-id"];
    if (sessionId !== undefined && options.knowsSession?.(sessionId) === false) {
      return new Response(null, { status: 404 });
    }
    return undefined;
  };
  const initialize = (message: McpMessage) => {
    sessions += 1;
    return jsonReply(
      message.id,
      { protocolVersion: "2025-06-18", capabilities: { tools: {} } },
      { "mcp-session-id": `session-${sessions}` },
    );
  };
  const listTools = (message: McpMessage) => {
    const pages = options.pages ?? [{ tools: [] }];
    const index = message.params?.cursor === undefined ? 0 : Number(message.params.cursor);
    return jsonReply(message.id, pages[index]);
  };
  const methodReply = (message: McpMessage): Effect.Effect<Response> => {
    switch (message.method) {
      case "initialize":
        return Effect.succeed(initialize(message));
      case "notifications/initialized":
        return Effect.succeed(new Response(null, { status: 202 }));
      case "tools/list":
        return Effect.succeed(listTools(message));
      default:
        return (
          options.call?.(message) ??
          Effect.succeed(jsonReply(message.id, { content: [{ type: "text", text: "ok" }] }))
        );
    }
  };
  const reply = (message: McpMessage, headers: RecordedRequest["headers"]) => {
    const rejected = rejection(headers);
    return rejected === undefined ? methodReply(message) : Effect.succeed(rejected);
  };
  const http = HttpClient.make((request) =>
    Effect.gen(function* () {
      const message = decodeBody(request);
      requests.push({ message, headers: request.headers });
      return HttpClientResponse.fromWeb(request, yield* reply(message, request.headers));
    }),
  );
  return { http, requests, sessions: () => sessions };
}

function decodeBody(request: HttpClientRequest.HttpClientRequest): McpMessage {
  if (request.body._tag !== "Uint8Array") throw new Error("Expected a JSON body");
  return JSON.parse(new TextDecoder().decode(request.body.body)) as McpMessage;
}

const clientFor = (http: HttpClient.HttpClient, readConfig: T3McpConfigReader) =>
  makeT3McpClient(readConfig).pipe(Effect.provideService(HttpClient.HttpClient, http));

const methods = (requests: ReadonlyArray<RecordedRequest>) =>
  requests.map((request) => request.message.method);

const CAPABILITIES_TOOL: T3McpTool = {
  name: "orchestrator_capabilities",
  description: "What this thread may do.",
};
const LOOKUP_TOOL: T3McpTool = {
  name: "thread_lookup",
  description: "Look up a thread.",
  inputSchema: {
    type: "object",
    properties: { query: { type: "string" } },
    required: ["query"],
  },
  annotations: { readOnlyHint: true },
};
const RENAME_TOOL: T3McpTool = {
  name: "thread_rename",
  inputSchema: { type: "object", properties: { title: { type: "string" } } },
};
const T3_TOOLS = [CAPABILITIES_TOOL, LOOKUP_TOOL, RENAME_TOOL];

describe("makeT3McpClient", () => {
  it.effect("initializes once, then lists every page with the session and protocol headers", () =>
    Effect.gen(function* () {
      const server = makeFakeMcpServer({
        pages: [
          { tools: [CAPABILITIES_TOOL, { description: "no name" }], nextCursor: "1" },
          { tools: [LOOKUP_TOOL] },
        ],
      });
      const client = yield* clientFor(server.http, Effect.succeed(credential("token-1")));

      const tools = yield* client.listTools;
      yield* client.callTool("thread_lookup", { query: "alpha" });

      assert.deepStrictEqual(tools, [CAPABILITIES_TOOL, LOOKUP_TOOL]);
      assert.deepStrictEqual(methods(server.requests), [
        "initialize",
        "notifications/initialized",
        "tools/list",
        "tools/list",
        "tools/call",
      ]);
      const [initialize, initialized, firstPage, secondPage, call] = server.requests;
      assert.deepStrictEqual(
        [initialize?.headers["mcp-protocol-version"], initialize?.headers["mcp-session-id"]],
        [undefined, undefined],
      );
      assert.strictEqual(initialize?.headers.accept, "application/json, text/event-stream");
      for (const request of [initialized, firstPage, secondPage, call]) {
        assert.strictEqual(request?.headers.authorization, "Bearer token-1");
        assert.strictEqual(request?.headers["mcp-protocol-version"], "2025-06-18");
        assert.strictEqual(request?.headers["mcp-session-id"], "session-1");
      }
      assert.deepStrictEqual(secondPage?.message.params, { cursor: "1" });
      assert.deepStrictEqual(call?.message.params, {
        name: "thread_lookup",
        arguments: { query: "alpha" },
      });
    }),
  );

  it.effect("reads an SSE tools/call reply and forwards only its own progress", () =>
    Effect.gen(function* () {
      const server = makeFakeMcpServer({
        call: (message) => {
          const token = (message.params?._meta as { progressToken?: string } | undefined)
            ?.progressToken;
          return Effect.succeed(
            sseReply([
              {
                jsonrpc: "2.0",
                method: "notifications/progress",
                params: { progressToken: token, progress: 1, total: 2, message: "halfway" },
              },
              {
                jsonrpc: "2.0",
                method: "notifications/progress",
                params: { progressToken: "someone-else", progress: 9 },
              },
              {
                jsonrpc: "2.0",
                method: "notifications/progress",
                params: { progressToken: token, progress: 2, total: 2 },
              },
              {
                jsonrpc: "2.0",
                id: message.id,
                result: { content: [{ type: "text", text: "done" }], structuredContent: { n: 1 } },
              },
            ]),
          );
        },
      });
      const client = yield* clientFor(server.http, Effect.succeed(credential("token-1")));
      const progress: Array<T3McpProgress> = [];

      const result = yield* client.callTool(
        "task_status",
        {},
        { onProgress: (update) => Effect.sync(() => progress.push(update)) },
      );

      assert.deepStrictEqual(result, {
        content: [{ type: "text", text: "done" }],
        structuredContent: { n: 1 },
      });
      assert.deepStrictEqual(progress, [
        { progress: 1, total: 2, message: "halfway" },
        { progress: 2, total: 2 },
      ]);
    }),
  );

  it.effect("initializes again for a new credential or a forgotten session, and retries once", () =>
    Effect.gen(function* () {
      let token = "token-1";
      let accepted = "Bearer token-1";
      let forgotten = new Set<string>();
      const server = makeFakeMcpServer({
        accepts: () => accepted,
        knowsSession: (sessionId) => !forgotten.has(sessionId),
      });
      const client = yield* clientFor(
        server.http,
        Effect.sync(() => credential(token)),
      );

      yield* client.callTool("thread_lookup", {});
      token = "token-2";
      accepted = "Bearer token-2";
      yield* client.callTool("thread_lookup", {});
      forgotten = new Set(["session-2"]);
      yield* client.callTool("thread_lookup", {});
      accepted = "Bearer token-3";
      const revoked = yield* Effect.flip(client.callTool("thread_lookup", {}));

      assert.strictEqual(server.sessions(), 3);
      assert.deepStrictEqual(
        [revoked.operation, revoked.reason, revoked.status],
        ["initialize", "http_status", 401],
      );
      assert.deepStrictEqual(methods(server.requests), [
        "initialize",
        "notifications/initialized",
        "tools/call",
        "initialize",
        "notifications/initialized",
        "tools/call",
        "tools/call",
        "initialize",
        "notifications/initialized",
        "tools/call",
        "tools/call",
        "initialize",
      ]);
    }),
  );

  it.effect("lists nothing and fails calls when the thread has no MCP session", () =>
    Effect.gen(function* () {
      const server = makeFakeMcpServer();
      const client = yield* clientFor(server.http, Effect.undefined);

      const tools = yield* client.listTools;
      const error = yield* Effect.flip(client.callTool("thread_lookup", {}));

      assert.deepStrictEqual(tools, []);
      assert.deepStrictEqual([error.operation, error.reason], ["tools/call", "no_session"]);
      assert.deepStrictEqual(server.requests, []);
    }),
  );

  it.effect("keeps a JSON-RPC error's message and fails a reply without the request id", () =>
    Effect.gen(function* () {
      const server = makeFakeMcpServer({
        call: (message) =>
          Effect.succeed(
            message.params?.name === "missing"
              ? Response.json({
                  jsonrpc: "2.0",
                  id: message.id,
                  error: { code: -32602, message: "Tool 'missing' not found" },
                })
              : jsonReply(Number(message.id) + 100, { content: [] }),
          ),
      });
      const client = yield* clientFor(server.http, Effect.succeed(credential("token-1")));

      const rpcError = yield* Effect.flip(client.callTool("missing", {}));
      const noReply = yield* Effect.flip(client.callTool("thread_lookup", {}));

      assert.deepStrictEqual(rpcError.rpcError, {
        code: -32602,
        message: "Tool 'missing' not found",
      });
      assert.strictEqual(rpcError.message, "T3 MCP tools/call failed: Tool 'missing' not found");
      assert.strictEqual(noReply.reason, "bad_response");
    }),
  );
});

describe("ompHostToolSet", () => {
  it("prefixes names, keeps only T3's direct tools essential, and declares empty properties", () => {
    const toolSet = ompHostToolSet(T3_TOOLS);

    assert.deepStrictEqual(toolSet.definitions, [
      {
        name: "mcp__t3-code__orchestrator_capabilities",
        label: "orchestrator_capabilities",
        description: "What this thread may do.",
        parameters: { type: "object", properties: {} },
        loadMode: "essential",
      },
      {
        name: "mcp__t3-code__thread_lookup",
        label: "thread_lookup",
        description: "Look up a thread.",
        parameters: {
          type: "object",
          properties: { query: { type: "string" } },
          required: ["query"],
        },
        loadMode: "discoverable",
      },
      {
        name: "mcp__t3-code__thread_rename",
        label: "thread_rename",
        description: "thread_rename",
        parameters: { type: "object", properties: { title: { type: "string" } } },
        loadMode: "discoverable",
      },
    ]);
    assert.deepStrictEqual(toolSet.readOnlyHostNames, ["mcp__t3-code__thread_lookup"]);
    assert.strictEqual(
      toolSet.mcpNameByHostName.get("mcp__t3-code__thread_rename"),
      "thread_rename",
    );
    assert.deepStrictEqual(toolSet.rejected, []);
  });

  it("names a tool whose description is blank, which omp would refuse for the whole set", () => {
    const toolSet = ompHostToolSet([
      { name: "empty", description: "" },
      { name: "spaces", description: "   " },
      { name: "padded", description: "  Reads a thread.  " },
    ]);

    assert.deepStrictEqual(
      toolSet.definitions.map((definition) => definition.description),
      ["empty", "spaces", "Reads a thread."],
    );
    assert.deepStrictEqual(toolSet.rejected, []);
  });

  it("rejects names and schemas that would poison the omp session", () => {
    const fromDefinition = (definition: {
      readonly name: string;
      readonly description: string;
      readonly parameters: Readonly<Record<string, unknown>>;
    }): T3McpTool => ({
      name: definition.name,
      description: definition.description,
      inputSchema: definition.parameters,
    });

    const toolSet = ompHostToolSet([
      fromDefinition(frames.badNameDefinition),
      fromDefinition(frames.bogusSchemaDefinition),
      { name: "x".repeat(115) },
      { name: "thread_read" },
      { name: "thread_read" },
      { name: "echo", inputSchema: { type: "string" } },
      { name: "tuple", inputSchema: { properties: { pair: { type: "array", items: [{}, {}] } } } },
      {
        name: "nested",
        inputSchema: { properties: { a: { anyOf: [{ type: ["string", "nope"] }] } } },
      },
      { name: "required", inputSchema: { required: "a" } },
    ]);

    assert.deepStrictEqual(
      toolSet.definitions.map((definition) => definition.name),
      ["mcp__t3-code__thread_read"],
    );
    assert.deepStrictEqual(toolSet.rejected, [
      { name: "t3 bad/name.x", reason: "name must match ^[a-zA-Z0-9_-]{1,128}$" },
      { name: "t3_bogus_type", reason: "parameters.properties.x.type is not a JSON Schema type" },
      { name: "x".repeat(115), reason: "name must match ^[a-zA-Z0-9_-]{1,128}$" },
      { name: "thread_read", reason: "duplicate name" },
      { name: "echo", reason: "parameters must be an object schema" },
      { name: "tuple", reason: "parameters.properties.pair.items is not a schema" },
      {
        name: "nested",
        reason: "parameters.properties.a.anyOf[0].type is not a JSON Schema type",
      },
      { name: "required", reason: "parameters.required must list property names" },
    ]);
  });
});

describe("ompHostToolResult", () => {
  it("keeps text and image blocks, falls back to structured JSON, and puts isError inside", () => {
    const details = { server: "t3-code", tool: "thread_read" };

    assert.deepStrictEqual(
      ompHostToolResult(
        {
          content: [
            { type: "text", text: "summary" },
            { type: "image", data: "aGk=", mimeType: "image/png" },
            { type: "resource_link", uri: "t3://thread" },
          ],
          structuredContent: { ignored: true },
        },
        "thread_read",
      ),
      {
        content: [
          { type: "text", text: "summary" },
          { type: "image", data: "aGk=", mimeType: "image/png" },
        ],
        details,
      },
    );
    assert.deepStrictEqual(
      ompHostToolResult({ content: [], structuredContent: { id: "t1" } }, "thread_read"),
      { content: [{ type: "text", text: '{"id":"t1"}' }], details },
    );
    assert.deepStrictEqual(
      ompHostToolResult({ content: [{ type: "text", text: "" }], isError: true }, "thread_read"),
      {
        content: [
          { type: "text", text: "" },
          { type: "text", text: "MCP tool t3-code/thread_read returned an error" },
        ],
        details,
        isError: true,
      },
    );
  });
});

describe("loadOmpHostTools", () => {
  it.effect("registers the listed tools without a notice", () =>
    Effect.gen(function* () {
      const server = makeFakeMcpServer({ pages: [{ tools: T3_TOOLS }] });
      const client = yield* clientFor(server.http, Effect.succeed(credential("token-1")));

      const load = yield* loadOmpHostTools(client);

      assert.deepStrictEqual(load, { toolSet: ompHostToolSet(T3_TOOLS), notice: undefined });
    }),
  );

  it.effect("falls back to no tools and one notice when the server is unreachable or slow", () =>
    Effect.gen(function* () {
      const unreachable = HttpClient.make((request) =>
        Effect.fail(
          new HttpClientError.HttpClientError({
            reason: new HttpClientError.TransportError({ request }),
          }),
        ),
      );
      const requested = yield* Deferred.make<void>();
      const slow = HttpClient.make(() =>
        Deferred.succeed(requested, undefined).pipe(Effect.andThen(Effect.never)),
      );
      const fallback = {
        toolSet: EMPTY_OMP_HOST_TOOL_SET,
        notice: OMP_HOST_TOOLS_UNAVAILABLE_NOTICE,
      };

      const refused = yield* loadOmpHostTools(
        yield* clientFor(unreachable, Effect.succeed(credential("token-1"))),
      );
      const pending = yield* loadOmpHostTools(
        yield* clientFor(slow, Effect.succeed(credential("token-1"))),
      ).pipe(Effect.forkChild);
      yield* Deferred.await(requested);
      yield* TestClock.adjust(T3_MCP_CONNECT_TIMEOUT_MS);
      const timedOut = yield* Fiber.join(pending);
      const noSession = yield* loadOmpHostTools(yield* clientFor(unreachable, Effect.undefined));

      assert.deepStrictEqual(refused, fallback);
      assert.deepStrictEqual(timedOut, fallback);
      assert.deepStrictEqual(noSession, { toolSet: EMPTY_OMP_HOST_TOOL_SET, notice: undefined });
    }),
  );
});

type SentFrame = OmpHostToolResultFrame | OmpHostToolUpdateFrame;

const fakeClient = (callTool: T3McpClient["callTool"]): T3McpClient => ({
  listTools: Effect.succeed([]),
  callTool,
});

const makeBridgeHarness = (callTool: T3McpClient["callTool"]) =>
  Effect.gen(function* () {
    const sent = yield* Queue.unbounded<SentFrame>();
    const bridge = yield* makeOmpHostToolBridge({
      client: fakeClient(callTool),
      toolSet: {
        ...EMPTY_OMP_HOST_TOOL_SET,
        mcpNameByHostName: new Map([
          [lookupCall.toolName, "thread_lookup"],
          [slowCall.toolName, "slow"],
        ]),
      },
      send: (frame) => Queue.offer(sent, frame).pipe(Effect.asVoid),
    });
    return { bridge, sent };
  });

describe("makeOmpHostToolBridge", () => {
  it.effect("answers with the MCP result, an MCP failure as isError, and an unknown tool", () =>
    Effect.gen(function* () {
      const { bridge, sent } = yield* makeBridgeHarness((name, args) =>
        name === "thread_lookup" && args.query === "alpha"
          ? Effect.succeed({ content: [{ type: "text", text: "thread t1" }] })
          : Effect.die("unexpected call"),
      );
      const failing = yield* makeBridgeHarness(() =>
        makeT3McpClient(Effect.undefined).pipe(
          Effect.provideService(HttpClient.HttpClient, makeFakeMcpServer().http),
          Effect.flatMap((client) => client.callTool("thread_lookup", {})),
        ),
      );

      yield* bridge.handleCall(lookupCall);
      yield* bridge.handleCall(unregisteredCall);
      yield* failing.bridge.handleCall(lookupCall);

      assert.deepStrictEqual(yield* Queue.clear(sent), [
        {
          type: "host_tool_result",
          id: lookupCall.id,
          result: {
            content: [{ type: "text", text: "thread t1" }],
            details: { server: "t3-code", tool: "thread_lookup" },
          },
        },
        {
          type: "host_tool_result",
          id: unregisteredCall.id,
          result: {
            content: [{ type: "text", text: 'Host tool "t3_discover" is not registered' }],
            details: {},
            isError: true,
          },
        },
      ]);
      assert.deepStrictEqual(yield* Queue.clear(failing.sent), [
        {
          type: "host_tool_result",
          id: lookupCall.id,
          result: {
            content: [
              {
                type: "text",
                text: "T3 Code tool thread_lookup failed. This thread has no T3 MCP session.",
              },
            ],
            details: { server: "t3-code", tool: "thread_lookup" },
            isError: true,
          },
        },
      ]);
    }),
  );

  it.effect("runs calls concurrently, streams progress, and never answers a cancelled call", () =>
    Effect.gen(function* () {
      const interrupted = yield* Deferred.make<ReadonlyArray<string>>();
      const cancelled: Array<string> = [];
      const { bridge, sent } = yield* makeBridgeHarness((name, args, options) =>
        name === "slow"
          ? (
              options?.onProgress?.({ progress: 1, message: `${String(args.label)} started` }) ??
              Effect.void
            ).pipe(
              Effect.andThen(Effect.never),
              Effect.onInterrupt(() =>
                Effect.sync(() => cancelled.push(String(args.label))).pipe(
                  Effect.flatMap((count) =>
                    count === 2 ? Deferred.succeed(interrupted, cancelled) : Effect.void,
                  ),
                ),
              ),
            )
          : Effect.succeed({ content: [{ type: "text", text: "thread t1" }] }),
      );

      yield* bridge.handleCall(slowCall);
      yield* bridge.handleCall(lookupCall);
      yield* bridge.handleCall({ ...slowCall, id: "second", arguments: { label: "y" } });
      const beforeCancel = yield* Queue.clear(sent);
      yield* bridge.handleCancel(slowCancel);
      yield* bridge.handleCancel({ ...slowCancel, targetId: "unknown" });
      yield* bridge.cancelAll;

      assert.deepStrictEqual(yield* Deferred.await(interrupted), ["x", "y"]);
      assert.deepStrictEqual(
        beforeCancel.map((frame) => [frame.type, frame.id]),
        [
          ["host_tool_update", slowCall.id],
          ["host_tool_result", lookupCall.id],
          ["host_tool_update", "second"],
        ],
      );
      assert.deepStrictEqual(beforeCancel[0], {
        type: "host_tool_update",
        id: slowCall.id,
        partialResult: {
          content: [{ type: "text", text: "x started" }],
          details: { server: "t3-code", tool: "slow" },
        },
      });
      assert.deepStrictEqual(yield* Queue.clear(sent), []);
    }),
  );
});
