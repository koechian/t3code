// local-build: product analytics is opt-in. These tests pin that nothing is recorded or sent
// unless T3CODE_TELEMETRY_ENABLED=true is set.
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as HostProcess from "@t3tools/shared/HostProcess";

import * as ServerConfig from "../config.ts";
import * as ManagedPolicy from "../policy/ManagedPolicy.ts";
import * as AnalyticsService from "./AnalyticsService.ts";

const recordAndFlush = (config: Record<string, unknown>) =>
  Effect.gen(function* () {
    let sends = 0;
    const client = HttpClient.make((request) =>
      Effect.sync(() => {
        sends += 1;
        return HttpClientResponse.fromWeb(request, new Response(null, { status: 204 }));
      }),
    );
    const layer = AnalyticsService.layer.pipe(
      Layer.provideMerge(
        ServerConfig.ServerConfig.layerTest(process.cwd(), { prefix: "t3-telemetry-opt-in-" }),
      ),
      Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(config))),
      Layer.provide(
        Layer.mergeAll(
          Layer.succeed(HostProcess.Platform, "darwin"),
          Layer.succeed(HostProcess.Architecture, "arm64"),
          Layer.succeed(HostProcess.Environment, {}),
          Layer.succeed(HttpClient.HttpClient, client),
          ManagedPolicy.layerTest(),
        ),
      ),
    );
    const recorded = yield* Effect.gen(function* () {
      const analytics = yield* AnalyticsService.AnalyticsService;
      const queued = yield* analytics.record("test.opt-in");
      yield* analytics.recordOwners([{ source: "device", ownerId: "a".repeat(64) }], "server");
      yield* analytics.flush;
      return queued;
    }).pipe(Effect.provide(layer));
    return { recorded, sends };
  });

it.layer(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))("analytics opt-in", (it) => {
  it.effect("records and sends nothing when T3CODE_TELEMETRY_ENABLED is unset", () =>
    Effect.gen(function* () {
      const result = yield* recordAndFlush({ T3CODE_POSTHOG_HOST: "http://localhost" });
      assert.deepEqual(result, { recorded: false, sends: 0 });
    }),
  );

  it.effect("sends only after an explicit T3CODE_TELEMETRY_ENABLED=true", () =>
    Effect.gen(function* () {
      const result = yield* recordAndFlush({
        T3CODE_TELEMETRY_ENABLED: true,
        T3CODE_POSTHOG_KEY: "phc_test_key",
        T3CODE_POSTHOG_HOST: "http://localhost",
      });
      assert.isTrue(result.recorded);
      assert.isAbove(result.sends, 0);
    }),
  );
});
