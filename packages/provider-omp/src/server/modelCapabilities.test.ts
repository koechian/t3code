import { describe, expect, it } from "vite-plus/test";

import picker from "./__fixtures__/modelCapabilities/model-picker.json" with { type: "json" };
import thinkingLevels from "./__fixtures__/modelCapabilities/thinking-levels.json" with { type: "json" };
import {
  EMPTY_OMP_MODEL_CAPABILITIES,
  ompServerModels,
  resolveOmpModelSlug,
  validateOmpThinkingLevel,
} from "./modelCapabilities.ts";

describe("ompServerModels", () => {
  const models = ompServerModels(picker.rows, [
    "custom/model",
    { slug: "anthropic/claude-opus-5-5", name: "Opus (team)" },
  ]);

  it("lists the default first, then each picker row once, then custom models", () => {
    expect(models.map((model) => [model.slug, model.name, model.isCustom])).toEqual([
      ["default", "OMP default", false],
      ["anthropic/claude-haiku-5-5", "Claude Haiku 5.5", false],
      ["anthropic/claude-opus-5-5", "Opus (team)", false],
      ["openrouter/aion-labs/aion-2.0", "Aion-2.0", false],
      [
        "amazon-bedrock/us.meta.llama4-scout-17b-instruct-v1:0",
        "Llama 4 Scout 17B Instruct",
        false,
      ],
      ["custom/model", "custom/model", true],
    ]);
  });

  it("offers each row's thinking steps with omp's current level as the default", () => {
    const capabilities = (slug: string) =>
      models.find((model) => model.slug === slug)?.capabilities;
    expect(capabilities("anthropic/claude-haiku-5-5")).toEqual({
      optionDescriptors: [
        {
          id: "thinking",
          label: "Thinking",
          type: "select",
          options: [
            { id: "auto", label: "Auto" },
            { id: "off", label: "Off", isDefault: true },
            { id: "low", label: "Low" },
            { id: "medium", label: "Medium" },
            { id: "high", label: "High" },
            { id: "xhigh", label: "Extra High" },
            { id: "max", label: "Max" },
          ],
        },
      ],
    });
    expect(capabilities("default")).toEqual(EMPTY_OMP_MODEL_CAPABILITIES);
    expect(capabilities("amazon-bedrock/us.meta.llama4-scout-17b-instruct-v1:0")).toEqual(
      EMPTY_OMP_MODEL_CAPABILITIES,
    );
  });
});

describe("resolveOmpModelSlug", () => {
  it.each([
    [
      "a picker row whose id contains a slash",
      "openrouter/aion-labs/aion-2.0",
      { provider: "openrouter", modelId: "aion-labs/aion-2.0" },
    ],
    [
      "an unlisted slug, split at the first slash",
      "local/qwen3/q4",
      { provider: "local", modelId: "qwen3/q4" },
    ],
    ["the default sentinel", "default", null],
    ["a slug without a provider", "/qwen3", null],
    ["a slug without a model", "local/", null],
    ["a slug without a slash", "qwen3", null],
  ])("resolves %s", (_label, slug, resolved) => {
    expect(resolveOmpModelSlug(slug, picker.rows)).toEqual(resolved);
  });
});

describe("validateOmpThinkingLevel", () => {
  it.each([
    ["an offered level", "high", thinkingLevels.levels, "high"],
    ["auto, which omp leaves out of the list", "auto", thinkingLevels.levels, "auto"],
    ["auto for a model without efforts", "auto", ["off"], undefined],
    ["a level this model lacks", "minimal", thinkingLevels.levels, undefined],
    ["omp's inherit selector", "inherit", thinkingLevels.levels, undefined],
    ["an unknown level", "bogus", thinkingLevels.levels, undefined],
    ["no selection", undefined, thinkingLevels.levels, undefined],
  ])("accepts or drops %s", (_label, requested, available, validated) => {
    expect(validateOmpThinkingLevel(requested, available)).toBe(validated);
  });
});
