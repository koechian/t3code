/**
 * `get_model_picker` rows → T3 models and the `thinking` option, plus model-slug and
 * thinking-level resolution for `set_model` / `set_thinking_level`.
 */
import type {
  CustomModelSetting,
  ModelCapabilities,
  ProviderOptionChoice,
  ServerProviderModel,
} from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import { providerModelsFromSettings } from "@t3tools/provider-core/server/snapshotProbe";

import type { OmpModelPickerRow, OmpThinkingSelector } from "./wire.ts";

/** Synthetic model: run omp's own configured default; never sent as a model id. */
export const OMP_DEFAULT_MODEL_SLUG = "default";

/** Picker rows fetched per probe; `get_available_models` (1.3 MB) is never used on hot paths. */
export const OMP_MODEL_PICKER_LIMIT = 200;

/** Selectors T3 may send, low to high; `auto` lets omp pick the effort per turn. */
const OMP_THINKING_LEVELS = [
  "auto",
  "off",
  "minimal",
  "low",
  "medium",
  "high",
  "xhigh",
  "max",
] as const satisfies ReadonlyArray<OmpThinkingSelector>;

export type OmpThinkingLevel = (typeof OMP_THINKING_LEVELS)[number];

const OMP_THINKING_LEVEL_LABELS: Record<OmpThinkingLevel, string> = {
  auto: "Auto",
  off: "Off",
  minimal: "Minimal",
  low: "Low",
  medium: "Medium",
  high: "High",
  xhigh: "Extra High",
  max: "Max",
};

export const EMPTY_OMP_MODEL_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

const OMP_DEFAULT_MODEL: ServerProviderModel = {
  slug: OMP_DEFAULT_MODEL_SLUG,
  name: "OMP default",
  isCustom: false,
  capabilities: EMPTY_OMP_MODEL_CAPABILITIES,
};

function isOmpThinkingLevel(value: string): value is OmpThinkingLevel {
  return OMP_THINKING_LEVELS.some((level) => level === value);
}

export function ompModelSlug(row: Pick<OmpModelPickerRow, "provider" | "id">): string {
  return `${row.provider}/${row.id}`;
}

/**
 * A `thinking` select (option id kept from Pi for handoff) from the row's slider steps. The row's
 * `thinkingLevel` is the level omp would run this model at, so it is the default choice.
 */
function ompModelCapabilities(row: OmpModelPickerRow): ModelCapabilities {
  const levels = [...new Set(row.thinking ?? [])].filter(isOmpThinkingLevel);
  if (levels.length === 0) return EMPTY_OMP_MODEL_CAPABILITIES;
  return createModelCapabilities({
    optionDescriptors: [
      {
        id: "thinking",
        label: "Thinking",
        type: "select",
        options: levels.map((level): ProviderOptionChoice => ({
          id: level,
          label: OMP_THINKING_LEVEL_LABELS[level],
          ...(level === row.thinkingLevel ? { isDefault: true } : {}),
        })),
      },
    ],
  });
}

/** Default model first, then picker rows (deduplicated by slug), then custom models. */
export function ompServerModels(
  rows: ReadonlyArray<OmpModelPickerRow>,
  customModels: ReadonlyArray<CustomModelSetting>,
): ReadonlyArray<ServerProviderModel> {
  const seen = new Set<string>([OMP_DEFAULT_MODEL_SLUG]);
  const models: Array<ServerProviderModel> = [OMP_DEFAULT_MODEL];
  for (const row of rows) {
    const slug = ompModelSlug(row);
    if (!hasModelIdentity(row) || seen.has(slug)) continue;
    seen.add(slug);
    models.push({
      slug,
      name: row.name.trim() || slug,
      subProvider: row.provider,
      isCustom: false,
      capabilities: ompModelCapabilities(row),
    });
  }
  return providerModelsFromSettings(models, customModels, EMPTY_OMP_MODEL_CAPABILITIES);
}

function hasModelIdentity(row: Pick<OmpModelPickerRow, "provider" | "id">): boolean {
  return row.provider.trim().length > 0 && row.id.trim().length > 0;
}

/** The picker row whose `provider/id` is exactly `slug`. */
export function findOmpPickerModel(
  slug: string,
  rows: ReadonlyArray<OmpModelPickerRow>,
): OmpModelPickerRow | undefined {
  return rows.find((row) => ompModelSlug(row) === slug);
}

/** Provider names and ids may both contain "/", so prefer an exact picker-row match. */
export function resolveOmpModelSlug(
  slug: string,
  rows: ReadonlyArray<OmpModelPickerRow>,
): { readonly provider: string; readonly modelId: string } | null {
  if (slug === OMP_DEFAULT_MODEL_SLUG) return null;
  const row = findOmpPickerModel(slug, rows);
  if (row !== undefined) return { provider: row.provider, modelId: row.id };
  const separator = slug.indexOf("/");
  if (separator <= 0 || separator === slug.length - 1) return null;
  return { provider: slug.slice(0, separator), modelId: slug.slice(separator + 1) };
}

/**
 * omp accepts any string and silently clears the level, so validate against
 * `get_available_thinking_levels` plus `auto`. Undefined means "do not send".
 * `auto` needs an effort to choose from, so a model offering only `off` rejects it.
 */
export function validateOmpThinkingLevel(
  requested: string | undefined,
  availableLevels: ReadonlyArray<string>,
): OmpThinkingLevel | undefined {
  if (requested === undefined || !isOmpThinkingLevel(requested)) return undefined;
  if (requested === "auto") {
    return availableLevels.some((level) => level !== "off") ? requested : undefined;
  }
  return availableLevels.includes(requested) ? requested : undefined;
}
