import {
  displayLimitWindows,
  type LimitPool,
  type LimitPoolWindow,
} from "@t3tools/shared/usageLimits";
import type { DailyTotals } from "@t3tools/shared/usageMerge";

const DAYS_PER_WEEK = 7;
// A month label needs room to render before the next one starts.
const MIN_WEEKS_BETWEEN_MONTH_LABELS = 3;
const LEVEL_QUANTILES = [0.25, 0.5, 0.75] as const;

export interface ActivityCell {
  readonly day: string;
  /** False for the days that pad the first and last week out to a full week. */
  readonly inWindow: boolean;
  readonly totalTokens: number;
  /** 0 for no activity, then 1 to 4 by quartile of active days. */
  readonly level: number;
}

/** A week runs Sunday to Saturday. */
export type ActivityWeek = readonly ActivityCell[];

export interface ActivityGrid {
  readonly weeks: readonly ActivityWeek[];
  readonly monthLabels: ReadonlyArray<{ readonly week: number; readonly day: string }>;
  readonly totalTokens: number;
}

const DAY_MS = 86_400_000;

function weekdayOf(day: string): number {
  return new Date(`${day}T00:00:00Z`).getUTCDay();
}

function addDays(day: string, count: number): string {
  return new Date(Date.parse(`${day}T00:00:00Z`) + count * DAY_MS).toISOString().slice(0, 10);
}

function paddingCells(from: string, count: number): ActivityCell[] {
  return Array.from({ length: count }, (_, offset) => ({
    day: addDays(from, offset),
    inWindow: false,
    totalTokens: 0,
    level: 0,
  }));
}

/** Quartiles of active days, so one heavy day does not wash out the rest. */
function levelThresholds(values: readonly number[]): readonly number[] {
  const active = values.filter((value) => value > 0).toSorted((left, right) => left - right);
  if (active.length === 0) return [];
  return LEVEL_QUANTILES.map(
    (quantile) => active[Math.max(0, Math.ceil(quantile * active.length) - 1)]!,
  );
}

function levelOf(value: number, thresholds: readonly number[]): number {
  if (value <= 0) return 0;
  return 1 + thresholds.filter((threshold) => value > threshold).length;
}

function monthLabelsOf(weeks: readonly ActivityWeek[]): ActivityGrid["monthLabels"] {
  const labels: { week: number; day: string }[] = [];
  let previousMonth: string | null = null;
  weeks.forEach((week, index) => {
    const first = week.find((cell) => cell.inWindow);
    const month = first?.day.slice(0, 7);
    if (!first || month === previousMonth) return;
    previousMonth = month ?? null;
    const previous = labels.at(-1);
    if (previous && index - previous.week < MIN_WEEKS_BETWEEN_MONTH_LABELS) labels.pop();
    labels.push({ week: index, day: first.day });
  });
  return labels;
}

/** Lays consecutive days out as calendar weeks for the activity heatmap. */
export function buildActivityGrid(
  days: readonly string[],
  daily: readonly DailyTotals[],
): ActivityGrid {
  const tokensByDay = new Map(daily.map((entry) => [entry.day, entry.totalTokens]));
  const values = days.map((day) => tokensByDay.get(day) ?? 0);
  const thresholds = levelThresholds(values);
  const first = days[0];
  const last = days.at(-1);
  if (first === undefined || last === undefined)
    return { weeks: [], monthLabels: [], totalTokens: 0 };
  const leading = weekdayOf(first);
  const trailing = DAYS_PER_WEEK - 1 - weekdayOf(last);
  const cells = [
    ...paddingCells(addDays(first, -leading), leading),
    ...days.map((day, index) => {
      const totalTokens = values[index] ?? 0;
      return { day, inWindow: true, totalTokens, level: levelOf(totalTokens, thresholds) };
    }),
    ...paddingCells(addDays(last, 1), trailing),
  ];
  const weeks: ActivityWeek[] = [];
  for (let start = 0; start < cells.length; start += DAYS_PER_WEEK) {
    weeks.push(cells.slice(start, start + DAYS_PER_WEEK));
  }
  return {
    weeks,
    monthLabels: monthLabelsOf(weeks),
    totalTokens: values.reduce((sum, value) => sum + value, 0),
  };
}

/** The window closest to running out decides when the pool stops. */
export function tightestLimitWindow(pool: LimitPool): LimitPoolWindow | undefined {
  return displayLimitWindows(pool).reduce<LimitPoolWindow | undefined>(
    (tightest, window) =>
      tightest === undefined || window.remainingPercent < tightest.remainingPercent
        ? window
        : tightest,
    undefined,
  );
}
