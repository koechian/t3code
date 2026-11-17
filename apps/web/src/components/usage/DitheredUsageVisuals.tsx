import type { UsageProviderKind } from "@t3tools/contracts";
import { formatDayShort, formatTokens } from "@t3tools/shared/usageFormat";
import type { DailyTotals } from "@t3tools/shared/usageMerge";

import { DitherCanvas, type DotGrid } from "./DitherCanvas";
import { isLit, rasterizeStackedArea } from "./ditherRaster";
import type { ActivityGrid } from "./newThreadUsage.logic";
import { PROVIDER_PRESENTATION } from "./usageProviders";

const AXIS_COLOR = "var(--border)";
const IDLE_COLOR = "var(--muted-foreground)";
const HEAT_COLOR = "var(--primary)";

// Chart and meter dots: 3px pitch with a 1px gap.
const CHART_DOT = 3;
// Heatmap: each day is a 5x5 block of 2px dots with one empty dot between days.
const HEAT_DOT = 2;
const HEAT_DAY_DOTS = 5;
export const HEATMAP_DAY_PITCH = (HEAT_DAY_DOTS + 1) * HEAT_DOT;
const HEAT_LEVEL_INTENSITY = [0.2, 0.35, 0.55, 0.8, 1] as const;
const METER_FILLED_INTENSITY = 0.95;
const METER_EMPTY_INTENSITY = 0.2;

export function DitheredAreaChart({
  providers,
  days,
  daily,
}: {
  readonly providers: readonly UsageProviderKind[];
  readonly days: readonly string[];
  readonly daily: readonly DailyTotals[];
}) {
  const byDay = new Map(daily.map((entry) => [entry.day, entry]));
  // A provider can have usage in the wider window but none in these days; leave it out.
  const totals = providers
    .map((provider) => {
      const values = days.map((day) => byDay.get(day)?.byProvider.get(provider)?.totalTokens ?? 0);
      return { provider, values, totalTokens: values.reduce((sum, value) => sum + value, 0) };
    })
    .filter((entry) => entry.totalTokens > 0);
  const series = totals.map((entry) => entry.values);
  const paint = (grid: DotGrid) => {
    // The bottom row is the axis; the areas fill the rows above it.
    const rows = grid.rows - 1;
    const cells = rasterizeStackedArea({ columns: grid.columns, rows, series });
    for (let column = 0; column < grid.columns; column += 1) {
      grid.dot(column, rows, AXIS_COLOR);
      for (let row = 0; row < rows; row += 1) {
        const entry = totals[(cells[column * rows + row] ?? 0) - 1];
        if (entry) grid.dot(column, row, PROVIDER_PRESENTATION[entry.provider].color);
      }
    }
  };
  const middleDay = days[Math.floor(days.length / 2)];
  return (
    <div className="flex flex-col gap-1">
      <DitherCanvas
        cellSize={CHART_DOT}
        className="h-14 w-full"
        label={`Daily tokens: ${totals
          .map(
            ({ provider, totalTokens }) =>
              `${PROVIDER_PRESENTATION[provider].label} ${formatTokens(totalTokens)}`,
          )
          .join(", ")}`}
        paint={paint}
      />
      <div aria-hidden className="flex justify-between text-3xs text-muted-foreground uppercase">
        <span>{days[0] ? formatDayShort(days[0]) : ""}</span>
        <span>{middleDay ? formatDayShort(middleDay) : ""}</span>
        <span>{days.at(-1) ? formatDayShort(days.at(-1)!) : ""}</span>
      </div>
      <ul aria-hidden className="flex flex-wrap gap-x-3 text-3xs text-muted-foreground">
        {totals.map(({ provider, totalTokens }) => (
          <li key={provider} className="flex items-center gap-1">
            <span
              className="size-1.5"
              style={{ backgroundColor: PROVIDER_PRESENTATION[provider].color }}
            />
            {PROVIDER_PRESENTATION[provider].label}
            <span className="tabular-nums">{formatTokens(totalTokens)}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}

function paintHeatDay(grid: DotGrid, week: number, weekday: number, level: number): void {
  const color = level === 0 ? IDLE_COLOR : HEAT_COLOR;
  const intensity = HEAT_LEVEL_INTENSITY[level] ?? 1;
  for (let x = 0; x < HEAT_DAY_DOTS; x += 1) {
    for (let y = 0; y < HEAT_DAY_DOTS; y += 1) {
      const column = week * (HEAT_DAY_DOTS + 1) + x;
      const row = weekday * (HEAT_DAY_DOTS + 1) + y;
      if (isLit(intensity, column, row)) grid.dot(column, row, color);
    }
  }
}

export function DitheredHeatmap({
  grid: activity,
  label,
}: {
  readonly grid: ActivityGrid;
  readonly label: string;
}) {
  const paint = (grid: DotGrid) => {
    activity.weeks.forEach((week, weekIndex) =>
      week.forEach((cell, weekday) => {
        if (cell.inWindow) paintHeatDay(grid, weekIndex, weekday, cell.level);
      }),
    );
  };
  return (
    <DitherCanvas
      cellSize={HEAT_DOT}
      dotGap={0}
      label={label}
      paint={paint}
      style={{
        width: activity.weeks.length * HEATMAP_DAY_PITCH,
        height: 7 * HEATMAP_DAY_PITCH,
      }}
    />
  );
}

export function DitheredMeter({
  percent,
  color,
  label,
}: {
  readonly percent: number;
  readonly color: string;
  readonly label: string;
}) {
  const paint = (grid: DotGrid) => {
    const filledColumns = Math.round((percent / 100) * grid.columns);
    for (let column = 0; column < grid.columns; column += 1) {
      const filled = column < filledColumns;
      const intensity = filled ? METER_FILLED_INTENSITY : METER_EMPTY_INTENSITY;
      for (let row = 0; row < grid.rows; row += 1) {
        if (isLit(intensity, column, row)) grid.dot(column, row, filled ? color : IDLE_COLOR);
      }
    }
  };
  return (
    <DitherCanvas cellSize={CHART_DOT} className="h-2.25 w-full" label={label} paint={paint} />
  );
}
