import { useAtomValue } from "@effect/atom-react";
import {
  collectLimitAccounts,
  collectLimitPools,
  type LimitPool,
} from "@t3tools/shared/usageLimits";
import type { DailyTotals } from "@t3tools/shared/usageMerge";
import { enumerateDays, formatTokens, formatUsd, makeWindow } from "@t3tools/shared/usageFormat";
import { Link } from "@tanstack/react-router";
import { ArrowUpRightIcon } from "lucide-react";
import { useState, type ReactNode } from "react";

import { usePrimarySettings } from "../../hooks/useSettings";
import { environmentPresentations } from "../../state/presentation";
import { useUsage } from "../../state/usage";
import { formatUpcomingTimestamp } from "../../timestampFormat";
import { ProviderInstanceIcon } from "../chat/ProviderInstanceIcon";
import { providerClients } from "../settings/providerDriverMeta";
import {
  DitheredAreaChart,
  DitheredHeatmap,
  DitheredMeter,
  HEATMAP_DAY_PITCH,
} from "./DitheredUsageVisuals";
import { PaceIcon, barColor } from "./UsageLimits";
import { PROVIDER_ORDER, providersWithUsage } from "./usageProviders";
import { buildActivityGrid, tightestLimitWindow, type ActivityGrid } from "./newThreadUsage.logic";

// The Usage page offers the same 90-day window, so both share one cached summary.
const ACTIVITY_DAYS = 90;
const TREND_DAYS = 30;
const WEEKDAY_LABELS = [
  { key: "sun", label: "" },
  { key: "mon", label: "Mon" },
  { key: "tue", label: "" },
  { key: "wed", label: "Wed" },
  { key: "thu", label: "" },
  { key: "fri", label: "Fri" },
  { key: "sat", label: "" },
] as const;
const MONTH_FORMAT = new Intl.DateTimeFormat(undefined, { month: "short", timeZone: "UTC" });

function OverviewCard({
  title,
  aside,
  children,
}: {
  readonly title: ReactNode;
  readonly aside?: ReactNode;
  readonly children: ReactNode;
}) {
  return (
    <section className="flex min-w-0 flex-col gap-1.5 rounded-xl border border-border/60 bg-card/70 px-3 py-2.5">
      <header className="flex items-center justify-between gap-2 text-xs">
        <h2 className="flex min-w-0 items-center gap-1.5 font-medium text-foreground">{title}</h2>
        {aside ? (
          <span className="shrink-0 text-muted-foreground tabular-nums">{aside}</span>
        ) : null}
      </header>
      {children}
    </section>
  );
}

function LimitPoolCard({ pool, now }: { readonly pool: LimitPool; readonly now: number }) {
  const timestampFormat = usePrimarySettings((settings) => settings.timestampFormat);
  const window = tightestLimitWindow(pool);
  if (!window) return null;
  const label = providerClients.get(pool.driver)?.label ?? String(pool.driver);
  const nextRefill = window.resets.find((reset) => reset.restoresPercent > 0);
  return (
    <OverviewCard
      title={
        <>
          <ProviderInstanceIcon
            driverKind={pool.driver}
            displayName={label}
            className="size-4"
            iconClassName="size-3.5 text-foreground/80"
          />
          <span className="truncate">{label}</span>
        </>
      }
      aside={window.label}
    >
      <p className="flex items-baseline gap-1.5">
        <span className="text-xl font-semibold text-foreground tabular-nums">
          {window.remainingPercent}%
        </span>
        <span className="text-xs text-muted-foreground">
          left{pool.accounts.length > 1 ? ` across ${pool.accounts.length} accounts` : ""}
        </span>
        {window.pace ? <PaceIcon pace={window.pace} /> : null}
      </p>
      <DitheredMeter
        percent={window.remainingPercent}
        color={barColor(pool.driver)}
        label={`${window.remainingPercent}% of the ${window.label.toLowerCase()} limit left`}
      />
      <p className="truncate text-xs text-muted-foreground tabular-nums">
        {nextRefill
          ? `+${nextRefill.restoresPercent}% ${formatUpcomingTimestamp(
              new Date(nextRefill.at).toISOString(),
              timestampFormat,
              now,
            )}`
          : "No refill scheduled"}
      </p>
    </OverviewCard>
  );
}

function ActivityHeatmap({ grid }: { readonly grid: ActivityGrid }) {
  return (
    <div className="flex gap-1.5 text-3xs text-muted-foreground">
      <div aria-hidden className="mt-3.5 grid grid-rows-7">
        {WEEKDAY_LABELS.map(({ key, label }) => (
          <span key={key} className="h-3 leading-3">
            {label}
          </span>
        ))}
      </div>
      <div className="min-w-0 flex-1 overflow-hidden">
        <div aria-hidden className="relative h-3.5">
          {grid.monthLabels.map((label) => (
            <span
              key={label.day}
              className="absolute"
              style={{ left: label.week * HEATMAP_DAY_PITCH }}
            >
              {MONTH_FORMAT.format(new Date(`${label.day}T00:00:00Z`))}
            </span>
          ))}
        </div>
        <DitheredHeatmap
          grid={grid}
          label={`${formatTokens(grid.totalTokens)} tokens in the last ${ACTIVITY_DAYS} days`}
        />
      </div>
    </div>
  );
}

function sumCost(days: readonly string[], daily: readonly DailyTotals[]): number {
  const included = new Set(days);
  return daily.reduce((sum, entry) => (included.has(entry.day) ? sum + entry.costUsd : sum), 0);
}

/** Limits, recent usage and activity under the empty composer. */
export function NewThreadUsageOverview() {
  const [now] = useState(() => Date.now());
  // useUsage compares the window by identity, so it must stay stable across renders.
  const [usageWindow] = useState(() => makeWindow(ACTIVITY_DAYS));
  const presentations = useAtomValue(environmentPresentations.presentationsWithProvidersAtom);
  const pools = collectLimitPools(collectLimitAccounts(presentations), now).filter(
    (pool) => tightestLimitWindow(pool) !== undefined,
  );
  const { shown, selectedEnvironments } = useUsage(usageWindow);
  const canReadUsage = selectedEnvironments.some((environment) => environment.canReadDiagnostics);
  const merged = shown?.merged;
  const days = enumerateDays(usageWindow.sinceDay, usageWindow.untilDay);
  const trendDays = days.slice(-TREND_DAYS);
  const providersWithData = new Set(merged ? providersWithUsage(merged.providers) : []);
  const providers = PROVIDER_ORDER.filter((provider) => providersWithData.has(provider));
  const showUsage = canReadUsage && merged !== undefined && merged.totalTokens > 0;

  if (pools.length === 0 && !showUsage) return null;

  return (
    <div className="flex flex-col gap-2">
      {pools.length > 0 ? (
        <div className="grid grid-cols-2 gap-2">
          {pools.map((pool) => (
            <LimitPoolCard key={pool.driver} pool={pool} now={now} />
          ))}
        </div>
      ) : null}
      {showUsage ? (
        <div className="grid grid-cols-2 gap-2">
          <OverviewCard
            title={
              <Link
                to="/usage"
                className="inline-flex items-center gap-0.5 transition-colors hover:text-muted-foreground"
              >
                Usage
                <ArrowUpRightIcon className="size-3" aria-hidden />
              </Link>
            }
            aside={`${formatUsd(sumCost(trendDays, merged.daily))} · ${TREND_DAYS} days`}
          >
            <DitheredAreaChart providers={providers} days={trendDays} daily={merged.daily} />
          </OverviewCard>
          <ActivityCard days={days} daily={merged.daily} />
        </div>
      ) : null}
    </div>
  );
}

function ActivityCard({
  days,
  daily,
}: {
  readonly days: readonly string[];
  readonly daily: readonly DailyTotals[];
}) {
  const grid = buildActivityGrid(days, daily);
  return (
    <OverviewCard title="Activity" aside={`${formatTokens(grid.totalTokens)} tokens`}>
      <ActivityHeatmap grid={grid} />
    </OverviewCard>
  );
}
