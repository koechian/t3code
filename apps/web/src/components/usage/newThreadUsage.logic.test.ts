import type { DailyTotals } from "@t3tools/shared/usageMerge";
import { describe, expect, it } from "vite-plus/test";

import { buildActivityGrid } from "./newThreadUsage.logic";

function daysFrom(start: string, count: number): string[] {
  const first = Date.parse(`${start}T00:00:00Z`);
  return Array.from({ length: count }, (_, index) =>
    new Date(first + index * 86_400_000).toISOString().slice(0, 10),
  );
}

function totals(entries: Record<string, number>): DailyTotals[] {
  return Object.entries(entries).map(([day, totalTokens]) => ({
    day,
    costUsd: 0,
    totalTokens,
    byProvider: new Map(),
  }));
}

describe("buildActivityGrid", () => {
  it("aligns days to Sunday-first weeks and pads both ends", () => {
    // 2026-11-04 is a Wednesday; ten days end on Friday 2026-11-13.
    const grid = buildActivityGrid(daysFrom("2026-11-04", 10), []);

    expect(grid.weeks).toHaveLength(2);
    expect(grid.weeks.every((week) => week.length === 7)).toBe(true);
    expect(grid.weeks[0]?.map((cell) => cell.inWindow)).toEqual([
      false,
      false,
      false,
      true,
      true,
      true,
      true,
    ]);
    expect(grid.weeks[0]?.[0]?.day).toBe("2026-11-01");
    expect(grid.weeks[0]?.[3]?.day).toBe("2026-11-04");
    expect(grid.weeks[1]?.[5]?.day).toBe("2026-11-13");
    expect(grid.weeks[1]?.[6]).toMatchObject({ day: "2026-11-14", inWindow: false });
  });

  it("ranks active days by quartile and leaves idle days at zero", () => {
    const days = daysFrom("2026-11-01", 5);
    const grid = buildActivityGrid(
      days,
      totals({ "2026-11-01": 10, "2026-11-02": 20, "2026-11-03": 30, "2026-11-04": 4_000 }),
    );

    expect(grid.weeks[0]?.map((cell) => cell.level)).toEqual([1, 2, 3, 4, 0, 0, 0]);
    expect(grid.totalTokens).toBe(4_060);
  });

  it("drops a month label that would crowd the next one", () => {
    // Starts on Sunday 2026-08-30: the first week belongs to August, September follows a week later.
    const grid = buildActivityGrid(daysFrom("2026-08-30", 49), []);

    expect(grid.monthLabels.map((label) => label.day.slice(0, 7))).toEqual(["2026-09", "2026-10"]);
  });
});
