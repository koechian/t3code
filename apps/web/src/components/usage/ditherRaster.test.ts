import { describe, expect, it } from "vite-plus/test";

import { bayerThreshold, rasterizeStackedArea } from "./ditherRaster";

function columnOf(cells: Uint8Array, rows: number, column: number): number[] {
  return Array.from(cells.subarray(column * rows, (column + 1) * rows));
}

describe("bayerThreshold", () => {
  it("spreads one threshold per cell of each 4x4 tile", () => {
    const tile = Array.from({ length: 16 }, (_, index) => bayerThreshold(index % 4, index >> 2));
    expect(new Set(tile).size).toBe(16);
    expect(Math.min(...tile)).toBeGreaterThan(0);
    expect(Math.max(...tile)).toBeLessThan(1);
    expect(bayerThreshold(5, 6)).toBe(bayerThreshold(1, 2));
  });
});

describe("rasterizeStackedArea", () => {
  it("draws the peak as a solid edge at the top row and leaves empty columns blank", () => {
    const rows = 8;
    const cells = rasterizeStackedArea({ columns: 3, rows, series: [[0, 10, 0]] });

    expect(columnOf(cells, rows, 1)[0]).toBe(1);
    expect(columnOf(cells, rows, 0).every((cell) => cell === 0)).toBe(true);
    expect(columnOf(cells, rows, 2).every((cell) => cell === 0)).toBe(true);
  });

  it("stacks later series above earlier ones without overwriting their edge", () => {
    const rows = 9;
    const cells = rasterizeStackedArea({
      columns: 1,
      rows,
      series: [[5], [5]],
    });
    const column = columnOf(cells, rows, 0);

    // Total 10 reaches the top row; the first series' edge sits halfway down.
    expect(column[0]).toBe(2);
    expect(column[4]).toBe(1);
    expect(column.slice(0, 4).every((cell) => cell === 0 || cell === 2)).toBe(true);
    expect(column.slice(4).every((cell) => cell === 0 || cell === 1)).toBe(true);
  });

  it("returns an empty grid when every value is zero", () => {
    const cells = rasterizeStackedArea({ columns: 4, rows: 4, series: [[0, 0]] });
    expect(cells.every((cell) => cell === 0)).toBe(true);
  });
});
