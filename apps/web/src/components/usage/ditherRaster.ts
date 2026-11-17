// Ordered (Bayer) dithering on a dot grid: a cell is lit when its intensity beats the cell's
// threshold, so a flat intensity becomes an even dot pattern instead of a flat tint.
const BAYER_4X4 = [0, 8, 2, 10, 12, 4, 14, 6, 3, 11, 1, 9, 15, 7, 13, 5] as const;
const BAYER_SIZE = 4;

/** Threshold in (0, 1) for a grid cell. */
export function bayerThreshold(column: number, row: number): number {
  const index = (row % BAYER_SIZE) * BAYER_SIZE + (column % BAYER_SIZE);
  return (BAYER_4X4[index]! + 0.5) / BAYER_4X4.length;
}

export function isLit(intensity: number, column: number, row: number): boolean {
  return intensity > bayerThreshold(column, row);
}

// Area fill fades from the edge line toward the baseline.
const EDGE_INTENSITY = 1;
const FILL_TOP_INTENSITY = 0.7;
const FILL_BOTTOM_INTENSITY = 0.15;

export interface AreaRasterInput {
  readonly columns: number;
  readonly rows: number;
  /** One value per period for each series, drawn bottom to top in this order. */
  readonly series: ReadonlyArray<readonly number[]>;
}

/**
 * Rasterizes stacked areas into a column-major grid. Each cell holds the series index plus one,
 * or 0 when unlit, so a renderer only needs the series colours.
 */
export function rasterizeStackedArea({ columns, rows, series }: AreaRasterInput): Uint8Array {
  const cells = new Uint8Array(columns * rows);
  const periods = series[0]?.length ?? 0;
  if (columns === 0 || rows < 2 || periods === 0) return cells;
  const sampled = series.map((values) => sampleColumns(values, columns));
  const max = Math.max(0, ...sampled[0]!.map((_, column) => stackTotal(sampled, column)));
  if (max === 0) return cells;
  const rowOf = (value: number) => rows - 1 - Math.round((value / max) * (rows - 1));
  for (let column = 0; column < columns; column += 1) {
    let lower = 0;
    sampled.forEach((values, seriesIndex) => {
      const upper = lower + (values[column] ?? 0);
      // A band stops one row above the band below so it never paints over that band's edge.
      const baseRow = lower > 0 ? rowOf(lower) - 1 : rows - 1;
      if (upper > lower && rowOf(upper) <= baseRow) {
        fillBand(cells, rows, column, rowOf(upper), baseRow, seriesIndex + 1);
      }
      lower = upper;
    });
  }
  return cells;
}

function stackTotal(sampled: ReadonlyArray<readonly number[]>, column: number): number {
  return sampled.reduce((sum, values) => sum + (values[column] ?? 0), 0);
}

/** Linear interpolation of period values onto grid columns. */
function sampleColumns(values: readonly number[], columns: number): number[] {
  if (values.length === 1 || columns === 1)
    return Array.from({ length: columns }, () => values[0]!);
  return Array.from({ length: columns }, (_, column) => {
    const position = (column / (columns - 1)) * (values.length - 1);
    const left = Math.floor(position);
    const right = Math.min(values.length - 1, left + 1);
    const weight = position - left;
    return values[left]! * (1 - weight) + values[right]! * weight;
  });
}

function fillBand(
  cells: Uint8Array,
  rows: number,
  column: number,
  topRow: number,
  baseRow: number,
  value: number,
): void {
  const span = Math.max(1, rows - 1 - topRow);
  for (let row = topRow; row <= baseRow; row += 1) {
    const depth = (row - topRow) / span;
    const intensity =
      row === topRow
        ? EDGE_INTENSITY
        : FILL_TOP_INTENSITY - (FILL_TOP_INTENSITY - FILL_BOTTOM_INTENSITY) * depth;
    if (isLit(intensity, column, row)) cells[column * rows + row] = value;
  }
}
