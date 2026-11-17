import { useEffectEvent, useLayoutEffect, useRef } from "react";

import { observeResize } from "../../lib/observeResize";

export interface DotGrid {
  readonly columns: number;
  readonly rows: number;
  /** Paints one dot. The colour may be any CSS colour, including var() and color-mix(). */
  dot(column: number, row: number, color: string): void;
}

/** Canvas cannot read CSS variables, so theme colours resolve through a probe element. */
function makeColorResolver(host: HTMLElement) {
  const resolved = new Map<string, string>();
  let probe: HTMLSpanElement | null = null;
  return {
    resolve(color: string): string {
      if (!color.includes("(")) return color;
      const known = resolved.get(color);
      if (known !== undefined) return known;
      probe ??= host.appendChild(document.createElement("span"));
      probe.style.color = color;
      const value = getComputedStyle(probe).color;
      resolved.set(color, value);
      return value;
    },
    dispose(): void {
      probe?.remove();
    },
  };
}

/**
 * A dot-matrix canvas: the paint callback lights cells of a grid sized from the element's CSS box.
 * It repaints on every render and on resize, and stays crisp at any device pixel ratio.
 */
export function DitherCanvas({
  cellSize,
  dotGap = 1,
  className,
  style,
  label,
  paint,
}: {
  readonly cellSize: number;
  readonly dotGap?: number;
  readonly className?: string;
  readonly style?: React.CSSProperties;
  readonly label: string;
  readonly paint: (grid: DotGrid) => void;
}) {
  const hostRef = useRef<HTMLDivElement | null>(null);
  const canvasRef = useRef<HTMLCanvasElement | null>(null);

  const draw = useEffectEvent(() => {
    const host = hostRef.current;
    const canvas = canvasRef.current;
    const context = canvas?.getContext("2d");
    if (!host || !canvas || !context) return;
    const { width, height } = host.getBoundingClientRect();
    const ratio = window.devicePixelRatio || 1;
    canvas.width = Math.round(width * ratio);
    canvas.height = Math.round(height * ratio);
    context.clearRect(0, 0, canvas.width, canvas.height);
    const colors = makeColorResolver(host);
    const pitch = cellSize * ratio;
    const size = Math.max(1, (cellSize - dotGap) * ratio);
    paint({
      columns: Math.floor(width / cellSize),
      rows: Math.floor(height / cellSize),
      dot: (column, row, color) => {
        context.fillStyle = colors.resolve(color);
        context.fillRect(Math.round(column * pitch), Math.round(row * pitch), size, size);
      },
    });
    colors.dispose();
  });

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- repaints the canvas after each render because the paint callback closes over new data
  useLayoutEffect(() => {
    draw();
  });

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- repaints the canvas when its CSS box resizes
  useLayoutEffect(() => {
    const host = hostRef.current;
    if (!host) return;
    return observeResize(host, () => draw());
  }, []);

  return (
    <div ref={hostRef} role="img" aria-label={label} className={className} style={style}>
      <canvas ref={canvasRef} aria-hidden className="block size-full" />
    </div>
  );
}
