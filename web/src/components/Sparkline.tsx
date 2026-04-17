/** 최근 종가 스파크라인 (KIS closes 배열) */
export function Sparkline({
  closes,
  changeRate,
}: {
  closes: number[];
  changeRate: number;
}) {
  const series =
    !closes || closes.length === 0
      ? []
      : closes.length === 1
        ? [closes[0], closes[0]]
        : closes;
  if (!series || series.length < 2) {
    return (
      <div className="flex h-[60px] items-center justify-center text-xs text-slate-600">
        데이터 없음
      </div>
    );
  }
  const w = 260;
  const h = 55;
  const pad = 2;
  const mn = Math.min(...series);
  const mx = Math.max(...series);
  const range = mx - mn || 1;
  const pts = series.map((v, i) => {
    const x = pad + (i / (series.length - 1)) * (w - pad * 2);
    const y = pad + (1 - (v - mn) / range) * (h - pad * 2);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  });
  const stroke = changeRate >= 0 ? "#22c55e" : "#ef4444";
  const fill =
    changeRate >= 0 ? "rgba(34,197,94,.12)" : "rgba(239,68,68,.12)";
  const lastPt = pts[pts.length - 1]!;
  const areaPath = `M${pts[0]} L${pts.join(" L")} L${w - pad},${h} L${pad},${h} Z`;
  return (
    <svg
      viewBox={`0 0 ${w} ${h}`}
      preserveAspectRatio="none"
      className="h-full w-full block"
    >
      <path d={areaPath} fill={fill} />
      <polyline
        points={pts.join(" ")}
        fill="none"
        stroke={stroke}
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <circle
        cx={lastPt.split(",")[0]}
        cy={lastPt.split(",")[1]}
        r="2.5"
        fill={stroke}
      />
    </svg>
  );
}
