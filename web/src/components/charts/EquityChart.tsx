import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts";
import type { TradeRow } from "../../types";
import { formatKstShort } from "../../utils/formatKst";

function buildSeries(trades: TradeRow[], marketFilter: "KR" | "US" | null) {
  const filtered =
    marketFilter == null
      ? trades
      : trades.filter((t) => (t.market || "KR").toUpperCase() === marketFilter);
  const sorted = [...filtered].sort(
    (a, b) =>
      new Date(a.timestamp || 0).getTime() -
      new Date(b.timestamp || 0).getTime(),
  );
  let cum = 0;
  const rows: { t: string; cum: number }[] = [];
  for (const tr of sorted) {
    if (tr.side === "sell" && typeof tr.pnl === "number") {
      cum += tr.pnl;
    }
    const label = tr.timestamp ? formatKstShort(tr.timestamp) : "";
    rows.push({ t: label, cum });
  }
  return rows;
}

export function EquityChart({
  trades,
  marketFilter = null,
}: {
  trades: TradeRow[];
  /** 국내만/미국만 누적손익 — both 모드·단일 시장 뷰용 */
  marketFilter?: "KR" | "US" | null;
}) {
  const data = buildSeries(trades, marketFilter);
  if (data.length < 2) {
    return (
      <p className="text-slate-500 text-sm py-8 text-center">
        매도 체결·손익 데이터가 쌓이면 누적 손익 곡선이 표시됩니다.
      </p>
    );
  }
  return (
    <div className="h-48 sm:h-56 w-full min-w-0">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
          <XAxis dataKey="t" tick={{ fill: "#64748b", fontSize: 10 }} />
          <YAxis
            tick={{ fill: "#64748b", fontSize: 10 }}
            tickFormatter={(v) =>
              typeof v === "number" ? v.toLocaleString() : String(v)
            }
          />
          <Tooltip
            contentStyle={{
              background: "#0f172a",
              border: "1px solid #334155",
              borderRadius: 8,
            }}
            labelStyle={{ color: "#94a3b8" }}
          />
          <Line
            type="monotone"
            dataKey="cum"
            name="누적 실현손익"
            stroke="#60a5fa"
            strokeWidth={2}
            dot={false}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
