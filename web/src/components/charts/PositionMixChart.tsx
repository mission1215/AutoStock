import {
  PieChart,
  Pie,
  Cell,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";
import type { PositionKr, PositionUs } from "../../types";

const COLORS = ["#3b82f6", "#8b5cf6", "#22c55e", "#f59e0b", "#ef4444", "#06b6d4"];

export function PositionMixChart({
  positionsKr,
  positionsUs,
}: {
  positionsKr: Record<string, PositionKr>;
  positionsUs: Record<string, PositionUs>;
}) {
  const slices: { name: string; value: number }[] = [];
  for (const [code, p] of Object.entries(positionsKr)) {
    const q = p.quantity ?? 0;
    const price = p.current_price ?? p.buy_price ?? 0;
    const v = q * price;
    if (v > 0) {
      slices.push({
        name: (p.stock_name || code).slice(0, 12),
        value: Math.round(v),
      });
    }
  }
  for (const [code, p] of Object.entries(positionsUs)) {
    const q = p.quantity ?? 0;
    const price = p.current_price ?? p.buy_price ?? 0;
    const v = q * price;
    if (v > 0) {
      slices.push({
        name: `${(p.stock_name || code).slice(0, 10)}(US)`,
        value: Math.round(v),
      });
    }
  }
  if (slices.length === 0) {
    return (
      <p className="text-slate-500 text-sm py-8 text-center">
        보유 종목이 없습니다.
      </p>
    );
  }
  return (
    <div className="h-48 sm:h-56 w-full min-w-0">
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Pie
            data={slices}
            dataKey="value"
            nameKey="name"
            cx="50%"
            cy="50%"
            outerRadius={68}
            label={false}
          >
            {slices.map((_, i) => (
              <Cell key={i} fill={COLORS[i % COLORS.length]} />
            ))}
          </Pie>
          <Tooltip
            formatter={(v) =>
              typeof v === "number" ? v.toLocaleString() + "원" : String(v)
            }
            contentStyle={{
              background: "#0f172a",
              border: "1px solid #334155",
              borderRadius: 8,
            }}
          />
          <Legend wrapperStyle={{ fontSize: "11px", color: "#94a3b8" }} />
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
}
