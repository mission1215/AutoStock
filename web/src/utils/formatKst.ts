/** 브라우저/서버 타임존과 무관하게 한국 표준시로 표시 */
const KST: Intl.DateTimeFormatOptions = {
  timeZone: "Asia/Seoul",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
};

export function formatKst(
  input: string | number | Date | undefined | null,
): string {
  if (input == null || input === "") return "—";
  const d =
    typeof input === "number" ? new Date(input) : new Date(input as string | Date);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString("ko-KR", KST);
}

/** 차트 축 등 짧은 라벨 */
export function formatKstShort(
  input: string | number | Date | undefined | null,
): string {
  if (input == null || input === "") return "";
  const d =
    typeof input === "number" ? new Date(input) : new Date(input as string | Date);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleString("ko-KR", {
    timeZone: "Asia/Seoul",
    month: "numeric",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}
