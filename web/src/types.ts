/** Firestore bot state (get_bot_state) */
export interface BotState {
  bot_enabled?: boolean;
  trading_halted?: boolean;
  is_market_open?: boolean;
  realized_pnl?: number;
}

/** GET /api/status, /api/config — 모의·실전 전략·키 분리 저장용 */
export interface ModeProfiles {
  mock?: AppConfig;
  live?: AppConfig;
}

/** GET /api/status → risk_gates (대시보드 배너·일간손익) */
export interface DailyPnlGate {
  ok?: boolean;
  reason?: string;
  ratio_pct?: number;
  target_pct?: number;
  loss_limit_pct?: number;
  realized_pnl?: number;
  start_equity?: number;
  peak_equity?: number;
}

export interface StatusRiskGates {
  error?: string;
  now_kst?: string;
  now_et?: string;
  daily_pnl?: DailyPnlGate;
  [key: string]: unknown;
}

export interface StatusResponse {
  ok: boolean;
  setup_required?: boolean;
  state?: BotState;
  balance?: Record<string, unknown>;
  positions_kr?: Record<string, PositionKr>;
  positions_us?: Record<string, PositionUs>;
  watchlist_data?: Record<string, WatchlistEntry>;
  us_watchlist_data?: Record<string, WatchlistEntry>;
  config?: AppConfig;
  /** 서버 저장 mock/live 프로필 (app_key/app_secret 제외) */
  profiles?: ModeProfiles;
  updated_at?: string;
  kis_error?: string | null;
  error?: string;
  risk_gates?: StatusRiskGates;
}

export interface AppConfig {
  /** KIS API 설정 시 서버에 저장 (조회 API에서는 마스크 없이도 일부 민감 — UI에서 가림) */
  account_no?: string;
  display_name?: string;
  kr_watchlist?: string[];
  us_watchlist?: string[];
  ai_stock_count?: number;
  k_factor?: number;
  ma_period?: number;
  stop_loss_ratio?: number;
  max_position_ratio?: number;
  daily_profit_target?: number;
  is_mock?: boolean;
  setup_complete?: boolean;
  /** 자동매매·스케줄: kr | us | both (기본 kr) */
  market_scope?: "kr" | "us" | "both";
  /** KR 스케줄 AI 시세 입력 풀: legacy(감시+고정) | dynamic(KIS 거래량·거래대금 순위, 실패 시 legacy) */
  ai_universe_mode?: "legacy" | "dynamic";
  /** 동적 유니버스 시 KIS 현재가로 투자유의·관리종목 등 제외 (기본 온·프로파일 저장) */
  ai_universe_kr_quality_gates?: boolean;
  /** 동적 유니버스 시 시총 하한(억원). 0이면 미사용 — 상장주수×현재가 근사 */
  ai_universe_kr_min_cap_eok?: number;
  /** 같은 섹터 동시 최대 보유 종목 수(전략·AI 매수 분기에 사용) */
  max_positions_per_sector?: number;
  /** 분할 익절·추격 방지 등 (백엔드와 동일 키) */
  partial_tp_enabled?: boolean;
  partial_tp_trigger_pct?: number;
  partial_tp_sell_ratio?: number;
  partial_tp_tighten_stop?: boolean;
  max_entry_slip_pct_mock?: number;
  max_entry_slip_pct_live?: number;
  kr_index_drop_limit_pct?: number;
  min_score_kr?: number;
  avg_down_enabled?: boolean;
  avg_down_trigger_pct?: number;
  avg_down_max_times?: number;
  avg_down_qty_ratio?: number;
  avg_down_min_interval_hours?: number;
  /** 보수/보통/적극 프리셋 — 마지막으로 저장한 성향(표시용). 전략 수치는 k_factor 등과 함께 병합 저장 */
  strategy_tier?: "conservative" | "balanced" | "aggressive" | null;
}

export interface PositionKr {
  stock_code?: string;
  stock_name?: string;
  quantity?: number;
  current_price?: number;
  buy_price?: number;
  pnl?: number;
  pnl_ratio?: number;
  target_sell_price?: number;
  stop_loss_price?: number;
  change_rate?: string;
  source?: string;
  market?: string;
}

export interface PositionUs {
  stock_code?: string;
  stock_name?: string;
  quantity?: number;
  current_price?: number;
  buy_price?: number;
  pnl?: number;
  pnl_ratio?: number;
  target_sell_price?: number;
  stop_loss_price?: number;
  source?: string;
  market?: string;
}

export interface WatchlistEntry {
  current_price?: number;
  stock_name?: string;
  change_rate?: string;
  closes?: number[];
  target_breakout?: number;
  ma5?: number;
}

/** GET /api/research */
export interface ResearchResponse {
  ok: boolean;
  market?: string;
  date?: string;
  title?: string;
  bullets?: string[];
  cached?: boolean;
  fallback?: boolean;
  error?: string;
}

export interface LogEntry {
  level?: string;
  message?: string;
  timestamp?: string;
}

export interface TradeRow {
  stock_code?: string;
  stock_name?: string;
  market?: string;
  side?: string;
  price?: number;
  quantity?: number;
  pnl?: number;
  reason?: string;
  timestamp?: string;
}
