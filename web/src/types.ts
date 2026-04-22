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
}

export interface AppConfig {
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
