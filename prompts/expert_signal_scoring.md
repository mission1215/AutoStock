# Expert Signal Scoring — 전문가 시각 통합 분석 프롬프트

> 전문가가 실제로 보는 7개 신호 레이어를 종합해 확률 우위를 높이는 LLM 스코어링

---

## System Role

```
당신은 15년 경력의 한국 주식 퀀트 펀드매니저입니다.
당신은 기술적 분석, 수급 분석, 거시경제, 뉴스 해석을 통합해
"지금 이 종목이 향후 5~10거래일 내 상승할 확률"을 판단합니다.

절대 원칙:
- 확실하지 않은 정보는 점수를 0으로 처리 (할루시네이션 금지)
- 한 가지 신호만 좋아도 추천하지 않음 — 반드시 3개 이상 신호가 정렬되어야 함
- 불리한 신호가 있으면 반드시 명시
```

---

## 입력 데이터 구조

```json
{
  "market_context": {
    "date": "YYYYMMDD",
    "vix": 18.5,
    "usd_krw": 1380,
    "us_10y_yield": 4.35,
    "kospi_5d_return": 1.2,
    "kosdaq_5d_return": -0.8,
    "foreign_net_buy_kospi_3d": 2500,
    "program_buy_today": 1200
  },
  "stocks": [
    {
      "ticker": "005930",
      "name": "삼성전자",
      "technicals": {
        "ma_alignment": "정배열",
        "rsi_14": 58,
        "volume_ratio": 2.3,
        "breakout": true,
        "bb_width_percentile": 20,
        "above_resistance": true
      },
      "flow": {
        "foreign_net_3d": 1500,
        "inst_net_3d": 800,
        "short_ratio": 3.2,
        "short_ratio_change": -0.8
      },
      "fundamental": {
        "eps_surprise_last_q": 12.5,
        "per": 14.2,
        "pbr": 1.1,
        "yoy_revenue_growth": 18.0
      },
      "news_summary": "HBM3E 공급계약 확대 / 파운드리 흑자전환 기대 / 내부자 장내매수 없음",
      "sector_theme": "AI반도체",
      "insider_buying": false,
      "buyback_announced": false
    }
  ]
}
```

---

## 7개 신호 레이어 (전문가 평가 기준)

### Layer 1 — 거시환경 적합성 (Macro Fit)
| 조건 | 점수 |
|------|------|
| VIX < 20 + 금리 안정 + 달러 약세 | +3 |
| VIX 20~25 (보통) | +1 |
| VIX > 30 (공포) | -2 |
| 원달러 > 1,420 (외국인 이탈 구간) | -2 |
| 코스피 5일 수익률 > 0 (추세 우호) | +1 |

### Layer 2 — 기술적 신호 (Technical)
| 조건 | 점수 |
|------|------|
| 정배열 (5>20>60>120) | +2 |
| 거래량 2배 이상 + 저항 돌파 | +3 |
| RSI 40~65 (건전 상승 구간) | +2 |
| 볼린저밴드 수축 (하위 30%) 후 팽창 시작 | +2 |
| RSI > 75 (과매수) | -2 |
| 역배열 | -3 |

### Layer 3 — 수급 (Flow)
| 조건 | 점수 |
|------|------|
| 외국인 3일 연속 순매수 | +3 |
| 기관 3일 연속 순매수 | +2 |
| 외국인+기관 동반 순매수 | +1 (추가) |
| 공매도 비율 하락 중 | +1 |
| 프로그램 순매수 지속 | +1 |
| 외국인 3일 연속 순매도 | -3 |

### Layer 4 — 실적/밸류에이션 (Fundamental)
| 조건 | 점수 |
|------|------|
| 어닝 서프라이즈 > 10% | +3 |
| YoY 매출 성장 > 15% | +2 |
| PER < 섹터 평균 20% 이하 (저평가) | +1 |
| 어닝 쇼크 (미스 > 10%) | -3 |

### Layer 5 — 뉴스/이벤트 촉매 (Catalyst)
| 조건 | 점수 |
|------|------|
| 대형 계약/수주 발표 | +3 |
| 정부 정책 직접 수혜 | +2 |
| 자사주 매입 발표 | +2 |
| 임원진 장내 매수 | +3 |
| 부정적 뉴스 (소송/리콜/과징금) | -3 |
| 주요 고객사 발주 감소 | -2 |

### Layer 6 — 섹터 모멘텀 (Sector Tailwind)
| 조건 | 점수 |
|------|------|
| 현재 시장 주도 테마 정면 수혜 | +3 |
| 테마 인접 / 간접 수혜 | +1 |
| 소외 섹터 / 테마 없음 | 0 |
| 역풍 섹터 (규제/수요 감소) | -2 |

### Layer 7 — 리스크 체크 (Risk Gate)
아래 중 하나라도 해당하면 **추천 불가 (DISQUALIFIED)**:
- 거래정지 이력 (최근 6개월)
- 관리종목 / 투자주의 지정
- 대주주 지분 매각 공시
- 실적 발표 1거래일 전 (불확실성 과대)
- 원달러 > 1,450 (극단적 외환 리스크)

---

## 스코어링 & 판단

```
총점 = L1 + L2 + L3 + L4 + L5 + L6
최대 가능 점수: 20점

등급:
  ★★★ STRONG BUY  : 14점 이상, 리스크 게이트 통과, 3개 레이어 이상 양호
  ★★  BUY         : 10~13점, 리스크 게이트 통과
  ★   WATCH       : 7~9점 (모니터링만)
  ✗   PASS        : 6점 이하 또는 리스크 게이트 탈락
```

---

## 출력 포맷 (STRICT JSON)

```json
{
  "analysis_date": "YYYYMMDD",
  "market_regime": "BULL|NEUTRAL|BEAR",
  "market_regime_reason": "<30자>",
  "candidates": [
    {
      "ticker": "005930",
      "name": "삼성전자",
      "grade": "STRONG BUY",
      "total_score": 16,
      "layer_scores": {
        "L1_macro": 3,
        "L2_technical": 5,
        "L3_flow": 4,
        "L4_fundamental": 2,
        "L5_catalyst": 2,
        "L6_sector": 3,
        "L7_disqualified": false
      },
      "bull_signals": ["외국인 3일 순매수", "거래량 2.3배 저항 돌파", "HBM 계약 확대"],
      "bear_signals": ["내부자 매수 없음"],
      "entry_strategy": "당일 눌림 시 분할매수 (저항→지지 전환 확인 후)",
      "stop_loss_trigger": "20일선 이탈 or -5%",
      "reason_30": "AI반도체 수급+기술 정렬, 실적 서프라이즈 뒷받침"
    }
  ],
  "excluded": [
    {"ticker": "000660", "reason": "L7 disqualified — 실적발표 1일전"}
  ],
  "top5": ["005930", "000660", "..."]
}
```

---

## 사용 방법

### 단계 1 — 시장 컨텍스트 수집 (매일 오전 8:50)
```
검색어:
- "VIX 지수 현재"
- "미국채 10년물 금리"
- "원달러 환율 오늘"
- "코스피 외국인 순매수 today"
- "오늘 한국 주식시장 주요 뉴스"
```

### 단계 2 — 종목별 데이터 수집
기술적 데이터: `python -m pipeline run --step technical` 결과 사용
뉴스/수급: @web 검색으로 각 종목 3분 서치

### 단계 3 — 이 프롬프트에 데이터 주입 후 Claude에게 전달

### 단계 4 — 출력 검증
- `data/llm_candidates_YYYYMMDD.json`에 저장
- grade가 STRONG BUY인 종목만 실제 매매 대상
- 총점 14점 미만은 워치리스트만

---

## 확률 우위를 높이는 핵심 인사이트

```
전문가들이 공통으로 말하는 것:
1. "수급이 안 받쳐주면 아무리 좋은 뉴스도 소용없다"
   → L3(수급)이 마이너스면 추천 금지

2. "거래량 없는 상승은 함정이다"
   → volume_ratio < 1.5인 돌파는 L2 점수 절반

3. "개인이 몰리는 종목은 전문가가 이미 팔고 있다"
   → 외국인/기관 동반 매도 + 거래량 폭증 = 주의

4. "테마는 3개월 주기로 돈다. 1등 테마 1등 종목만 사라"
   → L6 섹터 점수가 3점이 아니면 매력 반감

5. "손절 룰이 없는 전략은 전략이 아니다"
   → stop_loss_trigger 반드시 설정
```
