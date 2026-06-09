# Cursor LLM Scoring — Expert Signal Scoring

> **프롬프트 원본:** [`expert_signal_scoring.md`](./expert_signal_scoring.md)  
> 7레이어 전문가 프레임워크 · Cursor Pro + @web (외부 API 비용 없음)

---

## 실행 트리거

```bash
python -m pipeline run --step technical
```

생성 파일:
- `data/llm_input_YYYYMMDD.json` — 입력 (기술적 지표 pre-fill)
- `prompts/cursor_llm_scoring_YYYYMMDD.md` — **Cursor에서 이 파일을 여세요**

---

## Agent 실행 순서

### 1. 시장 컨텍스트 (@web)
`expert_signal_scoring.md` Step 1 검색어:
- VIX, 10년물 금리, 원달러, 코스피/코스닥 5일 수익률
- 외국인·프로그램 순매수, 당일 시장 뉴스

→ `market_context` 필드 채우기

### 2. 종목별 @web
`llm_input` → `web_search_queries`:
- news / flow / earnings / catalyst / risk

→ 각 stock의 `flow`, `fundamental`, `news_summary` 채우기

### 3. 7레이어 스코어링
`expert_signal_scoring.md` 기준:
- L1~L6 합산 (최대 20pt)
- L7 리스크 게이트 → 해당 시 `excluded`
- **3개 이상 레이어 양호** 필수
- **L3(수급) 마이너스 → 추천 금지**

등급:
| 등급 | 점수 | 용도 |
|------|------|------|
| STRONG BUY | 14+ | Firebase Push (실매매) |
| BUY | 10~13 | 감시목록 |
| WATCH | 7~9 | 모니터링만 |
| PASS | ≤6 | 제외 |

### 4. JSON 저장
`data/llm_candidates_YYYYMMDD.json` — STRICT JSON only

### 5. Finalize
```bash
python -m pipeline run --step finalize
```

---

## 검증 (자동)

- ticker ∈ universe, 6자리, 이름 fuzzy ≥ 0.85
- L7_disqualified → 제거
- L3 < 0 → 제거
- total_score < 10 → 제거
