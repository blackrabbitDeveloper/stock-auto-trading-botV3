# Stock Auto Trading Bot V3 — Design Spec

## Overview

한국투자증권 OpenAPI를 활용한 자동매매 봇. BacktesterV2에서 검증된 4개 전략을 실전 자동 실행한다.
Railway에 배포하며, 모의투자로 검증 후 실전 전환.

## Goals

- 4개 전략(volume_breakout, pullback_buy, high_breakout, combined_ac) 동시 자동매매
- 시초가(09:00) 시장가 매수/매도 + SL 예약주문 자동 등록
- Discord 알림 + 간단 웹 대시보드로 모니터링
- 모의투자 → 실전 전환이 환경변수 하나로 가능

## Architecture

단일 FastAPI 프로세스 (모놀리식). APScheduler로 내부 cron, PostgreSQL로 상태 영속.

```
stock-auto-trading-botV3/
├── app/
│   ├── main.py              # FastAPI + APScheduler 초기화
│   ├── config.py            # 설정 로드
│   ├── broker/              # 한투 API 래퍼
│   │   ├── __init__.py
│   │   ├── auth.py          # 토큰 발급/갱신
│   │   ├── client.py        # HTTP 클라이언트 (공통)
│   │   ├── order.py         # 주문 (시장가, 예약)
│   │   ├── account.py       # 잔고, 체결 내역
│   │   └── market.py        # 시세 조회
│   ├── strategy/            # 시그널 생성
│   │   ├── __init__.py
│   │   ├── base.py          # 공통 인터페이스
│   │   ├── volume_breakout.py
│   │   ├── pullback_buy.py
│   │   ├── high_breakout.py
│   │   ├── combined_ac.py
│   │   ├── indicators.py    # 지표 계산
│   │   ├── universe.py      # 유니버스 필터
│   │   └── market_filter.py # KOSPI MA20 필터
│   ├── trader/              # 주문 실행 오케스트레이션
│   │   ├── __init__.py
│   │   ├── executor.py      # 주문 실행
│   │   ├── sl_manager.py    # SL 예약주문 관리
│   │   └── position_sync.py # 체결 확인 → DB 동기화
│   ├── models/              # DB 모델
│   │   ├── __init__.py
│   │   ├── database.py      # SQLAlchemy async engine
│   │   ├── position.py
│   │   ├── order.py
│   │   └── trade.py
│   ├── dashboard/           # 웹 UI
│   │   ├── __init__.py
│   │   ├── router.py        # FastAPI router
│   │   └── templates/
│   │       └── dashboard.html
│   ├── notifier/            # Discord 알림
│   │   ├── __init__.py
│   │   └── discord.py
│   └── jobs/                # 스케줄 작업
│       ├── __init__.py
│       ├── signal_job.py    # 15:40 시그널 생성
│       ├── order_job.py     # 08:59 주문 제출
│       └── confirm_job.py   # 09:05 체결 확인
├── config/                  # 전략별 YAML
│   ├── volume_breakout.yaml
│   ├── pullback_buy.yaml
│   ├── high_breakout.yaml
│   └── combined_ac.yaml
├── Dockerfile
├── railway.toml
├── requirements.txt
└── .env.example
```

## Schedule

| 시간 | Job | 동작 |
|------|-----|------|
| 15:40 | `signal_job` | pykrx 일봉 수집 → 4전략 시그널 생성 → exit 체크 → pending_buy/pending_sell 결정 → DB 저장 → Discord 알림 |
| 08:59 | `order_job` | DB에서 pending 조회 → 매도 먼저 → 매수 시장가 주문 제출 |
| 09:05 | `confirm_job` | 체결 확인 → 매수 체결 시 SL 예약주문 등록 → 매도 체결 시 포지션 종료 → DB 업데이트 → Discord 알림 |

추가:
- 토큰 자동 갱신 (만료 1시간 전 재발급)
- 공휴일/휴장일 스킵 (pykrx 캘린더 활용)

## Broker Module (한투 OpenAPI)

### 인터페이스

```python
class KISBroker:
    async def get_token(self) -> str
    async def buy_market(self, symbol: str, qty: int) -> OrderResult
    async def sell_market(self, symbol: str, qty: int) -> OrderResult
    async def set_stop_loss(self, symbol: str, qty: int, price: int) -> OrderResult
    async def cancel_order(self, order_no: str) -> bool
    async def get_balance(self) -> AccountBalance
    async def get_filled_orders(self, date: str) -> list[FilledOrder]
    async def get_current_price(self, symbol: str) -> int
```

### 모의투자/실전 전환

환경변수 `KIS_ENV=paper|real`로 분기. API 도메인만 다름:
- 모의: `https://openapivts.koreainvestment.com:29443`
- 실전: `https://openapi.koreainvestment.com:9443`

### 에러 핸들링

- 토큰 만료 → 자동 재발급 후 1회 재시도
- 주문 실패 → Discord 알림, 재시도 없음 (수동 개입)
- 네트워크 타임아웃 → 3회 재시도 with exponential backoff

## Strategy Module

BacktesterV2에서 포팅:
- `src/signals.py` → 전략별 시그널 생성 함수
- `src/indicators.py` → ATR14, MA, 거래량 비율
- `src/universe.py` → 종목 필터링
- `src/config.py` → StrategyConfig 구조

### 전략별 독립 운용

- 각 전략은 독립 config (max_positions, capital_allocation 등)
- 종목 중복 방지: DB에서 전 전략 across 보유/대기 종목 조회 후 제외
- 전략별 capital_allocation: 전체 계좌 평가금액의 25%씩 배분 (기본값, config에서 변경 가능)
- 수량 계산: `qty = floor(계좌평가금액 * capital_allocation * position_weight / 현재가)`
- 예시: 4000만원 계좌 → 전략당 1000만원 → 포지션당 200만원 (5종목 균등)

### signal_job 흐름

1. 마켓 필터 체크 (KOSPI > MA20)
2. 유니버스 로드 + 필터 (pykrx)
3. 보유 종목 exit 조건 체크 (trailing stop, SL, time exit)
4. 신규 매수 시그널 생성
5. 중복 종목 제외
6. 상위 N개 → pending_buy 저장
7. Discord 알림

## Trader Module

### order_job (08:59)

1. pending_sell 조회 → 시장가 매도 (자금 확보 우선)
2. pending_buy 조회 → 수량 계산 → 시장가 매수
3. 수량 계산: `qty = (capital * allocation * position_weight) // price`

### confirm_job (09:05)

1. 체결 확인 (한투 API)
2. 매수 체결 → SL 예약주문 등록
3. 매도 체결 → 포지션 종료, trades 테이블 기록
4. 미체결 → Discord 알림

### SL 관리

- 매수 체결 즉시 SL 예약주문 등록 (entry - ATR * 0.5)
- signal_job에서 트레일링 스탑 업데이트 시:
  - effective_sl = max(fixed_sl, trail_price)
  - 기존 SL 주문 취소 → 새 가격으로 재등록

### 안전장치

- 일일 주문 한도: 전략당 max_positions 초과 불가
- 중복 주문 방지: DB 체크 (같은 종목 매수 주문 중복 방지)
- 주문 실패 시 재시도 없이 Discord 알림

## Database

PostgreSQL (Railway addon). SQLAlchemy async + asyncpg.

### Tables

**positions** — 현재 포지션 (보유 + 대기)
- id, strategy, symbol, name, status (pending_buy/active/pending_sell)
- signal_date, entry_date, entry_price, qty
- peak_price, sl_price, trail_price, sl_order_no
- holding_days, exit_reason, created_at, updated_at

**orders** — 주문 이력
- id, position_id (FK), strategy, symbol, side (buy/sell)
- order_type (market/limit/stop), qty, price, filled_price, filled_qty
- order_no, status (submitted/filled/cancelled/failed)
- submitted_at, filled_at

**trades** — 완료된 매매 (성과 분석용)
- id, strategy, symbol, name
- entry_date, exit_date, entry_price, exit_price, qty
- return_pct, pnl, holding_days, exit_reason, created_at

## Dashboard

단일 HTML 페이지 (Jinja2 + Tailwind CDN):
- 계좌 요약 (총 평가금액, 현금, 오늘 손익)
- 전략별 현황 (슬롯, 수익률, 상태)
- 보유 종목 리스트
- 오늘 주문 내역
- 최근 매매 10건

인증: Bearer 토큰 (환경변수 `DASHBOARD_TOKEN`)
자동 새로고침: 30초 간격

## Discord Notifications

| 이벤트 | 알림 내용 |
|--------|----------|
| signal_job 완료 | 내일 매수/매도 예정 종목 리스트 |
| order_job 완료 | 주문 제출 결과 |
| confirm_job 완료 | 체결 확인 + SL 등록 결과 |
| 에러 발생 | 주문 실패, API 오류, 토큰 만료 |

## Deployment (Railway)

### 환경변수

```
KIS_APP_KEY=
KIS_APP_SECRET=
KIS_ACCOUNT_NO=        # 8자리-2자리
KIS_ENV=paper          # paper / real
DATABASE_URL=          # Railway 자동 제공
DISCORD_WEBHOOK_URL=
DASHBOARD_TOKEN=
TZ=Asia/Seoul
```

### Dockerfile

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### railway.toml

```toml
[build]
builder = "dockerfile"

[deploy]
healthcheckPath = "/health"
healthcheckTimeout = 30
restartPolicyType = "always"
```

## Dependencies

```
fastapi
uvicorn
sqlalchemy[asyncio]
asyncpg
apscheduler
httpx
pykrx
jinja2
python-dotenv
discord-webhook
```

## Migration Path

1. 모의투자 배포 → 1~2주 검증
2. 시그널 정확도 확인 (BacktesterV2 알림봇과 비교)
3. 주문 체결/SL 동작 확인
4. `KIS_ENV=real` 전환 → 소액 실전
5. 안정화 후 자금 증액

## Out of Scope (향후)

- WebSocket 실시간 체결 (현재는 polling으로 충분)
- 멀티 계좌 지원
- 모바일 앱
- 자동 자금 재배분
