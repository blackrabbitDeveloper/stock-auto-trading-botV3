# Stock Auto Trading Bot V3

한국투자증권 OpenAPI 기반 자동매매 봇. BacktesterV2에서 검증된 4개 전략을 자동 실행합니다.

## 전략

| 전략 | 설명 | 핵심 조건 |
|------|------|----------|
| volume_breakout | 거래량 연속 증가 돌파 | 3일 연속 거래량↑ + 평균 3배 이상 |
| pullback_buy | 거래량 급증 후 눌림목 매수 | 급등 → 5일내 MA5 근처 조정 → 재돌파 |
| high_breakout | 20일 신고가 돌파 | 종가 > 전일 20일 최고가 + 거래량 증가 |
| combined_ac | A∩C 복합 | volume_breakout + high_breakout 동시 충족 |

## 운용 방식

- **매매 타이밍:** 시초가(09:00) 시장가 주문
- **손절:** ATR 기반 SL 예약주문 자동 등록, `atr_sl_enabled` 플래그로 on/off
- **Exit 방식 (3가지):** `trailing_stop` (기본), `ma_exit`, `fixed` — 전략별 config 설정
- **트레일링:** 매일 15:40 peak 업데이트 (look-ahead 방지: peak_before_today 사용)
- **전략 독립 운용:** 각 전략 별도 config, 종목 중복 방지

## 일일 스케줄

| 시간 | 작업 | 설명 |
|------|------|------|
| 08:00 | 토큰 갱신 | 한투 API 접근토큰 자동 갱신 |
| 08:59 | 주문 제출 | 매도(SL 취소 → 시장가 매도) → 매수(잔고 추적) |
| 09:00~15:30 | WebSocket SL | 실시간 체결가 감시, SL/Trail 도달 시 즉시 매도 |
| 09:05 | 체결 확인 | 체결가 확인 + SL 예약주문 등록 (실패 시 즉시 매도) |
| 15:40 | 시그널 생성 | 일봉 수집 → exit 체크 → breakeven SL 반영 → 신규 시그널 |

## 안전장치

| 장치 | 설명 |
|------|------|
| sl_skip_days 3중 방어 | signal_job + pos_map 필터 + WebSocket 콜백에서 체크 |
| 매도 전 SL 취소 | 브로커 SL 예약주문 cancel → 시장가 매도 (이중매도 방지) |
| SL 등록 실패 시 즉시 매도 | 무방비 포지션 방지 |
| WebSocket 이중매도 방지 | 매도 후 qty=0 설정, order_job 재매도 차단 |
| 다중 매수 잔고 추적 | spent 누적으로 중복 할당 방지 |
| SL cancel 실패 시 새 SL 차단 | 이중 SL 주문 방지 |
| qty=0 포지션 자동 삭제 | pending_buy 영구체류 방지 |
| 마켓 필터 | KOSPI < MA20이면 신규 매수 차단 |
| 전략당 max_positions | 최대 보유 종목 수 제한 |
| 종목 중복 방지 | 전 전략 across 체크 |

---

## 설정 가이드

### 1. 한투 OpenAPI 앱키 발급

1. [한국투자증권 OpenAPI](https://apiportal.koreainvestment.com/) 접속
2. 회원가입 → 앱 등록 (모의투자용)
3. **APP KEY**, **APP SECRET** 발급
4. 모의투자 계좌번호 확인 (예: `50123456-01`)

### 2. 환경변수 설정

`.env` 파일을 프로젝트 루트에 생성:

```env
# 한투 API (실전 또는 모의투자)
KIS_APP_KEY=PSxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
KIS_APP_SECRET=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
KIS_ACCOUNT_NO=50123456-01
KIS_ENV=paper          # paper=모의투자, real=실전

# 모의투자 전용 (실전일 때 주문용 별도 키)
KIS_PAPER_APP_KEY=...
KIS_PAPER_APP_SECRET=...
KIS_PAPER_ACCOUNT_NO=...

# Database (Railway에서 자동 제공됨)
DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/dbname

# Discord 알림
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/xxxx/yyyy

# 대시보드 접근 토큰 (원하는 값으로 설정)
DASHBOARD_TOKEN=my_secret_token_123

# 시스템
TZ=Asia/Seoul
```

### 3. Discord 웹훅 설정

1. Discord 서버 → 채널 설정 → 연동 → 웹훅
2. 새 웹훅 생성 → URL 복사 → `DISCORD_WEBHOOK_URL`에 입력

### 4. 전략 파라미터 수정 (선택)

`config/` 폴더의 YAML 파일에서 전략별 파라미터 조정 가능:

```yaml
# config/volume_breakout.yaml
name: volume_breakout
capital_allocation: 0.25    # 전체 자금의 25% 할당
max_positions: 5            # 최대 보유 종목 수
position_weight: 0.20       # 포지션당 비중 (1/max_positions)
exit_method: trailing_stop  # trailing_stop | ma_exit | fixed
trailing_stop_pct: 0.05     # 트레일링 스탑 5%
stop_loss_pct: 0.03         # 고정 SL 3% (ATR 미사용 시)
take_profit_pct: 0.10       # TP 10% (fixed exit_method 전용)
atr_sl_enabled: true        # ATR 기반 SL 사용 여부
atr_sl_multiplier: 0.5      # SL = 매수가 - ATR*0.5
sl_skip_days: 2             # 진입 후 2일간 SL 체크 안함
max_holding_days: 10        # 최대 보유일
ma_exit_period: 5           # MA exit 기간 (ma_exit 전용)
dynamic_holding: false      # 수익 중이면 time_exit 스킵
breakeven_stop: false       # 수익 시 SL을 매수가로 상향
```

---

## Railway 배포

### 1. Railway 프로젝트 생성

```bash
# Railway CLI 설치
npm install -g @railway/cli

# 로그인
railway login

# 프로젝트 생성 + 배포
cd D:\Projects\Python\stock-auto-trading-botV3
railway init
railway up
```

### 2. PostgreSQL 추가

Railway 대시보드에서:
1. New → Database → PostgreSQL 추가
2. Variables 탭에서 `DATABASE_URL` 자동 생성됨
3. 서비스의 Variables에 나머지 환경변수 추가

### 3. 환경변수 설정

Railway 대시보드 → 서비스 → Variables:
```
KIS_APP_KEY=...
KIS_APP_SECRET=...
KIS_ACCOUNT_NO=...
KIS_ENV=paper
DISCORD_WEBHOOK_URL=...
DASHBOARD_TOKEN=...
TZ=Asia/Seoul
```

> `DATABASE_URL`은 PostgreSQL addon 연결 시 자동 설정됩니다.
> Railway의 DATABASE_URL은 `postgresql://` 형식이므로, 앱에서 `+asyncpg` 접미사를 자동 처리합니다.

### 4. 배포 확인

- 헬스체크: `https://your-app.railway.app/health`
- 대시보드: `https://your-app.railway.app/dashboard?token=your_token`

---

## 로컬 개발

```bash
# 의존성 설치
pip install -r requirements.txt

# 테스트 실행 (49개)
python -m pytest tests/ -v

# 로컬 실행 (PostgreSQL 필요)
uvicorn app.main:app --reload
```

---

## 모의투자 → 실전 전환

검증 완료 후:

1. 한투 OpenAPI에서 **실전용 앱키** 별도 발급
2. Railway 환경변수 변경:
   ```
   KIS_APP_KEY=실전키
   KIS_APP_SECRET=실전시크릿
   KIS_ACCOUNT_NO=실전계좌-01
   KIS_ENV=real
   ```
3. 재배포 (코드 변경 없음)

---

## 모니터링

- **Discord:** 모든 주요 이벤트 알림 (시그널, 주문, 체결, SL HIT, 에러)
- **대시보드:** 실시간 포지션/매매 현황 (30초 자동새로고침, LIVE 태그)
- **헬스체크:** `/health` 엔드포인트 (Railway 자동 모니터링)
- **수동 트리거:** `POST /trigger/{job_name}?token=xxx` (signal_job, order_job 등)
