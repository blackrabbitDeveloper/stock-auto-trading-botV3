# Stock Auto Trading Bot V3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an automated stock trading bot that executes 4 backtested strategies via 한국투자증권 OpenAPI, deployed on Railway.

**Architecture:** Single FastAPI process with APScheduler for cron jobs, PostgreSQL for state persistence, pykrx for signal generation, 한투 API for order execution. Modular design with broker/, strategy/, trader/, jobs/, dashboard/, notifier/ packages.

**Tech Stack:** Python 3.11, FastAPI, SQLAlchemy (async), asyncpg, APScheduler, httpx, pykrx, Jinja2, Tailwind CDN

---

## File Structure

```
app/
├── __init__.py
├── main.py              # FastAPI app + APScheduler setup
├── config.py            # Settings (env vars + strategy YAML loading)
├── broker/
│   ├── __init__.py
│   ├── auth.py          # Token management
│   ├── client.py        # Base HTTP client with retry/error handling
│   ├── order.py         # Buy/sell/stop-loss orders
│   └── account.py       # Balance, filled orders, current price
├── strategy/
│   ├── __init__.py
│   ├── indicators.py    # Technical indicators (port from BacktesterV2)
│   ├── universe.py      # Universe filtering (port)
│   ├── signals.py       # All 4 signal generators (port)
│   └── market_filter.py # KOSPI MA20 filter
├── trader/
│   ├── __init__.py
│   ├── executor.py      # Order orchestration
│   ├── sl_manager.py    # SL order management
│   └── quantity.py      # Position sizing calculation
├── models/
│   ├── __init__.py
│   ├── database.py      # Engine, session factory
│   ├── position.py      # Position model
│   ├── order.py         # Order model
│   └── trade.py         # Trade (closed) model
├── jobs/
│   ├── __init__.py
│   ├── signal_job.py    # 15:40 signal generation
│   ├── order_job.py     # 08:59 order submission
│   └── confirm_job.py   # 09:05 fill confirmation
├── dashboard/
│   ├── __init__.py
│   ├── router.py        # Dashboard routes
│   └── templates/
│       └── dashboard.html
└── notifier/
    ├── __init__.py
    └── discord.py       # Discord webhook sender
config/
├── volume_breakout.yaml
├── pullback_buy.yaml
├── high_breakout.yaml
└── combined_ac.yaml
tests/
├── conftest.py
├── test_config.py
├── test_broker/
│   ├── test_auth.py
│   ├── test_client.py
│   └── test_order.py
├── test_strategy/
│   ├── test_indicators.py
│   ├── test_signals.py
│   └── test_universe.py
├── test_trader/
│   ├── test_executor.py
│   ├── test_quantity.py
│   └── test_sl_manager.py
├── test_models/
│   └── test_position.py
└── test_jobs/
    └── test_signal_job.py
Dockerfile
railway.toml
requirements.txt
.env.example
```

---

### Task 1: Project Scaffold & Configuration

**Files:**
- Create: `requirements.txt`, `Dockerfile`, `railway.toml`, `.env.example`
- Create: `app/__init__.py`, `app/config.py`
- Create: `config/volume_breakout.yaml`, `config/pullback_buy.yaml`, `config/high_breakout.yaml`, `config/combined_ac.yaml`
- Test: `tests/conftest.py`, `tests/test_config.py`

- [ ] **Step 1: Create requirements.txt**

```
fastapi==0.115.0
uvicorn[standard]==0.30.0
sqlalchemy[asyncio]==2.0.35
asyncpg==0.30.0
apscheduler==3.10.4
httpx==0.27.0
pykrx==1.0.45
jinja2==3.1.4
python-dotenv==1.0.1
pydantic==2.9.0
pydantic-settings==2.5.0
pandas==2.2.0
numpy==1.26.0
pyyaml==6.0.2
pytest==8.3.0
pytest-asyncio==0.24.0
```

- [ ] **Step 2: Create Dockerfile**

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

- [ ] **Step 3: Create railway.toml**

```toml
[build]
builder = "dockerfile"

[deploy]
healthcheckPath = "/health"
healthcheckTimeout = 30
restartPolicyType = "always"
```

- [ ] **Step 4: Create .env.example**

```env
# 한투 API
KIS_APP_KEY=your_app_key
KIS_APP_SECRET=your_app_secret
KIS_ACCOUNT_NO=12345678-01
KIS_ENV=paper

# Database
DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/dbname

# Discord
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/xxx/yyy

# Dashboard
DASHBOARD_TOKEN=your_secret_token

# System
TZ=Asia/Seoul
```

- [ ] **Step 5: Create app/config.py**

```python
from __future__ import annotations

from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel
from pydantic_settings import BaseSettings


class KISConfig(BaseSettings):
    app_key: str = ""
    app_secret: str = ""
    account_no: str = ""  # "12345678-01"
    env: str = "paper"  # paper | real

    class Config:
        env_prefix = "KIS_"

    @property
    def base_url(self) -> str:
        if self.env == "real":
            return "https://openapi.koreainvestment.com:9443"
        return "https://openapivts.koreainvestment.com:29443"

    @property
    def account_prefix(self) -> str:
        return self.account_no.split("-")[0]

    @property
    def account_suffix(self) -> str:
        return self.account_no.split("-")[1]


class UniverseConfig(BaseModel):
    include_kospi: bool = True
    include_kosdaq: bool = True
    exclude_preferred: bool = True
    exclude_spac: bool = True
    min_price: int = 1000
    min_avg_trading_value_20: int = 10_000_000_000


class StrategyParams(BaseModel):
    name: str
    capital_allocation: float = 0.25
    max_positions: int = 5
    position_weight: float = 0.20
    volume_increase_days: int = 3
    volume_ratio_threshold: float = 3.0
    max_return_5d: float = 0.20
    trailing_stop_pct: float = 0.05
    stop_loss_pct: float = 0.03
    atr_sl_multiplier: float = 0.5
    sl_skip_days: int = 2
    max_holding_days: int = 10


class MarketFilterConfig(BaseModel):
    enabled: bool = True
    index_code: str = "KS11"
    ma_period: int = 20


class AppSettings(BaseSettings):
    database_url: str = ""
    discord_webhook_url: str = ""
    dashboard_token: str = ""
    tz: str = "Asia/Seoul"

    class Config:
        env_prefix = ""


def load_strategy_configs(config_dir: str = "config") -> dict[str, StrategyParams]:
    """Load all strategy YAML configs from directory."""
    configs = {}
    config_path = Path(config_dir)
    for yaml_file in config_path.glob("*.yaml"):
        with open(yaml_file, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        params = StrategyParams(**data)
        configs[params.name] = params
    return configs
```

- [ ] **Step 6: Create strategy config YAMLs**

`config/volume_breakout.yaml`:
```yaml
name: volume_breakout
capital_allocation: 0.25
max_positions: 5
position_weight: 0.20
volume_increase_days: 3
volume_ratio_threshold: 3.0
max_return_5d: 0.20
trailing_stop_pct: 0.05
stop_loss_pct: 0.03
atr_sl_multiplier: 0.5
sl_skip_days: 2
max_holding_days: 10
```

`config/pullback_buy.yaml`:
```yaml
name: pullback_buy
capital_allocation: 0.25
max_positions: 5
position_weight: 0.20
volume_increase_days: 3
volume_ratio_threshold: 2.0
max_return_5d: 0.30
trailing_stop_pct: 0.05
stop_loss_pct: 0.03
atr_sl_multiplier: 0.5
sl_skip_days: 2
max_holding_days: 10
```

`config/high_breakout.yaml`:
```yaml
name: high_breakout
capital_allocation: 0.25
max_positions: 5
position_weight: 0.20
volume_increase_days: 3
volume_ratio_threshold: 1.5
max_return_5d: 0.25
trailing_stop_pct: 0.05
stop_loss_pct: 0.03
atr_sl_multiplier: 0.5
sl_skip_days: 2
max_holding_days: 10
```

`config/combined_ac.yaml`:
```yaml
name: combined_ac
capital_allocation: 0.25
max_positions: 5
position_weight: 0.20
volume_increase_days: 3
volume_ratio_threshold: 3.0
max_return_5d: 0.20
trailing_stop_pct: 0.05
stop_loss_pct: 0.03
atr_sl_multiplier: 0.5
sl_skip_days: 2
max_holding_days: 10
```

- [ ] **Step 7: Write config test**

`tests/conftest.py`:
```python
import pytest
from pathlib import Path


@pytest.fixture
def config_dir(tmp_path):
    """Create temp config directory with test YAML."""
    import yaml
    config = {
        "name": "test_strategy",
        "capital_allocation": 0.25,
        "max_positions": 5,
        "position_weight": 0.20,
        "volume_increase_days": 3,
        "volume_ratio_threshold": 3.0,
        "max_return_5d": 0.20,
        "trailing_stop_pct": 0.05,
        "stop_loss_pct": 0.03,
        "atr_sl_multiplier": 0.5,
        "sl_skip_days": 2,
        "max_holding_days": 10,
    }
    yaml_file = tmp_path / "test_strategy.yaml"
    yaml_file.write_text(yaml.dump(config))
    return str(tmp_path)
```

`tests/test_config.py`:
```python
from app.config import load_strategy_configs, KISConfig, AppSettings


def test_load_strategy_configs(config_dir):
    configs = load_strategy_configs(config_dir)
    assert "test_strategy" in configs
    assert configs["test_strategy"].max_positions == 5
    assert configs["test_strategy"].capital_allocation == 0.25


def test_kis_config_paper():
    cfg = KISConfig(app_key="k", app_secret="s", account_no="12345678-01", env="paper")
    assert "openapivts" in cfg.base_url
    assert cfg.account_prefix == "12345678"
    assert cfg.account_suffix == "01"


def test_kis_config_real():
    cfg = KISConfig(app_key="k", app_secret="s", account_no="12345678-01", env="real")
    assert "openapivts" not in cfg.base_url
```

- [ ] **Step 8: Run tests**

Run: `cd D:/Projects/Python/stock-auto-trading-botV3 && python -m pytest tests/test_config.py -v`
Expected: 3 tests PASS

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "feat: project scaffold with config, Dockerfile, railway.toml"
```

---

### Task 2: Database Models

**Files:**
- Create: `app/models/__init__.py`, `app/models/database.py`, `app/models/position.py`, `app/models/order.py`, `app/models/trade.py`
- Test: `tests/test_models/test_position.py`

- [ ] **Step 1: Create database.py**

```python
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase

engine = None
async_session_factory = None


class Base(DeclarativeBase):
    pass


def init_db(database_url: str):
    global engine, async_session_factory
    engine = create_async_engine(database_url, echo=False)
    async_session_factory = async_sessionmaker(engine, expire_on_commit=False)


async def get_session() -> AsyncSession:
    async with async_session_factory() as session:
        yield session


async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
```

- [ ] **Step 2: Create position.py model**

```python
from __future__ import annotations

from datetime import datetime, date
from typing import Optional

from sqlalchemy import String, Integer, Float, Date, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column

from app.models.database import Base


class Position(Base):
    __tablename__ = "positions"

    id: Mapped[int] = mapped_column(primary_key=True)
    strategy: Mapped[str] = mapped_column(String(30))
    symbol: Mapped[str] = mapped_column(String(10), index=True)
    name: Mapped[str] = mapped_column(String(50))
    status: Mapped[str] = mapped_column(String(15), index=True)  # pending_buy, active, pending_sell
    signal_date: Mapped[date] = mapped_column(Date)
    entry_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    entry_price: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    qty: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    peak_price: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    sl_price: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    trail_price: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    sl_order_no: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    holding_days: Mapped[int] = mapped_column(Integer, default=0)
    exit_reason: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    entry_atr: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())
```

- [ ] **Step 3: Create order.py model**

```python
from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import String, Integer, DateTime, ForeignKey, func
from sqlalchemy.orm import Mapped, mapped_column

from app.models.database import Base


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(primary_key=True)
    position_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("positions.id"), nullable=True)
    strategy: Mapped[str] = mapped_column(String(30))
    symbol: Mapped[str] = mapped_column(String(10))
    side: Mapped[str] = mapped_column(String(4))  # buy, sell
    order_type: Mapped[str] = mapped_column(String(10))  # market, stop
    qty: Mapped[int] = mapped_column(Integer)
    price: Mapped[int] = mapped_column(Integer, default=0)  # 0=market
    filled_price: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    filled_qty: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    order_no: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    status: Mapped[str] = mapped_column(String(10), default="submitted")  # submitted, filled, cancelled, failed
    submitted_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    filled_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
```

- [ ] **Step 4: Create trade.py model**

```python
from __future__ import annotations

from datetime import datetime, date
from typing import Optional

from sqlalchemy import String, Integer, Float, Date, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column

from app.models.database import Base


class Trade(Base):
    __tablename__ = "trades"

    id: Mapped[int] = mapped_column(primary_key=True)
    strategy: Mapped[str] = mapped_column(String(30))
    symbol: Mapped[str] = mapped_column(String(10))
    name: Mapped[str] = mapped_column(String(50))
    entry_date: Mapped[date] = mapped_column(Date)
    exit_date: Mapped[date] = mapped_column(Date)
    entry_price: Mapped[int] = mapped_column(Integer)
    exit_price: Mapped[int] = mapped_column(Integer)
    qty: Mapped[int] = mapped_column(Integer)
    return_pct: Mapped[float] = mapped_column(Float)
    pnl: Mapped[int] = mapped_column(Integer)
    holding_days: Mapped[int] = mapped_column(Integer)
    exit_reason: Mapped[str] = mapped_column(String(20))
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
```

- [ ] **Step 5: Create models/__init__.py**

```python
from app.models.database import Base, init_db, get_session, create_tables
from app.models.position import Position
from app.models.order import Order
from app.models.trade import Trade

__all__ = ["Base", "init_db", "get_session", "create_tables", "Position", "Order", "Trade"]
```

- [ ] **Step 6: Write model test**

`tests/test_models/__init__.py`: empty file

`tests/test_models/test_position.py`:
```python
from app.models.position import Position
from app.models.order import Order
from app.models.trade import Trade
from datetime import date


def test_position_model_instantiation():
    pos = Position(
        strategy="volume_breakout",
        symbol="005930",
        name="삼성전자",
        status="pending_buy",
        signal_date=date(2026, 5, 5),
    )
    assert pos.strategy == "volume_breakout"
    assert pos.status == "pending_buy"
    assert pos.holding_days == 0


def test_order_model_instantiation():
    order = Order(
        strategy="volume_breakout",
        symbol="005930",
        side="buy",
        order_type="market",
        qty=10,
        price=0,
        status="submitted",
    )
    assert order.side == "buy"
    assert order.price == 0


def test_trade_model_instantiation():
    trade = Trade(
        strategy="volume_breakout",
        symbol="005930",
        name="삼성전자",
        entry_date=date(2026, 5, 1),
        exit_date=date(2026, 5, 5),
        entry_price=72000,
        exit_price=75000,
        qty=10,
        return_pct=0.0417,
        pnl=30000,
        holding_days=4,
        exit_reason="trailing_stop",
    )
    assert trade.return_pct == 0.0417
```

- [ ] **Step 7: Run tests**

Run: `python -m pytest tests/test_models/ -v`
Expected: 3 tests PASS

- [ ] **Step 8: Commit**

```bash
git add app/models/ tests/test_models/
git commit -m "feat: database models (Position, Order, Trade)"
```

---

### Task 3: Broker Module — Auth & Client

**Files:**
- Create: `app/broker/__init__.py`, `app/broker/auth.py`, `app/broker/client.py`
- Test: `tests/test_broker/test_auth.py`, `tests/test_broker/test_client.py`

- [ ] **Step 1: Create app/broker/client.py**

```python
from __future__ import annotations

import logging
from typing import Any

import httpx

from app.config import KISConfig

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
TIMEOUT = 10.0


class KISClient:
    """Base HTTP client for 한투 OpenAPI."""

    def __init__(self, config: KISConfig):
        self.config = config
        self._client = httpx.AsyncClient(
            base_url=config.base_url,
            timeout=TIMEOUT,
        )
        self._token: str = ""
        self._token_expires: float = 0.0

    async def close(self):
        await self._client.aclose()

    def _headers(self, tr_id: str) -> dict[str, str]:
        return {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {self._token}",
            "appkey": self.config.app_key,
            "appsecret": self.config.app_secret,
            "tr_id": tr_id,
        }

    async def request(
        self,
        method: str,
        path: str,
        tr_id: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Make authenticated request with retry logic."""
        headers = self._headers(tr_id)

        for attempt in range(MAX_RETRIES):
            try:
                if method == "GET":
                    resp = await self._client.get(path, headers=headers, params=params)
                else:
                    resp = await self._client.post(path, headers=headers, json=json_body)

                data = resp.json()

                # Token expired — refresh and retry once
                if resp.status_code == 401 or data.get("msg_cd") == "EGW00123":
                    if attempt == 0:
                        await self.refresh_token()
                        headers = self._headers(tr_id)
                        continue
                    raise RuntimeError(f"KIS auth failed after refresh: {data}")

                if resp.status_code != 200:
                    raise RuntimeError(f"KIS API error {resp.status_code}: {data}")

                if data.get("rt_cd") != "0":
                    raise RuntimeError(f"KIS biz error: {data.get('msg1', data)}")

                return data

            except httpx.TimeoutException:
                if attempt < MAX_RETRIES - 1:
                    logger.warning(f"Timeout on attempt {attempt + 1}, retrying...")
                    continue
                raise

        raise RuntimeError("Max retries exceeded")

    async def refresh_token(self):
        """Refresh access token from auth module."""
        from app.broker.auth import get_access_token
        self._token = await get_access_token(self.config, self._client)

    @property
    def token(self) -> str:
        return self._token

    @token.setter
    def token(self, value: str):
        self._token = value
```

- [ ] **Step 2: Create app/broker/auth.py**

```python
from __future__ import annotations

import logging
import time

import httpx

from app.config import KISConfig

logger = logging.getLogger(__name__)

_cached_token: str = ""
_token_expires_at: float = 0.0


async def get_access_token(config: KISConfig, client: httpx.AsyncClient) -> str:
    """Get or refresh OAuth access token (24h validity)."""
    global _cached_token, _token_expires_at

    # Return cached if still valid (1 hour buffer)
    if _cached_token and time.time() < _token_expires_at - 3600:
        return _cached_token

    path = "/oauth2/tokenP"
    body = {
        "grant_type": "client_credentials",
        "appkey": config.app_key,
        "appsecret": config.app_secret,
    }

    resp = await client.post(
        f"{config.base_url}{path}",
        json=body,
        timeout=10.0,
    )
    data = resp.json()

    if "access_token" not in data:
        raise RuntimeError(f"Token request failed: {data}")

    _cached_token = data["access_token"]
    # Token valid for ~24h, set expiry conservatively
    _token_expires_at = time.time() + 23 * 3600

    logger.info("KIS access token refreshed")
    return _cached_token


def clear_token_cache():
    """Clear cached token (for testing)."""
    global _cached_token, _token_expires_at
    _cached_token = ""
    _token_expires_at = 0.0
```

- [ ] **Step 3: Create app/broker/__init__.py**

```python
from app.broker.client import KISClient

__all__ = ["KISClient"]
```

- [ ] **Step 4: Write auth test**

`tests/test_broker/__init__.py`: empty

`tests/test_broker/test_auth.py`:
```python
import pytest
from unittest.mock import AsyncMock, patch
from app.broker.auth import get_access_token, clear_token_cache
from app.config import KISConfig


@pytest.fixture(autouse=True)
def reset_cache():
    clear_token_cache()
    yield
    clear_token_cache()


@pytest.mark.asyncio
async def test_get_access_token_success():
    config = KISConfig(app_key="key", app_secret="secret", account_no="12345678-01", env="paper")
    mock_client = AsyncMock()
    mock_client.post.return_value.json.return_value = {
        "access_token": "test_token_123",
        "token_type": "Bearer",
        "expires_in": 86400,
    }

    token = await get_access_token(config, mock_client)
    assert token == "test_token_123"


@pytest.mark.asyncio
async def test_get_access_token_cached():
    config = KISConfig(app_key="key", app_secret="secret", account_no="12345678-01", env="paper")
    mock_client = AsyncMock()
    mock_client.post.return_value.json.return_value = {
        "access_token": "test_token_123",
        "token_type": "Bearer",
        "expires_in": 86400,
    }

    token1 = await get_access_token(config, mock_client)
    token2 = await get_access_token(config, mock_client)
    assert token1 == token2
    # Only called once because of cache
    assert mock_client.post.call_count == 1


@pytest.mark.asyncio
async def test_get_access_token_failure():
    config = KISConfig(app_key="key", app_secret="secret", account_no="12345678-01", env="paper")
    mock_client = AsyncMock()
    mock_client.post.return_value.json.return_value = {"error": "invalid_client"}

    with pytest.raises(RuntimeError, match="Token request failed"):
        await get_access_token(config, mock_client)
```

- [ ] **Step 5: Write client test**

`tests/test_broker/test_client.py`:
```python
import pytest
from app.broker.client import KISClient
from app.config import KISConfig


def test_client_headers():
    config = KISConfig(app_key="mykey", app_secret="mysecret", account_no="12345678-01", env="paper")
    client = KISClient(config)
    client.token = "test_token"
    headers = client._headers("TTTC0802U")
    assert headers["authorization"] == "Bearer test_token"
    assert headers["appkey"] == "mykey"
    assert headers["tr_id"] == "TTTC0802U"


def test_client_base_url_paper():
    config = KISConfig(app_key="k", app_secret="s", account_no="12345678-01", env="paper")
    client = KISClient(config)
    assert "openapivts" in client.config.base_url


def test_client_base_url_real():
    config = KISConfig(app_key="k", app_secret="s", account_no="12345678-01", env="real")
    client = KISClient(config)
    assert "openapivts" not in client.config.base_url
```

- [ ] **Step 6: Run tests**

Run: `python -m pytest tests/test_broker/ -v`
Expected: 6 tests PASS

- [ ] **Step 7: Commit**

```bash
git add app/broker/ tests/test_broker/
git commit -m "feat: broker auth and HTTP client with retry logic"
```

---

### Task 4: Broker Module — Orders & Account

**Files:**
- Create: `app/broker/order.py`, `app/broker/account.py`
- Test: `tests/test_broker/test_order.py`

- [ ] **Step 1: Create app/broker/order.py**

```python
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from app.broker.client import KISClient

logger = logging.getLogger(__name__)


@dataclass
class OrderResult:
    success: bool
    order_no: str = ""
    message: str = ""


class KISOrderAPI:
    """한투 주문 API 래퍼."""

    # tr_id mapping (모의투자 vs 실전)
    TR_IDS = {
        "paper": {"buy": "VTTC0802U", "sell": "VTTC0801U"},
        "real": {"buy": "TTTC0802U", "sell": "TTTC0801U"},
    }

    def __init__(self, client: KISClient):
        self.client = client

    def _tr_id(self, side: str) -> str:
        env = self.client.config.env
        return self.TR_IDS[env][side]

    async def buy_market(self, symbol: str, qty: int) -> OrderResult:
        """시장가 매수 주문."""
        return await self._place_order(symbol, qty, "buy", ord_type="01", price=0)

    async def sell_market(self, symbol: str, qty: int) -> OrderResult:
        """시장가 매도 주문."""
        return await self._place_order(symbol, qty, "sell", ord_type="01", price=0)

    async def set_stop_loss(self, symbol: str, qty: int, price: int) -> OrderResult:
        """지정가 매도 (SL 예약주문)."""
        return await self._place_order(symbol, qty, "sell", ord_type="00", price=price)

    async def cancel_order(self, order_no: str, qty: int) -> OrderResult:
        """주문 취소."""
        env = self.client.config.env
        tr_id = "VTTC0803U" if env == "paper" else "TTTC0803U"

        body = {
            "CANO": self.client.config.account_prefix,
            "ACNT_PRDT_CD": self.client.config.account_suffix,
            "KRX_FWDG_ORD_ORGNO": "",
            "ORGN_ODNO": order_no,
            "ORD_DVSN": "00",
            "RVSE_CNCL_DVSN_CD": "02",  # 02=취소
            "ORD_QTY": str(qty),
            "ORD_UNPR": "0",
        }

        try:
            data = await self.client.request("POST", "/uapi/domestic-stock/v1/trading/order-rvsecncl", tr_id, json_body=body)
            return OrderResult(success=True, order_no=data.get("output", {}).get("ODNO", ""))
        except Exception as e:
            logger.error(f"Cancel order failed: {e}")
            return OrderResult(success=False, message=str(e))

    async def _place_order(self, symbol: str, qty: int, side: str, ord_type: str, price: int) -> OrderResult:
        tr_id = self._tr_id(side)
        body = {
            "CANO": self.client.config.account_prefix,
            "ACNT_PRDT_CD": self.client.config.account_suffix,
            "PDNO": symbol,
            "ORD_DVSN": ord_type,  # 00=지정가, 01=시장가
            "ORD_QTY": str(qty),
            "ORD_UNPR": str(price),
        }

        try:
            data = await self.client.request("POST", "/uapi/domestic-stock/v1/trading/order-cash", tr_id, json_body=body)
            output = data.get("output", {})
            return OrderResult(
                success=True,
                order_no=output.get("ODNO", ""),
            )
        except Exception as e:
            logger.error(f"Order failed [{side} {symbol} qty={qty}]: {e}")
            return OrderResult(success=False, message=str(e))
```

- [ ] **Step 2: Create app/broker/account.py**

```python
from __future__ import annotations

import logging
from dataclasses import dataclass

from app.broker.client import KISClient

logger = logging.getLogger(__name__)


@dataclass
class AccountBalance:
    total_eval: int  # 총 평가금액
    cash: int  # 예수금
    stock_eval: int  # 주식 평가금액
    pnl_today: int  # 오늘 손익


@dataclass
class FilledOrder:
    order_no: str
    symbol: str
    side: str  # buy/sell
    qty: int
    price: int  # 체결 단가
    total_amount: int


class KISAccountAPI:
    """한투 잔고/체결 조회 API."""

    def __init__(self, client: KISClient):
        self.client = client

    async def get_balance(self) -> AccountBalance:
        """계좌 잔고 조회."""
        env = self.client.config.env
        tr_id = "VTTC8434R" if env == "paper" else "TTTC8434R"

        params = {
            "CANO": self.client.config.account_prefix,
            "ACNT_PRDT_CD": self.client.config.account_suffix,
            "AFHR_FLPR_YN": "N",
            "OFL_YN": "",
            "INQR_DVSN": "02",
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "00",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }

        data = await self.client.request("GET", "/uapi/domestic-stock/v1/trading/inquire-balance", tr_id, params=params)
        output2 = data.get("output2", [{}])
        summary = output2[0] if output2 else {}

        return AccountBalance(
            total_eval=int(summary.get("tot_evlu_amt", 0)),
            cash=int(summary.get("dnca_tot_amt", 0)),
            stock_eval=int(summary.get("scts_evlu_amt", 0)),
            pnl_today=int(summary.get("evlu_pfls_smtl_amt", 0)),
        )

    async def get_filled_orders(self, date_str: str) -> list[FilledOrder]:
        """당일 체결 내역 조회."""
        env = self.client.config.env
        tr_id = "VTTC8001R" if env == "paper" else "TTTC8001R"

        params = {
            "CANO": self.client.config.account_prefix,
            "ACNT_PRDT_CD": self.client.config.account_suffix,
            "INQR_STRT_DT": date_str.replace("-", ""),
            "INQR_END_DT": date_str.replace("-", ""),
            "SLL_BUY_DVSN_CD": "00",  # 전체
            "INQR_DVSN": "00",
            "PDNO": "",
            "CCLD_DVSN": "01",  # 체결만
            "ORD_GNO_BRNO": "",
            "ODNO": "",
            "INQR_DVSN_3": "00",
            "INQR_DVSN_1": "",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }

        data = await self.client.request("GET", "/uapi/domestic-stock/v1/trading/inquire-daily-ccld", tr_id, params=params)
        results = []
        for item in data.get("output1", []):
            side = "buy" if item.get("sll_buy_dvsn_cd") == "02" else "sell"
            results.append(FilledOrder(
                order_no=item.get("odno", ""),
                symbol=item.get("pdno", ""),
                side=side,
                qty=int(item.get("tot_ccld_qty", 0)),
                price=int(item.get("avg_prvs", 0)),
                total_amount=int(item.get("tot_ccld_amt", 0)),
            ))
        return results

    async def get_current_price(self, symbol: str) -> int:
        """현재가 조회."""
        tr_id = "FHKST01010100"
        params = {
            "FID_COND_MRKT_DIV_CD": "J",
            "FID_INPUT_ISCD": symbol,
        }
        data = await self.client.request("GET", "/uapi/domestic-stock/v1/quotations/inquire-price", tr_id, params=params)
        output = data.get("output", {})
        return int(output.get("stck_prpr", 0))
```

- [ ] **Step 3: Write order test**

`tests/test_broker/test_order.py`:
```python
import pytest
from unittest.mock import AsyncMock, MagicMock
from app.broker.order import KISOrderAPI, OrderResult
from app.broker.client import KISClient
from app.config import KISConfig


@pytest.fixture
def mock_client():
    config = KISConfig(app_key="k", app_secret="s", account_no="12345678-01", env="paper")
    client = KISClient(config)
    client.request = AsyncMock()
    return client


@pytest.mark.asyncio
async def test_buy_market_success(mock_client):
    mock_client.request.return_value = {"output": {"ODNO": "0001234567"}, "rt_cd": "0"}
    api = KISOrderAPI(mock_client)
    result = await api.buy_market("005930", 10)
    assert result.success is True
    assert result.order_no == "0001234567"


@pytest.mark.asyncio
async def test_sell_market_success(mock_client):
    mock_client.request.return_value = {"output": {"ODNO": "0001234568"}, "rt_cd": "0"}
    api = KISOrderAPI(mock_client)
    result = await api.sell_market("005930", 10)
    assert result.success is True


@pytest.mark.asyncio
async def test_set_stop_loss(mock_client):
    mock_client.request.return_value = {"output": {"ODNO": "0001234569"}, "rt_cd": "0"}
    api = KISOrderAPI(mock_client)
    result = await api.set_stop_loss("005930", 10, 70000)
    assert result.success is True


@pytest.mark.asyncio
async def test_buy_market_failure(mock_client):
    mock_client.request.side_effect = RuntimeError("주문 거부")
    api = KISOrderAPI(mock_client)
    result = await api.buy_market("005930", 10)
    assert result.success is False
    assert "주문 거부" in result.message
```

- [ ] **Step 4: Run tests**

Run: `python -m pytest tests/test_broker/ -v`
Expected: 10 tests PASS

- [ ] **Step 5: Commit**

```bash
git add app/broker/order.py app/broker/account.py tests/test_broker/test_order.py
git commit -m "feat: broker order and account APIs"
```

---

### Task 5: Strategy Module (Port from BacktesterV2)

**Files:**
- Create: `app/strategy/__init__.py`, `app/strategy/indicators.py`, `app/strategy/signals.py`, `app/strategy/universe.py`, `app/strategy/market_filter.py`
- Test: `tests/test_strategy/test_indicators.py`, `tests/test_strategy/test_signals.py`, `tests/test_strategy/test_universe.py`

- [ ] **Step 1: Create app/strategy/indicators.py**

```python
from __future__ import annotations

import pandas as pd


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Add technical indicators to OHLCV DataFrame.

    Expects columns: open, high, low, close, volume.
    """
    df = df.copy()

    # Moving averages
    df["ma5"] = df["close"].rolling(5).mean()
    df["ma10"] = df["close"].rolling(10).mean()
    df["ma20"] = df["close"].rolling(20).mean()
    df["ma60"] = df["close"].rolling(60).mean()

    # Volume
    df["avg_volume_20"] = df["volume"].rolling(20).mean()

    # Trading value
    df["trading_value"] = df["close"] * df["volume"]
    df["avg_trading_value_20"] = df["trading_value"].rolling(20).mean()

    # Returns
    df["return_1d"] = df["close"].pct_change(1)
    df["return_3d"] = df["close"] / df["close"].shift(3) - 1
    df["return_5d"] = df["close"] / df["close"].shift(5) - 1
    df["return_20d"] = df["close"] / df["close"].shift(20) - 1

    # High/Low ranges
    df["high_20"] = df["high"].rolling(20).max()
    df["low_20"] = df["low"].rolling(20).min()

    # ATR
    tr1 = df["high"] - df["low"]
    tr2 = (df["high"] - df["close"].shift(1)).abs()
    tr3 = (df["low"] - df["close"].shift(1)).abs()
    df["true_range"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["atr14"] = df["true_range"].rolling(14).mean()

    return df
```

- [ ] **Step 2: Create app/strategy/signals.py**

Port all 4 signal generators from BacktesterV2 `src/signals.py` — identical logic, only import path changes (`from app.config import StrategyParams`).

```python
from __future__ import annotations

import pandas as pd
import numpy as np

from app.config import StrategyParams


def _check_consecutive_volume_increase(volume: pd.Series, days: int) -> pd.Series:
    result = pd.Series(True, index=volume.index)
    for i in range(1, days + 1):
        result = result & (volume > volume.shift(i))
    return result


def generate_volume_breakout_signals(df: pd.DataFrame, params: StrategyParams) -> pd.DataFrame:
    if len(df) < 20:
        return pd.DataFrame()

    cond_vol_increase = _check_consecutive_volume_increase(df["volume"], params.volume_increase_days)
    cond_vol_ratio = df["volume"] > df["avg_volume_20"] * params.volume_ratio_threshold
    cond_above_ma5 = df["close"] > df["ma5"]
    cond_above_ma20 = df["close"] > df["ma20"]
    cond_ma5_above_ma20 = df["ma5"] > df["ma20"]
    cond_close_above_3d = df["close"] > df["close"].shift(3)
    cond_not_overheat = df["return_5d"] < params.max_return_5d
    cond_bullish = df["close"] > df["open"]

    all_conditions = (
        cond_vol_increase & cond_vol_ratio & cond_above_ma5
        & cond_above_ma20 & cond_ma5_above_ma20
        & cond_close_above_3d & cond_not_overheat & cond_bullish
    )

    signal_df = df.loc[all_conditions].copy()
    if signal_df.empty:
        return pd.DataFrame()

    volume_ratio = signal_df["volume"] / signal_df["avg_volume_20"]
    signal_df["signal_type"] = "volume_breakout"
    signal_df["volume_ratio"] = volume_ratio
    signal_df["score"] = volume_ratio

    return signal_df[["signal_type", "score", "volume_ratio", "return_3d", "return_5d", "close", "ma5", "ma20", "avg_trading_value_20"]]


def generate_pullback_signals(df: pd.DataFrame, params: StrategyParams) -> pd.DataFrame:
    if len(df) < 20:
        return pd.DataFrame()

    signals = []
    trigger_idx = None
    trigger_close = None
    trigger_volume = None
    pullback_detected = False

    for i in range(20, len(df)):
        row = df.iloc[i]

        vol_increasing = all(
            df["volume"].iloc[i - j] > df["volume"].iloc[i - j - 1]
            for j in range(params.volume_increase_days)
        )
        if (
            vol_increasing
            and row["volume"] > row["avg_volume_20"] * 2.0
            and row["close"] > row["ma20"]
            and 0.05 <= row["return_5d"] <= 0.30
        ):
            trigger_idx = i
            trigger_close = row["close"]
            trigger_volume = row["volume"]
            pullback_detected = False
            continue

        if trigger_idx is not None and not pullback_detected:
            days_since = i - trigger_idx
            if days_since > 5:
                trigger_idx = None
                continue
            if (
                row["close"] <= row["ma5"] * 1.02
                and row["close"] > row["ma20"]
                and row["volume"] < trigger_volume * 0.7
                and row["close"] > trigger_close * 0.90
            ):
                pullback_detected = True
                continue

        if pullback_detected and trigger_idx is not None:
            days_since = i - trigger_idx
            if days_since > 5:
                trigger_idx = None
                pullback_detected = False
                continue
            prev = df.iloc[i - 1]
            if row["high"] > prev["high"] and row["close"] > row["open"]:
                volume_ratio = row["volume"] / row["avg_volume_20"]
                signals.append({
                    "date": df.index[i],
                    "signal_type": "pullback_buy",
                    "score": volume_ratio,
                    "volume_ratio": volume_ratio,
                    "return_3d": row.get("return_3d", 0),
                    "return_5d": row.get("return_5d", 0),
                    "close": row["close"],
                    "ma5": row["ma5"],
                    "ma20": row["ma20"],
                    "avg_trading_value_20": row.get("avg_trading_value_20", 0),
                })
                trigger_idx = None
                pullback_detected = False

    if not signals:
        return pd.DataFrame()
    result = pd.DataFrame(signals).set_index("date")
    return result


def generate_high_breakout_signals(df: pd.DataFrame, params: StrategyParams) -> pd.DataFrame:
    if len(df) < 21:
        return pd.DataFrame()

    cond_breakout = df["close"] > df["high_20"].shift(1)
    cond_vol_ratio = df["volume"] > df["avg_volume_20"] * params.volume_ratio_threshold
    cond_above_ma5 = df["close"] > df["ma5"]
    cond_ma5_above_ma20 = df["ma5"] > df["ma20"]
    cond_not_overheat = df["return_5d"] < 0.25
    cond_bullish = df["close"] > df["open"]

    all_conditions = (
        cond_breakout & cond_vol_ratio & cond_above_ma5
        & cond_ma5_above_ma20 & cond_not_overheat & cond_bullish
    )

    signal_df = df.loc[all_conditions].copy()
    if signal_df.empty:
        return pd.DataFrame()

    volume_ratio = signal_df["volume"] / signal_df["avg_volume_20"]
    breakout_score = signal_df["close"] / signal_df["high_20"].shift(1) - 1

    signal_df["signal_type"] = "high_breakout"
    signal_df["volume_ratio"] = volume_ratio
    signal_df["score"] = volume_ratio * 0.6 + breakout_score.fillna(0).rank(pct=True) * 0.4

    return signal_df[["signal_type", "score", "volume_ratio", "return_3d", "return_5d", "close", "ma5", "ma20", "avg_trading_value_20"]]


def generate_combined_signals(df: pd.DataFrame, params: StrategyParams) -> pd.DataFrame:
    if len(df) < 21:
        return pd.DataFrame()

    cond_vol_increase = _check_consecutive_volume_increase(df["volume"], params.volume_increase_days)
    cond_vol_ratio = df["volume"] > df["avg_volume_20"] * params.volume_ratio_threshold
    cond_above_ma5 = df["close"] > df["ma5"]
    cond_above_ma20 = df["close"] > df["ma20"]
    cond_ma5_above_ma20 = df["ma5"] > df["ma20"]
    cond_close_above_3d = df["close"] > df["close"].shift(3)
    cond_not_overheat = df["return_5d"] < params.max_return_5d
    cond_bullish = df["close"] > df["open"]
    cond_breakout = df["close"] > df["high_20"].shift(1)

    all_conditions = (
        cond_vol_increase & cond_vol_ratio & cond_above_ma5
        & cond_above_ma20 & cond_ma5_above_ma20 & cond_close_above_3d
        & cond_not_overheat & cond_bullish & cond_breakout
    )

    signal_df = df.loc[all_conditions].copy()
    if signal_df.empty:
        return pd.DataFrame()

    volume_ratio = signal_df["volume"] / signal_df["avg_volume_20"]
    breakout_score = signal_df["close"] / signal_df["high_20"].shift(1) - 1

    signal_df["signal_type"] = "combined_ac"
    signal_df["volume_ratio"] = volume_ratio
    signal_df["score"] = volume_ratio * 0.5 + breakout_score.fillna(0).rank(pct=True) * 0.5

    return signal_df[["signal_type", "score", "volume_ratio", "return_3d", "return_5d", "close", "ma5", "ma20", "avg_trading_value_20"]]


SIGNAL_GENERATORS = {
    "volume_breakout": generate_volume_breakout_signals,
    "pullback_buy": generate_pullback_signals,
    "high_breakout": generate_high_breakout_signals,
    "combined_ac": generate_combined_signals,
}
```

- [ ] **Step 3: Create app/strategy/universe.py**

```python
from __future__ import annotations

import pandas as pd

from app.config import UniverseConfig


def is_preferred_stock(symbol: str) -> bool:
    if len(symbol) == 6 and symbol[-1] in ("5", "7", "8", "9"):
        return True
    return False


def is_spac(name: str) -> bool:
    spac_keywords = ["스팩", "SPAC", "기업인수"]
    return any(kw in name for kw in spac_keywords)


def filter_universe(
    symbols: list[str],
    data_map: dict[str, pd.DataFrame],
    config: UniverseConfig,
    name_map: dict[str, str] | None = None,
) -> list[str]:
    name_map = name_map or {}
    filtered = []

    for symbol in symbols:
        if config.exclude_preferred and is_preferred_stock(symbol):
            continue
        if config.exclude_spac and is_spac(name_map.get(symbol, "")):
            continue
        if symbol not in data_map:
            continue

        df = data_map[symbol]
        if len(df) < 20:
            continue

        last_row = df.iloc[-1]
        if last_row["close"] < config.min_price:
            continue
        if "avg_trading_value_20" in df.columns:
            if last_row["avg_trading_value_20"] < config.min_avg_trading_value_20:
                continue

        filtered.append(symbol)

    return filtered
```

- [ ] **Step 4: Create app/strategy/market_filter.py**

```python
from __future__ import annotations

import logging

import pandas as pd
from pykrx import stock as pykrx_stock

from app.config import MarketFilterConfig

logger = logging.getLogger(__name__)


def check_market_filter(config: MarketFilterConfig, today_str: str) -> bool:
    """Check if market index is above MA. Returns True if entries are allowed."""
    if not config.enabled:
        return True

    try:
        end = today_str.replace("-", "")
        # Need enough history for MA calculation
        from datetime import datetime, timedelta
        start_dt = datetime.strptime(today_str, "%Y-%m-%d") - timedelta(days=60)
        start = start_dt.strftime("%Y%m%d")

        index_df = pykrx_stock.get_index_ohlcv(start, end, config.index_code)
        if index_df.empty:
            logger.warning("Market filter: no index data, allowing entries")
            return True

        ma = index_df["종가"].rolling(config.ma_period).mean()
        last_close = index_df["종가"].iloc[-1]
        last_ma = ma.iloc[-1]

        if pd.isna(last_ma):
            return True

        allowed = last_close > last_ma
        logger.info(f"Market filter: KOSPI {last_close:,.0f} vs MA{config.ma_period} {last_ma:,.0f} -> {'OPEN' if allowed else 'BLOCKED'}")
        return allowed

    except Exception as e:
        logger.error(f"Market filter error: {e}")
        return True  # fail-open
```

- [ ] **Step 5: Create app/strategy/__init__.py**

```python
from app.strategy.signals import SIGNAL_GENERATORS
from app.strategy.indicators import add_indicators
from app.strategy.universe import filter_universe
from app.strategy.market_filter import check_market_filter

__all__ = ["SIGNAL_GENERATORS", "add_indicators", "filter_universe", "check_market_filter"]
```

- [ ] **Step 6: Write indicator test**

`tests/test_strategy/__init__.py`: empty

`tests/test_strategy/test_indicators.py`:
```python
import pandas as pd
import numpy as np
from app.strategy.indicators import add_indicators


def _make_ohlcv(n=30):
    """Create sample OHLCV data."""
    dates = pd.date_range("2026-01-01", periods=n, freq="B")
    np.random.seed(42)
    close = 50000 + np.cumsum(np.random.randn(n) * 500)
    return pd.DataFrame({
        "open": close - 200,
        "high": close + 300,
        "low": close - 400,
        "close": close,
        "volume": np.random.randint(100000, 1000000, n),
    }, index=dates)


def test_add_indicators_columns():
    df = _make_ohlcv(30)
    result = add_indicators(df)
    assert "ma5" in result.columns
    assert "ma20" in result.columns
    assert "atr14" in result.columns
    assert "avg_volume_20" in result.columns
    assert "return_5d" in result.columns
    assert "high_20" in result.columns


def test_add_indicators_no_mutation():
    df = _make_ohlcv(30)
    original_cols = list(df.columns)
    add_indicators(df)
    assert list(df.columns) == original_cols


def test_ma5_calculation():
    df = _make_ohlcv(30)
    result = add_indicators(df)
    expected_ma5 = df["close"].iloc[-5:].mean()
    assert abs(result["ma5"].iloc[-1] - expected_ma5) < 0.01
```

- [ ] **Step 7: Write signal test**

`tests/test_strategy/test_signals.py`:
```python
import pandas as pd
import numpy as np
from app.strategy.indicators import add_indicators
from app.strategy.signals import generate_volume_breakout_signals, SIGNAL_GENERATORS
from app.config import StrategyParams


def _make_breakout_data():
    """Create data that triggers volume breakout signal on last day."""
    n = 30
    dates = pd.date_range("2026-01-01", periods=n, freq="B")
    close = np.array([50000 + i * 100 for i in range(n)], dtype=float)
    volume = np.array([500000] * n, dtype=float)
    # Last 4 days: consecutive volume increase
    volume[-4] = 600000
    volume[-3] = 800000
    volume[-2] = 1200000
    volume[-1] = 5000000  # 10x avg

    df = pd.DataFrame({
        "open": close - 100,
        "high": close + 200,
        "low": close - 200,
        "close": close,
        "volume": volume,
    }, index=dates)
    return add_indicators(df)


def test_volume_breakout_signal_generated():
    df = _make_breakout_data()
    params = StrategyParams(name="volume_breakout", volume_increase_days=3, volume_ratio_threshold=3.0, max_return_5d=0.20)
    signals = generate_volume_breakout_signals(df, params)
    assert not signals.empty
    assert signals.iloc[-1]["signal_type"] == "volume_breakout"


def test_volume_breakout_no_signal_low_volume():
    n = 30
    dates = pd.date_range("2026-01-01", periods=n, freq="B")
    close = np.array([50000 + i * 100 for i in range(n)], dtype=float)
    volume = np.array([500000] * n, dtype=float)  # flat volume

    df = pd.DataFrame({
        "open": close - 100, "high": close + 200,
        "low": close - 200, "close": close, "volume": volume,
    }, index=dates)
    df = add_indicators(df)
    params = StrategyParams(name="volume_breakout")
    signals = generate_volume_breakout_signals(df, params)
    assert signals.empty


def test_signal_generators_registry():
    assert "volume_breakout" in SIGNAL_GENERATORS
    assert "pullback_buy" in SIGNAL_GENERATORS
    assert "high_breakout" in SIGNAL_GENERATORS
    assert "combined_ac" in SIGNAL_GENERATORS
```

- [ ] **Step 8: Write universe test**

`tests/test_strategy/test_universe.py`:
```python
import pandas as pd
import numpy as np
from app.strategy.universe import filter_universe, is_preferred_stock, is_spac
from app.config import UniverseConfig


def test_is_preferred_stock():
    assert is_preferred_stock("005935") is True
    assert is_preferred_stock("005930") is False


def test_is_spac():
    assert is_spac("교보11호스팩") is True
    assert is_spac("삼성전자") is False


def test_filter_universe_basic():
    config = UniverseConfig(min_price=1000, min_avg_trading_value_20=1_000_000)
    dates = pd.date_range("2026-01-01", periods=25, freq="B")
    good_df = pd.DataFrame({
        "close": [50000] * 25,
        "avg_trading_value_20": [10_000_000] * 25,
    }, index=dates)
    cheap_df = pd.DataFrame({
        "close": [500] * 25,
        "avg_trading_value_20": [10_000_000] * 25,
    }, index=dates)

    data_map = {"005930": good_df, "999990": cheap_df}
    result = filter_universe(["005930", "999990"], data_map, config)
    assert "005930" in result
    assert "999990" not in result
```

- [ ] **Step 9: Run tests**

Run: `python -m pytest tests/test_strategy/ -v`
Expected: 9 tests PASS

- [ ] **Step 10: Commit**

```bash
git add app/strategy/ tests/test_strategy/
git commit -m "feat: strategy module (indicators, signals, universe, market filter)"
```

---

### Task 6: Trader Module (Executor, SL Manager, Quantity)

**Files:**
- Create: `app/trader/__init__.py`, `app/trader/executor.py`, `app/trader/sl_manager.py`, `app/trader/quantity.py`
- Test: `tests/test_trader/test_quantity.py`, `tests/test_trader/test_executor.py`, `tests/test_trader/test_sl_manager.py`

- [ ] **Step 1: Create app/trader/quantity.py**

```python
from __future__ import annotations

import math


def calc_quantity(total_eval: int, capital_allocation: float, position_weight: float, price: int) -> int:
    """Calculate buy quantity for a position.

    Args:
        total_eval: Total account evaluation amount
        capital_allocation: Fraction allocated to this strategy (e.g., 0.25)
        position_weight: Fraction per position (e.g., 0.20)
        price: Current stock price

    Returns:
        Number of shares to buy (0 if insufficient)
    """
    if price <= 0:
        return 0
    budget = total_eval * capital_allocation * position_weight
    qty = math.floor(budget / price)
    return max(0, qty)
```

- [ ] **Step 2: Create app/trader/sl_manager.py**

```python
from __future__ import annotations

import logging

from app.broker.order import KISOrderAPI, OrderResult
from app.models.position import Position

logger = logging.getLogger(__name__)


class SLManager:
    """Manage stop-loss reservation orders."""

    def __init__(self, order_api: KISOrderAPI):
        self.order_api = order_api

    async def register_sl(self, position: Position) -> OrderResult:
        """Register initial SL order after buy fill."""
        if not position.sl_price or not position.qty:
            return OrderResult(success=False, message="Missing sl_price or qty")

        result = await self.order_api.set_stop_loss(
            position.symbol, position.qty, position.sl_price
        )
        if result.success:
            logger.info(f"SL registered: {position.symbol} qty={position.qty} @ {position.sl_price}")
        return result

    async def update_sl(self, position: Position, new_sl_price: int) -> OrderResult:
        """Update SL order (cancel old, place new)."""
        if position.sl_order_no:
            cancel_result = await self.order_api.cancel_order(position.sl_order_no, position.qty)
            if not cancel_result.success:
                logger.warning(f"Failed to cancel old SL for {position.symbol}: {cancel_result.message}")

        result = await self.order_api.set_stop_loss(
            position.symbol, position.qty, new_sl_price
        )
        if result.success:
            logger.info(f"SL updated: {position.symbol} new SL={new_sl_price}")
        return result
```

- [ ] **Step 3: Create app/trader/executor.py**

```python
from __future__ import annotations

import logging
from datetime import date, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.broker.order import KISOrderAPI
from app.broker.account import KISAccountAPI
from app.models.position import Position
from app.models.order import Order
from app.models.trade import Trade
from app.trader.quantity import calc_quantity
from app.trader.sl_manager import SLManager
from app.config import StrategyParams

logger = logging.getLogger(__name__)


class OrderExecutor:
    """Orchestrates buy/sell order execution."""

    def __init__(self, order_api: KISOrderAPI, account_api: KISAccountAPI, sl_manager: SLManager):
        self.order_api = order_api
        self.account_api = account_api
        self.sl_manager = sl_manager

    async def execute_sells(self, session: AsyncSession) -> list[dict]:
        """Execute all pending sell orders."""
        result = await session.execute(
            select(Position).where(Position.status == "pending_sell")
        )
        positions = result.scalars().all()
        results = []

        for pos in positions:
            order_result = await self.order_api.sell_market(pos.symbol, pos.qty)

            # Record order
            order = Order(
                position_id=pos.id,
                strategy=pos.strategy,
                symbol=pos.symbol,
                side="sell",
                order_type="market",
                qty=pos.qty,
                price=0,
                order_no=order_result.order_no if order_result.success else None,
                status="submitted" if order_result.success else "failed",
            )
            session.add(order)

            results.append({
                "symbol": pos.symbol,
                "name": pos.name,
                "success": order_result.success,
                "order_no": order_result.order_no,
                "message": order_result.message,
            })

        await session.commit()
        return results

    async def execute_buys(self, session: AsyncSession, strategy_configs: dict[str, StrategyParams]) -> list[dict]:
        """Execute all pending buy orders."""
        result = await session.execute(
            select(Position).where(Position.status == "pending_buy")
        )
        positions = result.scalars().all()
        results = []

        balance = await self.account_api.get_balance()

        for pos in positions:
            config = strategy_configs.get(pos.strategy)
            if not config:
                logger.error(f"No config for strategy {pos.strategy}")
                continue

            price = await self.account_api.get_current_price(pos.symbol)
            qty = calc_quantity(balance.total_eval, config.capital_allocation, config.position_weight, price)

            if qty <= 0:
                logger.warning(f"Insufficient funds for {pos.symbol}, skipping")
                results.append({"symbol": pos.symbol, "name": pos.name, "success": False, "message": "잔고 부족"})
                continue

            order_result = await self.order_api.buy_market(pos.symbol, qty)

            order = Order(
                position_id=pos.id,
                strategy=pos.strategy,
                symbol=pos.symbol,
                side="buy",
                order_type="market",
                qty=qty,
                price=0,
                order_no=order_result.order_no if order_result.success else None,
                status="submitted" if order_result.success else "failed",
            )
            session.add(order)

            if order_result.success:
                pos.qty = qty

            results.append({
                "symbol": pos.symbol,
                "name": pos.name,
                "qty": qty,
                "success": order_result.success,
                "order_no": order_result.order_no,
                "message": order_result.message,
            })

        await session.commit()
        return results

    async def confirm_fills(self, session: AsyncSession, today: str) -> list[dict]:
        """Check fills and update positions."""
        filled_orders = await self.account_api.get_filled_orders(today)
        results = []

        # Match fills to submitted orders
        stmt = select(Order).where(Order.status == "submitted")
        db_orders = (await session.execute(stmt)).scalars().all()

        for db_order in db_orders:
            fill = next((f for f in filled_orders if f.order_no == db_order.order_no), None)
            if not fill:
                continue

            db_order.status = "filled"
            db_order.filled_price = fill.price
            db_order.filled_qty = fill.qty
            db_order.filled_at = datetime.now()

            # Update position
            pos = await session.get(Position, db_order.position_id)
            if not pos:
                continue

            if db_order.side == "buy":
                pos.status = "active"
                pos.entry_date = date.today()
                pos.entry_price = fill.price
                pos.peak_price = fill.price
                pos.qty = fill.qty

                # Calculate and set SL
                if pos.entry_atr and pos.entry_atr > 0:
                    pos.sl_price = int(fill.price - pos.entry_atr * 0.5)
                else:
                    pos.sl_price = int(fill.price * 0.97)

                # Register SL order
                sl_result = await self.sl_manager.register_sl(pos)
                if sl_result.success:
                    pos.sl_order_no = sl_result.order_no

                results.append({
                    "type": "buy_filled",
                    "symbol": pos.symbol,
                    "name": pos.name,
                    "price": fill.price,
                    "qty": fill.qty,
                    "sl_price": pos.sl_price,
                })

            elif db_order.side == "sell":
                # Record trade
                trade = Trade(
                    strategy=pos.strategy,
                    symbol=pos.symbol,
                    name=pos.name,
                    entry_date=pos.entry_date,
                    exit_date=date.today(),
                    entry_price=pos.entry_price,
                    exit_price=fill.price,
                    qty=fill.qty,
                    return_pct=(fill.price / pos.entry_price - 1) if pos.entry_price else 0,
                    pnl=(fill.price - pos.entry_price) * fill.qty if pos.entry_price else 0,
                    holding_days=pos.holding_days,
                    exit_reason=pos.exit_reason or "manual",
                )
                session.add(trade)

                # Remove position
                await session.delete(pos)

                results.append({
                    "type": "sell_filled",
                    "symbol": trade.symbol,
                    "name": trade.name,
                    "price": fill.price,
                    "return_pct": trade.return_pct,
                    "pnl": trade.pnl,
                })

        await session.commit()
        return results
```

- [ ] **Step 4: Create app/trader/__init__.py**

```python
from app.trader.executor import OrderExecutor
from app.trader.quantity import calc_quantity
from app.trader.sl_manager import SLManager

__all__ = ["OrderExecutor", "calc_quantity", "SLManager"]
```

- [ ] **Step 5: Write quantity test**

`tests/test_trader/__init__.py`: empty

`tests/test_trader/test_quantity.py`:
```python
from app.trader.quantity import calc_quantity


def test_calc_quantity_basic():
    # 40M total, 25% allocation, 20% weight, price 72000
    qty = calc_quantity(40_000_000, 0.25, 0.20, 72000)
    # Budget = 40M * 0.25 * 0.20 = 2M / 72000 = 27.77 -> 27
    assert qty == 27


def test_calc_quantity_zero_price():
    assert calc_quantity(40_000_000, 0.25, 0.20, 0) == 0


def test_calc_quantity_expensive_stock():
    # Budget = 2M, price = 3M -> can't afford
    qty = calc_quantity(40_000_000, 0.25, 0.20, 3_000_000)
    assert qty == 0


def test_calc_quantity_cheap_stock():
    # Budget = 2M, price = 1000 -> 2000 shares
    qty = calc_quantity(40_000_000, 0.25, 0.20, 1000)
    assert qty == 2000
```

- [ ] **Step 6: Write SL manager test**

`tests/test_trader/test_sl_manager.py`:
```python
import pytest
from unittest.mock import AsyncMock
from app.trader.sl_manager import SLManager
from app.broker.order import KISOrderAPI, OrderResult
from app.models.position import Position
from datetime import date


@pytest.fixture
def sl_manager():
    mock_order_api = AsyncMock(spec=KISOrderAPI)
    return SLManager(mock_order_api)


@pytest.mark.asyncio
async def test_register_sl_success(sl_manager):
    sl_manager.order_api.set_stop_loss.return_value = OrderResult(success=True, order_no="SL001")
    pos = Position(symbol="005930", qty=10, sl_price=70000, strategy="vol", name="삼성", status="active", signal_date=date.today())

    result = await sl_manager.register_sl(pos)
    assert result.success is True
    assert result.order_no == "SL001"


@pytest.mark.asyncio
async def test_register_sl_missing_price(sl_manager):
    pos = Position(symbol="005930", qty=10, sl_price=None, strategy="vol", name="삼성", status="active", signal_date=date.today())
    result = await sl_manager.register_sl(pos)
    assert result.success is False


@pytest.mark.asyncio
async def test_update_sl(sl_manager):
    sl_manager.order_api.cancel_order.return_value = OrderResult(success=True)
    sl_manager.order_api.set_stop_loss.return_value = OrderResult(success=True, order_no="SL002")
    pos = Position(symbol="005930", qty=10, sl_price=70000, sl_order_no="SL001", strategy="vol", name="삼성", status="active", signal_date=date.today())

    result = await sl_manager.update_sl(pos, 71000)
    assert result.success is True
    sl_manager.order_api.cancel_order.assert_called_once_with("SL001", 10)
```

- [ ] **Step 7: Write executor test**

`tests/test_trader/test_executor.py`:
```python
from app.trader.quantity import calc_quantity


def test_executor_quantity_integration():
    """Verify quantity calculation in realistic scenario."""
    # 4000만원 계좌, volume_breakout (25%), 5종목 균등(20%)
    qty = calc_quantity(40_000_000, 0.25, 0.20, 72000)
    assert qty == 27
    assert qty * 72000 <= 40_000_000 * 0.25 * 0.20
```

- [ ] **Step 8: Run tests**

Run: `python -m pytest tests/test_trader/ -v`
Expected: 8 tests PASS

- [ ] **Step 9: Commit**

```bash
git add app/trader/ tests/test_trader/
git commit -m "feat: trader module (executor, SL manager, quantity calc)"
```

---

### Task 7: Notifier (Discord)

**Files:**
- Create: `app/notifier/__init__.py`, `app/notifier/discord.py`

- [ ] **Step 1: Create app/notifier/discord.py**

```python
from __future__ import annotations

import logging
import os

import httpx

logger = logging.getLogger(__name__)


class DiscordNotifier:
    """Send alerts via Discord webhook."""

    def __init__(self, webhook_url: str = ""):
        self.webhook_url = webhook_url or os.environ.get("DISCORD_WEBHOOK_URL", "")

    async def send(self, message: str) -> bool:
        if not self.webhook_url:
            logger.warning("Discord webhook not configured")
            return False

        chunks = self._split(message, 1900)
        async with httpx.AsyncClient() as client:
            for chunk in chunks:
                try:
                    resp = await client.post(
                        self.webhook_url,
                        json={"content": chunk},
                        timeout=10.0,
                    )
                    if resp.status_code not in (200, 204):
                        logger.error(f"Discord error: {resp.status_code}")
                        return False
                except Exception as e:
                    logger.error(f"Discord send failed: {e}")
                    return False
        return True

    async def send_signal_alert(self, alerts: dict) -> bool:
        msg = self._format_signal_alert(alerts)
        return await self.send(msg)

    async def send_order_alert(self, results: list[dict], title: str = "주문 결과") -> bool:
        lines = [f"## {title}"]
        for r in results:
            icon = "✅" if r.get("success") else "❌"
            lines.append(f"{icon} {r.get('symbol', '')} {r.get('name', '')} — {r.get('message', 'OK')}")
        return await self.send("\n".join(lines))

    async def send_error(self, error: str) -> bool:
        return await self.send(f"🚨 **ERROR:** {error}")

    def _format_signal_alert(self, alerts: dict) -> str:
        lines = [f"## 📊 Signal Alert {alerts.get('date', '')}"]

        if alerts.get("pending_sells"):
            lines.append("\n**🔻 SELL Tomorrow**")
            for s in alerts["pending_sells"]:
                lines.append(f"> {s['symbol']} {s['name']} | {s['exit_reason']} | {s.get('return_pct', 0):+.1%}")

        if alerts.get("pending_buys"):
            lines.append("\n**🔺 BUY Tomorrow**")
            for b in alerts["pending_buys"]:
                lines.append(f"> {b['symbol']} {b['name']} | score {b.get('score', 0):.1f} | SL {b.get('sl_price', 0):,}")

        if not alerts.get("pending_sells") and not alerts.get("pending_buys"):
            lines.append("\n💤 No action needed")

        return "\n".join(lines)

    @staticmethod
    def _split(text: str, max_len: int) -> list[str]:
        if len(text) <= max_len:
            return [text]
        chunks = []
        current = ""
        for line in text.split("\n"):
            if len(current) + len(line) + 1 > max_len:
                chunks.append(current)
                current = line
            else:
                current = f"{current}\n{line}" if current else line
        if current:
            chunks.append(current)
        return chunks
```

- [ ] **Step 2: Create app/notifier/__init__.py**

```python
from app.notifier.discord import DiscordNotifier

__all__ = ["DiscordNotifier"]
```

- [ ] **Step 3: Commit**

```bash
git add app/notifier/
git commit -m "feat: Discord notifier module"
```

---

### Task 8: Jobs (Scheduler Tasks)

**Files:**
- Create: `app/jobs/__init__.py`, `app/jobs/signal_job.py`, `app/jobs/order_job.py`, `app/jobs/confirm_job.py`
- Test: `tests/test_jobs/__init__.py`, `tests/test_jobs/test_signal_job.py`

- [ ] **Step 1: Create app/jobs/signal_job.py**

```python
from __future__ import annotations

import logging
from datetime import datetime, timedelta, date

import pandas as pd
from pykrx import stock as pykrx_stock
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import StrategyParams, MarketFilterConfig, UniverseConfig
from app.models.position import Position
from app.strategy import SIGNAL_GENERATORS, add_indicators, filter_universe, check_market_filter
from app.notifier.discord import DiscordNotifier

logger = logging.getLogger(__name__)


async def run_signal_job(
    session: AsyncSession,
    strategy_configs: dict[str, StrategyParams],
    market_filter_config: MarketFilterConfig,
    universe_config: UniverseConfig,
    notifier: DiscordNotifier,
):
    """Daily signal generation job (runs at 15:40)."""
    today = date.today()
    today_str = today.strftime("%Y-%m-%d")
    today_fmt = today.strftime("%Y%m%d")
    lookback_start = (today - timedelta(days=120)).strftime("%Y%m%d")

    logger.info(f"Signal job started for {today_str}")

    # Check market filter
    market_open = check_market_filter(market_filter_config, today_str)

    # Get all currently held/pending symbols across all strategies
    stmt = select(Position.symbol).where(Position.status.in_(["active", "pending_buy"]))
    existing_symbols = set((await session.execute(stmt)).scalars().all())

    alerts = {"date": today_str, "pending_buys": [], "pending_sells": []}

    for strategy_name, config in strategy_configs.items():
        gen_func = SIGNAL_GENERATORS.get(strategy_name)
        if not gen_func:
            logger.warning(f"No signal generator for {strategy_name}")
            continue

        # 1. Check exits for active positions of this strategy
        stmt = select(Position).where(
            Position.strategy == strategy_name,
            Position.status == "active",
        )
        active_positions = (await session.execute(stmt)).scalars().all()

        for pos in active_positions:
            try:
                df = pykrx_stock.get_market_ohlcv(lookback_start, today_fmt, pos.symbol)
                if df.empty:
                    continue

                df.columns = ["open", "high", "low", "close", "volume"]
                today_close = int(df["close"].iloc[-1])
                today_high = int(df["high"].iloc[-1])

                pos.holding_days += 1
                pos.peak_price = max(pos.peak_price or 0, today_high)
                pos.trail_price = int(pos.peak_price * (1 - config.trailing_stop_pct))

                exit_reason = _check_exit(pos, today_close, config)
                if exit_reason:
                    pos.status = "pending_sell"
                    pos.exit_reason = exit_reason
                    alerts["pending_sells"].append({
                        "symbol": pos.symbol,
                        "name": pos.name,
                        "exit_reason": exit_reason,
                        "return_pct": (today_close / pos.entry_price - 1) if pos.entry_price else 0,
                    })
            except Exception as e:
                logger.error(f"Exit check failed for {pos.symbol}: {e}")

        # 2. Generate new buy signals (if market allows)
        if not market_open:
            continue

        stmt = select(Position).where(
            Position.strategy == strategy_name,
            Position.status.in_(["active", "pending_buy"]),
        )
        current_count = len((await session.execute(stmt)).scalars().all())
        available_slots = config.max_positions - current_count

        if available_slots <= 0:
            continue

        try:
            # Load universe
            all_symbols = pykrx_stock.get_market_ticker_list(today_fmt, market="ALL")

            # Load OHLCV for all symbols (batch)
            data_map = {}
            name_map = {}
            for sym in all_symbols:
                try:
                    ohlcv = pykrx_stock.get_market_ohlcv(lookback_start, today_fmt, sym)
                    if ohlcv.empty or len(ohlcv) < 20:
                        continue
                    ohlcv.columns = ["open", "high", "low", "close", "volume"]
                    data_map[sym] = add_indicators(ohlcv)
                    name_map[sym] = pykrx_stock.get_market_ticker_name(sym)
                except Exception:
                    continue

            # Filter universe
            filtered = filter_universe(list(data_map.keys()), data_map, universe_config, name_map)

            # Generate signals
            candidates = []
            for sym in filtered:
                if sym in existing_symbols:
                    continue
                df = data_map[sym]
                signals = gen_func(df, config)
                if not signals.empty and df.index[-1] in signals.index:
                    row = signals.loc[df.index[-1]]
                    entry_atr = float(df["atr14"].iloc[-1]) if "atr14" in df.columns else 0.0
                    candidates.append({
                        "symbol": sym,
                        "name": name_map.get(sym, sym),
                        "score": float(row["score"]),
                        "close": float(df["close"].iloc[-1]),
                        "entry_atr": entry_atr,
                    })

            # Top N by score
            candidates.sort(key=lambda x: x["score"], reverse=True)
            for c in candidates[:available_slots]:
                sl_price = int(c["close"] - c["entry_atr"] * config.atr_sl_multiplier) if c["entry_atr"] > 0 else int(c["close"] * (1 - config.stop_loss_pct))

                pos = Position(
                    strategy=strategy_name,
                    symbol=c["symbol"],
                    name=c["name"],
                    status="pending_buy",
                    signal_date=today,
                    entry_atr=c["entry_atr"],
                    sl_price=sl_price,
                )
                session.add(pos)
                existing_symbols.add(c["symbol"])

                alerts["pending_buys"].append({
                    "symbol": c["symbol"],
                    "name": c["name"],
                    "score": c["score"],
                    "sl_price": sl_price,
                })

        except Exception as e:
            logger.error(f"Signal generation failed for {strategy_name}: {e}")

    await session.commit()

    # Send Discord alert
    await notifier.send_signal_alert(alerts)
    logger.info(f"Signal job complete: {len(alerts['pending_buys'])} buys, {len(alerts['pending_sells'])} sells")


def _check_exit(pos: Position, today_close: int, config: StrategyParams) -> str | None:
    """Check exit conditions. Returns reason or None."""
    # Skip SL check for first N days
    if pos.holding_days <= config.sl_skip_days:
        return None

    # Stop loss
    if pos.sl_price and today_close <= pos.sl_price:
        return "stop_loss"

    # Trailing stop
    if pos.trail_price and today_close <= pos.trail_price:
        return "trailing_stop"

    # Time exit
    if pos.holding_days >= config.max_holding_days:
        return "time_exit"

    return None
```

- [ ] **Step 2: Create app/jobs/order_job.py**

```python
from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import AsyncSession

from app.trader.executor import OrderExecutor
from app.notifier.discord import DiscordNotifier
from app.config import StrategyParams

logger = logging.getLogger(__name__)


async def run_order_job(
    session: AsyncSession,
    executor: OrderExecutor,
    strategy_configs: dict[str, StrategyParams],
    notifier: DiscordNotifier,
):
    """Order submission job (runs at 08:59)."""
    logger.info("Order job started")

    # 1. Execute sells first
    sell_results = await executor.execute_sells(session)
    if sell_results:
        await notifier.send_order_alert(sell_results, "매도 주문")

    # 2. Execute buys
    buy_results = await executor.execute_buys(session, strategy_configs)
    if buy_results:
        await notifier.send_order_alert(buy_results, "매수 주문")

    total = len(sell_results) + len(buy_results)
    logger.info(f"Order job complete: {len(sell_results)} sells, {len(buy_results)} buys")
```

- [ ] **Step 3: Create app/jobs/confirm_job.py**

```python
from __future__ import annotations

import logging
from datetime import date

from sqlalchemy.ext.asyncio import AsyncSession

from app.trader.executor import OrderExecutor
from app.notifier.discord import DiscordNotifier

logger = logging.getLogger(__name__)


async def run_confirm_job(
    session: AsyncSession,
    executor: OrderExecutor,
    notifier: DiscordNotifier,
):
    """Fill confirmation job (runs at 09:05)."""
    logger.info("Confirm job started")

    today_str = date.today().strftime("%Y-%m-%d")
    results = await executor.confirm_fills(session, today_str)

    if results:
        lines = ["## ✅ 체결 확인"]
        for r in results:
            if r["type"] == "buy_filled":
                lines.append(f"> 매수 {r['symbol']} {r['name']} @ {r['price']:,} x {r['qty']} → SL {r['sl_price']:,}")
            elif r["type"] == "sell_filled":
                lines.append(f"> 매도 {r['symbol']} {r['name']} @ {r['price']:,} | {r['return_pct']:+.1%} | PnL {r['pnl']:+,}")
        await notifier.send("\n".join(lines))

    logger.info(f"Confirm job complete: {len(results)} fills confirmed")
```

- [ ] **Step 4: Create app/jobs/__init__.py**

```python
from app.jobs.signal_job import run_signal_job
from app.jobs.order_job import run_order_job
from app.jobs.confirm_job import run_confirm_job

__all__ = ["run_signal_job", "run_order_job", "run_confirm_job"]
```

- [ ] **Step 5: Write signal job test**

`tests/test_jobs/__init__.py`: empty

`tests/test_jobs/test_signal_job.py`:
```python
from app.jobs.signal_job import _check_exit
from app.models.position import Position
from app.config import StrategyParams
from datetime import date


def test_check_exit_stop_loss():
    pos = Position(
        strategy="vol", symbol="005930", name="삼성", status="active",
        signal_date=date.today(), holding_days=3, sl_price=70000, trail_price=72000,
    )
    config = StrategyParams(name="vol", sl_skip_days=2, max_holding_days=10, trailing_stop_pct=0.05)
    result = _check_exit(pos, 69000, config)
    assert result == "stop_loss"


def test_check_exit_trailing_stop():
    pos = Position(
        strategy="vol", symbol="005930", name="삼성", status="active",
        signal_date=date.today(), holding_days=5, sl_price=68000, trail_price=72000,
    )
    config = StrategyParams(name="vol", sl_skip_days=2, max_holding_days=10, trailing_stop_pct=0.05)
    result = _check_exit(pos, 71000, config)
    assert result == "trailing_stop"


def test_check_exit_time():
    pos = Position(
        strategy="vol", symbol="005930", name="삼성", status="active",
        signal_date=date.today(), holding_days=10, sl_price=68000, trail_price=72000,
    )
    config = StrategyParams(name="vol", sl_skip_days=2, max_holding_days=10, trailing_stop_pct=0.05)
    result = _check_exit(pos, 80000, config)
    assert result == "time_exit"


def test_check_exit_skip_days():
    pos = Position(
        strategy="vol", symbol="005930", name="삼성", status="active",
        signal_date=date.today(), holding_days=1, sl_price=68000, trail_price=72000,
    )
    config = StrategyParams(name="vol", sl_skip_days=2, max_holding_days=10, trailing_stop_pct=0.05)
    result = _check_exit(pos, 60000, config)  # Below SL but in skip period
    assert result is None


def test_check_exit_no_exit():
    pos = Position(
        strategy="vol", symbol="005930", name="삼성", status="active",
        signal_date=date.today(), holding_days=5, sl_price=68000, trail_price=70000,
    )
    config = StrategyParams(name="vol", sl_skip_days=2, max_holding_days=10, trailing_stop_pct=0.05)
    result = _check_exit(pos, 75000, config)
    assert result is None
```

- [ ] **Step 6: Run tests**

Run: `python -m pytest tests/test_jobs/ -v`
Expected: 5 tests PASS

- [ ] **Step 7: Commit**

```bash
git add app/jobs/ tests/test_jobs/
git commit -m "feat: scheduler jobs (signal, order, confirm)"
```

---

### Task 9: Dashboard

**Files:**
- Create: `app/dashboard/__init__.py`, `app/dashboard/router.py`, `app/dashboard/templates/dashboard.html`

- [ ] **Step 1: Create app/dashboard/router.py**

```python
from __future__ import annotations

from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import get_session, Position, Order, Trade
from app.config import AppSettings

router = APIRouter()
templates = Jinja2Templates(directory="app/dashboard/templates")


def verify_token(request: Request):
    """Simple bearer token auth for dashboard."""
    settings = AppSettings()
    if not settings.dashboard_token:
        return  # No auth configured
    token = request.query_params.get("token", "")
    if token != settings.dashboard_token:
        auth = request.headers.get("Authorization", "")
        if auth != f"Bearer {settings.dashboard_token}":
            raise HTTPException(status_code=401, detail="Unauthorized")


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, session: AsyncSession = Depends(get_session), _=Depends(verify_token)):
    # Active positions
    stmt = select(Position).where(Position.status.in_(["active", "pending_buy", "pending_sell"])).order_by(Position.strategy)
    positions = (await session.execute(stmt)).scalars().all()

    # Recent trades
    stmt = select(Trade).order_by(desc(Trade.created_at)).limit(10)
    trades = (await session.execute(stmt)).scalars().all()

    # Today's orders
    stmt = select(Order).order_by(desc(Order.submitted_at)).limit(20)
    orders = (await session.execute(stmt)).scalars().all()

    # Strategy summary
    strategy_summary = {}
    for pos in positions:
        if pos.strategy not in strategy_summary:
            strategy_summary[pos.strategy] = {"count": 0, "total_pnl_pct": 0.0}
        strategy_summary[pos.strategy]["count"] += 1
        if pos.entry_price and pos.status == "active":
            # Approximate current return (will be updated by signal_job)
            pass

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "positions": positions,
        "trades": trades,
        "orders": orders,
        "strategy_summary": strategy_summary,
    })


@router.get("/api/status")
async def api_status(session: AsyncSession = Depends(get_session), _=Depends(verify_token)):
    stmt = select(func.count()).select_from(Position).where(Position.status == "active")
    active_count = (await session.execute(stmt)).scalar()

    stmt = select(func.count()).select_from(Position).where(Position.status == "pending_buy")
    pending_buy_count = (await session.execute(stmt)).scalar()

    return {
        "active_positions": active_count,
        "pending_buys": pending_buy_count,
        "status": "running",
    }
```

- [ ] **Step 2: Create dashboard.html**

```html
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="refresh" content="30">
    <title>Stock Auto Trading Bot V3</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-900 text-gray-100 p-6">
    <div class="max-w-6xl mx-auto">
        <h1 class="text-2xl font-bold mb-6">Stock Auto Trading Bot V3</h1>

        <!-- Strategy Summary -->
        <div class="grid grid-cols-4 gap-4 mb-6">
            {% for name, info in strategy_summary.items() %}
            <div class="bg-gray-800 rounded-lg p-4">
                <div class="text-sm text-gray-400">{{ name }}</div>
                <div class="text-xl font-bold">{{ info.count }} positions</div>
            </div>
            {% endfor %}
        </div>

        <!-- Active Positions -->
        <div class="bg-gray-800 rounded-lg p-4 mb-6">
            <h2 class="text-lg font-semibold mb-3">보유 종목</h2>
            <table class="w-full text-sm">
                <thead class="text-gray-400 border-b border-gray-700">
                    <tr>
                        <th class="text-left py-2">종목</th>
                        <th class="text-left">전략</th>
                        <th class="text-right">매수가</th>
                        <th class="text-right">SL</th>
                        <th class="text-right">보유일</th>
                        <th class="text-left">상태</th>
                    </tr>
                </thead>
                <tbody>
                    {% for pos in positions %}
                    <tr class="border-b border-gray-700/50">
                        <td class="py-2">{{ pos.symbol }} {{ pos.name }}</td>
                        <td>{{ pos.strategy }}</td>
                        <td class="text-right">{{ "{:,}".format(pos.entry_price or 0) }}</td>
                        <td class="text-right">{{ "{:,}".format(pos.sl_price or 0) }}</td>
                        <td class="text-right">{{ pos.holding_days }}d</td>
                        <td>
                            {% if pos.status == 'active' %}
                            <span class="text-green-400">보유중</span>
                            {% elif pos.status == 'pending_buy' %}
                            <span class="text-blue-400">매수대기</span>
                            {% elif pos.status == 'pending_sell' %}
                            <span class="text-red-400">매도대기</span>
                            {% endif %}
                        </td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>

        <!-- Recent Trades -->
        <div class="bg-gray-800 rounded-lg p-4 mb-6">
            <h2 class="text-lg font-semibold mb-3">최근 매매</h2>
            <table class="w-full text-sm">
                <thead class="text-gray-400 border-b border-gray-700">
                    <tr>
                        <th class="text-left py-2">날짜</th>
                        <th class="text-left">종목</th>
                        <th class="text-left">전략</th>
                        <th class="text-right">수익률</th>
                        <th class="text-right">손익</th>
                        <th class="text-left">사유</th>
                    </tr>
                </thead>
                <tbody>
                    {% for trade in trades %}
                    <tr class="border-b border-gray-700/50">
                        <td class="py-2">{{ trade.exit_date }}</td>
                        <td>{{ trade.symbol }} {{ trade.name }}</td>
                        <td>{{ trade.strategy }}</td>
                        <td class="text-right {% if trade.return_pct >= 0 %}text-green-400{% else %}text-red-400{% endif %}">
                            {{ "{:+.1%}".format(trade.return_pct) }}
                        </td>
                        <td class="text-right {% if trade.pnl >= 0 %}text-green-400{% else %}text-red-400{% endif %}">
                            {{ "{:+,}".format(trade.pnl) }}
                        </td>
                        <td>{{ trade.exit_reason }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>

        <!-- Recent Orders -->
        <div class="bg-gray-800 rounded-lg p-4">
            <h2 class="text-lg font-semibold mb-3">최근 주문</h2>
            <table class="w-full text-sm">
                <thead class="text-gray-400 border-b border-gray-700">
                    <tr>
                        <th class="text-left py-2">시간</th>
                        <th class="text-left">종목</th>
                        <th class="text-left">구분</th>
                        <th class="text-right">수량</th>
                        <th class="text-right">체결가</th>
                        <th class="text-left">상태</th>
                    </tr>
                </thead>
                <tbody>
                    {% for order in orders %}
                    <tr class="border-b border-gray-700/50">
                        <td class="py-2">{{ order.submitted_at.strftime('%m-%d %H:%M') if order.submitted_at else '' }}</td>
                        <td>{{ order.symbol }}</td>
                        <td class="{% if order.side == 'buy' %}text-red-400{% else %}text-blue-400{% endif %}">
                            {{ '매수' if order.side == 'buy' else '매도' }}
                        </td>
                        <td class="text-right">{{ order.qty }}</td>
                        <td class="text-right">{{ "{:,}".format(order.filled_price or 0) }}</td>
                        <td>{{ order.status }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
    </div>
</body>
</html>
```

- [ ] **Step 3: Create app/dashboard/__init__.py**

```python
from app.dashboard.router import router

__all__ = ["router"]
```

- [ ] **Step 4: Commit**

```bash
git add app/dashboard/
git commit -m "feat: web dashboard with positions, trades, orders view"
```

---

### Task 10: Main App (FastAPI + APScheduler)

**Files:**
- Create: `app/main.py`, `app/__init__.py`

- [ ] **Step 1: Create app/__init__.py**

```python
```

- [ ] **Step 2: Create app/main.py**

```python
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI

from app.config import KISConfig, AppSettings, MarketFilterConfig, UniverseConfig, load_strategy_configs
from app.models import init_db, create_tables, async_session_factory
from app.broker.client import KISClient
from app.broker.order import KISOrderAPI
from app.broker.account import KISAccountAPI
from app.trader import OrderExecutor, SLManager
from app.notifier import DiscordNotifier
from app.dashboard import router as dashboard_router
from app.jobs import run_signal_job, run_order_job, run_confirm_job

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler(timezone="Asia/Seoul")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown."""
    settings = AppSettings()
    kis_config = KISConfig()
    strategy_configs = load_strategy_configs("config")

    # Init DB
    init_db(settings.database_url)
    await create_tables()

    # Init broker
    client = KISClient(kis_config)
    await client.refresh_token()
    order_api = KISOrderAPI(client)
    account_api = KISAccountAPI(client)
    sl_manager = SLManager(order_api)
    executor = OrderExecutor(order_api, account_api, sl_manager)

    # Init notifier
    notifier = DiscordNotifier(settings.discord_webhook_url)

    # Store in app state
    app.state.client = client
    app.state.executor = executor
    app.state.notifier = notifier
    app.state.strategy_configs = strategy_configs

    # Schedule jobs
    market_filter_config = MarketFilterConfig()
    universe_config = UniverseConfig()

    async def _signal_job():
        async with async_session_factory() as session:
            await run_signal_job(session, strategy_configs, market_filter_config, universe_config, notifier)

    async def _order_job():
        async with async_session_factory() as session:
            await run_order_job(session, executor, strategy_configs, notifier)

    async def _confirm_job():
        async with async_session_factory() as session:
            await run_confirm_job(session, executor, notifier)

    # Token refresh (every 12 hours)
    async def _refresh_token():
        try:
            await client.refresh_token()
            logger.info("Token refreshed successfully")
        except Exception as e:
            logger.error(f"Token refresh failed: {e}")
            await notifier.send_error(f"Token refresh failed: {e}")

    scheduler.add_job(_signal_job, CronTrigger(hour=15, minute=40), id="signal_job", misfire_grace_time=300)
    scheduler.add_job(_order_job, CronTrigger(hour=8, minute=59), id="order_job", misfire_grace_time=60)
    scheduler.add_job(_confirm_job, CronTrigger(hour=9, minute=5), id="confirm_job", misfire_grace_time=60)
    scheduler.add_job(_refresh_token, CronTrigger(hour=8, minute=0), id="token_refresh")

    scheduler.start()
    logger.info(f"Scheduler started with {len(strategy_configs)} strategies")
    await notifier.send("🟢 Auto Trading Bot started")

    yield

    # Shutdown
    scheduler.shutdown()
    await client.close()
    logger.info("Shutdown complete")


app = FastAPI(title="Stock Auto Trading Bot V3", lifespan=lifespan)
app.include_router(dashboard_router)


@app.get("/health")
async def health():
    jobs = {job.id: str(job.next_run_time) for job in scheduler.get_jobs()}
    return {"status": "ok", "jobs": jobs}
```

- [ ] **Step 3: Commit**

```bash
git add app/__init__.py app/main.py
git commit -m "feat: main app with FastAPI, APScheduler, lifespan setup"
```

---

### Task 11: Integration Test & Final Wiring

**Files:**
- Create: `tests/test_integration.py`
- Verify all imports work

- [ ] **Step 1: Create import smoke test**

`tests/test_integration.py`:
```python
"""Smoke tests to verify all modules import correctly."""


def test_import_config():
    from app.config import KISConfig, AppSettings, load_strategy_configs, StrategyParams


def test_import_models():
    from app.models import Base, Position, Order, Trade


def test_import_broker():
    from app.broker import KISClient
    from app.broker.order import KISOrderAPI
    from app.broker.account import KISAccountAPI


def test_import_strategy():
    from app.strategy import SIGNAL_GENERATORS, add_indicators, filter_universe, check_market_filter


def test_import_trader():
    from app.trader import OrderExecutor, SLManager, calc_quantity


def test_import_jobs():
    from app.jobs import run_signal_job, run_order_job, run_confirm_job


def test_import_notifier():
    from app.notifier import DiscordNotifier


def test_import_dashboard():
    from app.dashboard import router


def test_signal_generators_complete():
    from app.strategy import SIGNAL_GENERATORS
    expected = {"volume_breakout", "pullback_buy", "high_breakout", "combined_ac"}
    assert set(SIGNAL_GENERATORS.keys()) == expected
```

- [ ] **Step 2: Run all tests**

Run: `python -m pytest tests/ -v --tb=short`
Expected: All tests PASS

- [ ] **Step 3: Commit**

```bash
git add tests/test_integration.py
git commit -m "feat: integration smoke tests"
```

---

### Task 12: Git Init & Final Setup

- [ ] **Step 1: Initialize git repo**

```bash
cd D:/Projects/Python/stock-auto-trading-botV3
git init
git add -A
git commit -m "initial commit: auto trading bot project structure"
```

- [ ] **Step 2: Create .gitignore**

```
__pycache__/
*.py[cod]
.env
.venv/
venv/
*.egg-info/
dist/
build/
.pytest_cache/
```

- [ ] **Step 3: Verify project runs (dry)**

```bash
python -c "from app.config import load_strategy_configs; print(load_strategy_configs('config'))"
```
Expected: Prints 4 strategy configs

- [ ] **Step 4: Final commit**

```bash
git add .gitignore
git commit -m "chore: add .gitignore"
```
