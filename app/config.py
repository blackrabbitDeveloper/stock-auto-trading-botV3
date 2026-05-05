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


class KISPaperConfig(BaseSettings):
    """모의투자 전용 설정 (주문 실행용)."""
    app_key: str = ""
    app_secret: str = ""
    account_no: str = ""
    env: str = "paper"

    class Config:
        env_prefix = "KIS_PAPER_"

    @property
    def base_url(self) -> str:
        return "https://openapivts.koreainvestment.com:29443"

    @property
    def account_prefix(self) -> str:
        return self.account_no.split("-")[0] if "-" in self.account_no else self.account_no

    @property
    def account_suffix(self) -> str:
        return self.account_no.split("-")[1] if "-" in self.account_no else "01"


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
        extra = "ignore"


def load_strategy_configs(config_dir: str = "config") -> dict[str, StrategyParams]:
    """Load all strategy YAML configs from directory."""
    configs = {}
    config_path = Path(config_dir)
    if not config_path.exists():
        return configs
    for yaml_file in config_path.glob("*.yaml"):
        with open(yaml_file, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        params = StrategyParams(**data)
        configs[params.name] = params
    return configs
