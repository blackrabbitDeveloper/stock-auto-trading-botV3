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
