import pytest
from pathlib import Path
import yaml


@pytest.fixture
def config_dir(tmp_path):
    """Create temp config directory with test YAML."""
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
