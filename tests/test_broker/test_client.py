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
