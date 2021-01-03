import re
from typing import (
    Optional,
    Tuple)
from hummingbot.core.utils.tracking_nonce import get_tracking_nonce
from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_methods import using_exchange
import pandas as pd


CENTRALIZED = True


EXAMPLE_PAIR = "BTC-USDT"


DEFAULT_FEES = [0.1, 0.1]


TRADING_PAIR_SPLITTER = re.compile(r"^(\w+)(BTC|ETH|BNB|DAI|XRP|USDT|USDC|USDS|TUSD|PAX|TRX|BUSD|NGN|RUB|TRY|EUR|IDRT|ZAR|UAH|GBP|BKRW|BIDR)$")


def timestamp_to_int(timestamp_str):
    timestamp = pd.Timestamp(timestamp_str, tz="UTC").timestamp()
    return timestamp


def split_trading_pair(trading_pair: str) -> Optional[Tuple[str, str]]:
    try:
        m = TRADING_PAIR_SPLITTER.match(trading_pair)
        return m.group(1), m.group(2)
    # Exceptions are now logged as warnings in trading pair fetcher
    except Exception:
        return None


def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> Optional[str]:
    if split_trading_pair(exchange_trading_pair) is None:
        return None
    # Huobi uses lowercase (btcusdt)
    base_asset, quote_asset = split_trading_pair(exchange_trading_pair)
    return f"{base_asset.upper()}-{quote_asset.upper()}"


def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    # Binance does not split BASEQUOTE (BTCUSDT)
    return hb_trading_pair.replace("/", "-")


def convert_to_exchange_trading_pair_ws(hb_trading_pair: str) -> str:
    # Binance does not split BASEQUOTE (BTCUSDT)
    return hb_trading_pair.replace("-", "/")


def get_new_client_order_id(is_buy: bool, trading_pair: str) -> str:
    side = "0" if is_buy else "1"
    return f"{trading_pair}-{side}-{get_tracking_nonce()}"


KEYS = {
    "ftx_api_key":
        ConfigVar(key="ftx_api_key",
                  prompt="Enter your Ftx API key >>> ",
                  required_if=using_exchange("ftx"),
                  is_secure=True,
                  is_connect_key=True),
    "ftx_secret_key":
        ConfigVar(key="ftx_secret_key",
                  prompt="Enter your Ftx secret key >>> ",
                  required_if=using_exchange("ftx"),
                  is_secure=True,
                  is_connect_key=True),
    "ftx_subaccount_name":
        ConfigVar(key="ftx_subaccount_name",
                  prompt="Enter your Ftx subaccount name (optional) >>> ",
                  required_if=using_exchange("ftx"),
                  is_secure=True,
                  is_connect_key=True),
}
