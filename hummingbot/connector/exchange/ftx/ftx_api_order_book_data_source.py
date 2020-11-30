#!/usr/bin/env python

import asyncio
import aiohttp
import logging
import pandas as pd
from typing import (
    Any,
    Dict,
    List,
    Optional
)
import re
import time
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.ftx.ftx_order_book import FtxOrderBook
from hummingbot.connector.exchange.ftx.ftx_websocket import FtxWebsocket
from hummingbot.connector.exchange.ftx.ftx_utils import convert_to_exchange_trading_pair_ws, timestamp_to_int

TRADING_PAIR_FILTER = re.compile(r"(BTC|ETH|USDT)$")
FTX_WEBSOCKET = "wss://ftx.com/ws/"
FTX_MARKET_SPOT = "https://ftx.com/api/markets?type=spot"
FTX_MARKET = "https://ftx.com/api/markets"


class FtxAPIOrderBookDataSource(OrderBookTrackerDataSource):
    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0
    SNAPSHOT_LIMIT_SIZE = 18
    _baobds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._baobds_logger is None:
            cls._baobds_logger = logging.getLogger(__name__)
        return cls._baobds_logger

    def __init__(self, trading_pairs: List[str]):
        super().__init__(trading_pairs)
        self._order_book_create_function = lambda: OrderBook()

    @classmethod
    async def get_last_traded_prices(cls, trading_pairs: List[str]) -> Dict[str, float]:
        tasks = [cls.get_last_traded_price(t_pair) for t_pair in trading_pairs]
        results = await safe_gather(*tasks)
        return {t_pair: result for t_pair, result in zip(trading_pairs, results)}

    @classmethod
    async def get_last_traded_price(cls, trading_pair: str) -> float:
        async with aiohttp.ClientSession() as client:
            resp = await client.get(f"{FTX_MARKET}/{convert_to_exchange_trading_pair_ws(trading_pair)}")
            resp_json = await resp.json()
            return float(resp_json['result']['last'])

    @staticmethod
    async def fetch_trading_pairs() -> List[str]:
        try:
            async with aiohttp.ClientSession() as client:
                async with client.get(f"{FTX_MARKET}", timeout=5) as response:
                    if response.status == 200:
                        all_trading_pairs: List[Dict[str, Any]] = await response.json()
                        return [item["name"].replace('/', '-')
                                for item in all_trading_pairs['result']
                                if item["enabled"] is True and item['type'] == 'spot']
        except Exception:
            pass
        return []

    @staticmethod
    async def get_snapshot(client: aiohttp.ClientSession, trading_pair: str, limit: int = 1000) -> Dict[str, Any]:
        async with client.get(f"{FTX_MARKET}/{ convert_to_exchange_trading_pair_ws(trading_pair)}/orderbook?depth=100", ) as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f"Error fetching FTX market snapshot for {trading_pair}. "
                              f"HTTP status is {response.status}.")
            data: Dict[str, Any] = await response.json()
            return data['result']

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        async with aiohttp.ClientSession() as client:
            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair=trading_pair)
            snapshot_timestamp: float = time.time()
            snapshot_msg: OrderBookMessage = FtxOrderBook.snapshot_message_from_exchange(
                snapshot,
                snapshot_timestamp,
                metadata={"trading_pair": trading_pair}
            )
            order_book = self.order_book_create_function()
            order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.timestamp)
            return order_book

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                trading_pairs: List[str] = self._trading_pairs
                if not trading_pairs:
                    await asyncio.sleep(5)
                    continue
                for trading_pair in trading_pairs:
                    ws = FtxWebsocket()
                    await ws.connect()
                    await ws.subscribe({'channel': 'trades', 'market': convert_to_exchange_trading_pair_ws(trading_pair)})
                    async for response in ws.on_message():
                        if response.get("data") is None:
                            continue

                        for trade in response["data"]:
                            trade['market'] = convert_to_exchange_trading_pair_ws(trading_pair)
                            trade: Dict[Any] = trade
                            trade_timestamp = timestamp_to_int(trade["time"])
                            trade_msg: OrderBookMessage = FtxOrderBook.trade_message_from_exchange(
                                trade,
                                int(trade_timestamp),
                                metadata={"trading_pair": convert_to_exchange_trading_pair_ws(trading_pair)}
                            )
                            output.put_nowait(trade_msg)
            except Exception as err:
                self.logger().error(err)
                self.logger().network(
                    "Unexpected error with WebSocket connection.",
                    exc_info=True,
                    app_warning_msg="Unexpected error with WebSocket connection. "
                                    f"Retrying in {int(self.MESSAGE_TIMEOUT)} seconds. "
                                    "Check network connection."
                )
                await asyncio.sleep(5)

    async def listen_for_order_book_diffs(self,
                                          ev_loop: asyncio.BaseEventLoop,
                                          output: asyncio.Queue):
        while True:
            try:
                trading_pairs: List[str] = self._trading_pairs
                if not trading_pairs:
                    await asyncio.sleep(5)
                    continue
                for trading_pair in trading_pairs:
                    ws = FtxWebsocket()
                    await ws.connect()
                    await ws.subscribe({'channel': 'orderbook', 'market': convert_to_exchange_trading_pair_ws(trading_pair)})
                    async for response in ws.on_message():

                        if response.get("data") is None:
                            continue
                        snapshot = response.get("data")
                        if snapshot:
                            snapshot_timestamp: float = time.time()
                            snapshot_msg: OrderBookMessage = FtxOrderBook.snapshot_message_from_exchange(
                                snapshot,
                                snapshot_timestamp,
                                metadata={"trading_pair": response['market']}
                            )
                            output.put_nowait(snapshot_msg)
                        else:
                            continue
            except Exception as err:
                self.logger().error(err)
                self.logger().network(
                    "Unexpected error with WebSocket connection.",
                    exc_info=True,
                    app_warning_msg="Unexpected error with WebSocket connection. "
                                    f"Retrying in {int(self.MESSAGE_TIMEOUT)} seconds. "
                                    "Check network connection."
                )
                await asyncio.sleep(5)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                async with aiohttp.ClientSession() as client:
                    if not self._trading_pairs:
                        await asyncio.sleep(5)
                        continue
                    for trading_pair in self._trading_pairs:
                        try:
                            trading_pair = convert_to_exchange_trading_pair_ws(trading_pair)
                            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
                            snapshot_timestamp: float = time.time()
                            snapshot_msg: OrderBookMessage = FtxOrderBook.snapshot_message_from_exchange(
                                snapshot,
                                snapshot_timestamp,
                                metadata={"trading_pair": trading_pair}
                            )
                            output.put_nowait(snapshot_msg)
                            self.logger().debug(f"Saved order book snapshot for {trading_pair}")
                            await asyncio.sleep(5.0)
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            self.logger().error("Unexpected error.", exc_info=True)
                            await asyncio.sleep(5.0)
                    this_hour: pd.Timestamp = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
                    next_hour: pd.Timestamp = this_hour + pd.Timedelta(hours=1)
                    delta: float = next_hour.timestamp() - time.time()
                    await asyncio.sleep(delta)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await asyncio.sleep(5.0)
