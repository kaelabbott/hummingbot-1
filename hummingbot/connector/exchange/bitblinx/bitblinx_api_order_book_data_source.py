#!/usr/bin/env python

import asyncio
import aiohttp
import logging
import pandas as pd
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
    Optional
)
import re
import time
import ujson
import websockets
from websockets.exceptions import ConnectionClosed
from hummingbot.core.data_type.order_book_row import OrderBookRow
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.bitblinx.bitblinx_order_book import BitblinxOrderBook
from hummingbot.connector.exchange.bitblinx.bitblinx_utils import convert_to_exchange_trading_pair_ws
TRADING_PAIR_FILTER = re.compile(r"(BTC|ETH|USDT)$")

SNAPSHOT_REST_URL = "https://trade.bitblinx.com/api/book"
BITBLINX_WEBSOCKET = "wss://trade.bitblinx.com/ws"
BITBLINX_PRICE_CHANGE_URL = "https://trade.bitblinx.com/api/prices"
BITBLINX_SYMBOLS_URL = "https://trade.bitblinx.com/api/symbols"


class BitblinxAPIOrderBookDataSource(OrderBookTrackerDataSource):

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

    def _prepare_trade(self, raw_response: str) -> Optional[Dict[str, Any]]:
        content = ujson.loads(raw_response)
        return {
            "id": content['result']['tradeId'],
            "mts": content['result']['created'],
            "amount": content['result']['quantity'],
            "price": content['result']['price'],
        }

    def _prepare_snapshot(self, pair: str, raw_snapshot: dict) -> Dict[str, Any]:
        raw_snapshot = ujson.loads(raw_snapshot)
        """
        Return structure of three elements:
            symbol: traded pair symbol
            bids: List of OrderBookRow for bids
            asks: List of OrderBookRow for asks
        """
        update_id = time.time()
        bids = [OrderBookRow(i['price'], i['quantity'], update_id) for i in raw_snapshot['result']['buy']]
        asks = [OrderBookRow(i['price'], i['quantity'], update_id) for i in raw_snapshot['result']['sell']]
        if not bids or not asks:
            return False
        return {
            "symbol": pair,
            "bids": bids,
            "asks": asks,
        }

    async def _get_response(self, ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        try:
            while True:
                try:
                    msg: str = await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
                    yield msg
                except asyncio.TimeoutError:
                    raise
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            return
        except ConnectionClosed:
            return
        finally:
            await ws.close()

    @classmethod
    async def get_last_traded_prices(cls, trading_pairs: List[str]) -> Dict[str, float]:
        tasks = [cls.get_last_traded_price(t_pair) for t_pair in trading_pairs]
        results = await safe_gather(*tasks)
        return {t_pair: result for t_pair, result in zip(trading_pairs, results)}

    @classmethod
    async def get_last_traded_price(cls, trading_pair: str) -> float:
        async with aiohttp.ClientSession() as client:
            resp = await client.get(f"{BITBLINX_PRICE_CHANGE_URL}/{trading_pair}")
            resp_json = await resp.json()
            return float(resp_json['result'][0]['last'])

    @staticmethod
    async def fetch_trading_pairs() -> List[str]:
        try:
            async with aiohttp.ClientSession() as client:
                async with client.get(f"{BITBLINX_SYMBOLS_URL}", timeout=5) as response:
                    if response.status == 200:
                        all_trading_pairs: List[Dict[str, Any]] = await response.json()
                        return [item["symbol"].replace('/', '-')
                                for item in all_trading_pairs['result']
                                if item["isActive"] is True]
        except Exception:
            # Do nothing if the request fails -- there will be no autocomplete for bittrex trading pairs
            pass
        return []

    @staticmethod
    async def get_snapshot(client: aiohttp.ClientSession, trading_pair: str, limit: int = 1000) -> Dict[str, Any]:
        async with client.get(f"{SNAPSHOT_REST_URL}/{trading_pair}", ) as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f"Error fetching bitblinx market snapshot for {trading_pair}. "
                              f"HTTP status is {response.status}.")
            data: Dict[str, Any] = await response.json()
            data['result']['bids'] = data['result'].pop('buy')
            data['result']['asks'] = data['result'].pop('sell')
            return data['result']

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        async with aiohttp.ClientSession() as client:
            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair=trading_pair)
            snapshot_timestamp: float = time.time()
            snapshot_msg: OrderBookMessage = BitblinxOrderBook.snapshot_message_from_exchange(
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
                for trading_pair in trading_pairs:
                    async with websockets.connect(BITBLINX_WEBSOCKET) as ws:
                        logging.getLogger('asyncio').setLevel(logging.ERROR)
                        logging.getLogger('asyncio.coroutines').setLevel(logging.ERROR)
                        logging.getLogger('websockets.server').setLevel(logging.ERROR)
                        logging.getLogger('websockets.protocol').setLevel(logging.ERROR)
                        payload: Dict[str, Any] = {
                            "method": "subscribeTrades",
                            "params": {
                                "symbol": convert_to_exchange_trading_pair_ws(trading_pair),
                                "limit": 25,
                            },
                            "id": 1
                        }
                        await ws.send(ujson.dumps(payload))
                        await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)  # response
                        # await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)  # subscribe info
                        # await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)  # snapshot

                        async for raw_msg in self._get_response(ws):
                            msg = self._prepare_trade(raw_msg)
                            if msg:
                                msg_book: OrderBookMessage = BitblinxOrderBook.trade_message_from_exchange(
                                    msg,
                                    metadata={"symbol": f"{trading_pair}"}
                                )
                                output.put_nowait(msg_book)

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
                for trading_pair in trading_pairs:
                    async with websockets.connect(BITBLINX_WEBSOCKET) as ws:
                        logging.getLogger('asyncio').setLevel(logging.ERROR)
                        logging.getLogger('asyncio.coroutines').setLevel(logging.ERROR)
                        logging.getLogger('websockets.server').setLevel(logging.ERROR)
                        logging.getLogger('websockets.protocol').setLevel(logging.ERROR)
                        payload: Dict[str, Any] = {
                            "method": "subscribeBook",
                            "params": {
                                "symbol": convert_to_exchange_trading_pair_ws(trading_pair),
                            },
                            "id": 1
                        }
                        await ws.send(ujson.dumps(payload))
                        await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)  # response
                        await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)  # subscribe info
                        async for raw_msg in self._get_response(ws):
                            if raw_msg:
                                snapshot = self._prepare_snapshot(trading_pair, raw_msg)
                            if snapshot:
                                snapshot_timestamp: float = time.time()
                                snapshot_msg: OrderBookMessage = BitblinxOrderBook.snapshot_message_from_exchange(
                                    snapshot,
                                    snapshot_timestamp
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
                    for trading_pair in self._trading_pairs:
                        try:
                            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
                            snapshot_timestamp: float = time.time()
                            snapshot_msg: OrderBookMessage = BitblinxOrderBook.snapshot_message_from_exchange(
                                snapshot,
                                snapshot_timestamp,
                                metadata={"trading_pair": trading_pair}
                            )
                            output.put_nowait(snapshot_msg)
                            self.logger().debug(f"Saved order book snapshot for {trading_pair}")
                            # Be careful not to go above bitblinx's API rate limits.
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
