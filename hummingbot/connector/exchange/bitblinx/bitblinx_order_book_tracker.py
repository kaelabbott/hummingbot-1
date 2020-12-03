from collections import defaultdict, deque
import logging
import time
from typing import Deque, Dict, List, Optional

import asyncio

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import (
    OrderBookMessageType,
    OrderBookMessage,
)
from hummingbot.core.data_type.order_book_tracker import (
    OrderBookTracker,
)
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.bitblinx.bitblinx_active_order_tracker import \
    BitblinxActiveOrderTracker
from hummingbot.connector.exchange.bitblinx.bitblinx_order_book import BitblinxOrderBook
from hummingbot.connector.exchange.bitblinx.bitblinx_order_book_message import \
    BitblinxOrderBookMessage
from hummingbot.connector.exchange.bitblinx.bitblinx_order_book_tracker_entry import \
    BitblinxOrderBookTrackerEntry
from .bitblinx_api_order_book_data_source import BitblinxAPIOrderBookDataSource

SAVED_MESSAGES_QUEUE_SIZE = 1000
CALC_STAT_MINUTE = 60.0

QUEUE_TYPE = Dict[str, Deque[OrderBookMessage]]
TRACKER_TYPE = Dict[str, BitblinxOrderBookTrackerEntry]


class BitblinxOrderBookTracker(OrderBookTracker):
    _logger: Optional[HummingbotLogger] = None

    EXCEPTION_TIME_SLEEP = 5.0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, trading_pairs: List[str]):
        super().__init__(
            BitblinxAPIOrderBookDataSource(trading_pairs),
            trading_pairs
        )
        self._order_book_diff_stream: asyncio.Queue = asyncio.Queue()
        self._order_book_snapshot_stream: asyncio.Queue = asyncio.Queue()
        self._ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        self._saved_message_queues: QUEUE_TYPE = defaultdict(
            lambda: deque(maxlen=SAVED_MESSAGES_QUEUE_SIZE)
        )
        self._trading_pairs: Optional[List[str]] = trading_pairs
        self._active_order_trackers: TRACKER_TYPE = defaultdict(BitblinxActiveOrderTracker)

    @property
    def exchange_name(self) -> str:
        return "bitblinx"

    async def _order_book_diff_router(self):
        last_message_timestamp: float = time.time()
        messages_queued: int = 0
        messages_accepted: int = 0
        messages_rejected: int = 0
        while True:
            try:
                order_book_message: BitblinxOrderBookMessage = await self._order_book_diff_stream.get()
                trading_pair: str = order_book_message.trading_pair
                if trading_pair not in self._tracking_message_queues:
                    messages_queued += 1
                    # Save diff messages received before snapshots are ready
                    self._saved_message_queues[trading_pair].append(order_book_message)
                    continue
                message_queue: asyncio.Queue = self._tracking_message_queues[trading_pair]
                order_book: BitblinxOrderBook = self._order_books[trading_pair]
                if order_book.snapshot_uid > order_book_message.update_id:
                    messages_rejected += 1
                    continue
                await message_queue.put(order_book_message)
                messages_accepted += 1
                # Log some statistics.
                now: float = time.time()
                if int(now / CALC_STAT_MINUTE) > int(last_message_timestamp / CALC_STAT_MINUTE):
                    self.logger().debug("Diff messages processed: %d, rejected: %d, queued: %d",
                                        messages_accepted,
                                        messages_rejected,
                                        messages_queued)
                    messages_accepted = 0
                    messages_rejected = 0
                    messages_queued = 0
                last_message_timestamp = now
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unexpected error routing order book messages.",
                    exc_info=True,
                    app_warning_msg="Unexpected error routing order book messages. "
                                    f"Retrying after {int(self.EXCEPTION_TIME_SLEEP)} seconds."
                )
                await asyncio.sleep(self.EXCEPTION_TIME_SLEEP)

    async def _track_single_book(self, trading_pair: str):
        message_queue: asyncio.Queue = self._tracking_message_queues[trading_pair]
        order_book: OrderBook = self._order_books[trading_pair]
        last_message_timestamp: float = time.time()
        diff_messages_accepted: int = 0
        while True:
            try:
                message: OrderBookMessage = await message_queue.get()
                if message.type is OrderBookMessageType.DIFF:
                    # Huobi websocket messages contain the entire order book state so they should be treated as snapshots
                    order_book.apply_snapshot(message.bids, message.asks, message.update_id)
                    diff_messages_accepted += 1

                    # Output some statistics periodically.
                    now: float = time.time()
                    if int(now / 60.0) > int(last_message_timestamp / 60.0):
                        self.logger().debug("Processed %d order book diffs for %s.",
                                            diff_messages_accepted, trading_pair)
                        diff_messages_accepted = 0
                    last_message_timestamp = now
                elif message.type is OrderBookMessageType.SNAPSHOT:
                    order_book.apply_snapshot(message.bids, message.asks, message.update_id)
                    self.logger().debug("Processed order book snapshot for %s.", trading_pair)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    f"Unexpected error tracking order book for {trading_pair}.",
                    exc_info=True,
                    app_warning_msg="Unexpected error tracking order book. Retrying after 5 seconds."
                )
                await asyncio.sleep(5.0)

    def _convert_diff_message_to_order_book_row(self, message):
        """
        Convert an incoming diff message to Tuple of np.arrays, and then convert to OrderBookRow
        :returns: Tuple(List[bids_row], List[asks_row])
        """
        bids = [message.content["bids"]] if "bids" in message.content else []
        asks = [message.content["asks"]] if "asks" in message.content else []
        return bids, asks
