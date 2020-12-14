from collections import defaultdict, deque
import logging
import time
from typing import Deque, Dict, List, Optional
import asyncio
from hummingbot.core.data_type.order_book_message import (
    OrderBookMessageType,
    OrderBookMessage,
)
from hummingbot.core.data_type.order_book_tracker import (
    OrderBookTracker,
)
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.ftx.ftx_active_order_tracker import \
    FtxActiveOrderTracker
from hummingbot.connector.exchange.ftx.ftx_order_book import FtxOrderBook
from hummingbot.connector.exchange.ftx.ftx_order_book_message import \
    FtxOrderBookMessage
from hummingbot.connector.exchange.ftx.ftx_order_book_tracker_entry import \
    FtxOrderBookTrackerEntry
from .ftx_api_order_book_data_source import FtxAPIOrderBookDataSource
from hummingbot.core.data_type.order_book import OrderBook


SAVED_MESSAGES_QUEUE_SIZE = 1000
CALC_STAT_MINUTE = 60.0

QUEUE_TYPE = Dict[str, Deque[OrderBookMessage]]
TRACKER_TYPE = Dict[str, FtxOrderBookTrackerEntry]


class FtxOrderBookTracker(OrderBookTracker):
    _logger: Optional[HummingbotLogger] = None

    EXCEPTION_TIME_SLEEP = 5.0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, trading_pairs: List[str]):
        super().__init__(
            FtxAPIOrderBookDataSource(trading_pairs),
            trading_pairs
        )
        self._order_book_diff_stream: asyncio.Queue = asyncio.Queue()
        self._order_book_snapshot_stream: asyncio.Queue = asyncio.Queue()
        self._ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        self._saved_message_queues: QUEUE_TYPE = defaultdict(
            lambda: deque(maxlen=SAVED_MESSAGES_QUEUE_SIZE)
        )
        self._trading_pairs: Optional[List[str]] = trading_pairs
        self._active_order_trackers: TRACKER_TYPE = defaultdict(FtxActiveOrderTracker)

    @property
    def exchange_name(self) -> str:
        return "ftx"

    async def _order_book_diff_router(self):
        """
        Route the real-time order book diff messages to the correct order book.
        """
        last_message_timestamp: float = time.time()
        message_queued: int = 0
        message_accepted: int = 0
        message_rejected: int = 0
        while True:
            try:
                ob_message: FtxOrderBookMessage = await self._order_book_diff_stream.get()
                trading_pair: str = ob_message.trading_pair
                if trading_pair not in self._tracking_message_queues:
                    message_queued += 1
                    # Save diff messages received before snaphsots are ready
                    self._saved_message_queues[trading_pair].append(ob_message)
                    continue
                message_queue: asyncio.Queue = self._tracking_message_queues[trading_pair]
                # Check the order book's initial update ID. If it's larger, don't bother.
                order_book: FtxOrderBook = self._order_books[trading_pair]

                if order_book.snapshot_uid > ob_message.update_id:
                    message_rejected += 1
                    continue
                await message_queue.put(ob_message)
                message_accepted += 1

                # Log some statistics
                now: float = time.time()
                if int(now / 60.0) > int(last_message_timestamp / 60.0):
                    self.logger().debug(
                        f"Diff message processed: "
                        f"{message_accepted}, "
                        f"rejected: {message_rejected}, "
                        f"queued: {message_queue}"
                    )
                    message_accepted = 0
                    message_rejected = 0
                    message_queued = 0

                last_message_timestamp = now

            except asyncio.CancelledError:
                raise

            except Exception:
                self.logger().network(
                    "Unexpected error routing order book messages.",
                    exc_info=True,
                    app_warning_msg="Unexpected error routing order book messages. Retrying after 5 seconds.",
                )
                await asyncio.sleep(5.0)

    async def _track_single_book(self, trading_pair: str):
        past_diffs_window: Deque[OrderBookMessage] = deque()
        self._past_diffs_windows[trading_pair] = past_diffs_window

        message_queue: asyncio.Queue = self._tracking_message_queues[trading_pair]
        order_book: OrderBook = self._order_books[trading_pair]
        last_message_timestamp: float = time.time()
        diff_messages_accepted: int = 0

        while True:
            try:
                message: OrderBookMessage = None
                saved_messages: Deque[OrderBookMessage] = self._saved_message_queues[trading_pair]

                # Process saved messages first if there are any
                if len(saved_messages) > 0:
                    message = saved_messages.popleft()
                else:
                    message = await message_queue.get()

                if message.type is OrderBookMessageType.DIFF:
                    order_book.apply_diffs(message.bids, message.asks, message.update_id)
                    past_diffs_window.append(message)
                    while len(past_diffs_window) > self.PAST_DIFF_WINDOW_SIZE:
                        past_diffs_window.popleft()
                    diff_messages_accepted += 1

                    # Output some statistics periodically.
                    now: float = time.time()
                    if int(now / 60.0) > int(last_message_timestamp / 60.0):
                        self.logger().debug("Processed %d order book diffs for %s.",
                                            diff_messages_accepted, trading_pair)
                        diff_messages_accepted = 0
                    last_message_timestamp = now
                elif message.type is OrderBookMessageType.SNAPSHOT:
                    past_diffs: List[OrderBookMessage] = list(past_diffs_window)
                    order_book.restore_from_snapshot_and_diffs(message, past_diffs)
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
