from collections import defaultdict, deque
import logging
import time
from typing import Deque, Dict, List, Optional
import asyncio
import bisect
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
from hummingbot.connector.exchange.ftx.ftx_utils import convert_to_exchange_trading_pair


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

    async def _track_single_book(self, trading_pair: str):
        """
        Update an order book with changes from the latest batch of received messages
        """
        trading_pair = convert_to_exchange_trading_pair(trading_pair)
        past_diffs_window: Deque[FtxOrderBookMessage] = deque()
        self._past_diffs_windows[trading_pair] = past_diffs_window
        message_queue: asyncio.Queue = self._tracking_message_queues[trading_pair]
        order_book: FtxOrderBook = self._order_books[trading_pair]
        active_order_tracker: FtxActiveOrderTracker = self._active_order_trackers[trading_pair]

        last_message_timestamp: float = time.time()
        diff_messages_accepted: int = 0

        while True:
            try:
                message: FtxOrderBookMessage = None
                saved_messages: Deque[FtxOrderBookMessage] = self._saved_message_queues[trading_pair]
                # Process saved messages first if there are any
                if len(saved_messages) > 0:
                    message = saved_messages.popleft()
                else:
                    message = await message_queue.get()

                if message.type is OrderBookMessageType.DIFF:
                    bids, asks = active_order_tracker.convert_diff_message_to_order_book_row(message)
                    order_book.apply_diffs(bids, asks, message.update_id)
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
                    past_diffs: List[FtxOrderBookMessage] = list(past_diffs_window)
                    # only replay diffs later than snapshot, first update active order with snapshot then replay diffs
                    replay_position = bisect.bisect_right(past_diffs, message)
                    replay_diffs = past_diffs[replay_position:]
                    s_bids, s_asks = active_order_tracker.convert_snapshot_message_to_order_book_row(message)
                    order_book.apply_snapshot(s_bids, s_asks, message.update_id)
                    for diff_message in replay_diffs:
                        d_bids, d_asks = active_order_tracker.convert_diff_message_to_order_book_row(diff_message)
                        order_book.apply_diffs(d_bids, d_asks, diff_message.update_id)

                    self.logger().debug("Processed order book snapshot for %s.", trading_pair)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    f"Unexpected error processing order book messages for {trading_pair}.",
                    exc_info=True,
                    app_warning_msg="Unexpected error processing order book messages. Retrying after 5 seconds."
                )
                await asyncio.sleep(5.0)
