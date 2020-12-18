import logging
from typing import (
    Dict,
    List,
    Optional,
    Any,
    AsyncIterable,
)
from decimal import Decimal
import asyncio
import aiohttp
import math
import time
from async_timeout import timeout

from hummingbot.connector.derivative.position import Position
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.logger import HummingbotLogger
from hummingbot.core.clock import Clock
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.event.events import (
    MarketEvent,
    BuyOrderCompletedEvent,
    SellOrderCompletedEvent,
    OrderFilledEvent,
    OrderCancelledEvent,
    BuyOrderCreatedEvent,
    SellOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderType,
    TradeType,
    TradeFee,
    PositionSide,
)
from hummingbot.connector.derivative_base import DerivativeBase
from hummingbot.connector.derivative.ftx_perpetual.ftx_perpetual_order_book_tracker import FtxPerpetualOrderBookTracker
from hummingbot.connector.derivative.ftx_perpetual.ftx_perpetual_user_stream_tracker import FtxPerpetualUserStreamTracker
from hummingbot.connector.derivative.ftx_perpetual.ftx_perpetual_auth import FtxPerpetualAuth
from hummingbot.connector.derivative.ftx_perpetual.ftx_perpetual_in_flight_order import FtxPerpetualInFlightOrder
from hummingbot.connector.derivative.ftx_perpetual import ftx_perpetual_utils
from hummingbot.connector.derivative.ftx_perpetual.ftx_perpetual_client import FtxPerpetualClient
ctce_logger = None
s_decimal_NaN = Decimal("nan")


class FtxPerpetualDerivative(DerivativeBase):
    """
    FtxPerpetualDerivative connects with Crypto.com exchange and provides order book pricing, user account tracking and
    trading functionality.
    """
    API_CALL_TIMEOUT = 15.0
    SHORT_POLL_INTERVAL = 10.0
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 15.0
    LONG_POLL_INTERVAL = 120.0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global ctce_logger
        if ctce_logger is None:
            ctce_logger = logging.getLogger(__name__)
        return ctce_logger

    def __init__(self,
                 ftx_perpetual_api_key: str,
                 ftx_perpetual_secret_key: str,
                 ftx_perpetual_subaccount_name: str = None,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True
                 ):
        """
        :param ftx_api_key: The API key to connect to private ftx APIs.
        :param trading_pairs: The market trading pairs which to track order book data.
        :param trading_required: Whether actual trading is needed.
        """
        super().__init__()
        self._trading_required = trading_required
        self._ftx_auth = FtxPerpetualAuth(ftx_perpetual_api_key, ftx_perpetual_secret_key, ftx_perpetual_subaccount_name)
        self._order_book_tracker = FtxPerpetualOrderBookTracker(trading_pairs=trading_pairs)
        self._user_stream_tracker = FtxPerpetualUserStreamTracker(self._ftx_auth, trading_pairs)
        self._ev_loop = asyncio.get_event_loop()
        self._shared_client = None
        self._poll_notifier = asyncio.Event()
        self._last_timestamp = 0
        self._in_flight_orders = {}  # Dict[client_order_id:str, CryptoComInFlightOrder]
        self._order_not_found_records = {}  # Dict[client_order_id:str, count:int]
        self._trading_rules = {}  # Dict[trading_pair:str, TradingRule]
        self._status_polling_task = None
        self._user_stream_event_listener_task = None
        self._trading_rules_polling_task = None
        self._last_poll_timestamp = 0
        self._api_rest_client = FtxPerpetualClient(ftx_perpetual_api_key, ftx_perpetual_secret_key, ftx_perpetual_subaccount_name)
        self._check_network_interval = 60.0
        self._start_network_task_ftx = None
        self._start_network_task_ftx = safe_ensure_future(self._networking_polling_loop())
        self._position_mode = None
        self._funding_rate = 0
        self._account_positions = {}

    @property
    def name(self) -> str:
        return "ftx_perpetual"

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self._order_book_tracker.order_books

    @property
    def trading_rules(self) -> Dict[str, TradingRule]:
        return self._trading_rules

    @property
    def in_flight_orders(self) -> Dict[str, FtxPerpetualInFlightOrder]:
        return self._in_flight_orders

    @property
    def status_dict(self) -> Dict[str, bool]:
        """
        A dictionary of statuses of various connector's components.
        """
        return {
            "order_books_initialized": self._order_book_tracker.ready,
            "account_balance": len(self._account_balances) > 0 if self._trading_required else True,
            "trading_rule_initialized": len(self._trading_rules) > 0
        }

    @property
    def ready(self) -> bool:
        """
        :return True when all statuses pass, this might take 5-10 seconds for all the connector's components and
        services to be ready.
        """
        return all(self.status_dict.values())

    @property
    def limit_orders(self) -> List[LimitOrder]:
        return [
            in_flight_order.to_limit_order()
            for in_flight_order in self._in_flight_orders.values()
        ]

    @property
    def tracking_states(self) -> Dict[str, any]:
        """
        :return active in-flight orders in json format, is used to save in sqlite db.
        """
        return {
            key: value.to_json()
            for key, value in self._in_flight_orders.items()
            if not value.is_done
        }

    def restore_tracking_states(self, saved_states: Dict[str, any]):
        """
        Restore in-flight orders from saved tracking states, this is st the connector can pick up on where it left off
        when it disconnects.
        :param saved_states: The saved tracking_states.
        """
        self._in_flight_orders.update({
            key: FtxPerpetualInFlightOrder.from_json(value)
            for key, value in saved_states.items()
        })

    def supported_order_types(self) -> List[OrderType]:
        """
        :return a list of OrderType supported by this connector.
        Note that Market order type is no longer required and will not be used.
        """
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    def start(self, clock: Clock, timestamp: float):
        """
        This function is called automatically by the clock.
        """
        super().start(clock, timestamp)

    def stop(self, clock: Clock):
        """
        This function is called automatically by the clock.
        """
        super().stop(clock)

    async def start_network(self):
        """
        This function is required by NetworkIterator base class and is called automatically.
        It starts tracking order book, polling trading rules,
        updating statuses and tracking user data.
        """
        self._order_book_tracker.start()
        self._trading_rules_polling_task = safe_ensure_future(self._trading_rules_polling_loop())
        if self._trading_required:
            self._status_polling_task = safe_ensure_future(self._status_polling_loop())
            self._user_stream_tracker_task = safe_ensure_future(self._user_stream_tracker.start())
            self._user_stream_event_listener_task = safe_ensure_future(self._user_stream_event_listener())
        """
        This function is required by NetworkIterator base class and is called automatically.
        It starts tracking order book, polling trading rules,
        updating statuses and tracking user data.
        """
    def _stop_network(self):
        self._order_book_tracker.stop()
        if self._status_polling_task is not None:
            self._status_polling_task.cancel()
            self._status_polling_task = None
        if self._trading_rules_polling_task is not None:
            self._trading_rules_polling_task.cancel()
            self._trading_rules_polling_task = None
        if self._status_polling_task is not None:
            self._status_polling_task.cancel()
            self._status_polling_task = None
        if self._user_stream_event_listener_task is not None:
            self._user_stream_event_listener_task.cancel()
            self._user_stream_event_listener_task = None

    async def stop_network(self):
        self._stop_network()

    async def check_network(self) -> NetworkStatus:
        """
        This function is required by NetworkIterator base class and is called periodically to check
        the network connection. Simply ping the network (or call any light weight public API).
        """
        try:
            await self._api_rest_client.get_single_market('BTC-PERP')
        except asyncio.CancelledError:
            raise
        except Exception:
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED

    async def _http_client(self) -> aiohttp.ClientSession:
        """
        :returns Shared client session instance
        """
        if self._shared_client is None:
            self._shared_client = aiohttp.ClientSession()
        return self._shared_client

    async def _networking_polling_loop(self):
        """
        Periodically update trading rule.
        """
        while True:
            try:
                await safe_gather(self.start_network())
                await asyncio.sleep(self._check_network_interval)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().network(f"Unexpected error while fetching trading rules. Error: {str(e)}",
                                      exc_info=True,
                                      app_warning_msg="Could not fetch new trading rules from Crypto.com. "
                                                      "Check network connection.")
                await asyncio.sleep(0.5)

    async def _trading_rules_polling_loop(self):
        """
        Periodically update trading rule.
        """
        while True:
            try:
                await self._update_trading_rules()
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().network(f"Unexpected error while fetching trading rules. Error: {str(e)}",
                                      exc_info=True,
                                      app_warning_msg="Could not fetch new trading rules from Crypto.com. "
                                                      "Check network connection.")
                await asyncio.sleep(0.5)

    async def _update_trading_rules(self):
        symbol_info = self._api_rest_client.list_futures()
        self._trading_rules.clear()
        self._trading_rules = self._format_trading_rules(symbol_info)

    def _format_trading_rules(self, symbol_info) -> Dict[str, TradingRule]:
        """
        Converts json API response into a dictionary of trading rules.
        :param symbol: The json API response
        :return A dictionary of trading rules.
        Response Example:
        "ask": 4196,
        "bid": 4114.25,
        "change1h": 0,
        "change24h": 0,
        "changeBod": 0,
        "volumeUsd24h": 100000000,
        "volume": 24390.24,
        "description": "Bitcoin March 2019 Futures",
        "enabled": true,
        "expired": false,
        "expiry": "2019-03-29T03:00:00+00:00",
        "index": 3919.58841011,
        "imfFactor": 0.002,
        "last": 4196,
        "lowerBound": 3663.75,
        "mark": 3854.75,
        "name": "BTC-0329",
        "perpetual": false,
        "positionLimitWeight": 1.0,
        "postOnly": false,
        "priceIncrement": 0.25,
        "sizeIncrement": 0.001,
        "underlying": "BTC",
        "upperBound": 4112.2,
        "type": "future"
        """
        result = {}
        for rule in symbol_info:
            try:
                trading_pair = rule["name"]
                price_decimals = Decimal(str(rule["priceIncrement"]))
                quantity_decimals = Decimal(str(rule["sizeIncrement"]))
                result[trading_pair] = TradingRule(trading_pair,
                                                   min_price_increment=price_decimals,
                                                   min_base_amount_increment=quantity_decimals)
            except Exception:
                self.logger().error(f"Error parsing the trading pair rule {rule}. Skipping.", exc_info=True)
        return result

    def get_order_price_quantum(self, trading_pair: str, price: Decimal):
        """
        Returns a price step, a minimum price increment for a given trading pair.
        """
        trading_rule = self._trading_rules[ftx_perpetual_utils.convert_to_exchange_trading_pair(trading_pair)]
        return trading_rule.min_price_increment

    def get_order_size_quantum(self, trading_pair: str, order_size: Decimal):
        """
        Returns an order amount step, a minimum amount increment for a given trading pair.
        """
        trading_rule = self._trading_rules[ftx_perpetual_utils.convert_to_exchange_trading_pair(trading_pair)]
        return Decimal(trading_rule.min_base_amount_increment)

    def get_order_book(self, trading_pair: str) -> OrderBook:
        if trading_pair not in self._order_book_tracker.order_books:
            raise ValueError(f"No order book exists for '{trading_pair}'.")
        return self._order_book_tracker.order_books[trading_pair]

    def buy(self, trading_pair: str, amount: Decimal, order_type=OrderType.MARKET,
            price: Decimal = s_decimal_NaN, **kwargs) -> str:
        """
        Buys an amount of base asset (of the given trading pair). This function returns immediately.
        To see an actual order, you'll have to wait for BuyOrderCreatedEvent.
        :param trading_pair: The market (e.g. BTC-USDT) to buy from
        :param amount: The amount in base token value
        :param order_type: The order type
        :param price: The price (note: this is no longer optional)
        :returns A new internal order id
        """
        order_id: str = ftx_perpetual_utils.get_new_client_order_id(True, trading_pair)
        safe_ensure_future(self._create_order(TradeType.BUY, order_id, trading_pair, amount, order_type, price))
        return order_id

    def sell(self, trading_pair: str, amount: Decimal, order_type=OrderType.MARKET,
             price: Decimal = s_decimal_NaN, **kwargs) -> str:
        """
        Sells an amount of base asset (of the given trading pair). This function returns immediately.
        To see an actual order, you'll have to wait for SellOrderCreatedEvent.
        :param trading_pair: The market (e.g. BTC-USDT) to sell from
        :param amount: The amount in base token value
        :param order_type: The order type
        :param price: The price (note: this is no longer optional)
        :returns A new internal order id
        """
        order_id: str = ftx_perpetual_utils.get_new_client_order_id(False, trading_pair)
        safe_ensure_future(self._create_order(TradeType.SELL, order_id, trading_pair, amount, order_type, price))
        return order_id

    def cancel(self, trading_pair: str, order_id: str):
        """
        Cancel an order. This function returns immediately.
        To get the cancellation result, you'll have to wait for OrderCancelledEvent.
        :param trading_pair: The market (e.g. BTC-USDT) of the order.
        :param order_id: The internal order id (also called client_order_id)
        """
        safe_ensure_future(self._execute_cancel(trading_pair, order_id))
        return order_id

    async def _create_order(self,
                            trade_type: TradeType,
                            order_id: str,
                            trading_pair: str,
                            amount: Decimal,
                            order_type: OrderType,
                            price: Decimal):
        """
        Calls create-order API end point to place an order, starts tracking the order and triggers order created event.
        :param trade_type: BUY or SELL
        :param order_id: Internal order id (also called client_order_id)
        :param trading_pair: The market to place order
        :param amount: The order amount (in base token value)
        :param order_type: The order type
        :param price: The order price
        """
        trading_pair = ftx_perpetual_utils.convert_to_exchange_trading_pair(trading_pair)
        if not order_type.is_limit_type():
            raise Exception(f"Unsupported order type: {order_type}")
        trading_rule = self._trading_rules[trading_pair]
        amount = self.quantize_order_amount(trading_pair, amount)
        price = self.quantize_order_price(trading_pair, price)
        if amount < trading_rule.min_order_size:
            raise ValueError(f"Buy order amount {amount} is lower than the minimum order size "
                             f"{trading_rule.min_order_size}.")
        # Buy long and Sell short.
        api_params = {"market": trading_pair,
                      "side": trade_type.name.lower(),
                      "type": "limit",
                      "price": f"{price:f}",
                      "size": f"{amount:f}",
                      "clientId": order_id,
                      }
        self.start_tracking_order(order_id,
                                  None,
                                  trading_pair,
                                  trade_type,
                                  price,
                                  amount,
                                  order_type
                                  )
        try:
            if order_type is OrderType.LIMIT_MAKER:
                api_params["postOnly"] = True
            order_result = self._api_rest_client._post('orders', api_params)
            exchange_order_id = str(order_result["id"])
            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is not None:
                self.logger().info(f"Created {order_type.name} {trade_type.name} order {order_id} for "
                                   f"{amount} {trading_pair}.")
                tracked_order.exchange_order_id = exchange_order_id
            event_tag = MarketEvent.BuyOrderCreated if trade_type is TradeType.BUY else MarketEvent.SellOrderCreated
            event_class = BuyOrderCreatedEvent if trade_type is TradeType.BUY else SellOrderCreatedEvent
            self.trigger_event(event_tag,
                               event_class(
                                   self.current_timestamp,
                                   order_type,
                                   trading_pair,
                                   amount,
                                   price,
                                   order_id
                               ))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.stop_tracking_order(order_id)
            self.logger().network(
                f"Error submitting {trade_type.name} {order_type.name} order to bitblinx for "
                f"{amount} {trading_pair} "
                f"{price}.",
                exc_info=True,
                app_warning_msg=str(e)
            )
            self.trigger_event(MarketEvent.OrderFailure,
                               MarketOrderFailureEvent(self.current_timestamp, order_id, order_type))

    def start_tracking_order(self,
                             order_id: str,
                             exchange_order_id: str,
                             trading_pair: str,
                             trade_type: TradeType,
                             price: Decimal,
                             amount: Decimal,
                             order_type: OrderType):
        """
        Starts tracking an order by simply adding it into _in_flight_orders dictionary.
        """
        self._in_flight_orders[order_id] = FtxPerpetualInFlightOrder(
            client_order_id=order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=trading_pair,
            order_type=order_type,
            trade_type=trade_type,
            price=price,
            amount=amount
        )

    def stop_tracking_order(self, order_id: str):
        """
        Stops tracking an order by simply removing it from _in_flight_orders dictionary.
        """
        if order_id in self._in_flight_orders:
            del self._in_flight_orders[order_id]

    async def _execute_cancel(self, trading_pair: str, order_id: str, wait_for_status: bool = False) -> str:
        """
        Executes order cancellation process by first calling cancel-order API. The API result doesn't confirm whether
        the cancellation is successful, it simply states it receives the request.
        :param trading_pair: The market trading pair
        :param order_id: The internal order id
        :param wait_for_status: Whether to wait for the cancellation result, this is done by waiting for
        order.last_state to change to CANCELED
        """
        try:
            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is None:
                raise ValueError(f"Failed to cancel order - {order_id}. Order not found.")
            if tracked_order.exchange_order_id is None:
                await tracked_order.get_exchange_order_id()
            ex_order_id = tracked_order.exchange_order_id
            result = self._api_rest_client._delete(f'orders/{ex_order_id}')
            if result:
                tracked_order.last_state = 'cancelled'
                if wait_for_status:
                    from hummingbot.core.utils.async_utils import wait_til
                    await wait_til(lambda: tracked_order.is_cancelled)
                return order_id
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().network(
                f"Failed to cancel order {order_id}: {str(e)}",
                exc_info=True,
                app_warning_msg=f"Failed to cancel the order {order_id} on FTX. "
                                f"Check API key and network connection."
            )

    async def _status_polling_loop(self):
        """
        Periodically update user balances and order status via REST API. This serves as a fallback measure for web
        socket API updates.
        """
        while True:
            try:
                self._poll_notifier = asyncio.Event()
                await self._poll_notifier.wait()
                await safe_gather(
                    self._update_balances(),
                    self._update_order_status(),
                    self._update_positions()
                )
                self._last_poll_timestamp = self.current_timestamp
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(str(e), exc_info=True)
                self.logger().network("Unexpected error while fetching account updates.",
                                      exc_info=True,
                                      app_warning_msg="Could not fetch account updates from FTX. "
                                                      "Check API key and network connection.")
                await asyncio.sleep(0.5)

    async def _update_balances(self):
        """
        Calls REST API to update total and available balances.
        """
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()
        account_info = self._api_rest_client.get_balances()
        if type(account_info) == list:
            for currency in account_info:
                asset_name = currency['coin']
                self._account_available_balances[asset_name] = Decimal(str(currency['free']))
                self._account_balances[asset_name] = Decimal(str(currency['total']))
                remote_asset_names.add(asset_name)
            asset_names_to_remove = local_asset_names.difference(remote_asset_names)
            for asset_name in asset_names_to_remove:
                del self._account_available_balances[asset_name]
                del self._account_balances[asset_name]
        else:
            if account_info.get('success') is False:
                if not account_info['success']:
                    self.logger().info(f"Info - API error: {account_info}")

    async def _update_order_status(self):
        """
        Calls REST API to get status update for each in-flight order.
        """
        last_tick = self._last_poll_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL
        current_tick = self.current_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL
        if current_tick > last_tick and len(self._in_flight_orders) > 0:
            tracked_orders = list(self._in_flight_orders.values())
            tasks = []
            for tracked_order in tracked_orders:
                order_id = await tracked_order.get_exchange_order_id()
                tasks.append(self._api_rest_client._get(f'orders/{order_id}'))
            self.logger().debug(f"Polling for order status updates of {len(tasks)} orders.")
            update_results = tasks
            for update_result in update_results:
                if isinstance(update_result, Exception):
                    raise update_result
                if update_result:
                    await safe_gather(self._process_order_message(update_result))

    async def _process_order_message(self, order_msg: Dict[str, Any]):
        """
        Updates in-flight order and triggers cancellation or failure event if needed.
        :param order_msg: The order response from either REST or web socket API (they are of the same format)
        """
        client_order_id = order_msg["clientId"]
        if client_order_id not in self._in_flight_orders:
            return
        tracked_order = self._in_flight_orders[client_order_id]
        # Update order execution status
        tracked_order.last_state = order_msg["status"]
        if tracked_order.is_cancelled:
            self.logger().info(f"Successfully cancelled order {client_order_id}.")
            self.trigger_event(MarketEvent.OrderCancelled,
                               OrderCancelledEvent(
                                   self.current_timestamp,
                                   client_order_id))
            tracked_order.cancelled_event.set()
            self.stop_tracking_order(client_order_id)
        elif tracked_order.is_failure:
            self.logger().info(f"The market order {client_order_id} has failed according to order status API. ")
            self.trigger_event(MarketEvent.OrderFailure,
                               MarketOrderFailureEvent(
                                   self.current_timestamp,
                                   client_order_id,
                                   tracked_order.order_type
                               ))
            self.stop_tracking_order(client_order_id)

    async def _process_trade_message(self, trade_msg: Dict[str, Any]):
        """
        Updates in-flight order and trigger order filled event for trade message received. Triggers order completed
        event if the total executed amount equals to the specified order amount.
        """
        for order in self._in_flight_orders.values():
            await order.get_exchange_order_id()
        track_order = [o for o in self._in_flight_orders.values() if str(trade_msg["orderId"]) == o.exchange_order_id]
        if not track_order:
            return
        tracked_order = track_order[0]
        updated = tracked_order.update_with_trade_update(trade_msg)
        if not updated:
            return
        print(tracked_order)
        print(trade_msg)
        self.trigger_event(
            MarketEvent.OrderFilled,
            OrderFilledEvent(
                self.current_timestamp,
                tracked_order.client_order_id,
                tracked_order.trading_pair,
                tracked_order.trade_type,
                tracked_order.order_type,
                Decimal(str(trade_msg["price"])),
                Decimal(str(trade_msg["size"])),
                TradeFee(0.0, [(trade_msg["feeCurrency"], Decimal(str(trade_msg["fee"])))]),
                exchange_trade_id=trade_msg["tradeId"]
            )
        )
        if(math.isclose(tracked_order.executed_amount_base, tracked_order.amount) or
                tracked_order.executed_amount_base >= tracked_order.amount):
            tracked_order.last_state = "FILLED"
            self.logger().info(f"The {tracked_order.trade_type.name} order "
                               f"{tracked_order.client_order_id} has completed "
                               f"according to order status API.")
            event_tag = MarketEvent.BuyOrderCompleted if tracked_order.trade_type is TradeType.BUY \
                else MarketEvent.SellOrderCompleted
            event_class = BuyOrderCompletedEvent if tracked_order.trade_type is TradeType.BUY \
                else SellOrderCompletedEvent
            self.trigger_event(event_tag,
                               event_class(self.current_timestamp,
                                           tracked_order.client_order_id,
                                           tracked_order.base_asset,
                                           tracked_order.quote_asset,
                                           tracked_order.fee_asset,
                                           tracked_order.executed_amount_base,
                                           tracked_order.executed_amount_quote,
                                           tracked_order.fee_paid,
                                           tracked_order.order_type))
            self.stop_tracking_order(tracked_order.client_order_id)

    async def cancel_all(self, timeout_seconds: float):
        """
        Cancels all in-flight orders and waits for cancellation results.
        Used by bot's top level stop and exit commands (cancelling outstanding orders on exit)
        :param timeout_seconds: The timeout at which the operation will be canceled.
        :returns List of CancellationResult which indicates whether each order is successfully cancelled.
        """
        incomplete_orders = [o for o in self._in_flight_orders.values() if not o.is_done]
        tasks = [self._execute_cancel(o.trading_pair, o.client_order_id, True) for o in incomplete_orders]
        order_id_set = set([o.client_order_id for o in incomplete_orders])
        successful_cancellations = []
        try:
            async with timeout(timeout_seconds):
                results = await safe_gather(*tasks, return_exceptions=True)
                for result in results:
                    if result is not None and not isinstance(result, Exception):
                        order_id_set.remove(result)
                        successful_cancellations.append(CancellationResult(result, True))
        except Exception:
            self.logger().error("Cancel all failed.", exc_info=True)
            self.logger().network(
                "Unexpected error cancelling orders.",
                exc_info=True,
                app_warning_msg="Failed to cancel order on Ftx. Check API key and network connection."
            )
        failed_cancellations = [CancellationResult(oid, False) for oid in order_id_set]
        return successful_cancellations + failed_cancellations

    async def _get_funding_rate(self, trading_pair):
        # TODO: Note --- the "premiumIndex" endpoint can get markPrice, indexPrice, and nextFundingTime as well
        funding_rates = self._api_rest_client.get_funding_rate(ftx_perpetual_utils.convert_to_exchange_trading_pair(trading_pair))
        self._funding_rate = Decimal(funding_rates[0])

    def get_funding_rate(self, trading_pair):
        safe_ensure_future(self._get_funding_rate(ftx_perpetual_utils.convert_to_exchange_trading_pair(trading_pair)))
        return self._funding_rate

    async def _set_margin(self, trading_pair: str, leverage: int = 1):
        params = {
            "leverage": leverage
        }
        set_leverage = self._api_rest_client._post(
            'account/leverage',
            params=params
        )
        if set_leverage["leverage"] == leverage:
            self.logger().info(f"Leverage Successfully set to {leverage}.")
        else:
            self.logger().error("Unable to set leverage.")
        return leverage

    def set_margin(self, trading_pair: str, leverage: int = 1):
        safe_ensure_future(self._set_margin(trading_pair, leverage))

    """
    async def get_position_pnl(self, trading_pair: str):
        await self._update_positions()
        return self._account_positions.get(trading_pair)
    """
    async def _update_positions(self):
        # local_position_names = set(self._account_positions.keys())
        # remote_position_names = set()
        account_info = self._api_rest_client.get_account_info()
        for position in account_info['positions']:
            if position.get("entryPrice") is None:
                position_entry_price_ftx = '0.0'
            else:
                position_entry_price_ftx = position.get("entryPrice")
            position_side_ftx = 'SHORT' if position.get('side') == 'sell' else 'LONG'
            trading_pair = position.get("future")
            position_side = PositionSide[position_side_ftx]
            unrealized_pnl = Decimal(position.get("unrealizedPnl"))
            entry_price = Decimal(position_entry_price_ftx)
            amount = Decimal(position.get("size"))
            leverage = Decimal(account_info.get("leverage"))
            if amount > 0:
                self._account_positions[trading_pair + position_side.name] = Position(
                    trading_pair=ftx_perpetual_utils.convert_to_exchange_trading_pair(trading_pair),
                    position_side=position_side,
                    unrealized_pnl=unrealized_pnl,
                    entry_price=entry_price,
                    amount=amount,
                    leverage=leverage
                )

    def tick(self, timestamp: float):
        """
        Is called automatically by the clock for each clock's tick (1 second by default).
        It checks if status polling task is due for execution.
        """
        now = time.time()
        poll_interval = (self.SHORT_POLL_INTERVAL
                         if now - self._user_stream_tracker.last_recv_time > 60.0
                         else self.LONG_POLL_INTERVAL)
        last_tick = self._last_timestamp / poll_interval
        current_tick = timestamp / poll_interval
        if current_tick > last_tick:
            if not self._poll_notifier.is_set():
                self._poll_notifier.set()
        self._last_timestamp = timestamp

    def get_fee(self,
                base_currency: str,
                quote_currency: str,
                order_type: OrderType,
                order_side: TradeType,
                amount: Decimal,
                price: Decimal = s_decimal_NaN) -> TradeFee:
        """
        To get trading fee, this function is simplified by using fee override configuration. Most parameters to this
        function are ignore except order_type. Use OrderType.LIMIT_MAKER to specify you want trading fee for
        maker order.
        """
        is_maker = order_type is OrderType.LIMIT_MAKER
        return TradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _set_hedge_mode(self, position_mode: str = None):
        return False

    async def _get_hedge_mode(self):
        return self._position_mode

    def set_hedge_mode(self, position_mode: str = None):
        safe_ensure_future(self._set_hedge_mode(position_mode))

    async def _iter_user_event_queue(self) -> AsyncIterable[Dict[str, any]]:
        while True:
            try:
                yield await self._user_stream_tracker.user_stream.get()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unknown error. Retrying after 1 seconds.",
                    exc_info=True,
                    app_warning_msg="Could not fetch user events from Ftx. Check API key and network connection."
                )
                await asyncio.sleep(1.0)

    async def _user_stream_event_listener(self):
        """
        Listens to message in _user_stream_tracker.user_stream queue. The messages are put in by
        FtxAPIUserStreamDataSource.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                if event_message.get('type') == 'update':
                    channel = event_message["channel"]
                    if "fills" in channel:
                        await self._process_trade_message(event_message['data'])
                        await safe_gather(self._update_balances())
                    elif "orders" in channel:
                        await self._process_order_message(event_message['data'])
                        await safe_gather(self._update_balances())
                else:
                    continue
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await asyncio.sleep(5.0)
