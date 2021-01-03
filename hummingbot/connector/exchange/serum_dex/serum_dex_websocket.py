#!/usr/bin/env python
import asyncio
import logging
import websockets
import ujson

from typing import Optional, AsyncIterable, Any, Dict
from websockets.exceptions import ConnectionClosed
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.ftx.ftx_auth import FtxAuth

# reusable websocket class


class SerumDexWebsocket():
    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, auth: Optional[FtxAuth] = None):
        self._auth: Optional[FtxAuth] = auth
        self._WS_URL = "wss://ftx.com/ws/"
        self._client: Optional[websockets.WebSocketClientProtocol] = None

    # connect to exchange
    async def connect(self):
        try:
            self._client = await websockets.connect(self._WS_URL)
            return self._client
        except Exception as e:
            self.logger().error(f"Websocket error: '{str(e)}'", exc_info=True)

    # disconnect from exchange
    async def disconnect(self):
        if self._client is None:
            return
        await self._client.close()

    # receive & parse messages
    async def _messages(self) -> AsyncIterable[Any]:
        try:
            while True:
                try:
                    await asyncio.wait_for(self._client.recv(), timeout=self.MESSAGE_TIMEOUT)
                    raw_msg_str: str = await asyncio.wait_for(self._client.recv(), timeout=self.MESSAGE_TIMEOUT)
                    raw_msg = ujson.loads(raw_msg_str)
                    yield raw_msg
                except asyncio.TimeoutError:
                    await asyncio.wait_for(self._client.ping(), timeout=self.PING_TIMEOUT)
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            return
        except ConnectionClosed:
            return
        finally:
            await self.disconnect()

    async def _messages_public(self) -> AsyncIterable[Any]:
        try:
            while True:
                try:
                    raw_msg_str: str = await asyncio.wait_for(self._client.recv(), timeout=self.MESSAGE_TIMEOUT)
                    raw_msg = ujson.loads(raw_msg_str)
                    message_type = raw_msg['type']
                    if message_type == 'partial':
                        continue
                    yield raw_msg
                except asyncio.TimeoutError:
                    await asyncio.wait_for(self._client.ping(), timeout=self.PING_TIMEOUT)
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            return
        except ConnectionClosed:
            return
        finally:
            await self.disconnect()

    # emit messages
    async def _emit(self, data: Optional[Any] = {}) -> None:
        await self._client.send(ujson.dumps(data))
        return None

    # request via websocket
    async def request(self, data: Optional[Any] = {}):

        return await self._emit(data)

    # subscribe to a method
    async def subscribe(self, subscription: Dict):
        subscriptions = {'op': 'subscribe', **subscription}
        return await self.request(subscriptions)

    # unsubscribe to a method
    async def unsubscribe(self, subscription: Dict):
        subscriptions = {'op': 'unsubscribe', **subscription}
        return await self.request(subscriptions)

    # listen to messages by method
    async def on_message(self) -> AsyncIterable[Any]:
        async for msg in self._messages():
            yield msg

    async def on_message_public(self) -> AsyncIterable[Any]:
        async for msg in self._messages_public():
            yield msg

    async def authenticate(self):
        if self._auth is None:
            raise "auth not provided"
        payload = self._auth.generate_auth_payload()
        return await self.request(payload)
