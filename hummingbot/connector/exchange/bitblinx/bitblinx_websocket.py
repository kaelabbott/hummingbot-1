#!/usr/bin/env python
import asyncio
import logging
import websockets
import ujson

from typing import Optional, AsyncIterable, Any
from websockets.exceptions import ConnectionClosed
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.bitblinx import BITBLINX_WS
from hummingbot.connector.exchange.bitblinx.bitblinx_auth import BitblinxAuth


# reusable websocket class
class BitblinxWebsocket():
    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, auth: Optional[BitblinxAuth]):
        self._client = None
        self._auth = auth
        self._consumers = dict()
        self._listening = False

    # connect to exchange
    async def connect(self):
        try:
            self._client = await websockets.connect(BITBLINX_WS)
            return self
        except Exception as e:
            self.logger().error(f"Websocket error: '{str(e)}'")

    # disconnect from exchange
    async def disconnect(self):
        if self._client is None:
            return

        await self._client.wait_closed()

    # listen for new websocket messages and add them to queue
    async def _messages(self) -> AsyncIterable[Any]:
        try:
            while True:
                try:
                    raw_msg_str: str = await asyncio.wait_for(self._client.recv(), timeout=self.MESSAGE_TIMEOUT)
                    raw_msg = ujson.loads(raw_msg_str)
                    if raw_msg['method'] == 'authorize':
                        raw_msg_order: str = await asyncio.wait_for(self._client.recv(), timeout=self.MESSAGE_TIMEOUT)
                        continue
                    if raw_msg['method'] == 'newUserTrade':
                        raw_msg_order: str = await asyncio.wait_for(self._client.recv(), timeout=self.MESSAGE_TIMEOUT)
                        raw_msg_order = ujson.loads(raw_msg_order)
                        raw_msg['result']['orderUpdate'] = raw_msg_order.get('result')
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

    # listen to messages by method
    async def on_message(self) -> AsyncIterable[Any]:
        async for msg in self._messages():
            yield msg

    async def authenticate(self):
        if self._auth is None:
            raise "auth not provided"
        payload = self._auth.generate_auth_payload()
        return await self.request(payload)
