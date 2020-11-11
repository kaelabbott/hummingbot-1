import asyncio
import unittest
import conf

from hummingbot.connector.exchange.bitblinx.bitblinx_auth import BitblinxAuth
from hummingbot.connector.exchange.bitblinx.bitblinx_user_stream_tracker import BitblinxUserStreamTracker

api_key =''
class BitblinxUserStreamTrackerUnitTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        
        cls.ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        cls.bitblinx_auth = BitblinxAuth(api_key)
        cls.trading_pair = ["BTC/USDT"]   
        cls.user_stream_tracker: BitblinxUserStreamTracker = BitblinxUserStreamTracker(
            bitblinx_auth=cls.bitblinx_auth, trading_pairs=cls.trading_pair)
        cls.user_stream_tracker_task: asyncio.Task = asyncio.ensure_future(
            cls.user_stream_tracker.start())

    def test_user_stream(self):
        # Wait process some msgs.
        self.ev_loop.run_until_complete(asyncio.sleep(120.0))
        print(self.user_stream_tracker.user_stream.qsize())
        assert self.user_stream_tracker.user_stream.qsize() > 0
