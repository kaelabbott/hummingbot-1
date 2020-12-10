import hashlib
import hmac
import time


class FtxAuth():
    def __init__(self, api_key: str, secret_key: str, subaccount_name: str = None):
        self.api_key = api_key
        self.secret_key = secret_key
        self.subaccount_name = subaccount_name
        self.last_nonce = 0

    def _sign_payload(self, payload) -> str:
        sig = hmac.new(self.secret_key.encode('utf8'),
                       payload.encode('utf8'),
                       hashlib.sha384).hexdigest()
        return sig

    def get_nonce(self) -> int:
        nonce = int(round(time.time() * 1_000_000))

        if self.last_nonce == nonce:
            nonce = nonce + 1
        elif self.last_nonce > nonce:
            nonce = self.last_nonce + 1

        self.last_nonce = nonce

        return nonce

    def generate_auth_payload(self):
        """
        Sign payload
        """
        ts = int(time.time() * 1000)
        payload = {'op': 'login', 'args': {
            'key': self.api_key,
            'sign': hmac.new(
                self.secret_key.encode(), f'{ts}websocket_login'.encode(), 'sha256').hexdigest(),
            'time': ts,
        }}
        if self.subaccount_name is not None:
            payload['args']['subaccount'] = self.subaccount_name
        return payload
