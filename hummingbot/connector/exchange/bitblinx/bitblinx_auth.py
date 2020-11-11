import hashlib
import hmac
import time


class BitblinxAuth():
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.secret_key = None # Not needed for bitblinx.
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
        payload = {
            "method": 'authorize',
            "params":{
                "token": self.api_key,
                 "isApiKey": True
            },
            "id": 1
        }

        return payload

    def generate_api_headers(self):
        """
        Generate headers for a signed payload
        """
     #   nonce = str(self.get_nonce())
    #    signature = "/api/" + path + nonce + body

   #     sig = self._sign_payload(signature)

        return {
            'Authorization': "api-sx <{0}>".format(self.api_key),
            'Accept-Language': 'en',
            "content-type": "application/json"
        }
