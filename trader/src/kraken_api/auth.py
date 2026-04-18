# src/kraken_api/auth.py

"""
Kraken API HMAC-SHA512 signing for private REST requests.

Spot REST signing spec:
https://docs.kraken.com/api/docs/guides/spot-rest-auth/
"""

import base64
import hashlib
import hmac
import urllib.parse
from typing import Any


def get_kraken_signature(urlpath: str, data: dict[str, Any], secret: str) -> str:
    """
    Compute Kraken HMAC-SHA512 signature for a private REST request.

    Algorithm:
        HMAC-SHA512(
            key  = base64_decode(secret),
            msg  = urlpath.encode() + SHA256(nonce + urlencode(data))
        ) → base64 encode
    """
    if "nonce" not in data:
        raise ValueError("Private requests must include 'nonce'")

    postdata = urllib.parse.urlencode(data)
    encoded = (str(data["nonce"]) + postdata).encode()
    message = urlpath.encode() + hashlib.sha256(encoded).digest()
    mac = hmac.new(base64.b64decode(secret), message, hashlib.sha512)
    return base64.b64encode(mac.digest()).decode()
