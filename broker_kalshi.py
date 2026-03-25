import os
import base64
import time
from typing import Any
from urllib.parse import urlparse

import requests

try:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
except Exception:
    hashes = None
    serialization = None
    padding = None


class BrokerError(RuntimeError):
    pass


class KalshiBroker:
    def __init__(
        self,
        base_url: str,
        user_agent: str,
        timeout_sec: int = 20,
        bearer_token: str = "",
        api_key: str = "",
        key_id: str = "",
        private_key_pem: str = "",
        private_key_path: str = "",
        extra_auth_header: str = "",
        extra_auth_value: str = "",
        order_path: str = "/portfolio/orders",
    ):
        self.base_url = base_url.rstrip("/")
        self.user_agent = user_agent
        self.timeout_sec = timeout_sec
        self.bearer_token = bearer_token
        self.api_key = api_key
        self.key_id = key_id
        self.private_key_pem = private_key_pem
        self.private_key_path = private_key_path
        self.extra_auth_header = extra_auth_header
        self.extra_auth_value = extra_auth_value
        self.order_path = order_path
        self._private_key = None

    @classmethod
    def from_env(cls, user_agent: str) -> "KalshiBroker":
        timeout_raw = os.getenv("KALSHI_TIMEOUT_SEC", "20")
        try:
            timeout_sec = int(timeout_raw) if timeout_raw not in (None, "") else 20
        except Exception:
            timeout_sec = 20
        return cls(
            base_url=os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2"),
            user_agent=user_agent,
            timeout_sec=timeout_sec,
            bearer_token=os.getenv("KALSHI_BEARER_TOKEN", ""),
            api_key=os.getenv("KALSHI_API_KEY", ""),
            key_id=os.getenv("KALSHI_KEY_ID", "") or os.getenv("KALSHI_API_KEY", ""),
            private_key_pem=os.getenv("KALSHI_PRIVATE_KEY", ""),
            private_key_path=os.getenv("KALSHI_PRIVATE_KEY_PATH", ""),
            extra_auth_header=os.getenv("KALSHI_AUTH_HEADER", ""),
            extra_auth_value=os.getenv("KALSHI_AUTH_VALUE", ""),
            order_path=os.getenv("KALSHI_ORDER_PATH", "/portfolio/orders"),
        )

    def has_auth(self) -> bool:
        return bool(
            self.bearer_token
            or (self.key_id and (self.private_key_pem or self.private_key_path))
            or self.api_key
            or (self.extra_auth_header and self.extra_auth_value)
        )

    def _load_private_key(self):
        if self._private_key is not None:
            return self._private_key
        if serialization is None:
            raise BrokerError("cryptography package is required for Kalshi RSA signed auth")

        pem: str = self.private_key_pem or ""
        if self.private_key_path:
            try:
                with open(self.private_key_path, "rb") as f:
                    pem_bytes = f.read()
            except Exception as e:
                raise BrokerError(f"Unable to read KALSHI_PRIVATE_KEY_PATH: {e}") from e
        elif pem:
            # Support escaped newlines in .env: \n
            pem_bytes = pem.replace("\\n", "\n").encode("utf-8")
        else:
            raise BrokerError("Missing private key: set KALSHI_PRIVATE_KEY or KALSHI_PRIVATE_KEY_PATH")

        try:
            self._private_key = serialization.load_pem_private_key(pem_bytes, password=None)
        except Exception as e:
            raise BrokerError(f"Unable to parse Kalshi private key PEM: {e}") from e
        return self._private_key

    def _signed_headers(self, method: str, url: str) -> dict[str, str]:
        if not self.key_id:
            raise BrokerError("Missing KALSHI_KEY_ID for signed auth")
        private_key = self._load_private_key()
        ts_ms = str(int(time.time() * 1000))
        parsed = urlparse(url)
        path = parsed.path or "/"
        message = f"{ts_ms}{method.upper()}{path}".encode("utf-8")
        try:
            signature = private_key.sign(
                message,
                padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
                hashes.SHA256(),
            )
        except Exception as e:
            raise BrokerError(f"Failed to sign Kalshi request: {e}") from e
        sig_b64 = base64.b64encode(signature).decode("ascii")
        return {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts_ms,
            "KALSHI-ACCESS-SIGNATURE": sig_b64,
        }

    def _headers(self, method: str, url: str) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": self.user_agent,
        }
        if self.key_id and (self.private_key_pem or self.private_key_path):
            headers.update(self._signed_headers(method, url))
            return headers
        if self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"
        if self.api_key:
            headers["KALSHI-API-KEY"] = self.api_key
        if self.extra_auth_header and self.extra_auth_value:
            headers[self.extra_auth_header] = self.extra_auth_value
        return headers

    def _url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        return f"{self.base_url}/{path.lstrip('/')}"

    def request(self, method: str, path: str, params: dict | None = None, body: dict | None = None) -> dict[str, Any]:
        url = self._url(path)
        try:
            r = requests.request(
                method=method.upper(),
                url=url,
                headers=self._headers(method, url),
                params=params,
                json=body,
                timeout=self.timeout_sec,
            )
        except requests.RequestException as e:
            raise BrokerError(f"Request failed {method.upper()} {url}: {e}") from e

        if r.status_code >= 400:
            msg = r.text.strip()
            raise BrokerError(f"HTTP {r.status_code} {method.upper()} {url}: {msg[:600]}")

        try:
            return r.json()
        except Exception as e:
            raise BrokerError(f"Non-JSON response from {method.upper()} {url}: {e}") from e

    def get_market(self, ticker: str) -> dict[str, Any]:
        data = self.request("GET", f"/markets/{ticker}")
        market = data.get("market")
        if isinstance(market, dict):
            return market
        if isinstance(data, dict) and data:
            return data
        raise BrokerError(f"Missing market payload for ticker={ticker}")

    def place_order(self, order_payload: dict[str, Any]) -> dict[str, Any]:
        return self.request("POST", self.order_path, body=order_payload)
