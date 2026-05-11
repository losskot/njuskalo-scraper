"""
Azure proxy client - routes HTTP requests through Azure Functions
deployed in multiple regions for IP diversity.

Usage:
    client = AzureProxyClient("azure_proxies.json")
    html = await client.fetch(url, headers, cookies)
"""

import json
import os
import random
import time
import asyncio
import aiohttp
import logging

logger = logging.getLogger(__name__)


class AzureProxyClient:
    """Rotates requests across Azure Function proxies in multiple regions."""

    def __init__(self, config_path=None):
        if config_path is None:
            config_path = os.path.join(os.path.dirname(__file__), "azure_proxies.json")

        self.proxies = []
        self._index = 0
        self._session = None

        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                self.proxies = json.load(f)
            print(f"[AZURE] Loaded {len(self.proxies)} Azure proxy regions: "
                  f"{[p['region'] for p in self.proxies]}")
        else:
            print(f"[AZURE] No config at {config_path} — Azure proxies disabled")

    @property
    def available(self):
        return len(self.proxies) > 0

    def _next_proxy(self):
        """Round-robin through available Azure proxies."""
        proxy = self.proxies[self._index % len(self.proxies)]
        self._index += 1
        return proxy

    def _random_proxy(self):
        """Pick a random Azure proxy (reduces pattern detection)."""
        return random.choice(self.proxies)

    async def _get_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def fetch(self, url, headers=None, cookies=None, timeout=15):
        """
        Fetch a URL through a random Azure Function proxy.

        Returns:
            tuple: (status_code: int, body: str, region: str)

        Raises:
            Exception on all-proxies-exhausted or critical error.
        """
        if not self.proxies:
            raise RuntimeError("No Azure proxies configured")

        # Try up to 3 different regions on failure
        tried = set()
        last_error = None

        for attempt in range(min(3, len(self.proxies))):
            proxy = self._random_proxy()
            if proxy["region"] in tried:
                proxy = self._next_proxy()
            tried.add(proxy["region"])

            func_url = proxy["url"]
            func_key = proxy["key"]

            payload = {
                "url": url,
                "headers": headers or {},
                "cookies": cookies or {},
                "timeout": timeout
            }

            try:
                session = await self._get_session()
                async with session.post(
                    func_url,
                    json=payload,
                    params={"code": func_key},
                    timeout=aiohttp.ClientTimeout(total=timeout + 10)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        status = data.get("status_code", 0)
                        body = data.get("body", "")
                        logger.info(f"[AZURE:{proxy['region']}] {url} -> {status} ({len(body)} bytes)")
                        return status, body, proxy["region"]
                    else:
                        error_text = await resp.text()
                        last_error = f"HTTP {resp.status}: {error_text[:200]}"
                        logger.warning(f"[AZURE:{proxy['region']}] {url} failed: {last_error}")

            except asyncio.TimeoutError:
                last_error = f"Timeout from {proxy['region']}"
                logger.warning(f"[AZURE:{proxy['region']}] {url} timed out")
            except Exception as e:
                last_error = str(e)
                logger.warning(f"[AZURE:{proxy['region']}] {url} error: {e}")

        raise RuntimeError(f"All Azure proxies failed for {url}. Last error: {last_error}")

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
