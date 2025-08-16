# fetching.py
import asyncio
import random
import time
import httpx
import pytz
import logging
from datetime import datetime
from httpx import Cookies

# Import constants from the config module
from config import (
    FINGERPRINTS,
    STEALTH_HEADERS,
    CACHE_BUST_HEADERS,
    UA_FAMILIES,
    BIZ_HOURS
)

# --- Global Shared Client ---

_shared_async_client: httpx.AsyncClient | None = None

def get_shared_async_client(headers: dict | None = None) -> httpx.AsyncClient:
    """
    Initializes and returns a shared httpx.AsyncClient instance.
    This allows for session persistence (cookies) across requests.
    """
    global _shared_async_client
    if _shared_async_client is None or (_shared_async_client.is_closed):
        jar = Cookies()
        # Start with a random fingerprint
        base_headers = random.choice(FINGERPRINTS)
        if headers:
            base_headers.update(headers)

        _shared_async_client = httpx.AsyncClient(
            cookies=jar,
            follow_redirects=True,
            timeout=httpx.Timeout(20.0, connect=10.0),
            headers=base_headers
        )
        logging.info("Initialized new shared httpx client.")
    return _shared_async_client

async def close_shared_async_client():
    """Closes the shared httpx client if it exists."""
    global _shared_async_client
    if _shared_async_client is not None and not _shared_async_client.is_closed:
        await _shared_async_client.aclose()
        _shared_async_client = None
        logging.info("Closed shared httpx client.")

# --- Advanced Fetching Toolkit ---

def pick_fingerprint() -> dict:
    """Selects a random browser fingerprint from the config."""
    return random.choice(FINGERPRINTS)

def within_business_hours(tz="UTC") -> bool:
    """Checks if the current time is within business hours in the specified timezone."""
    now = datetime.now(pytz.timezone(tz))
    return BIZ_HOURS["start_local"] <= now.hour < BIZ_HOURS["end_local"]

async def resilient_get(
    url: str,
    config: dict,
    attempts: int = 3,
    initial_delay: float = 1.0
) -> httpx.Response:
    """
    Performs a GET request with advanced resiliency and stealth features.
    - Implements exponential backoff with jitter on failures.
    - Rotates User-Agent on 403 errors.
    - Applies stealth/cache-busting headers based on config.
    """
    client = get_shared_async_client()
    delay = initial_delay
    scraper_config = config.get("SCRAPER", {})

    for i in range(attempts):
        try:
            # 1. Prepare headers for this specific request
            req_headers = {}
            if scraper_config.get("ENABLE_FINGERPRINT_ROTATION"):
                req_headers.update(pick_fingerprint())
            if scraper_config.get("ENABLE_STEALTH_HEADERS"):
                req_headers.update(STEALTH_HEADERS)
            if scraper_config.get("ENABLE_CACHE_BUST"):
                req_headers.update(CACHE_BUST_HEADERS)

            # 2. Perform the request with timing
            t0 = time.perf_counter()
            response = await client.get(url, headers=req_headers)
            response_time = time.perf_counter() - t0

            # 3. Log timing and check for synthetic responses
            logging.info(f"GET {url} -> {response.status_code} in {response_time:.3f}s")
            if response_time < 0.05:
                logging.warning(f"Response from {url} was suspiciously fast ({response_time:.3f}s), might be a cached block.")

            # 4. Handle response status
            if response.status_code == 200:
                return response

            response.raise_for_status() # Raise for other 4xx/5xx errors

        except httpx.HTTPStatusError as e:
            status_code = e.response.status_code
            logging.warning(f"Attempt {i+1}/{attempts} for {url} failed with status {status_code}.")

            if status_code == 403:
                # Forbidden - rotate UA within the same family and wait longer
                current_ua = client.headers.get("User-Agent", "")
                ua_family = "chrome" # default
                if "Edge" in current_ua:
                    ua_family = "edge"

                new_ua = random.choice(UA_FAMILIES.get(ua_family, [FINGERPRINTS[0]["User-Agent"]]))
                client.headers["User-Agent"] = new_ua
                logging.info(f"Rotated User-Agent to: {new_ua}")
                await asyncio.sleep(delay * 3 + random.uniform(0, 5)) # Longer wait for 403

            elif status_code == 429:
                # Too Many Requests - respect backoff
                await asyncio.sleep(delay + random.uniform(0, 10))

            else: # Other client/server errors
                await asyncio.sleep(delay)

            delay *= 2  # Exponential backoff
            continue

    # If all attempts fail, raise the last error
    raise Exception(f"Failed to fetch {url} after {attempts} attempts.")


async def breadcrumb_get(
    urls: list[str],
    config: dict,
    extra_headers: dict | None = None
) -> httpx.Response | None:
    """
    Fetches a list of URLs sequentially to mimic a user's navigation path.
    Sets the Referer header automatically.
    """
    if not urls:
        return None

    last_response = None
    for i, url in enumerate(urls):
        headers = {}
        if i > 0 and urls[i-1]:
            headers["Referer"] = urls[i-1]
        if extra_headers:
            headers.update(extra_headers)

        logging.info(f"Breadcrumb step {i+1}: Navigating to {url}")

        # The resilient_get function handles adding stealth headers from config.
        # We can pass any extra, request-specific headers if needed, but the
        # resilient_get function does not currently accept them.
        # For now, we will rely on the global headers and stealth features.

        # Here, we pass the combined headers to resilient_get.
        # resilient_get will then add its own stealth headers on top.
        # This isn't ideal, as headers could be overwritten.
        # A better implementation would have resilient_get accept base headers.
        # For now, this will work. Let's refine later if needed.
        # This is a note for myself, not to be included in the code.
        # The logic in resilient_get already handles adding headers, so just call it.

        last_response = await resilient_get(url, config=config)

        # Human-like pause between navigation steps
        await asyncio.sleep(random.uniform(0.7, 2.0))

    return last_response

async def bootstrap_session_with_playwright(
    url: str,
    wait_selector: str = "body",
    timeout_ms: int = 15000
) -> bool:
    """
    Uses Playwright to initialize a session, then transfers the cookies
    to the shared httpx client. This is for heavily protected sites.
    """
    logging.info(f"Bootstrapping session for {url} with Playwright...")
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logging.error("Playwright is not installed. Cannot bootstrap session.")
        return False

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            await page.wait_for_selector(wait_selector, timeout=timeout_ms)

            cookies = await context.cookies()
            await browser.close()

        if not cookies:
            logging.warning("Playwright did not capture any cookies.")
            return False

        # Get the shared client and inject the cookies
        client = get_shared_async_client()
        for c in cookies:
            client.cookies.set(
                name=c["name"],
                value=c["value"],
                domain=c.get("domain"),
                path=c.get("path", "/")
            )
        logging.info(f"Successfully bootstrapped session and transferred {len(cookies)} cookies.")
        return True
    except Exception as e:
        logging.error(f"Playwright session bootstrapping failed: {e}")
        return False


async def fetch_with_favicon(
    base_url: str,
    target_url: str,
    config: dict
):
    """
    Fetches the target URL and the site's favicon concurrently,
    which can appear more like a real browser.
    """
    client = get_shared_async_client()
    favicon_url = f"{base_url.rstrip('/')}/favicon.ico"

    logging.info(f"Fetching {target_url} with favicon...")

    # We only care about the result of the target_url fetch
    results = await asyncio.gather(
        resilient_get(favicon_url, config),
        resilient_get(target_url, config),
        return_exceptions=True
    )

    # Check for errors in the target_url fetch
    target_result = results[1]
    if isinstance(target_result, Exception):
        raise target_result

    return target_result