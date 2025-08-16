#!/usr/bin/env python3
import json
import logging
import sys
import random

# --- Advanced Scraping Constants ---

# Headers to mimic a direct browser navigation request
STEALTH_HEADERS = {
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-User": "?1",
    "Sec-Fetch-Dest": "document"
}

# Business hours for time-based request patterns (in UTC)
BIZ_HOURS = {"start_local": 8, "end_local": 22}

# A pool of browser fingerprints to rotate through
FINGERPRINTS = [
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Viewport-Width": "1920"
    },
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"macOS"',
        "Viewport-Width": "1728"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
        "Sec-Ch-Ua": '"Microsoft Edge";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Viewport-Width": "1536"
    }
]

# User-Agent families for rotation on 403 errors
UA_FAMILIES = {
    "chrome": [fp["User-Agent"] for fp in FINGERPRINTS if "Chrome" in fp["User-Agent"] and "Edge" not in fp["User-Agent"]],
    "edge": [fp["User-Agent"] for fp in FINGERPRINTS if "Edge" in fp["User-Agent"]],
}
if not UA_FAMILIES["chrome"]: UA_FAMILIES["chrome"] = [FINGERPRINTS[0]["User-Agent"]] # Fallback
if not UA_FAMILIES["edge"]: UA_FAMILIES["edge"] = [FINGERPRINTS[-1]["User-Agent"]] # Fallback


# Headers to try and prevent getting a cached response
CACHE_BUST_HEADERS = {
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "If-Modified-Since": "Thu, 01 Jan 1970 00:00:00 GMT"
}

# Headers to mimic a request coming from behind a corporate proxy
SOCIAL_HEADERS = {
  "Via": "1.1 corporate-proxy.internal.net"
}

# --- End Constants ---
from pathlib import Path
from typing import Dict, Any

def load_config(path: str = 'config.json') -> Dict[str, Any]:
    """
    Loads the main configuration file.
    On critical errors (file not found, parse error), it logs the error
    and exits the application to prevent running in a broken state.
    """
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.critical(f"FATAL: Configuration file '{path}' not found. Application cannot continue.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        logging.critical(f"FATAL: Could not parse configuration file '{path}': {e}. Application cannot continue.")
        sys.exit(1)
    return {} # Should not be reached in error cases