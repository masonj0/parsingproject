#!/usr/bin/env python3
"""Paddock Parser Toolkit - Enhanced Scanner & Prefetcher Module (v1.4)
This module is now fully integrated with the "enabled" flag in config.json.
All functions, including the scanner, prefetcher, and connection tester,
will now ignore any data source that is marked as disabled.

Future Exploration Ideas:
- When making HTTP requests, consider setting the 'Referer' header to a common
  source like 'https://www.google.com/' or the base URL of the target site
  itself, as many APIs check this.
- For sites that are difficult to scrape, inspect the Network tab in browser
  developer tools for hidden API calls, as we discovered with Sporting Life
  and UKRacingForm.
"""
import sys
import asyncio
import argparse  # <-- Added import
import httpx
import json
import logging
from datetime import date
from pathlib import Path
from typing import Dict, Any, List, Optional
from jinja2 import Environment, FileSystemLoader, select_autoescape
from bs4 import BeautifulSoup
import re
import unicodedata
import hashlib # <-- Added import for hash-based ID generation if needed
import datetime as dt
from fetching import breadcrumb_get
from sources import (
    SourceAdapter,
    RawRaceDocument,
    RunnerDoc,
    FieldConfidence,
    register_adapter,
)

# Import the configuration loader
try:
    from config import load_config
except ImportError:
    print("FATAL: Could not import config.py. Ensure it's in the same directory.", file=sys.stderr)
    sys.exit(1)

# Shared Intelligence: The new normalizer is used in the main pipeline.
# This legacy import is no longer needed for the adapter-based architecture.

# --- CONFIGURATION HELPERS ---
def build_httpx_client_kwargs(config: Dict) -> Dict[str, Any]:
    """
    Builds kwargs for httpx.AsyncClient with corporate proxy and CA support.
    - HTTP_CLIENT.VERIFY_SSL: bool (default True)
    - HTTP_CLIENT.CA_BUNDLE: path to corporate root CA (optional)
    - HTTP_CLIENT.PROXIES: dict or string accepted by httpx (optional)
    """
    http_client = config.get("HTTP_CLIENT", {})
    verify_ssl = http_client.get("VERIFY_SSL", True)
    ca_bundle = http_client.get("CA_BUNDLE")  # e.g., "C:/company/ca.pem" or "/etc/ssl/certs/corp-ca.pem"
    proxies = http_client.get("PROXIES")      # e.g., {"http": "http://user:pass@proxy:8080", "https": "..."} or "http://..."

    # If a CA bundle path is provided, use it for verification.
    # Otherwise, use the VERIFY_SSL boolean flag.
    verify: Any = ca_bundle if ca_bundle else verify_ssl
    kwargs: Dict[str, Any] = {"verify": verify}
    if proxies:
        kwargs["proxies"] = proxies
    return kwargs
# --- END CONFIGURATION HELPERS ---


# --- Source Adapters ---
# This is the new, preferred way to add data sources.
# Eventually, all sources will be converted to this pattern.

@register_adapter
class TimeformAdapter:
    """
    Adapter for fetching racecards from Timeform.
    Uses breadcrumb navigation to appear more human.
    """
    source_id = "timeform"

    def _find_site_config(self, config: dict) -> dict | None:
        """Finds the specific configuration for Timeform from the main config."""
        for category in config.get("DATA_SOURCES", []):
            for site in category.get("sites", []):
                if "timeform" in site.get("name", "").lower():
                    return site
        return None

    async def fetch(self, config: dict) -> list[RawRaceDocument]:
        """
        Fetches the Timeform racecards page and performs a mock parse.
        """
        site_config = self._find_site_config(config)
        if not site_config:
            logging.error("Timeform site configuration not found.")
            return []

        base_url = site_config.get("base_url")
        target_url = site_config.get("url")

        if not base_url or not target_url:
            logging.error("Timeform base_url or url not configured.")
            return []

        # Use breadcrumb navigation to fetch the page
        logging.info("Fetching Timeform data using the new adapter...")
        try:
            response = await breadcrumb_get(urls=[base_url, target_url], config=config)
            if not response:
                logging.error("Failed to fetch Timeform data.")
                return []
            html_content = response.text
        except Exception as e:
            logging.error(f"An error occurred while fetching Timeform: {e}")
            return []

        # --- Save the fetched content to a file ---
        input_dir = Path(config.get("INPUT_DIR", "html_input"))
        input_dir.mkdir(exist_ok=True, parents=True)
        filename = sanitize_filename(site_config['name']) + ".html"
        output_path = input_dir / filename
        try:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(html_content)
            logging.info(f"ADAPTER_SUCCESS: Saved '{site_config['name']}' to {output_path}")
        except Exception as e:
            logging.error(f"ADAPTER_ERROR: Failed to write file for '{site_config['name']}': {e}")
            # Continue to parsing even if file save fails, as content is in memory

        # --- Placeholder Parsing Logic ---
        logging.info(f"Successfully fetched {len(html_content)} bytes from Timeform.")
        logging.info("Parsing Timeform HTML (placeholder)...")

        soup = BeautifulSoup(html_content, 'lxml')
        page_title = soup.title.string if soup.title else "No Title Found"

        dummy_runner = RunnerDoc(
            runner_id="timeform-dummy-runner-1",
            name=FieldConfidence(value="My Horse", confidence=0.9, provenance="title_tag"),
        )

        dummy_race = RawRaceDocument(
            source_id=self.source_id,
            fetched_at=dt.datetime.now(dt.timezone.utc).isoformat(),
            track_key="dummy_track",
            race_key="dummy_track_r1",
            start_time_iso=dt.datetime.now(dt.timezone.utc).isoformat(),
            runners=[dummy_runner],
            extras={
                "page_title": FieldConfidence(value=page_title, confidence=1.0)
            }
        )

        logging.info("Successfully created a dummy RawRaceDocument from Timeform.")
        return [dummy_race]



# - Helper Function for Filename Sanitization -
def sanitize_filename(name: str) -> str:
    """Cleans a string to be a valid filename."""
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')
    name = re.sub(r'[^\w\s-]', '_', name).strip()
    name = re.sub(r'\s+', '_', name)
    return name

# --- Core Fetching & Parsing Functions ---

async def fetch_url(client: httpx.AsyncClient, url: str, config: Dict) -> str:
    """Fetches content from a URL using the 'Perfect Disguise' browser headers
    from the configuration file for maximum compatibility."""
    logging.info(f"Fetching URL: {url}")
    headers = config.get("HTTP_HEADERS", {})
    if not headers:
        logging.warning("HTTP_HEADERS not found in config.json. Using a basic User-Agent.")
        headers = {'User-Agent': 'Mozilla/5.0'}
    timeout = config.get("HTTP_CLIENT", {}).get("REQUEST_TIMEOUT", 30.0)
    try:
        # Use the client passed in, which has proxy/CA settings
        response = await client.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        logging.info(f"[SUCCESS] Fetched {len(response.text)} chars from {url}")
        return response.text
    except httpx.HTTPStatusError as e:
        logging.error(f"[ERROR] HTTP Error {e.response.status_code} for {url}: {e}")
    except httpx.RequestError as e:
        logging.error(f"[ERROR] Request Error for {url}: {e}")
    except Exception as e:
        logging.error(f"[ERROR] Unexpected error fetching {url}: {e}")
    return ""

# --- Prefetching Logic ---

async def prefetch_source(client: httpx.AsyncClient, site: Dict[str, Any], config: Dict, today_str: str) -> bool:
    """Fetches and saves a single data source to the input directory."""
    input_dir = Path(config["INPUT_DIR"])
    input_dir.mkdir(exist_ok=True, parents=True)
    url = site["url"].format(date_str_iso=today_str)
    logging.info(f"Prefetching: {site['name']}")
    content = await fetch_url(client, url, config) # Pass client with settings
    if content:
        filename = sanitize_filename(site['name']) + ".html"
        output_path = input_dir / filename
        try:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(content)
            logging.info(f"[SUCCESS] Saved '{site['name']}' to {output_path}")
            return True
        except Exception as e:
            logging.error(f"[ERROR] Failed to write file for '{site['name']}': {e}")
    return False

async def run_batch_prefetch(config: Dict):
    """Automatically downloads all enabled data sources to the input folder."""
    logging.info("Starting Automated Pre-Fetch of Enabled Sources...")
    today_str = date.today().strftime("%Y-%m-%d")
    SKIP_LIST = ["(DISABLED)", "IGNORE", "SKIP"] # Hard-coded skip list

    # --- Use the helper to create the client with proxy/CA settings ---
    async with httpx.AsyncClient(follow_redirects=True, **build_httpx_client_kwargs(config)) as client:
        prefetch_tasks = []
        for category in config.get("DATA_SOURCES", []):
            logging.info(f"- Processing Category: {category['title']} -")
            sites = [site for site in category.get("sites", []) if site.get("enabled", True)]
            for site in sites:
                if any(skip_item in site['name'] for skip_item in SKIP_LIST):
                    logging.info(f"--> Skipping '{site['name']}' (on hard-coded skip list).")
                    continue

                # NEW: Skip if an adapter exists for this source.
                # This prevents fetching the same data twice.
                from sources import ADAPTERS
                adapter_source_ids = [adapter.source_id for adapter in ADAPTERS]
                if any(adapter_id in site['name'].lower() for adapter_id in adapter_source_ids):
                    logging.info(f"--> Skipping '{site['name']}' (handled by modern adapter).")
                    continue
                if site.get("url"):
                    task = asyncio.create_task(prefetch_source(client, site, config, today_str)) # Pass client
                    prefetch_tasks.append(task)
        results = await asyncio.gather(*prefetch_tasks)
        success_count = sum(1 for r in results if r)
        logging.info("-" * 50)
        logging.info(f"Automated Pre-Fetch Complete. Successfully downloaded {success_count} of {len(prefetch_tasks)} sources.")
        logging.info(f"Files are located in the '{config['INPUT_DIR']}' directory.")
        logging.info("You can now run the 'Parse Local Files' option from the main menu.")
        logging.info("-" * 50)

# --- Connection Testing Logic ---

async def test_scanner_connections(config: Dict):
    """Tests all enabled scanner connections to ensure URLs are reachable."""
    logging.info("Testing all enabled data source connections...")
    today_str = date.today().strftime("%Y-%m-%d")
    headers = config.get("HTTP_HEADERS", {})

    # --- Use the helper to create the client with proxy/CA settings ---
    async with httpx.AsyncClient(headers=headers, **build_httpx_client_kwargs(config)) as client:
        for category in config.get("DATA_SOURCES", []):
            logging.info(f"- Testing Category: {category['title']} -")
            sites = [site for site in category.get("sites", []) if site.get("enabled", True)]
            for site in sites:
                if site.get("url"):
                    url = site["url"].format(date_str_iso=today_str)
                    try:
                        # Use HEAD request for testing
                        response = await client.head(url, timeout=15.0, follow_redirects=True)
                        if 200 <= response.status_code < 400:
                            logging.info(f"[SUCCESS] ({response.status_code}) - {site['name']}")
                        else:
                            logging.warning(f"[WARNING] ({response.status_code}) - {site['name']} at {url}")
                    except httpx.RequestError as e:
                        logging.error(f"[ERROR] FAILED - {site['name']} at {url} ({type(e).__name__})")

# --- Scanner Logic (Quick Strike) ---

# Helper function potentially used by the scanner logic (corrected regex)
async def fetch_and_parse(client: httpx.AsyncClient, url: str, source_name: str, config: Dict) -> List[Dict[str, Any]]:
    """Helper coroutine to fetch and parse a single URL for the scanner."""
    html_content = await fetch_url(client, url, config)
    if not html_content:
        return []

    soup = BeautifulSoup(html_content, 'html.parser')
    races_data = []
    # Example parsing logic (this would be expanded based on actual needs)
    race_cards = soup.find_all("div", class_=re.compile(r"race-card|racecard|race-item", re.I))
    if not race_cards:
        race_cards = soup.find_all(["article", "section"], class_=re.compile(r"race|meeting", re.I))
    logging.info(f"Found {len(race_cards)} potential race cards from {source_name}")
    
    for card in race_cards:
        try:
            # Example: extract course name
            course_elem = card.find(["h1", "h2", "h3", "h4"], class_=re.compile(r"course|track|meeting", re.I))
            course_name = course_elem.get_text(strip=True) if course_elem else "Unknown Course"

            # Example: extract race time (assuming it's in a specific format or element)
            time_elem = card.find(class_=re.compile(r"time|post-time", re.I))
            raw_time = time_elem.get_text(strip=True) if time_elem else ""
            # Use the normalizer to parse the time
            standardized_time = parse_hhmm_any(raw_time)

            # Example: extract race URL (if available)
            link_elem = card.find("a", href=True)
            race_url = link_elem['href'] if link_elem else ""

            # --- Example: Generate a unique ID (using the fixed regex) ---
            # This mimics the logic that would be in a full parser
            # Assuming we have a date object for today
            race_date = date.today() # Or parsed date
            # The key generation with the CORRECTED regex
            key = f"{normalize_course_name(course_name)}|{race_date.isoformat()}|{re.sub(r'[^\d]', '', standardized_time or '')}"
            unique_id = hashlib.sha1(key.encode()).hexdigest()[:12] # Example ID generation

            races_data.append({
                'id': unique_id,
                'course': course_name,
                'race_time': standardized_time,
                'race_url': race_url,
                'source_file': source_name,
                # Add other fields as needed...
            })
        except Exception as e:
            logging.error(f"Error parsing a race card from {source_name}: {e}")
            continue
    return races_data


async def run_automated_scan(config: Dict, args: Optional[argparse.Namespace]):
    """Main function to run the full automated 'Quick Strike' scan process."""
    logging.info("Starting automated 'Quick Strike' scan...")
    all_races = []
    today_str = (args.date if args and args.date else date.today().strftime("%Y-%m-%d"))
    output_dir = Path(config['DEFAULT_OUTPUT_DIR'])
    output_dir.mkdir(exist_ok=True, parents=True)

    # --- Use the helper to create the client with proxy/CA settings ---
    async with httpx.AsyncClient(follow_redirects=True, **build_httpx_client_kwargs(config)) as client:
        tasks = []
        for category in config.get("DATA_SOURCES", []):
            sites = [site for site in category.get("sites", []) if site.get("enabled", True)]
            for site in sites:
                if site.get("url"):
                    url = site["url"].format(date_str_iso=today_str)
                    logging.info(f"Scanning: {site['name']}")
                    # Pass the client with proxy/CA settings to the helper
                    task = asyncio.create_task(fetch_and_parse(client, url, site['name'], config))
                    tasks.append(task)

        # Gather results from all fetch/parse tasks
        results_lists = await asyncio.gather(*tasks)
        for races_list in results_lists:
            all_races.extend(races_list)

    # Example: Generate and save a simple report
    if all_races:
        try:
            # Simple text report for now
            report_content = f"Quick Strike Scan Results ({today_str})\n"
            report_content += "=" * 40 + "\n"
            for race in all_races:
                 report_content += f"ID: {race['id']}, Time: {race['race_time']}, Course: {race['course']}, Source: {race['source_file']}\n"
            report_path = output_dir / f"quick_strike_report_{today_str}.txt"
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(report_content)
            logging.info(f"[SUCCESS] Quick Strike report saved to {report_path}")
        except Exception as e:
            logging.error(f"[ERROR] Failed to generate/save Quick Strike report: {e}")
    else:
        logging.warning("No races found during Quick Strike scan.")

    logging.info("-" * 50)
    logging.info("Quick Strike Scan Complete.")
    logging.info("-" * 50)


# --- Main Execution Guard ---
if __name__ == "__main__":
    # This allows running the scanner directly for testing
    config = load_config()
    if not config:
        sys.exit(1)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    # Running the batch prefetch by default for this test.
    asyncio.run(run_batch_prefetch(config))
