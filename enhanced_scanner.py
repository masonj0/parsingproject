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
import random
import unicodedata
import hashlib # <-- Added import for hash-based ID generation if needed
import datetime as dt
from fetching import breadcrumb_get, fetch_with_favicon, resilient_get
from sources import (
    SourceAdapter,
    RawRaceDocument,
    RunnerDoc,
    FieldConfidence,
    register_adapter,
)
from normalizer import NormalizedRace

# Import the configuration loader
try:
    from config import load_config
except ImportError:
    print("FATAL: Could not import config.py. Ensure it's in the same directory.", file=sys.stderr)
    sys.exit(1)

# Shared Intelligence: The new normalizer is used in the main pipeline.
from normalizer import canonical_track_key, canonical_race_key

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
    This adapter performs a two-stage fetch:
    1. Fetches the main racecards page to get a list of all races.
    2. Fetches the individual page for each race to get runner details.
    """
    source_id = "timeform"

    def _parse_runner_data(self, race_soup: BeautifulSoup) -> List[RunnerDoc]:
        """Parses the runner data from a single race page using the correct selectors."""
        runners = []
        runner_rows = race_soup.select("tbody.rp-horse-row")

        for row in runner_rows:
            try:
                horse_name_el = row.select_one("td.rp-td-horse-name a.rp-horse")
                saddle_cloth_el = row.select_one("td.rp-td-horse-entry span.rp-entry-number")
                jockey_el = row.select_one("td.rp-td-horse-jockey a")
                trainer_el = row.select_one("td.rp-td-horse-trainer a")
                odds_el = row.select_one("td.rp-td-horse-prices a.price")

                # Odds are important for the analysis engine, but we can proceed without them
                # if they are not available for a particular runner.
                if not all([horse_name_el, saddle_cloth_el, jockey_el, trainer_el]):
                    missing = [
                        "name" if not horse_name_el else None,
                        "number" if not saddle_cloth_el else None,
                        "jockey" if not jockey_el else None,
                        "trainer" if not trainer_el else None
                    ]
                    logging.warning(f"Skipping a runner row, missing elements: {[m for m in missing if m]}")
                    continue

                horse_name = horse_name_el.get_text(strip=True)
                saddle_cloth = saddle_cloth_el.get_text(strip=True)
                jockey_name = jockey_el.get_text(strip=True)
                trainer_name = trainer_el.get_text(strip=True)

                odds = None
                if odds_el and odds_el.has_attr('data-price'):
                    odds_val = odds_el['data-price']
                    odds = FieldConfidence(odds_val, 0.9, "td.rp-td-horse-prices a.price[data-price]")

                runner_id = f"{saddle_cloth}-{horse_name}".lower().replace(" ", "-")

                runners.append(RunnerDoc(
                    runner_id=runner_id,
                    name=FieldConfidence(horse_name, 0.95, "td.rp-td-horse-name a.rp-horse"),
                    number=FieldConfidence(saddle_cloth, 0.95, "td.rp-td-horse-entry span.rp-entry-number"),
                    odds=odds,
                    jockey=FieldConfidence(jockey_name, 0.9, "td.rp-td-horse-jockey a"),
                    trainer=FieldConfidence(trainer_name, 0.9, "td.rp-td-horse-trainer a")
                ))
            except Exception as e:
                logging.error(f"Failed to parse a runner row on Timeform: {e}", exc_info=True)
        return runners

    async def fetch(self, config: dict) -> list[RawRaceDocument]:
        """
        Fetches the Timeform racecards page, then fetches each individual
        race page to extract detailed runner information.
        """
        site_config = None
        for source in config.get("SCRAPER", {}).get("sources", []):
            if source.get("id") == self.source_id:
                site_config = source
                break

        if not site_config:
            logging.error("Timeform site configuration not found.")
            return []

        base_url = site_config.get("base_url")
        target_url = site_config.get("url")

        if not base_url or not target_url:
            logging.error("Timeform base_url or url not configured.")
            return []

        # 1. Fetch the main race list page
        logging.info("Fetching Timeform race list...")
        try:
            list_response = await resilient_get(target_url, config=config)
            list_html = list_response.text
        except Exception as e:
            logging.error(f"An error occurred while fetching Timeform race list: {e}")
            return []

        # 2. Parse the race list to get individual race URLs
        soup = BeautifulSoup(list_html, 'lxml')
        race_docs = []
        meeting_containers = soup.select(".w-racecard-grid-meeting")

        for meeting in meeting_containers:
            course_name = meeting.select_one("h2").get_text(strip=True)
            track_key = canonical_track_key(course_name)

            for item in meeting.select(".w-racecard-grid-meeting-races-compact li a"):
                href = item.get('href')
                if not href: continue

                race_time_str = item.select_one("b").get_text(strip=True)
                race_num_match = re.search(r'/(\d+)/?$', href)
                race_num = race_num_match.group(1) if race_num_match else race_time_str.replace(":", "")

                race_docs.append(RawRaceDocument(
                    source_id=self.source_id,
                    fetched_at=dt.datetime.now(dt.timezone.utc).isoformat(),
                    track_key=track_key,
                    race_key=canonical_race_key(track_key, race_num),
                    start_time_iso=f"{date.today().isoformat()}T{race_time_str}:00Z",
                    runners=[],
                    extras={"race_url": FieldConfidence(f"https://www.timeform.com{href}", 0.95, "a[href]")}
                ))

        # 3. Fetch each individual race page to get runner data
        logging.info(f"Found {len(race_docs)} races. Now fetching individual pages for runner data...")
        for doc in race_docs:
            try:
                race_url = doc.extras["race_url"].value
                if not race_url: continue

                await asyncio.sleep(random.uniform(1, 3)) # Respectful delay

                logging.info(f"Fetching detail for race: {doc.race_key} at {race_url}")
                detail_response = await resilient_get(race_url, config=config)
                detail_soup = BeautifulSoup(detail_response.text, 'lxml')

                # Parse runners and add them to the document
                doc.runners = self._parse_runner_data(detail_soup)
                logging.info(f"-> Found {len(doc.runners)} runners for race {doc.race_key}")

            except Exception as e:
                logging.error(f"Failed to fetch or parse detail for race {doc.race_key}: {e}")

        return race_docs



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


@register_adapter
class RacingPostAdapter:
    """
    Adapter for fetching racecards from Racing Post.
    This adapter performs a two-stage fetch:
    1. Fetches the main racecards page to get a list of all races.
    2. Fetches the individual page for each race to get runner details.
    """
    source_id = "racingpost"

    def _parse_runner_data(self, race_soup: BeautifulSoup) -> List[RunnerDoc]:
        """Parses the runner data from a single race page using placeholder selectors."""
        # TO-DO: The selectors below are placeholders and need to be updated
        # based on the actual HTML structure of a Racing Post race detail page.
        runners = []
        # Placeholder: This selector should target each row representing a runner.
        runner_rows = race_soup.select("div.rp-horse-row")

        for row in runner_rows:
            try:
                # Placeholder selectors for each piece of data
                horse_name_el = row.select_one("a.rp-horse-name")
                saddle_cloth_el = row.select_one("span.rp-saddle-cloth")
                jockey_el = row.select_one("a.rp-jockey-name")
                trainer_el = row.select_one("a.rp-trainer-name")
                odds_el = row.select_one("span.rp-bet-odds")

                if not all([horse_name_el, saddle_cloth_el, jockey_el, trainer_el]):
                    continue

                horse_name = horse_name_el.get_text(strip=True)
                saddle_cloth = saddle_cloth_el.get_text(strip=True)
                jockey_name = jockey_el.get_text(strip=True)
                trainer_name = trainer_el.get_text(strip=True)
                odds = odds_el.get_text(strip=True) if odds_el else None

                runner_id = f"{saddle_cloth}-{horse_name}".lower().replace(" ", "-")

                runners.append(RunnerDoc(
                    runner_id=runner_id,
                    name=FieldConfidence(horse_name, 0.9, "a.rp-horse-name"),
                    number=FieldConfidence(saddle_cloth, 0.9, "span.rp-saddle-cloth"),
                    odds=FieldConfidence(odds, 0.9, "span.rp-bet-odds") if odds else None,
                    jockey=FieldConfidence(jockey_name, 0.9, "a.rp-jockey-name"),
                    trainer=FieldConfidence(trainer_name, 0.9, "a.rp-trainer-name")
                ))
            except Exception as e:
                logging.error(f"Failed to parse a runner row on Racing Post: {e}", exc_info=True)
        return runners

    async def fetch(self, config: dict) -> list[RawRaceDocument]:
        """
        Fetches the Racing Post racecards page, then fetches each individual
        race page to extract detailed runner information.
        """
        site_config = None
        for source in config.get("SCRAPER", {}).get("sources", []):
            if source.get("id") == self.source_id:
                site_config = source
                break

        if not site_config:
            logging.error("Racing Post site configuration not found.")
            return []

        base_url = site_config.get("base_url")
        target_url = site_config.get("url")

        if not base_url or not target_url:
            logging.error("Racing Post base_url or url not configured.")
            return []

        # 1. Fetch the main race list page
        logging.info("Fetching Racing Post race list...")
        try:
            list_response = await resilient_get(target_url, config=config)
            list_html = list_response.text
        except Exception as e:
            logging.error(f"An error occurred while fetching Racing Post race list: {e}")
            return []

        # Save for debugging
        with open("debug_racingpost_list.html", "w", encoding="utf-8") as f:
            f.write(list_html)

        # 2. Parse the race list to get individual race URLs
        soup = BeautifulSoup(list_html, 'lxml')
        race_docs = []
        # TO-DO: Implement the actual selector for race links.
        race_links = soup.select('a.race-card-link') # Placeholder

        for link in race_links:
            href = link.get('href')
            if not href: continue

            detail_url = f"{base_url}{href}"

            # 3. Fetch each individual race page
            logging.info(f"Fetching detail for race: {href}")
            try:
                detail_response = await resilient_get(detail_url, config=config)
                detail_html = detail_response.text

                track_key = "placeholder_track" # Placeholder
                race_key = "placeholder_race" # Placeholder

                race_docs.append(RawRaceDocument(
                    source_id=self.source_id,
                    fetched_at=dt.datetime.now(dt.timezone.utc).isoformat(),
                    track_key=track_key,
                    race_key=race_key,
                    start_time_iso=None, # Placeholder
                    runners=self._parse_runner_data(BeautifulSoup(detail_html, 'lxml')),
                    extras={"race_url": FieldConfidence(detail_url, 0.95)}
                ))
            except Exception as e:
                logging.error(f"Failed to fetch or parse detail for race at {detail_url}: {e}")

        return race_docs

    def parse(self, raw_document: RawRaceDocument) -> Optional[NormalizedRace]:
        """
        Parses the content of a single race detail page.
        """
        # Since parsing is done during fetch for this adapter, we just return the data
        return NormalizedRace(
            race_id=f"{raw_document.track_key}_{raw_document.race_key}",
            source=self.source_id,
            url=raw_document.extras["race_url"].value,
            race_title="Placeholder Race Title", # Placeholder
            runners=[asdict(runner) for runner in raw_document.runners],
            fetched_at=raw_document.fetched_at,
            version="2.0"
        )


# --- Main Execution Guard ---
if __name__ == "__main__":
    # This allows running the scanner directly for testing
    config = load_config()
    if not config:
        sys.exit(1)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    # Running the batch prefetch by default for this test.
    asyncio.run(run_batch_prefetch(config))
