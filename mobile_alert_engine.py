#!/usr/bin/env python3
"""
Paddock Parser Toolkit - Mobile Alerting Engine v1.3

This is a standalone, continuously running service designed for Termux on Android.
It autonomously scans for high-value racing opportunities and sends proactive
notifications to the device.
"""

import os
import sys
import json
import logging
import time
import asyncio
import httpx
import hashlib
import re
from datetime import date, datetime
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Any, Set

# --- Shared Intelligence Imports ---
try:
    from normalizer import normalize_course_name, parse_hhmm_any, convert_odds_to_fractional_decimal
    from analysis import EnhancedValueScorer, Runner, RaceData
except ImportError:
    print("FATAL: Ensure normalizer.py and analysis.py are in the same directory.", file=sys.stderr)
    sys.exit(1)

# =============================================================================
# --- CONFIGURATION & LOGGING ---
# =============================================================================

def load_config(path: str = 'mobile_config.json') -> Dict[str, Any]:
    """Loads the mobile-specific configuration file."""
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.critical(f"FATAL: Could not load or parse '{path}'. Error: {e}")
        sys.exit(1)
    return {}

def setup_logging(log_file: str):
    """Configures logging for the mobile application."""
    log_dir = Path(log_file).parent
    log_dir.mkdir(exist_ok=True, parents=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, mode='w'),
            logging.StreamHandler(sys.stdout)
        ]
    )

# =============================================================================
# --- CORE DATA & STATE MANAGEMENT ---
# =============================================================================

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

    verify: Any = ca_bundle if ca_bundle else verify_ssl
    kwargs: Dict[str, Any] = {"verify": verify}
    if proxies:
        kwargs["proxies"] = proxies
    return kwargs

def generate_race_id(course: str, race_date: date, time: str) -> str:
    """Creates a unique, deterministic ID for a race."""
    key = f"{normalize_course_name(course)}|{race_date.isoformat()}|{re.sub(r'[^\d]', '', time or '')}"
    return hashlib.sha1(key.encode()).hexdigest()[:12]

def load_alert_state(state_file_path: Path) -> Set[str]:
    """Loads the set of already-alerted race IDs from the state file."""
    if not state_file_path.exists():
        return set()
    try:
        with open(state_file_path, 'r', encoding='utf-8') as f:
            # Check if the file is empty before trying to load JSON
            content = f.read()
            if not content:
                return set()
            return set(json.loads(content))
    except (json.JSONDecodeError, TypeError):
        logging.warning(f"Could not parse state file '{state_file_path}'. Starting fresh.")
        return set()

def save_alert_state(state_file_path: Path, alerted_ids: Set[str]):
    """Saves the set of alerted race IDs to the state file."""
    try:
        with open(state_file_path, 'w', encoding='utf-8') as f:
            json.dump(list(alerted_ids), f, indent=2)
    except Exception as e:
        logging.error(f"Failed to save alert state to '{state_file_path}': {e}")

# =============================================================================
# --- DATA ACQUISITION & PARSING ---
# =============================================================================

async def fetch_url(client: httpx.AsyncClient, url: str, config: Dict) -> str:
    """
    Fetches content from a URL using the 'Perfect Disguise' browser headers.
    """
    logging.info(f"Fetching: {url}")
    headers = config.get("HTTP_HEADERS", {})
    timeout = config.get("HTTP_CLIENT", {}).get("REQUEST_TIMEOUT", 30.0)
    try:
        response = await client.get(url, timeout=timeout, headers=headers, follow_redirects=True)
        response.raise_for_status()
        return response.text
    except httpx.HTTPStatusError as e:
        logging.warning(f"HTTP error for {url}: {e.response.status_code}")
    except httpx.RequestError as e:
        logging.warning(f"Request error for {url}: {e}")
    return ""

def parse_source(html_content: str, source_name: str) -> List[RaceData]:
    """
    A simplified parser for the mobile engine. It focuses on extracting basic
    race data from common HTML structures found in the soft targets.
    """
    from bs4 import BeautifulSoup
    
    soup = BeautifulSoup(html_content, 'html.parser')
    races = []
    
    # Generic approach: find all elements that look like a meeting container
    meeting_containers = soup.select('[class*="meeting"], [class*="accordion__row"]')
    
    if not meeting_containers:
        # Fallback if no specific meeting containers are found
        meeting_containers = [soup]

    for container in meeting_containers:
        try:
            course_element = container.select_one('h1, h2, [class*="courseName"], [class*="course-name"]')
            course_name = course_element.get_text(strip=True) if course_element else "Unknown Course"
            
            # Find all elements that look like a race within this meeting
            race_elements = container.select('[class*="race-item"], [class*="meetingItem"], li a[href*="racecards/"]')
            
            for race_el in race_elements:
                time_el = race_el.select_one('[class*="raceTime"], [class*="time"]')
                if not time_el: continue

                race_time = time_el.get_text(strip=True)
                
                # Extract field size if available
                runners_el = race_el.select_one('[class*="runners"], [class*="numberOfRunners"]')
                field_size = 0
                if runners_el:
                    runners_match = re.search(r'(\d+)', runners_el.get_text())
                    if runners_match:
                        field_size = int(runners_match.group(1))

                # Create a simplified RaceData object
                race = RaceData(
                    id=generate_race_id(course_name, date.today(), race_time),
                    course=normalize_course_name(course_name),
                    race_time=parse_hhmm_any(race_time),
                    race_type="Unknown", # Not critical for initial alert
                    field_size=field_size,
                    # We don't parse individual runners in this lightweight version
                    runners=[], favorite=None, second_favorite=None 
                )
                races.append(race)
        except Exception as e:
            logging.warning(f"Could not parse a container from {source_name}: {e}")
            continue
            
    return races

# =============================================================================
# --- NOTIFICATION ENGINE ---
# =============================================================================

def send_termux_notification(title: str, content: str):
    """
    Uses the Termux API to send a native Android notification.
    """
    logging.info(f"Sending Notification: '{title}' - '{content}'")
    try:
        # Sanitize inputs to prevent command injection issues
        safe_title = json.dumps(title)
        safe_content = json.dumps(content)
        command = f'termux-notification --title {safe_title} --content {safe_content}'
        os.system(command)
    except Exception as e:
        logging.error(f"Failed to send Termux notification: {e}")

# =============================================================================
# --- MAIN MONITORING LOOP ---
# =============================================================================

async def perform_scan_and_alert(config: Dict, scorer: EnhancedValueScorer, alerted_ids: Set[str]):
    """
    Performs a single, full cycle of fetching, parsing, scoring, and alerting.
    """
    logging.info("="*20 + " Starting New Scan Cycle " + "="*20)
    all_races: Dict[str, RaceData] = {}
    
    async with httpx.AsyncClient(**build_httpx_client_kwargs(config)) as client:
        fetch_tasks = [
            fetch_url(client, source["url"], config) for source in config["SOFT_TARGET_SOURCES"]
        ]
        html_contents = await asyncio.gather(*fetch_tasks)

    for i, html in enumerate(html_contents):
        if not html:
            continue
        source_name = config["SOFT_TARGET_SOURCES"][i]["name"]
        parsed_races = parse_source(html, source_name)
        
        # Merge parsed races into our master dictionary to de-duplicate
        for race in parsed_races:
            if race.id not in all_races:
                all_races[race.id] = race
            else:
                # Basic merge: update field size if the new one is better
                if race.field_size > all_races[race.id].field_size:
                    all_races[race.id].field_size = race.field_size

    logging.info(f"Scan complete. Found {len(all_races)} unique races.")

    # --- Target Hunting & Alerting ---
    new_alerts = 0
    min_score_to_alert = config.get("ALERTS", {}).get("MINIMUM_SCORE_TO_ALERT", 90.0)
    
    # We need to simulate some runner data for the scorer to work
    # In a real scenario, this would be a deeper parse or a secondary API call
    for race in all_races.values():
        if race.field_size > 0:
            # Simulate a favorite and second favorite for scoring purposes
            race.runners = [Runner("Dummy", "1/1", 1.0)] * race.field_size
            race.favorite = Runner("Simulated Fav", "1/1", 1.0)
            race.second_favorite = Runner("Simulated 2nd Fav", "3/1", 3.0)
            
            race.value_score = scorer.calculate_score(race)
            
            if race.value_score >= min_score_to_alert and race.id not in alerted_ids:
                title = f"[ALERT] Target Found! (Score: {race.value_score})"
                content = f"{race.race_time} {race.course} ({race.field_size} runners)"
                send_termux_notification(title, content)
                alerted_ids.add(race.id)
                new_alerts += 1
    
    if new_alerts > 0:
        logging.info(f"Found and sent {new_alerts} new alerts.")
        save_alert_state(Path(config["ALERTS"]["STATE_FILE"]), alerted_ids)
    else:
        logging.info("No new target opportunities found this cycle.")

# =============================================================================
# --- SCRIPT ENTRY POINT ---
# =============================================================================

def main():
    """
    Main function to initialize and run the continuous monitoring loop.
    """
    # Load configuration
    config = load_config()
    if not config:
        return

    # Setup logging
    setup_logging(config.get("LOG_FILE", "mobile_app.log"))
    logging.info(f"--- {config.get('APP_NAME', 'Mobile Alerter')} Starting Up ---")

    # Initialize the Scorer and State
    scorer = EnhancedValueScorer(config)
    state_file = Path(config.get("ALERTS", {}).get("STATE_FILE", "daily_alerts.json"))
    alerted_race_ids = load_alert_state(state_file)
    logging.info(f"Loaded {len(alerted_race_ids)} previously alerted races from state file.")

    # Get the check interval
    check_interval = config.get("MONITORING_LOOP", {}).get("CHECK_INTERVAL_SECONDS", 900)

    # --- The Continuous Loop ---
    try:
        while True:
            # Run the main async task
            asyncio.run(perform_scan_and_alert(config, scorer, alerted_race_ids))
            
            # Wait for the next cycle
            logging.info(f"Cycle complete. Sleeping for {check_interval} seconds...")
            time.sleep(check_interval)

    except KeyboardInterrupt:
        logging.info("\nCtrl+C detected. Shutting down the engine.")
        save_alert_state(state_file, alerted_race_ids)
        logging.info("Final alert state saved.")
    except Exception as e:
        logging.critical(f"A critical error occurred in the main loop: {e}", exc_info=True)
        save_alert_state(state_file, alerted_race_ids)
        logging.info("Alert state saved before emergency shutdown.")
        sys.exit(1)

if __name__ == "__main__":
    main()
