#!/usr/bin/env python3
"""
Paddock Parser Toolkit - Enhanced Racing Scanner v10.2 (enhanced_scanner.py)

---
NOTE TO SELF (The Constitution - Preamble):
This is our "Quick Strike" tool. Its philosophy is to provide a fast, fully
automated, "good enough" snapshot of the day's racing. It is a "best-effort"
tool. We explicitly acknowledge that its automated nature makes it vulnerable
to website blocks and corporate firewalls. It is designed to be a complementary
"scout" to the 100% reliable, manual-first Paddock Parser. Its --json-out flag
is the critical bridge that allows its findings to be fed into the Paddock Parser
for deeper analysis.
---
"""

import asyncio
import json
import logging
import os
import sys
import time
import webbrowser
import csv
import re
import random
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from urllib.parse import urljoin, urlparse
from collections import defaultdict

# Shared normalization utils with a safe fallback
try:
    import normalizer as norm
except ImportError:
    # This fallback ensures the script can be understood even if normalizer.py is missing,
    # but the primary design relies on the shared module.
    print("FATAL: 'normalizer.py' not found. This module is essential for the toolkit.")
    sys.exit(1)

# Dependency checking
try:
    import httpx
    from bs4 import BeautifulSoup
except ImportError as e:
    print(f"FATAL: Missing required package: {e}. Please run 'pip install -r requirements.txt'")
    sys.exit(1)

try:
    from zoneinfo import ZoneInfo
except ImportError:
    try:
        from backports.zoneinfo import ZoneInfo
    except ImportError:
        print("WARNING: Timezone package not found, using UTC fallback.")
        ZoneInfo = None

# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class RaceInfo:
    course: str
    race_time: str
    local_time_full: str
    field_size: int
    country: str
    discipline: str
    race_url: str
    meeting_url: str = ""
    source: str = "Unknown"
    race_number: int = 0
    distance: str = ""
    
    def __post_init__(self):
        self.course = self.course.strip()
        self.race_time = self.race_time.strip()
        self.country = self.country.upper()
        if self.field_size < 1: self.field_size = 1
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass
class ScanResults:
    races: List[RaceInfo] = field(default_factory=list)
    total_found: int = 0
    by_country: Dict[str, int] = field(default_factory=dict)
    by_discipline: Dict[str, int] = field(default_factory=dict)
    by_source: Dict[str, int] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    duration: float = 0.0
    scan_timestamp: str = ""
    
    def __post_init__(self):
        self.scan_timestamp = datetime.now().isoformat()

# =============================================================================
# CORPORATE NETWORK HANDLER
# =============================================================================

class CorporateNetworkHandler:
    def __init__(self):
        self.firewall_blocks = set()
        self.proxy_patterns = [
            "blocked by administrator", "access denied", "corporate policy",
            "websense", "bluecoat", "fortiguard", "sophos", "opendns",
            "this site has been blocked", "category: gambling", "content filter",
            "barracuda", "checkpoint", "forcepoint", "symantec"
        ]
    
    def detect_corporate_interference(self, url: str, content: str) -> str:
        if not content: return "no_content"
        content_lower = content.lower()
        domain = urlparse(url).netloc
        if any(pattern in content_lower for pattern in self.proxy_patterns):
            self.firewall_blocks.add(domain)
            logging.warning(f"🚫 Corporate firewall blocking detected for {domain}")
            return "firewall_block"
        if ("login" in content_lower and "authentication" in content_lower) or \
           ("captive" in content_lower and "portal" in content_lower):
            logging.warning(f"🔐 Captive portal detected for {domain}")
            return "captive_portal"
        if len(content) < 200 and "error" in content_lower:
            return "possible_block"
        return "ok"
    
    def is_blocked(self, domain: str) -> bool:
        return domain in self.firewall_blocks

# =============================================================================
# ENHANCED HTTP CLIENT
# =============================================================================

class EnhancedHttpClient:
    """
    NOTE TO SELF (The Constitution - Article II: The Corporate Agent):
    This HTTP client is the core of our automated fetching strategy. It is
    specifically designed to be resilient in hostile network environments like
    corporate firewalls. Key features like user-agent rotation, randomized
    delays, exponential backoff, and detection of proxy block pages give it
    the best possible chance of success. We accept that it may still fail,
    which is why the Paddock Parser exists as the 100% reliable alternative.
    """
    def __init__(self, config: Dict):
        self.config = config
        self.client = None
        self.session_stats = {"requests": 0, "failures": 0, "retries": 0, "corporate_blocks": 0}
        self.corporate_handler = CorporateNetworkHandler()
        
    async def __aenter__(self):
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.config["REQUEST_TIMEOUT"]),
            follow_redirects=True,
            verify=self.config.get("VERIFY_TLS", True),
            headers=self._get_random_headers()
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client: await self.client.aclose()
    
    def _get_random_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": random.choice(self.config["USER_AGENTS"]),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "max-age=0",
            "DNT": "1"
        }
    
    async def get(self, url: str, expect_json: bool = False) -> Optional[str]:
        domain = urlparse(url).netloc
        if self.corporate_handler.is_blocked(domain):
            logging.debug(f"🚫 Skipping {domain} - known corporate block")
            return None

        self.session_stats["requests"] += 1
        self.client.headers.update(self._get_random_headers())
        base_delay = self.config["RETRY_DELAY"]
        
        for attempt in range(self.config["MAX_RETRIES"]):
            try:
                if attempt > 0:
                    delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0.5, 2.0)
                    self.session_stats["retries"] += 1
                    logging.debug(f"🔄 Retry {attempt+1}/{self.config['MAX_RETRIES']} for {url}, waiting {delay:.2f}s")
                    await asyncio.sleep(delay)
                
                logging.debug(f"📡 Fetching {url}")
                response = await self.client.get(url)
                
                if response.status_code == 200:
                    content = response.text
                    interference = self.corporate_handler.detect_corporate_interference(url, content)
                    if interference in ["firewall_block", "captive_portal"]:
                        self.session_stats["corporate_blocks"] += 1
                        return None
                    if self._validate_content(content, expect_json):
                        logging.debug(f"✅ Successfully fetched {url} ({len(content)} chars)")
                        return content
                    else:
                        logging.warning(f"⚠️ Invalid or empty content from {url}")
                        
                elif response.status_code in [403, 429, 502, 503, 504]:
                    logging.warning(f"🚦 Server/proxy issue ({response.status_code}) from {url}, retrying...")
                    if response.status_code == 429:
                        retry_after = response.headers.get("Retry-After")
                        wait = base_delay * (attempt + 2)
                        if retry_after:
                            try: wait = float(retry_after)
                            except ValueError: pass
                        await asyncio.sleep(min(30.0, wait))
                    continue
                else:
                    logging.warning(f"🌐 HTTP {response.status_code} from {url}")
                    
            except (httpx.TimeoutException, asyncio.TimeoutError):
                logging.warning(f"⏰ Timeout on {url} (attempt {attempt + 1})")
            except httpx.HTTPError as e:
                logging.warning(f"🌐 HTTP error on {url}: {e}")
            except Exception as e:
                logging.error(f"❌ Unexpected error fetching {url}: {e}", exc_info=True)
                break
                
        self.session_stats["failures"] += 1
        logging.error(f"❌ Failed to fetch {url} after {self.config['MAX_RETRIES']} attempts.")
        return None
    
    def _validate_content(self, content: str, expect_json: bool) -> bool:
        if not content or len(content) < 100: return False
        if expect_json:
            try:
                data = json.loads(content)
                return isinstance(data, list) and len(data) > 0
            except json.JSONDecodeError: return False
        content_lower = content.lower()
        return any(tag in content_lower for tag in ['<html', '<body', 'race', 'meeting'])

# =============================================================================
# ENHANCED RACING SCANNER
# =============================================================================

class EnhancedRacingScanner:
    def __init__(self, config: Dict, verbose: bool = False):
        self.config = config
        self.verbose = verbose
        
    async def scan(self, target_date: Optional[datetime] = None) -> ScanResults:
        target_date = target_date or datetime.now()
        start_time = time.time(); results = ScanResults()
        
        logging.info(f"🚀 Starting {self.config['APP_NAME']} v{self.config['VERSION']} (Quick Strike)")
        logging.info(f"📅 Scanning for: {target_date.strftime('%Y-%m-%d')}")
        
        async with EnhancedHttpClient(self.config) as client:
            all_races = await self._fetch_with_priority(client, target_date, results)
            results.races = self._deduplicate_races(all_races)
            self._calculate_statistics(results)
            stats = client.session_stats
            logging.info(f"📊 Session: {stats['requests']} reqs, {stats['failures']} fails, {stats['retries']} retries, {stats['corporate_blocks']} blocks")
        
        results.duration = time.time() - start_time
        return results
    
    async def _fetch_with_priority(self, client: EnhancedHttpClient, 
                                  target_date: datetime, results: ScanResults) -> List[RaceInfo]:
        """
        NOTE TO SELF (The Constitution - Article I: The Fetching Strategy):
        This function implements our core resiliency pattern. It attempts to
        fetch data from the most reliable source (the JSON API) first. If, and
        only if, that source provides insufficient data, it falls back to the
        more brittle (but still valuable) HTML scraping methods.
        """
        all_races = []
        source_priority = [
            ("Racing & Sports", self._fetch_racing_sports_api, None),
            ("Sky Sports", self._fetch_generic_html_source, "Sky Sports"),
            ("Timeform", self._fetch_generic_html_source, "Timeform"),
            ("Sporting Life", self._fetch_generic_html_source, "Sporting Life")
        ]
        
        for source_name, fetch_method, method_arg in source_priority:
            try:
                logging.info(f"🔄 Trying source: {source_name}")
                if source_name == "Racing & Sports":
                    races = await fetch_method(client, target_date, results)
                else:
                    source_config = next((s for s in self.config["BACKUP_SOURCES"] if s['name'] == source_name), None)
                    if source_config:
                        races = await fetch_method(client, source_config['url'], method_arg, results)
                    else: continue
                
                if races:
                    all_races.extend(races)
                    logging.info(f"✅ {source_name} found {len(races)} races")
                    if len(all_races) >= 50:
                        logging.info("Sufficient race data collected, stopping further fetches.")
                        break
            except Exception as e:
                results.warnings.append(f"Source {source_name} failed: {e}")
                logging.warning(f"Source {source_name} failed: {e}", exc_info=self.verbose)
        
        return all_races

    async def _fetch_racing_sports_api(self, client: EnhancedHttpClient, 
                                     target_date: datetime, results: ScanResults) -> List[RaceInfo]:
        """Fetch from the reliable Racing & Sports JSON API."""
        races = []
        content = await client.get(self.config["PRIMARY_API"], expect_json=True)
        if not content:
            results.errors.append("Failed to fetch primary API: Racing & Sports"); return []
        
        try:
            raw_data = json.loads(content)
            logging.info(f"✅ Racing & Sports API responded with {len(raw_data)} discipline groups.")
            for discipline_group in raw_data:
                discipline = norm.map_discipline(discipline_group.get("Discipline", "Unknown"))
                for country_group in discipline_group.get("Countries", []):
                    country_code = country_group.get("Code", "").upper()
                    for meeting in country_group.get("Meetings", []):
                        races.extend(self._process_rs_meeting(meeting, country_code, discipline))
        except Exception as e:
            results.errors.append(f"Error processing R&S API data: {e}")
            logging.error(f"Error processing R&S API data: {e}", exc_info=True)
        return races

    def _process_rs_meeting(self, meeting: Dict, country_code: str, discipline: str) -> List[RaceInfo]:
        races = []
        course = meeting.get("Course", "").strip()
        if not course: return []
        meeting_url = meeting.get("Url", "")
        for i, race_item in enumerate(meeting.get("Races", [])):
            race_info = self._process_rs_race(race_item, course, country_code, discipline, meeting_url, i + 1)
            if race_info and self.validate_race_data(race_info):
                races.append(self.clean_race_data(race_info))
        return races

    def _process_rs_race(self, race: Dict, course: str, country_code: str,
                        discipline: str, meeting_url: str, race_number: int) -> Optional[RaceInfo]:
        race_time = norm.parse_hhmm_any(race.get("RaceTimeLocal", ""))
        if not race_time: return None
        field_size = int(race.get("FieldSize", 0))
        if field_size == 0: return None
        race_url = race.get("Url", "") or meeting_url
        timezone_name = self.config["TIMEZONES"].get(country_code, "UTC")
        local_time_full = self._format_enhanced_local_time(race_time, timezone_name, country_code)
        return RaceInfo(course=course, race_time=race_time, local_time_full=local_time_full, field_size=field_size,
                        country=country_code, discipline=discipline, race_url=race_url, meeting_url=meeting_url,
                        source="Racing & Sports", race_number=race_number, distance=race.get("Distance", ""))

    async def _fetch_generic_html_source(self, client: EnhancedHttpClient, base_url: str, source_name: str, results: ScanResults) -> List[RaceInfo]:
        """Generic scraping logic for backup HTML sources."""
        races = []
        content = await client.get(base_url)
        if not content:
            results.warnings.append(f"No content from {source_name}"); return []

        soup = BeautifulSoup(content, 'html.parser')
        meeting_containers = soup.select('div.sdc-site-racing-meetings__meeting, li.race-card, div.meeting, .rp-meeting-block')

        for meeting in meeting_containers:
            try:
                course_elem = meeting.select_one('a.sdc-site-racing-meetings__course-name, h3, a.meeting-header-title, .RC-meeting-name')
                if not course_elem: continue
                course_text = course_elem.get_text(strip=True)

                for race_container in meeting.select('div.sdc-site-racing-meetings__event, li.race-card-summary, div.race-summary, .RC-raceStream'):
                    try:
                        container_text = race_container.get_text()
                        time_match = re.search(r'(\d{1,2}:\d{2})', container_text)
                        if not time_match: continue
                        race_time = time_match.group(1)

                        runner_match = re.search(r'(\d+)\s*runners', container_text, re.I)
                        field_size = int(runner_match.group(1)) if runner_match else 0
                        if field_size == 0: continue

                        link_elem = race_container.find('a', href=True)
                        race_url = urljoin(base_url, link_elem['href']) if link_elem else ""
                        
                        country_match = re.search(r'\((IRE|GB|FR|USA)\)', container_text)
                        country_code = country_match.group(1) if country_match else "GB"
                        
                        timezone_name = self.config["TIMEZONES"].get(country_code, "UTC")
                        local_time_full = self._format_enhanced_local_time(race_time, timezone_name, country_code)

                        race_obj = RaceInfo(course=course_text, race_time=race_time, local_time_full=local_time_full,
                                            field_size=field_size, country=country_code, discipline="thoroughbred",
                                            race_url=race_url, meeting_url=race_url, source=source_name)
                        
                        if self.validate_race_data(race_obj):
                            races.append(self.clean_race_data(race_obj))
                    except Exception as e: logging.debug(f"Error parsing race container from {source_name}: {e}")
            except Exception as e: logging.debug(f"Error parsing meeting container from {source_name}: {e}")
        return races
    
    def validate_race_data(self, race: RaceInfo) -> bool:
        if not all([race.course, race.race_time, race.country]): return False
        if not isinstance(race.field_size, int) or race.field_size < 1: return False
        if not re.match(r'^\d{2}:\d{2}$', race.race_time): return False
        return True
        
    def clean_race_data(self, race: RaceInfo) -> RaceInfo:
        race.course = re.sub(r'\s+', ' ', race.course.strip())
        race.country = race.country.upper()
        race.discipline = race.discipline.lower().strip()
        if race.race_url and not race.race_url.startswith('http'):
            parsed_base = urlparse(self.config["PRIMARY_API"])
            race.race_url = urljoin(f"{parsed_base.scheme}://{parsed_base.netloc}", race.race_url)
        return race

    def _deduplicate_races(self, races: List[RaceInfo]) -> List[RaceInfo]:
        """Remove duplicate races based on a composite key."""
        seen = set()
        unique_races = []
        for race in races:
            key = (race.course.lower().strip(), race.race_time, race.country)
            if key not in seen:
                seen.add(key); unique_races.append(race)
        unique_races.sort(key=lambda r: (r.country, r.course, r.race_time))
        logging.info(f"🧹 Deduplication: {len(races)} -> {len(unique_races)} races")
        return unique_races
    
    def _calculate_statistics(self, results: ScanResults):
        """Calculate comprehensive statistics."""
        results.total_found = len(results.races)
        for race in results.races:
            results.by_country[race.country] = results.by_country.get(race.country, 0) + 1
            results.by_discipline[race.discipline] = results.by_discipline.get(race.discipline, 0) + 1
            results.by_source[race.source] = results.by_source.get(race.source, 0) + 1
    
    def _format_enhanced_local_time(self, race_time: str, timezone_name: str, country_code: str) -> str:
        """Enhanced local time formatting."""
        if not ZoneInfo: return f"{race_time} Local"
        try:
            today = datetime.now().date()
            race_dt = datetime.combine(today, datetime.strptime(race_time, "%H:%M").time())
            if timezone_name != "UTC":
                race_dt = race_dt.replace(tzinfo=ZoneInfo(timezone_name))
                return race_dt.strftime(f'%H:%M %Z')
        except Exception as e: logging.debug(f"Timezone formatting error: {e}")
        return f"{race_time} {country_code}"
    
    def _map_discipline(self, discipline_name: str) -> str:
        return norm.map_discipline(discipline_name)

# =============================================================================
# OUTPUT AND MENU SYSTEM
# =============================================================================

class OutputManager:
    """Handles the generation of all reports for the scanner."""
    def __init__(self, config: Dict, output_dir: Path):
        self.config = config
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True, parents=True)

    def generate_html_report(self, results: ScanResults) -> str:
        """Generate comprehensive HTML report with enhanced styling and features."""
        meetings = defaultdict(list)
        for race in results.races:
            meetings[(race.course, race.country, race.discipline)].append(race)
        
        sorted_meetings = sorted(meetings.items(), key=lambda x: (x[0][1], x[0][2], x[0][0]))

        body_html = ""
        if not results.races:
            body_html = "<div class='no-races'><h3>No races found for today.</h3></div>"
        else:
            for (course, country, discipline), meeting_races in sorted_meetings:
                meeting_races.sort(key=lambda r: r.race_time)
                races_html = ""
                for race in meeting_races:
                    field_class = "small" if race.field_size <= 6 else "medium" if race.field_size <= 10 else "large"
                    links = f'<a href="{race.race_url}" target="_blank" class="btn btn-primary">🏇 Racecard</a>' if race.race_url else ''
                    details = f"Race {race.race_number}" if race.race_number > 0 else "Race"
                    if race.distance: details += f" • {race.distance}"

                    races_html += f"""
                    <div class="race">
                        <div class="race-time">{race.race_time}</div>
                        <div class="race-details">
                            <div class="race-number">{details}</div>
                            <div class="race-info">{race.local_time_full} • Source: {race.source}</div>
                        </div>
                        <div class="field-size {field_class}">{race.field_size} runners</div>
                        <div class="race-links">{links}</div>
                    </div>
                    """
                
                body_html += f"""
                <div class="meeting">
                    <div class="meeting-header">{course} ({country})<span class="discipline-badge">{discipline.title()}</span></div>
                    {races_html}
                </div>
                """

        stats_html = "".join([f"<div class='error'><strong>Error:</strong> {e}</div>" for e in results.errors])
        stats_html += "".join([f"<div class='warning'><strong>Warning:</strong> {w}</div>" for w in results.warnings])

        # This is a simplified template. A full version would use Jinja2.
        html_template = f"""
<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>Racing Report</title><style>
body{{font-family:sans-serif;background:#f8fafc;color:#1e293b;padding:20px}}
.container{{max-width:1200px;margin:auto;background:white;padding:20px;border-radius:12px;box-shadow:0 2px 10px rgba(0,0,0,0.1)}}
h1{{color:#3b82f6;text-align:center;margin-bottom:20px}} h2{{color:#1e40af;margin-top:30px;border-bottom:2px solid #e2e8f0;padding-bottom:10px}}
.meeting{{margin-top:20px;border:1px solid #e2e8f0;border-radius:8px}}
.meeting-header{{background:#3b82f6;color:white;padding:15px;font-size:1.2em;font-weight:bold;border-radius:8px 8px 0 0}}
.discipline-badge{{background:rgba(255,255,255,0.2);padding:4px 8px;border-radius:12px;font-size:0.8em;margin-left:10px}}
.race{{display:grid;grid-template-columns:auto 1fr auto auto;gap:15px;padding:15px;border-bottom:1px solid #e2e8f0;align-items:center}}
.race:last-child{{border-bottom:none}}
.race-time{{font-weight:bold;color:#1e40af;font-size:1.1em}}
.field-size{{background:#dbeafe;color:#1e40af;padding:6px 12px;border-radius:12px;font-weight:bold}}
.btn{{padding:8px 12px;border-radius:6px;text-decoration:none;font-size:0.9em;background:#3b82f6;color:white}}
.error{{background:#fef2f2;color:#dc2626;padding:15px;border-left:4px solid #ef4444;margin-top:15px}}
.warning{{background:#fefbeb;color:#d97706;padding:15px;border-left:4px solid #f59e0b;margin-top:15px}}
</style></head><body><div class="container"><h1>{self.config['APP_NAME']} Report</h1>{stats_html}{body_html}</div></body></html>
        """
        return html_template

    def save_csv_report(self, results: ScanResults) -> Path:
        filename = self.output_dir / f"scanner_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(asdict(results.races[0]).keys() if results.races else [])
            for race in results.races:
                writer.writerow(asdict(race).values())
        logging.info(f"📄 CSV report saved to: {filename}")
        return filename

    def export_json_schedule(self, results: ScanResults, target_date: datetime, out_path: Path):
        """
        Exports a normalized schedule JSON that Paddock Parser can ingest directly.
        This is the critical "Scout -> Analyzer" bridge.
        """
        payload = {
            "schema": "quickstrike_schedule_v1",
            "date": target_date.strftime("%Y-%m-%d"),
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "races": []
        }
        for r in results.races:
            payload["races"].append({
                "course": r.course, "country": r.country, "discipline": r.discipline,
                "race_time": r.race_time, "field_size": r.field_size,
                "race_url": r.race_url, "meeting_url": r.meeting_url, "source": r.source,
            })
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        logging.info(f"🧭 JSON schedule for Paddock Parser exported to: {out_path}")

# =============================================================================
# MAIN EXECUTION AND MENU
# =============================================================================

async def run_scan_and_report(config, days_ahead=0, verbose=False, no_html=False, out_dir=None, json_out=None):
    """Main function to run a scan and generate reports."""
    scanner = EnhancedRacingScanner(config, verbose=verbose)
    target_dt = datetime.now() + timedelta(days=days_ahead)
    results = await scanner.scan(target_dt)
    
    print(f"\nScan Complete: Found {results.total_found} races in {results.duration:.2f}s")
    if results.errors: print(f"❌ {len(results.errors)} errors occurred: {results.errors[0]}")
    if results.warnings: print(f"⚠️ {len(results.warnings)} warnings: {results.warnings[0]}")

    if results.races:
        output_dir = out_dir or Path("output")
        output_manager = OutputManager(config, output_dir)

        if json_out:
            output_manager.export_json_schedule(results, target_dt, Path(json_out))
        if not no_html:
            html_content = output_manager.generate_html_report(results)
            report_file = output_dir / f"scanner_report_{datetime.now().strftime('%Y%m%d%H%M%S')}.html"
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            print(f"📄 HTML report saved to {report_file}")
            if config.get("AUTO_OPEN_REPORT", True):
                webbrowser.open(f"file://{os.path.abspath(report_file)}")
        
        output_manager.save_csv_report(results)

async def test_connections(config):
    """Test connectivity to primary and backup sources."""
    print("\nTesting connections...")
    async with EnhancedHttpClient(config) as client:
        sources = [("Primary API", config["PRIMARY_API"])] + [(s['name'], s['url']) for s in config["BACKUP_SOURCES"]]
        for name, url in sources:
            print(f"  - {name}: ", end="")
            content = await client.get(url)
            print("✅ OK" if content else "❌ FAILED")
            await asyncio.sleep(0.5)

def main_menu(config):
    """Display the main interactive menu."""
    while True:
        print("\n" + "="*40)
        print(f" {config['APP_NAME']} Menu")
        print("="*40)
        print("1. Scan Today's Racing")
        print("2. Scan Tomorrow's Racing")
        print("3. Test Connections")
        print("Q. Quit")
        choice = input("Enter choice: ").upper()

        if choice == '1': asyncio.run(run_scan_and_report(config, days_ahead=0, verbose=True))
        elif choice == '2': asyncio.run(run_scan_and_report(config, days_ahead=1, verbose=True))
        elif choice == '3': asyncio.run(test_connections(config))
        elif choice == 'Q': break
        else: print("Invalid choice.")

def main():
    """Main entry point for the scanner."""
    parser = argparse.ArgumentParser(description="Enhanced Racing Scanner (Quick Strike)")
    parser.add_argument("--date", help="Target date (YYYY-MM-DD). Default: today")
    parser.add_argument("--json-out", help="Path to export normalized schedule JSON (bridge to Paddock)")
    parser.add_argument("--no-html", action="store_true", help="Skip HTML report generation")
    parser.add_argument("--out", help="Output directory for reports (default: ./output)")
    parser.add_argument("--quiet", action="store_true", help="Reduce console logging to essentials")
    parser.add_argument("--interactive", action="store_true", help="Force interactive menu to open")
    args = parser.parse_args()

    # Setup logging first
    log_dir = Path("logs"); log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"scanner_main_{datetime.now().strftime('%Y%m%d')}.log"
    logging.basicConfig(level=logging.DEBUG if not args.quiet else logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.StreamHandler(), logging.FileHandler(log_file, encoding='utf-8')])

    # Decide on mode
    if len(sys.argv) > 1 and not args.interactive:
        # CLI Mode
        days_ahead = 0
        if args.date:
            try:
                target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
                days_ahead = (target_date - date.today()).days
            except ValueError:
                print("Invalid --date format. Use YYYY-MM-DD."); sys.exit(1)
        
        out_dir = Path(args.out) if args.out else None
        asyncio.run(run_scan_and_report(CONFIG, days_ahead=days_ahead, verbose=not args.quiet, no_html=args.no_html, out_dir=out_dir, json_out=args.json_out))
    else:
        # Interactive Menu Mode
        main_menu(CONFIG)

if __name__ == "__main__":
    main()
