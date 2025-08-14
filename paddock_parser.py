#!/usr/bin/env python3
"""
Paddock Parser v1.1 (The Persistent Engine)

---
NOTE TO SELF (The Constitution - Preamble):
This is our "Deep Dive" tool. Its philosophy is absolute reliability and
analytical depth, achieved through a manual-first workflow. It is designed to
be an "always-on" analytical partner that builds a progressively richer and
more complete dataset throughout the day. It is the definitive source of
truth, immune to the flakiness of automated scraping. All architectural
decisions serve this core mission of reliability and intelligence.
---

Changelog v1.1:
- **New Persistent Mode:** A `--persistent` CLI flag enables an always-on
  engine that accepts pasted data blocks repeatedly throughout the day.
- **Crash-Safe State:** Automatic JSON backup of the in-memory cache after
  every successful update, with a daily restore prompt on startup.
- **Intelligent In-Memory Cache:** The engine maintains a daily cache of races,
  which is intelligently merged and enriched with each new data paste.
- **Robust Paste Listener:** Uses a sentinel-based input reader (`KABOOM`)
  that works reliably in interactive terminals on all operating systems.
- **Smart Merge Logic:** A sophisticated merge algorithm updates cached races,
  preferring richer data (like known odds over SP).
- **Dual-Proxy Link Helper:** The clickable link helper generates multiple
  proxy links for each source, driven by the config, for maximum flexibility.
"""

import json
import logging
import os
import re
import hashlib
import sys
import time
import webbrowser
import csv
import argparse
from dataclasses import dataclass, field, asdict, fields
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from collections import defaultdict
import concurrent.futures
from urllib.parse import quote

# User-friendly dependency checking
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo
try:
    from jinja2 import Environment, FileSystemLoader
    from bs4 import BeautifulSoup
    from tqdm import tqdm
except ImportError:
    print("FATAL: Required libraries are not installed.", file=sys.stderr)
    print("Please run: pip install -r requirements.txt", file=sys.stderr)
    sys.exit(1)

# =============================================================================
# DATA CLASSES
# =============================================================================
@dataclass
class Runner:
    """Represents a single runner in a race with its odds."""
    name: str
    odds_str: str
    odds_decimal: float

@dataclass
class SourceError:
    source_name: str
    error_message: str
    filename: str
    timestamp: datetime

@dataclass
class RaceData:
    """Represents a single race, now enriched with runner data and a value score."""
    id: str
    course: str
    race_time: str
    race_type: str
    utc_datetime: Optional[datetime]
    local_time: str
    timezone_name: str
    field_size: int
    country: str
    discipline: str
    source_file: str
    race_url: str = ""
    runners: List[Runner] = field(default_factory=list)
    favorite: Optional[Runner] = None
    second_favorite: Optional[Runner] = None
    value_score: float = 0.0
    data_sources: List[str] = field(default_factory=list)

@dataclass
class ParseStatistics:
    total_files_processed: int = 0
    total_files_failed: int = 0
    individual_race_errors: int = 0
    total_races_found: int = 0
    races_after_dedup: int = 0
    duration_seconds: float = 0.0
    per_source_counts: Dict[str, int] = field(default_factory=dict)
    source_errors: List[SourceError] = field(default_factory=list)

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================
def normalize_course_name(name: str) -> str:
    """Cleans and standardizes a racetrack name."""
    if not name: return ""
    name = name.lower().strip()
    name = re.sub(r' at .*$', '', name)
    name = re.sub(r'\s*\([^)]*\)', '', name)
    replacements = {'park': '', 'raceway': '', 'racecourse': '', 'track': '', 'stadium': '', 'greyhound': '', 'harness': ''}
    for old, new in replacements.items(): name = name.replace(old, new)
    return " ".join(name.split())

def get_track_timezone(course: str, country: str, config: Dict, tz_abbreviation: Optional[str] = None) -> str:
    """Finds the IANA timezone for a track using the config file."""
    if tz_abbreviation and tz_abbreviation in config["TIMEZONES"]["ABBREVIATIONS"]:
        return config["TIMEZONES"]["ABBREVIATIONS"][tz_abbreviation]
    norm_course = normalize_course_name(course).replace(" ", "-")
    return config["TIMEZONES"]["TRACKS"].get(norm_course, config["TIMEZONES"]["COUNTRIES"].get(country.upper(), "UTC"))

def generate_race_id(course: str, race_date: date, time: str) -> str:
    """Creates a unique, deterministic ID for a race."""
    key = f"{normalize_course_name(course)}|{race_date.isoformat()}|{re.sub(r'[^\d]', '', time or '')}"
    return hashlib.sha1(key.encode()).hexdigest()[:12]

def parse_local_hhmm_pm(time_text: str) -> Optional[str]:
    """Parses a time string (e.g., '7:30 PM') into 24-hour format 'HH:MM'."""
    if not time_text: return None
    match = re.search(r"\b(\d{1,2}):(\d{2})\s*([Pp][Mm])?\b", time_text)
    if not match: return None
    h, mm, pm = match.groups(); hour = int(h)
    if pm and hour != 12: hour += 12
    if not pm and hour == 12: hour = 0
    return f"{hour:02d}:{mm}"

def convert_odds_to_fractional_decimal(odds_str: str) -> float:
    """Converts fractional, 'EVS', or 'SP' odds string to a decimal float of the fraction (e.g., 5/2 -> 2.5)."""
    if not isinstance(odds_str, str) or not odds_str.strip(): return 999.0
    s = odds_str.strip().upper().replace("-", "/")
    if s in {"SP", "NR", "SCR"}: return 999.0
    if s in {"EVS", "EVENS"}: return 1.0
    if "/" in s:
        try:
            num, den = map(float, s.split("/", 1))
            return num / den if den > 0 else 999.0
        except (ValueError, ZeroDivisionError): return 999.0
    try:
        dec = float(s)
        return dec - 1.0 if dec > 1 else 999.0
    except ValueError: return 999.0

def normalize_race_type(raw_type: str) -> str:
    """Standardizes different variations of race types."""
    raw_type = raw_type.lower().strip()
    mapping = {
        'mdn clm': 'Maiden Claiming', 'maiden claiming': 'Maiden Claiming',
        'mdn sp wt': 'Maiden Special Weight', 'maiden special weight': 'Maiden Special Weight',
        'clm': 'Claiming', 'claiming': 'Claiming',
        'alw': 'Allowance', 'allowance': 'Allowance',
        'stk': 'Stakes', 'stakes': 'Stakes',
        'optional claiming': 'Allowance Optional Claiming', 'alw opt clm': 'Allowance Optional Claiming',
        'hcap': 'Handicap', 'handicap': 'Handicap'
    }
    for key, value in mapping.items():
        if key in raw_type:
            return value
    return raw_type.title()

def setup_logging(log_path: str):
    """Configures logging to file and console."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", 
                       handlers=[logging.FileHandler(log_path, mode='w'), logging.StreamHandler(sys.stdout)])

# =============================================================================
# CLI ARGUMENT PARSING
# =============================================================================
def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Paddock Parser v1.1: Persistent racing data analysis engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
  # Batch mode (as before)
  python paddock_parser.py

  # Persistent mode with sentinel
  python paddock_parser.py --persistent
  # Paste data blocks; end each block with: KABOOM

  # Persistent with auto-restore and custom cache dir
  python paddock_parser.py --persistent --auto-restore --cache-dir ./.paddock_cache
        """
    )

    core_group = parser.add_argument_group('Core Configuration')
    core_group.add_argument('-c', '--config', default='config.json', help="Path to config file (default: %(default)s)")
    core_group.add_argument('-i', '--input', help="Path to input directory (overrides config)")
    core_group.add_argument('-o', '--output', help="Path to output directory (overrides config)")
    core_group.add_argument('-l', '--log', help="Path to log file (overrides config)")
    core_group.add_argument('-t', '--template', help="Path to HTML template file (overrides config)")

    scoring_group = parser.add_argument_group('Value Scoring & Filtering')
    scoring_group.add_argument('--min-score', type=float, metavar='SCORE', help="Minimum value score (0-100) to include")
    scoring_group.add_argument('--no-odds-mode', action='store_true', help="Bypass scoring and sort by time; for use when odds are unavailable.")

    field_group = parser.add_argument_group('Field Size Filtering')
    field_group.add_argument('--min-field-size', type=int, metavar='N', help="Minimum field size to include")
    field_group.add_argument('--max-field-size', type=int, metavar='N', help="Maximum field size to include")

    type_group = parser.add_argument_group('Race Type Filtering')
    type_group.add_argument('--exclude-race-types', metavar='TYPES', help="Comma-separated list of race types to exclude")

    output_group = parser.add_argument_group('Output Control')
    output_group.add_argument('--sort-by', choices=['score', 'time', 'field_size', 'course'], default='score', help="Sort final report by criteria (default: %(default)s)")
    output_group.add_argument('--limit', type=int, metavar='N', help="Limit output to top N races after sorting")

    mode_group = parser.add_argument_group('Persistent Mode')
    mode_group.add_argument('--persistent', action='store_true', help="Run the always-on engine with an in-memory daily cache.")
    mode_group.add_argument('--paste-sentinel', default='KABOOM', help="Line delimiter to end a pasted block in persistent mode (default: %(default)s).")

    cache_group = parser.add_argument_group('Cache & Recovery')
    cache_group.add_argument('--cache-dir', help="Directory for daily cache backups (defaults to OUTPUT dir).")
    cache_group.add_argument('--auto-restore', action='store_true', help="Auto-restore today's cache backup on start without prompting.")
    cache_group.add_argument('--disable-cache-backup', action='store_true', help="Disable automatic cache backups after updates.")

    return parser.parse_args()

def load_config(path: str) -> Dict:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"CRITICAL: Could not load or parse config file '{path}'. Error: {e}", file=sys.stderr)
        sys.exit(1)

# =============================================================================
# --- ENHANCED VALUE SCORER ---
# =============================================================================
class EnhancedValueScorer:
    """
    NOTE TO SELF (The Constitution - Article IV: The Analytical Engine):
    The purpose of this scorer is to find STRUCTURALLY ADVANTAGEOUS betting
    opportunities. It analyzes Field Size, Race Type, and the shape of the
    Odds market. It explicitly IGNORES traditional handicapping metrics like
    jockey, trainer, or past performance, as that is not our strategic goal.
    """
    def __init__(self, config: Dict):
        self.config = config
        self.weights = config.get("SCORER_WEIGHTS", {
            "FIELD_SIZE_WEIGHT": 0.35, "FAVORITE_ODDS_WEIGHT": 0.45,
            "ODDS_SPREAD_WEIGHT": 0.15, "DATA_QUALITY_WEIGHT": 0.05
        })

    def calculate_score(self, race: RaceData) -> float:
        """Calculates the final value score for a race."""
        if not race.runners or not race.favorite:
            return 0.0

        base_score = (self._calculate_field_score(race.field_size) * self.weights["FIELD_SIZE_WEIGHT"] +
                      self._calculate_favorite_odds_score(race.favorite) * self.weights["FAVORITE_ODDS_WEIGHT"] +
                      self._calculate_odds_spread_score(race.favorite, race.second_favorite) * self.weights["ODDS_SPREAD_WEIGHT"] +
                      self._calculate_data_quality_score(race) * self.weights["DATA_QUALITY_WEIGHT"])
        
        multiplier = 1.0
        if self._has_live_odds(race): multiplier *= 1.2
        if race.discipline == "greyhound": multiplier *= 1.1
        if race.field_size <= 6 and self._calculate_odds_spread_score(race.favorite, race.second_favorite) > 80: multiplier *= 1.15
        
        final_score = base_score * multiplier
        return min(100.0, max(0.0, final_score))

    def _calculate_field_score(self, size: int) -> float:
        if 3 <= size <= 5: return 100.0
        elif 6 <= size <= 8: return 85.0
        elif 9 <= size <= 12: return 60.0
        else: return 20.0

    def _calculate_favorite_odds_score(self, favorite: Runner) -> float:
        odds = favorite.odds_decimal
        if odds == 999.0: return 50.0
        if 1.0 <= odds <= 1.5: return 100.0
        elif 1.5 < odds <= 2.5: return 90.0
        elif 2.5 < odds <= 4.0: return 75.0
        elif 0.5 <= odds < 1.0: return 85.0
        elif odds < 0.5: return 60.0
        else: return 40.0

    def _calculate_odds_spread_score(self, favorite: Runner, second_favorite: Optional[Runner]) -> float:
        if not second_favorite: return 50.0
        fav_odds, sec_odds = favorite.odds_decimal, second_favorite.odds_decimal
        if fav_odds == 999.0 or sec_odds == 999.0: return 50.0
        spread = sec_odds - fav_odds
        if spread >= 2.0: return 100.0
        elif spread >= 1.5: return 90.0
        elif spread >= 1.0: return 80.0
        elif spread >= 0.5: return 60.0
        else: return 40.0

    def _calculate_data_quality_score(self, race: RaceData) -> float:
        score = 0.0
        if race.runners and any(r.odds_str for r in race.runners): score += 40.0
        if race.favorite and race.second_favorite: score += 30.0
        if race.race_url: score += 20.0
        if len(race.data_sources) > 1: score += 10.0
        return min(100.0, score)

    def _has_live_odds(self, race: RaceData) -> bool:
        return any(r.odds_str and r.odds_str not in ["", "SP", "NR", "VOID", "SCR"] for r in race.runners)

# =============================================================================
# --- DATA COLLECTION HELPER (with Dual-Proxy Link Generator) ---
# =============================================================================

def create_and_launch_link_helper(config: Dict, output_dir: Path):
    """
    NOTE TO SELF (The Constitution - Article III: The Collector & Proxies):
    This function generates our interactive data collection dashboard. It is
    NOT just a list of links. The dual-proxy system is our "Firewall Buster,"
    a key strategic feature that makes manual collection viable from any network.
    """
    today = date.today()
    date_str_iso = today.strftime("%Y-%m-%d")
    
    proxy_viewers = config.get("PROXY_VIEWERS", [])
    
    link_sections_html = ""
    source_categories = config.get("DATA_SOURCES", [])
    
    for category in source_categories:
        title = category.get("title", "Unknown Category")
        sites = category.get("sites", [])
        
        link_sections_html += f"<h2>{title}</h2>\n<ul>\n"
        for site in sites:
            name = site.get("name", "Unnamed Link")
            url = site.get("url", "#").format(date_str_iso=date_str_iso)
            
            proxy_links_html = ""
            for viewer in proxy_viewers:
                if viewer.get("ENABLED", False):
                    proxy_url_template = viewer.get("TOOL_URL", "")
                    proxy_link_text = viewer.get("LINK_TEXT", "View via Proxy")
                    if proxy_url_template:
                        encoded_url = quote(url, safe=':/')
                        proxy_full_url = proxy_url_template.format(target_url=encoded_url)
                        proxy_links_html += f' | <a href="{proxy_full_url}" target="_blank">{proxy_link_text}</a>'

            link_sections_html += f'    <li><strong>{name}:</strong> <a href="{url}" target="_blank">Direct Link</a>{proxy_links_html}</li>\n'
        link_sections_html += "</ul>\n"
    
    html_content = f"""
<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Paddock Parser - Data Collection Blueprint</title><style>
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif; margin: 0; padding: 25px; line-height: 1.6; background: #f8f9fa; color: #212529; }}
.container {{ max-width: 900px; margin: 0 auto; background: #fff; padding: 25px; border-radius: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.08); }}
h1, h2 {{ color: #2c3e50; border-bottom: 2px solid #e9ecef; padding-bottom: 10px; }}
h1 {{ font-size: 2rem; }} h2 {{ font-size: 1.5rem; margin-top: 30px; }} p {{ color: #6c757d; }}
ul {{ list-style-type: none; padding-left: 0; }}
li {{ margin-bottom: 15px; background: #f8f9fa; padding: 15px; border-radius: 8px; border-left: 4px solid #007bff; }}
a {{ color: #007bff; text-decoration: none; font-weight: 500; }}
a:hover {{ text-decoration: underline; }} strong {{ color: #343a40; }}
</style></head><body><div class="container">
<h1>🐎 {config['APP_NAME']} v{config['SCHEMA_VERSION']} - Master Blueprint</h1>
<p>Your '{config['INPUT_DIR']}' folder is empty. Use the links below to collect data. Each link will open in a new tab. When you are done, run the script again to parse the saved files.</p>
{link_sections_html}
</div></body></html>"""
    
    helper_path = output_dir / "_link_helper.html"
    try:
        with open(helper_path, "w", encoding="utf-8") as f: f.write(html_content)
        logging.info(f"Generated clickable link helper: {helper_path.resolve()}")
        webbrowser.open(f"file://{helper_path.resolve()}")
    except Exception as e:
        logging.error(f"Could not create or open the link helper HTML file: {e}")

# =============================================================================
# --- PARSERS (Full Suite Implemented) ---
# =============================================================================

def parse_equibase_text(content: str, filename: str, config: Dict, _: date, stats: ParseStatistics) -> List[RaceData]:
    """Parses Equibase text, now extracting race type, runners, and odds."""
    races = []
    meetings = re.split(r"Equibase Company is the Official Supplier of Racing", content)
    for meeting_text in meetings:
        if not meeting_text.strip(): continue
        try:
            header_match = re.search(r"^\s*([A-Za-z\s&.'-]+(?:Downs|Park|Racetrack|Meadows|Casino Racetrack & Resort|Casino|Resort))\s*\|\s*(.*?\d{1,2},\s*\d{4})", meeting_text, re.MULTILINE)
            if not header_match: continue
            course, date_str = header_match.group(1).strip(), header_match.group(2).strip()
            parse_date = datetime.strptime(date_str, '%b %d, %Y').date()
            
            race_blocks = re.split(r'^\s*(?:Race\s+)?(\d+)\s*$', meeting_text, flags=re.MULTILINE)[1:]
            
            for i in range(0, len(race_blocks), 2):
                if i + 1 >= len(race_blocks): continue
                race_num, race_content = race_blocks[i], race_blocks[i+1]
                
                try:
                    race_type_raw_match = re.match(r"^\s*([\w\s\(\)]+)", race_content)
                    race_type_raw = race_type_raw_match.group(1).strip() if race_type_raw_match else "Unknown"
                    race_type = normalize_race_type(race_type_raw)
                    
                    race_line_match = re.search(r"(\d{1,2}:\d{2}\s*[AP]M)\s+([A-Z]{2})\s+(\d+)\s+Starters", race_content, re.DOTALL)
                    if not race_line_match: continue

                    time_str, tz_abbr, starters = race_line_match.groups()
                    race_time = parse_local_hhmm_pm(time_str); field_size = int(starters)
                    if not race_time or field_size <= 0: continue
                    
                    runners = []
                    runner_matches = re.findall(r"^\s*\d+\s+([\w\s.'-]+?)\s+\([^)]*?(\d+/\d+|\d+-\d+|[Ee][Vv][Ss]?|\d+\.\d+|\d+)[^)]*\)", race_content, re.MULTILINE)
                    for name, odds_str in runner_matches:
                        clean_name = re.sub(r'\s{2,}', ' ', name.strip())
                        runners.append(Runner(name=clean_name, odds_str=odds_str, odds_decimal=convert_odds_to_fractional_decimal(odds_str)))

                    country = "CA" if normalize_course_name(course) in config["CANADIAN_TRACKS"] else "US"
                    tz_name = get_track_timezone(course, country, config, tz_abbr)
                    local_dt = datetime.combine(parse_date, datetime.strptime(race_time, "%H:%M").time()).replace(tzinfo=ZoneInfo(tz_name))
                    
                    races.append(RaceData(id=generate_race_id(course, parse_date, race_time), course=course, race_time=race_time, race_type=race_type,
                                          utc_datetime=local_dt.astimezone(ZoneInfo("UTC")), local_time=local_dt.strftime("%H:%M"),
                                          timezone_name=tz_name, field_size=field_size, country=country, discipline="thoroughbred",
                                          source_file=filename, runners=runners, data_sources=["EquibaseText"]))
                except Exception as e: stats.individual_race_errors += 1; logging.warning(f"Error parsing Equibase race #{race_num} in {filename}: {e}", exc_info=False)
        except Exception as e: stats.individual_race_errors += 1; logging.warning(f"Error processing Equibase meeting in {filename}: {e}", exc_info=False)
    return races

def parse_racingandsports_json(content: str, filename: str, config: Dict, parse_date: date, stats: ParseStatistics) -> List[RaceData]:
    """Parses RacingAndSports JSON feed, including runners and odds."""
    races = []
    try: data = json.loads(content)
    except json.JSONDecodeError: logging.error(f"Invalid JSON in {filename}"); return []
        
    for meeting_data in data:
        try:
            course = meeting_data.get('VenueName','').strip(); country_code = meeting_data.get('CountryCode', 'AU')
            discipline = norm.map_discipline(meeting_data.get('Discipline', 'thoroughbred'))
            if not course: continue

            for race_info in meeting_data.get('Races', []):
                try:
                    race_time = norm.parse_hhmm_any(race_info.get('RaceTime', ''))
                    if not race_time: continue
                    
                    runners_data = race_info.get('Runners', [])
                    runners = []
                    for runner_data in runners_data:
                        name = runner_data.get('RunnerName', 'Unknown').strip()
                        odds_str = runner_data.get('WinOdds', 'SP')
                        runners.append(Runner(name=name, odds_str=odds_str, odds_decimal=convert_odds_to_fractional_decimal(odds_str)))

                    if not runners: continue
                    race_type = normalize_race_type(race_info.get('RaceName', 'Unknown').strip())
                    tz_name = get_track_timezone(course, country_code, config)
                    local_dt = datetime.combine(parse_date, datetime.strptime(race_time, "%H:%M").time()).replace(tzinfo=ZoneInfo(tz_name))
                    
                    races.append(RaceData(id=generate_race_id(course, parse_date, race_time), course=course, race_time=race_time, race_type=race_type,
                                          utc_datetime=local_dt.astimezone(ZoneInfo("UTC")), local_time=local_dt.strftime("%H:%M"),
                                          timezone_name=tz_name, field_size=len(runners), country=country_code, discipline=discipline,
                                          race_url=race_info.get('RaceUrl', ''), source_file=filename, runners=runners, data_sources=["RacingAndSports"]))
                except Exception: stats.individual_race_errors += 1
        except Exception: stats.individual_race_errors += 1
    return races

def parse_drf_text(content: str, filename: str, config: Dict, _: date, stats: ParseStatistics) -> List[RaceData]:
    """Parses Daily Racing Form entries text, including runners and odds."""
    races = []
    track_sections = re.split(r'\n([A-Z\s]+(?:PARK|DOWNS|RACECOURSE|TRACK|MEADOWS))\s+-\s+(\w+,\s+\w+\s+\d+,\s+\d{4})', content)
    i = 1
    while i < len(track_sections):
        try:
            course, date_str, section_content = track_sections[i].strip(), track_sections[i + 1].strip(), track_sections[i + 2]
            parse_date = datetime.strptime(date_str, '%A, %B %d, %Y').date()
            
            race_blocks = re.split(r'Race\s+(\d+)', section_content)[1:]
            for j in range(0, len(race_blocks), 2):
                race_num, block_content = race_blocks[j], race_blocks[j+1]
                try:
                    time_match = re.search(r'(\d{1,2}:\d{2}\s*[AP]M)\s+\(([A-Z]{2})\)', block_content)
                    if not time_match: continue
                    time_str, tz_abbr = time_match.groups()
                    race_time = parse_local_hhmm_pm(time_str)

                    race_type_raw = block_content.split('\n')[1].strip()
                    race_type = normalize_race_type(race_type_raw)
                    
                    runners = []
                    # DRF odds are often M/L 5-2, 20-1 etc.
                    runner_matches = re.findall(r'^\s*\d+\s+([A-Za-z\s\'.]+?)\s+\(\d+\)\s+[\w\s]+\s+L\s+[\d.]+\s+(\d+-\d+|\d+)', block_content, re.MULTILINE)
                    for prog_num, name, odds_str in runner_matches:
                        runners.append(Runner(name=name.strip(), odds_str=odds_str, odds_decimal=convert_odds_to_fractional_decimal(odds_str)))

                    if not race_time or not runners: continue
                    country = "CA" if normalize_course_name(course) in config["CANADIAN_TRACKS"] else "US"
                    tz_name = get_track_timezone(course, country, config, tz_abbr)
                    local_dt = datetime.combine(parse_date, datetime.strptime(race_time, "%H:%M").time()).replace(tzinfo=ZoneInfo(tz_name))

                    races.append(RaceData(id=generate_race_id(course, parse_date, race_time), course=course, race_time=race_time, race_type=race_type,
                                          utc_datetime=local_dt.astimezone(ZoneInfo("UTC")), local_time=local_dt.strftime("%H:%M"),
                                          timezone_name=tz_name, field_size=len(runners), country=country, discipline="thoroughbred",
                                          source_file=filename, runners=runners, data_sources=["DRF"]))
                except Exception as e: stats.individual_race_errors += 1; logging.warning(f"Error parsing DRF race #{race_num} in {filename}: {e}", exc_info=False)
        except IndexError:
             pass # Reached end of track sections
        except Exception as e: stats.individual_race_errors += 1; logging.warning(f"Error processing DRF meeting in {filename}: {e}", exc_info=False)
        i += 3
    return races

def parse_racingpost(html: str, filename: str, config: Dict, parse_date: date, stats: ParseStatistics) -> List[RaceData]:
    """
    Parses Racing Post HTML page. Since the main page doesn't list all odds,
    this creates placeholder runners with "SP" odds, ensuring the race can be
    scored and processed by the analytical engine.
    """
    races = []; soup = BeautifulSoup(html, 'html.parser')
    meeting_containers = soup.select('div[data-test-selector="RC-meetingItem"]')
    if not meeting_containers: meeting_containers = [soup] 

    for meeting in meeting_containers:
        try:
            course_element = meeting.select_one('a[data-test-selector="link-meetingCourseName"]') or soup.select_one('h1')
            if not course_element: continue
            course = course_element.get_text(strip=True)
            
            country_element = meeting.select_one('span[data-test-selector="RC-meetingCountry"]')
            country_text = country_element.get_text(strip=True) if country_element else "GB"
            country = country_text.replace('(', '').replace(')', '')

            for race_container in meeting.select('div[data-test-selector^="racecard-raceStream"]'):
                try:
                    time_tag = race_container.select_one('span[data-test-selector="racecard-raceTime"]')
                    if not time_tag: continue
                    race_time = parse_local_hhmm_pm(time_tag.get_text(strip=True))
                    if not race_time: continue
                    
                    runner_tag = race_container.select_one('span[data-test-selector="racecard-header-runners"]')
                    field_size_match = re.search(r'(\d+)', runner_tag.get_text()) if runner_tag else None
                    field_size = int(field_size_match.group(1)) if field_size_match else 0
                    if field_size <= 0: continue
                    
                    runners = []
                    for i in range(1, field_size + 1):
                        runners.append(Runner(
                            name=f"Runner #{i}",
                            odds_str="SP",
                            odds_decimal=convert_odds_to_fractional_decimal("SP")
                        ))
                    
                    race_type_raw = (race_container.select_one('span[data-test-selector="racecard-raceTitle"]') or soup.new_tag('span')).get_text(strip=True)
                    race_type = normalize_race_type(race_type_raw)
                    
                    race_link_tag = race_container.select_one('a[data-test-selector="racecard-raceTitleLink"]')
                    race_url = f"https://www.racingpost.com{race_link_tag['href']}" if race_link_tag else ""
                    
                    tz_name = get_track_timezone(course, country, config)
                    local_dt = datetime.combine(parse_date, datetime.strptime(race_time, "%H:%M").time()).replace(tzinfo=ZoneInfo(tz_name))

                    races.append(RaceData(id=generate_race_id(course, parse_date, race_time), course=course, race_time=race_time, race_type=race_type,
                                          utc_datetime=local_dt.astimezone(ZoneInfo("UTC")), local_time=local_dt.strftime("%H:%M"),
                                          timezone_name=tz_name, field_size=field_size, country=country, discipline="thoroughbred", race_url=race_url,
                                          source_file=filename, runners=runners, data_sources=["RacingPost"]))
                except Exception as e: stats.individual_race_errors += 1; logging.warning(f"Error parsing RP race in {filename} ({course}): {e}", exc_info=False)
        except Exception as e: stats.individual_race_errors += 1; logging.warning(f"Error processing RP meeting in {filename}: {e}", exc_info=False)
    return races


PARSER_MAP = {
    "EquibaseText": parse_equibase_text,
    "RacingAndSportsJSON": parse_racingandsports_json,
    "DRF": parse_drf_text,
    "RacingPost": parse_racingpost,
}

# =============================================================================
# --- CORE LOGIC & PERSISTENT ENGINE HELPERS (GPT-5 Architecture) ---
# =============================================================================

def identify_source(content: str, filename: str) -> str:
    """Enhanced source identification for various data formats."""
    if "Daily Racing Form" in content or "DRF.com" in content: return "DRF"
    if "Equibase Company is the Official Supplier of Racing" in content: return "EquibaseText"
    try:
        data = json.loads(content)
        if isinstance(data, list) and data:
            if "Discipline" in data[0] or "VenueName" in data[0]: return "RacingAndSportsJSON"
    except (json.JSONDecodeError, TypeError): pass
    if 'data-test-selector="link-meetingCourseName"' in content: return "RacingPost"
    if 'sdc-site-racing-meetings__event' in content: return "SkySports"
    return "Unknown"

def determine_parse_date(filepath: Path, content: str) -> date:
    """Intelligently determine the date for a given file."""
    if match := re.search(r'(\d{4}-\d{2}-\d{2})', filepath.name): return datetime.strptime(match.group(1), '%Y-%m-%d').date()
    if match := re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{1,2},\s+\d{4}', content, re.IGNORECASE):
        try: return datetime.strptime(match.group(0), '%b %d, %Y').date()
        except ValueError:
            try: return datetime.strptime(match.group(0), '%B %d, %Y').date()
            except ValueError: pass
    if match := re.search(r'(\d{4}-\d{2}-\d{2})', content): return datetime.strptime(match.group(1), '%Y-%m-%d').date()
    return date.today()

def _normalize_runner_key(name: str) -> str:
    return re.sub(r'\s+', ' ', (name or '').strip()).lower()

def _has_known_odds(r: Runner) -> bool:
    return r and (r.odds_decimal is not None) and (r.odds_decimal != 999.0)

def _choose_better_runner(existing: Runner, incoming: Runner) -> Runner:
    if _has_known_odds(incoming) and not _has_known_odds(existing): return incoming
    if _has_known_odds(existing) and not _has_known_odds(incoming): return existing
    if _has_known_odds(existing) and _has_known_odds(incoming): return incoming
    return existing

def _merge_runner_lists(existing: List[Runner], incoming: List[Runner]) -> List[Runner]:
    merged: Dict[str, Runner] = { _normalize_runner_key(r.name): r for r in existing }
    for r in incoming:
        key = _normalize_runner_key(r.name)
        if key in merged: merged[key] = _choose_better_runner(merged[key], r)
        else: merged[key] = r
    result = list(merged.values())
    result.sort(key=lambda rr: rr.odds_decimal)
    return result

def smart_merge_race(existing: RaceData, new: RaceData) -> RaceData:
    existing.data_sources = sorted(set((existing.data_sources or []) + (new.data_sources or [])))
    if not existing.race_url and new.race_url: existing.race_url = new.race_url
    if (not existing.race_type or existing.race_type == "Unknown") and (new.race_type and new.race_type != "Unknown"):
        existing.race_type = new.race_type
    existing.runners = _merge_runner_lists(existing.runners or [], new.runners or [])
    existing.field_size = len(existing.runners)
    if not existing.local_time and new.local_time: existing.local_time = new.local_time
    if not existing.utc_datetime and new.utc_datetime: existing.utc_datetime = new.utc_datetime
    if not existing.timezone_name and new.timezone_name: existing.timezone_name = new.timezone_name
    if not existing.country and new.country: existing.country = new.country
    if not existing.discipline and new.discipline: existing.discipline = new.discipline
    return existing

def deduplicate_and_enrich_races(races: List[RaceData]) -> List[RaceData]:
    """Merges duplicate race entries and enriches them with sorted runner data."""
    unique_races = {}
    for race in races:
        key = race.id
        if key not in unique_races:
            unique_races[key] = race
        else:
            existing = unique_races[key]
            smart_merge_race(existing, race)
    
    final_races = list(unique_races.values())
    for race in final_races:
        if race.runners:
            race.runners.sort(key=lambda r: r.odds_decimal)
            race.favorite = race.runners[0] if race.runners else None
            race.second_favorite = race.runners[1] if len(race.runners) > 1 else None
            race.field_size = len(race.runners)
    return final_races

# =============================================================================
# --- PERSISTENT ENGINE HELPERS (GPT-5 Architecture) ---
# =============================================================================

def read_multiline_block(sentinel: str) -> str:
    """Reads lines from stdin until a line equals the sentinel (case sensitive)."""
    print(f"Paste your data, then type {sentinel} on its own line and press Enter.")
    lines = []
    while True:
        try: line = input()
        except EOFError: break
        if line.strip() == sentinel: break
        lines.append(line)
    return "\n".join(lines)

def parse_pasted_block(pasted: str, config: Dict) -> List[RaceData]:
    """Tries each known parser over the pasted block."""
    all_races: List[RaceData] = []
    parse_date = determine_parse_date(Path("STDIN"), pasted)
    for source_name, parser_fn in PARSER_MAP.items():
        stats = ParseStatistics()
        try:
            races = parser_fn(pasted, "STDIN", config, parse_date, stats)
            if races: all_races.extend(races)
        except Exception: continue
    return all_races

def get_cache_path(base_dir: Path, d: date) -> Path:
    """Get the cache file path for a given date."""
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir / f"paddock_cache_{d.isoformat()}.json"

def race_to_dict(r: RaceData) -> Dict[str, Any]:
    """Convert RaceData to dictionary with safe datetime serialization."""
    d = asdict(r)
    if r.utc_datetime:
        d['utc_datetime'] = r.utc_datetime.isoformat()
    else:
        d['utc_datetime'] = None
    return d

def race_from_dict(d: Dict[str, Any]) -> RaceData:
    """Convert dictionary back to RaceData with safe datetime deserialization."""
    runners = [Runner(**rr) for rr in d.get('runners', [])]
    utc_dt = None
    if d.get('utc_datetime'):
        try:
            utc_dt = datetime.fromisoformat(d['utc_datetime'])
        except (ValueError, TypeError):
            utc_dt = None
    
    race_fields = {f.name for f in fields(RaceData)}
    init_data = {k: v for k, v in d.items() if k in race_fields}
    init_data['runners'] = runners
    init_data['utc_datetime'] = utc_dt
    return RaceData(**init_data)

def atomic_write_json(path: Path, payload: Any):
    """Atomically write JSON to a file using a temporary file."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, 'w', encoding='utf-8') as f: json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def save_cache(cache: Dict[str, RaceData], path: Path):
    """Save the race cache to a JSON file."""
    payload = {
        "schema": "paddock_cache_v1", 
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "count": len(cache), 
        "races": {rid: race_to_dict(r) for rid, r in cache.items()}
    }
    atomic_write_json(path, payload)

def load_cache(path: Path) -> Dict[str, RaceData]:
    """Load the race cache from a JSON file."""
    with open(path, 'r', encoding='utf-8') as f: 
        data = json.load(f)
    races = data.get("races", {})
    return {rid: race_from_dict(rdict) for rid, rdict in races.items()}

# =============================================================================
# PERSISTENT ENGINE
# =============================================================================
def run_persistent_engine(config: Dict, args: argparse.Namespace):
    """
    NOTE TO SELF (The Constitution - Article I & II: The Persistent Workflow & Cache):
    This is the heart of the "Deep Dive" tool. It's an always-on loop that
    listens for pasted data, merges it into an intelligent, crash-safe daily
    cache, and re-analyzes the complete dataset with every new piece of
    information. This entire block enables our core "session-based" workflow.
    """
    output_dir = Path(args.output or config["DEFAULT_OUTPUT_DIR"])
    cache_dir = Path(args.cache_dir) if args.cache_dir else output_dir
    template_path = args.template or config.get("TEMPLATE_FILE", "template.html")
    output_dir.mkdir(exist_ok=True, parents=True)
    setup_logging(args.log or config.get("LOG_FILE", "parser.log"))
    
    logging.info(f"Starting {config['APP_NAME']} v{config['SCHEMA_VERSION']} in persistent mode.")
    print("\nPersistent mode is active.")
    print(f"- Paste data, then type {args.paste_sentinel} on a new line and press Enter.")
    print("- Exit with Ctrl+C (your cache will be saved).\n")
    
    daily_cache: Dict[str, RaceData] = {}
    current_parse_date: Optional[date] = None
    scorer = EnhancedValueScorer(config)

    try:
        while True:
            today = date.today()
            backup_path = get_cache_path(cache_dir, today)
            if current_parse_date != today:
                logging.info(f"New day ({today.isoformat()}) detected.")
                daily_cache, current_parse_date = {}, today
                if backup_path.exists():
                    restore = args.auto_restore
                    if not restore:
                        try:
                            ans = input(f"Found cache backup for today. Restore it? [y/N]: ").strip().lower()
                            restore = ans == 'y'
                        except EOFError: 
                            restore = False
                    
                    if restore:
                        try:
                            daily_cache = load_cache(backup_path)
                            logging.info(f"Restored {len(daily_cache)} races from backup.")
                        except Exception as e: 
                            logging.warning(f"Could not restore cache: {e}")
                try: 
                    create_and_launch_link_helper(config, output_dir)
                except Exception as e: 
                    logging.warning(f"Could not launch link helper: {e}")

            logging.info("Waiting for a data block...")
            pasted = read_multiline_block(args.paste_sentinel)
            if not pasted.strip(): 
                logging.warning("No data received. Waiting again.")
                continue
            
            logging.info("Data received. Parsing...")
            new_races = parse_pasted_block(pasted, config)
            if not new_races: 
                logging.warning("No races parsed from the pasted block.")
                continue

            added, updated = 0, 0
            for r in new_races:
                if r.id in daily_cache: 
                    smart_merge_race(daily_cache[r.id], r)
                    updated += 1
                else: 
                    daily_cache[r.id] = r
                    added += 1
            
            enriched = deduplicate_and_enrich_races(list(daily_cache.values()))
            for r in enriched: 
                daily_cache[r.id] = r
            
            if not args.no_odds_mode:
                for race in enriched: 
                    race.value_score = scorer.calculate_score(race)
            
            filtered_races = enriched
            if args.min_score is not None and not args.no_odds_mode: filtered_races = [r for r in filtered_races if r.value_score >= args.min_score]
            if args.min_field_size is not None: filtered_races = [r for r in filtered_races if r.field_size >= args.min_field_size]
            if args.max_field_size is not None: filtered_races = [r for r in filtered_races if r.field_size <= args.max_field_size]
            if args.exclude_race_types:
                excluded_types = {t.strip().lower() for t in args.exclude_race_types.split(',')}
                filtered_races = [r for r in filtered_races if r.race_type.lower() not in excluded_types]

            sort_key = 'time' if args.no_odds_mode else args.sort_by
            reverse_sort = True if sort_key == 'score' else False
            if sort_key == 'time': final_races = sorted(filtered_races, key=lambda r: r.utc_datetime or datetime.min.replace(tzinfo=ZoneInfo("UTC")), reverse=reverse_sort)
            elif sort_key == 'field_size': final_races = sorted(filtered_races, key=lambda r: r.field_size, reverse=reverse_sort)
            elif sort_key == 'course': final_races = sorted(filtered_races, key=lambda r: r.course, reverse=reverse_sort)
            else: final_races = sorted(filtered_races, key=lambda r: r.value_score, reverse=True)

            if args.limit is not None: final_races = final_races[:args.limit]
            
            if final_races:
                om = OutputManager(config, output_dir, template_path)
                now_utc = datetime.now(ZoneInfo("UTC"))
                filters_config = config.get("FILTERS", {})
                
                report_data = {
                    'all_races': final_races,
                    'value_races': [r for r in final_races if r.value_score >= 70],
                    'next_to_jump': sorted([r for r in final_races if r.utc_datetime and r.utc_datetime > now_utc], key=lambda x: x.utc_datetime or datetime.max.replace(tzinfo=ZoneInfo("UTC")))[:20],
                    'perfect_tips': [r for r in final_races if r.field_size < 7 and r.favorite and r.favorite.odds_decimal >= 1.0 and r.second_favorite and r.second_favorite.odds_decimal >= 3.0],
                    'filtered_races': [r for r in final_races if filters_config.get("MIN_FIELD_SIZE", 4) <= r.field_size <= filters_config.get("MAX_FIELD_SIZE", 12)]
                }
                
                report_stats = ParseStatistics(total_races_found = len(enriched), races_after_dedup=len(daily_cache))
                
                formats = config["OUTPUT"]["OUTPUT_FORMATS"]
                if "html" in formats: om.write_html_report(report_data, report_stats)
                if "json" in formats: om.write_json_report(final_races, report_stats)
                if "csv" in formats: om.write_csv_report(final_races)

            if not args.disable_cache_backup:
                try:
                    save_cache(daily_cache, backup_path)
                    logging.info(f"Backup saved to {backup_path}")
                except Exception as e: logging.warning(f"Failed to save cache backup: {e}")
            
            logging.info(f"Merge complete. Added: {added}, Updated: {updated}, Cache size: {len(daily_cache)}")
            logging.info("-" * 60)
            
    except KeyboardInterrupt:
        if not args.disable_cache_backup and daily_cache:
            backup_path = get_cache_path(cache_dir, date.today())
            try:
                save_cache(daily_cache, backup_path)
                logging.info(f"Final backup saved to {backup_path}")
            except Exception as e:
                logging.warning(f"Failed to save final backup: {e}")
        print("\nGoodbye! 👋")

# =============================================================================
# MAIN FUNCTION
# =============================================================================
def main():
    """Main entry point orchestrating both batch and persistent modes."""
    args = parse_arguments()
    config = load_config(args.config)
    config['CANADIAN_TRACKS'] = set(config.get('CANADIAN_TRACKS', []))

    if args.persistent:
        run_persistent_engine(config, args)
        return

    input_dir = Path(args.input or config["INPUT_DIR"])
    output_dir = Path(args.output or config["DEFAULT_OUTPUT_DIR"])
    log_path = args.log or config.get("LOG_FILE", "parser.log")
    template_path = args.template or config.get("TEMPLATE_FILE", "template.html")
    setup_logging(log_path)
    input_dir.mkdir(exist_ok=True); output_dir.mkdir(exist_ok=True)

    if not any(input_dir.iterdir()):
        logging.info(f"Input directory '{input_dir}' is empty. Generating and launching data collection helper...")
        create_and_launch_link_helper(config, output_dir)
        sys.exit(0)
    
    start_time = time.time()
    logging.info(f"Starting {config['APP_NAME']} v{config['SCHEMA_VERSION']}")
    
    files_to_process = [f for f in input_dir.iterdir() if f.is_file()]
    master_races, master_stats = [], ParseStatistics()

    with concurrent.futures.ProcessPoolExecutor() as executor:
        future_to_file = {executor.submit(parse_file_worker, fp, config): fp for fp in files_to_process}
        for future in tqdm(concurrent.futures.as_completed(future_to_file), total=len(files_to_process), desc="Parsing Files", unit="file"):
            try:
                races, stats = future.result()
                master_races.extend(races); master_stats.total_files_processed += stats.total_files_processed
                master_stats.total_files_failed += stats.total_files_failed; master_stats.individual_race_errors += stats.individual_race_errors
                master_stats.source_errors.extend(stats.source_errors)
                for source, count in stats.per_source_counts.items(): master_stats.per_source_counts[source] = master_stats.per_source_counts.get(source, 0) + count
            except Exception as e: logging.error(f"A worker process failed unexpectedly: {e}", exc_info=True)

    master_stats.total_races_found = len(master_races)
    logging.info("Deduplicating races and identifying favorites...")
    enriched_races = deduplicate_and_enrich_races(master_races)
    master_stats.races_after_dedup = len(enriched_races)
    
    if not args.no_odds_mode:
        logging.info("Calculating value scores for all races...")
        scorer = EnhancedValueScorer(config)
        for race in enriched_races:
            race.value_score = scorer.calculate_score(race)

    filtered_races = enriched_races
    if args.min_score is not None and not args.no_odds_mode: filtered_races = [r for r in filtered_races if r.value_score >= args.min_score]
    if args.min_field_size is not None: filtered_races = [r for r in filtered_races if r.field_size >= args.min_field_size]
    if args.max_field_size is not None: filtered_races = [r for r in filtered_races if r.field_size <= args.max_field_size]
    if args.exclude_race_types:
        excluded_types = {t.strip().lower() for t in args.exclude_race_types.split(',')}
        filtered_races = [r for r in filtered_races if r.race_type.lower() not in excluded_types]

    sort_key = 'time' if args.no_odds_mode else args.sort_by
    reverse_sort = True if sort_key == 'score' else False
    if sort_key == 'time': final_races = sorted(filtered_races, key=lambda r: r.utc_datetime if r.utc_datetime else datetime.min.replace(tzinfo=ZoneInfo("UTC")), reverse=reverse_sort)
    elif sort_key == 'field_size': final_races = sorted(filtered_races, key=lambda r: r.field_size, reverse=reverse_sort)
    elif sort_key == 'course': final_races = sorted(filtered_races, key=lambda r: r.course, reverse=reverse_sort)
    else: final_races = sorted(filtered_races, key=lambda r: r.value_score, reverse=True)

    if args.limit is not None: final_races = final_races[:args.limit]
    
    master_stats.duration_seconds = time.time() - start_time
    logging.info(f"Analysis Complete. Found {len(final_races)} matching races in {master_stats.duration_seconds:.2f}s.")
    
    if final_races:
        output_manager = OutputManager(config, output_dir, template_path)
        formats = config["OUTPUT"]["OUTPUT_FORMATS"]
        now_utc = datetime.now(ZoneInfo("UTC"))
        filters_config = config.get("FILTERS", {})
        report_data = {
            'all_races': final_races,
            'value_races': [r for r in final_races if r.value_score >= 70],
            'next_to_jump': sorted([r for r in final_races if r.utc_datetime and r.utc_datetime > now_utc], key=lambda x: x.utc_datetime),
            'perfect_tips': [r for r in final_races if r.field_size < 7 and r.favorite and r.favorite.odds_decimal >= 1.0 and r.second_favorite and r.second_favorite.odds_decimal >= 3.0],
            'filtered_races': [r for r in final_races if filters_config.get("MIN_FIELD_SIZE", 4) <= r.field_size <= filters_config.get("MAX_FIELD_SIZE", 12)]
        }
        if "html" in formats: output_manager.write_html_report(report_data, master_stats)
        if "json" in formats: output_manager.write_json_report(final_races, master_stats)
        if "csv" in formats:
            output_manager.write_csv_report(final_races)
            output_manager.write_error_report(master_stats)
        
    logging.info("Paddock Parser has finished its work.")

if __name__ == "__main__":
    main()
