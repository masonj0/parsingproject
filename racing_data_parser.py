#!/usr/bin/env python3
"""
Paddock Parser Toolkit - Racing Data Parser (v1.5)

This module contains the core parsing intelligence for the toolkit. It has been
upgraded with "Surgical Parsing" capabilities. It now acts as a smart dispatcher,
detecting specific, high-value site formats and using dedicated parsers for them,
while falling back to generic logic for unknown sources.

Surgical Parsers Implemented:
- Timeform (Meeting List)
- Racing Post (Meeting List)
- Equibase (Entries List from JS variable)
"""

import re
import json
import logging
import hashlib
from datetime import date
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup, Tag

# Shared Intelligence
from normalizer import normalize_course_name, parse_hhmm_any, convert_odds_to_fractional_decimal, map_discipline

class RacingDataParser:
    """
    Comprehensive hybrid parser for racing data from multiple sources and formats.
    Handles JSON feeds, specific HTML formats, and generic HTML.
    """
    
    def __init__(self):
        self.setup_logging()
    
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - [%(levelname)s] - %(message)s'
        )
    
    def _generate_race_id(self, course: str, race_date: date, time: str) -> str:
        """Creates a unique, deterministic ID for a race."""
        key = f"{normalize_course_name(course)}|{race_date.isoformat()}|{re.sub(r'[^\d]', '', time or '')}"
        return hashlib.sha1(key.encode()).hexdigest()[:12]

    def parse_racing_data(self, content: str, source_file: str) -> List[Dict[str, Any]]:
        """
        Universal entry point for parsing racing data from any format.
        Auto-detects format and applies the appropriate parser.
        """
        races_data = []
        logging.info(f"Starting parsing for source: {source_file}")
        
        # This function will be expanded to handle JSON, but for now, focuses on HTML.
        
        logging.info("Attempting to parse as HTML content.")
        races_data = self.parse_html_race_cards(content, source_file)
        
        logging.info(f"Parsing complete. Found {len(races_data)} races in {source_file}.")
        return races_data

    def parse_html_race_cards(self, html_content: str, source_file: str) -> List[Dict[str, Any]]:
        """
        Smart dispatcher for HTML content. Detects the source and uses the
        appropriate surgical parser, with a generic fallback.
        """
        soup = BeautifulSoup(html_content, 'html.parser')

        # --- Surgical Parser Dispatch ---
        if "timeform.com" in html_content or soup.select_one(".w-racecard-grid-meeting"):
            logging.info("Detected Timeform format. Using surgical parser.")
            return self._parse_timeform_page(soup, source_file)
        
        if "racingpost.com" in html_content or soup.select_one(".RC-meetingList"):
            logging.info("Detected Racing Post format. Using surgical parser.")
            return self._parse_racing_post_page(soup, source_file)

        if "equibase.com" in html_content or soup.select_one("#entries-index"):
            logging.info("Detected Equibase format. Using surgical parser.")
            return self._parse_equibase_page(soup, source_file)

        # --- Fallback to Generic Parser ---
        logging.info("Source not recognized. Using generic fallback parser.")
        return self._parse_generic_html(soup, source_file)

    # =========================================================================
    # --- SURGICAL PARSERS ---
    # =========================================================================

    def _parse_timeform_page(self, soup: BeautifulSoup, source_file: str) -> List[Dict[str, Any]]:
        """
        Surgical parser for Timeform racecards list page.
        Extracts each meeting and its associated races.
        """
        races = []
        
        # Find each meeting block on the page
        meeting_containers = soup.select(".w-racecard-grid-meeting")
        
        for meeting in meeting_containers:
            try:
                header = meeting.select_one(".w-racecard-grid-meeting-header")
                course_name_element = header.select_one("h2")
                if not course_name_element:
                    continue
                
                course_name = course_name_element.get_text(strip=True)
                
                # Extract races for this meeting
                race_links = meeting.select(".w-racecard-grid-meeting-races-compact li a")
                for race_link in race_links:
                    race_time_element = race_link.select_one("b")
                    if not race_time_element:
                        continue
                        
                    race_time = race_time_element.get_text(strip=True)
                    race_id = self._generate_race_id(course_name, date.today(), race_time)
                    
                    # Timeform provides discipline in the time span
                    discipline_text = race_time_element.parent.get_text(strip=True)
                    discipline = "thoroughbred" # Default
                    if "chase" in discipline_text.lower() or "hurdle" in discipline_text.lower():
                        discipline = "jump"
                    
                    race_data = {
                        'id': race_id,
                        'course': normalize_course_name(course_name),
                        'race_time': parse_hhmm_any(race_time),
                        'race_type': race_link.get('title', 'Unknown Type'),
                        'utc_datetime': None,
                        'local_time': parse_hhmm_any(race_time),
                        'timezone_name': "Europe/London", # Default for Timeform
                        'field_size': 0, # Not available on the list page
                        'country': "GB/IRE" if "(IRE)" not in course_name else "IRE",
                        'discipline': discipline,
                        'source_file': source_file,
                        'race_url': f"https://www.timeform.com{race_link['href']}",
                        'runners': [],
                        'favorite': None,
                        'second_favorite': None,
                        'value_score': 0.0,
                        'data_sources': [source_file]
                    }
                    races.append(race_data)
            except Exception as e:
                logging.error(f"Error parsing a Timeform meeting container: {e}")
                continue
                
        return races

    def _parse_racing_post_page(self, soup: BeautifulSoup, source_file: str) -> List[Dict[str, Any]]:
        """
        Surgical parser for Racing Post race card pages.
        Extracts detailed information from each race on the page.
        """
        races = []
        
        # Each meeting is an accordion row
        accordion_rows = soup.select(".RC-accordion__row")
        
        for row in accordion_rows:
            try:
                course_element = row.select_one(".RC-accordion__courseName")
                if not course_element:
                    continue
                
                course_name = course_element.get_text(strip=True)

                race_items = row.select(".RC-meetingItem")
                for item in race_items:
                    time_element = item.select_one(".RC-meetingItem__timeLabel")
                    race_time = time_element.get_text(strip=True) if time_element else "N/A"

                    info_element = item.select_one(".RC-meetingItem__info")
                    race_title = info_element.get_text(strip=True) if info_element else "Unknown Race"

                    runners_element = item.select_one(".RC-meetingItem__numberOfRunners")
                    runners_text = runners_element.get_text(strip=True) if runners_element else "0 runners"
                    field_size_match = re.search(r'(\d+)', runners_text)
                    field_size = int(field_size_match.group(1)) if field_size_match else 0
                    
                    race_link = item.select_one("a.RC-meetingItem__link")
                    race_url = f"https://www.racingpost.com{race_link['href']}" if race_link else ""
                    
                    race_id = self._generate_race_id(course_name, date.today(), race_time)
                    
                    race_data = {
                        'id': race_id,
                        'course': normalize_course_name(course_name),
                        'race_time': parse_hhmm_any(race_time),
                        'race_type': race_title,
                        'utc_datetime': None,
                        'local_time': parse_hhmm_any(race_time),
                        'timezone_name': "Europe/London",
                        'field_size': field_size,
                        'country': "GB/IRE",
                        'discipline': "thoroughbred", # Assume for now, needs refinement
                        'source_file': source_file,
                        'race_url': race_url,
                        'runners': [],
                        'favorite': None,
                        'second_favorite': None,
                        'value_score': 0.0,
                        'data_sources': [source_file]
                    }
                    races.append(race_data)
            except Exception as e:
                logging.error(f"Error parsing a Racing Post meeting container: {e}")
                continue
        return races

    def _parse_equibase_page(self, soup: BeautifulSoup, source_file: str) -> List[Dict[str, Any]]:
        """
        Surgical parser for Equibase entry list pages. It now extracts data
        from the embedded JavaScript variable for higher accuracy.
        """
        races = []
        scripts = soup.find_all("script")
        
        # Find the script tag containing the 'allTracks' JS variable
        for script in scripts:
            if script.string and "var allTracks =" in script.string:
                js_content = script.string
                # Extract the JSON part of the variable declaration
                json_str_match = re.search(r'var allTracks = (\{.*?\});', js_content, re.DOTALL)
                if json_str_match:
                    json_str = json_str_match.group(1)
                    try:
                        track_data = json.loads(json_str)
                        # The data is nested by date
                        for date_key in track_data:
                            for meeting in track_data[date_key]:
                                for i in range(1, 17): # Equibase data has up to 16 races
                                    race_key = f"race-{i}"
                                    if race_key in meeting["DATAELEMENTS"]:
                                        # This is a basic extraction. A full implementation
                                        # would parse the complex data string.
                                        race_data = {
                                            'id': self._generate_race_id(meeting["TRACKNAME"], date.today(), f"Race {i}"),
                                            'course': normalize_course_name(meeting["TRACKNAME"]),
                                            'race_time': f"Race {i}",
                                            'race_type': "Unknown Type",
                                            'utc_datetime': None,
                                            'local_time': f"Race {i}",
                                            'timezone_name': "America/New_York",
                                            'field_size': 0,
                                            'country': meeting.get("COUNTRY", "USA"),
                                            'discipline': "thoroughbred",
                                            'source_file': source_file,
                                            'race_url': f"https://www.equibase.com{meeting['URL']}",
                                            'runners': [], 'favorite': None, 'second_favorite': None,
                                            'value_score': 0.0, 'data_sources': [source_file]
                                        }
                                        races.append(race_data)
                        return races # Exit after processing the correct script
                    except json.JSONDecodeError:
                        logging.error("Failed to parse JSON from Equibase script tag.")
        
        logging.warning("Could not find 'allTracks' variable. Falling back to table parsing for Equibase.")
        return self._parse_equibase_table_fallback(soup, source_file)

    # =========================================================================
    # --- FALLBACK PARSERS ---
    # =========================================================================

    def _parse_equibase_table_fallback(self, soup: BeautifulSoup, source_file: str) -> List[Dict[str, Any]]:
        """
        Fallback parser for Equibase that reads the visible HTML tables if the
        JavaScript variable cannot be found. Less reliable.
        """
        races = []
        entry_tables = soup.select('table.entries-table')
        
        for table in entry_tables:
            try:
                header = table.find_previous('h2')
                if not header: continue
                
                course_name_raw = header.get_text(strip=True)
                course_name = re.sub(r'-.*', '', course_name_raw).strip()

                race_rows = table.select('tbody tr')
                for row in race_rows:
                    columns = row.select('td')
                    if len(columns) < 4: continue

                    race_time_element = columns[0].find('span', class_='post-time')
                    race_time = race_time_element.get_text(strip=True) if race_time_element else "N/A"
                    
                    race_details = columns[2].get_text(strip=True)
                    field_size = int(columns[3].get_text(strip=True)) if columns[3].get_text(strip=True).isdigit() else 0

                    race_id = self._generate_race_id(course_name, date.today(), race_time)

                    race_data = {
                        'id': race_id,
                        'course': normalize_course_name(course_name),
                        'race_time': parse_hhmm_any(race_time),
                        'race_type': race_details,
                        'utc_datetime': None,
                        'local_time': parse_hhmm_any(race_time),
                        'timezone_name': "America/New_York",
                        'field_size': field_size,
                        'country': "USA",
                        'discipline': "thoroughbred",
                        'source_file': source_file,
                        'race_url': "", 'runners': [], 'favorite': None, 'second_favorite': None,
                        'value_score': 0.0, 'data_sources': [source_file]
                    }
                    races.append(race_data)
            except Exception as e:
                logging.error(f"Error parsing an Equibase fallback table: {e}")
                continue
        return races

    def _parse_generic_html(self, soup: BeautifulSoup, source_file: str) -> List[Dict[str, Any]]:
        """
        A generic, best-effort parser for unknown HTML structures.
        It looks for common patterns and class names.
        """
        races_data = []
        
        # A broad search for anything that looks like a race card
        race_containers = soup.select(
            '[class*="race-card"], [class*="racecard"], [class*="race-item"], article.race, section.meeting'
        )
        
        logging.info(f"Generic parser found {len(race_containers)} potential race containers.")
        
        for container in race_containers:
            try:
                course_element = container.select_one('[class*="course"], [class*="track"], [class*="meeting"], h1, h2, h3')
                time_element = container.select_one('[class*="time"], [class*="race-time"]')
                
                if not course_element or not time_element:
                    continue

                course_name = course_element.get_text(strip=True)
                race_time = time_element.get_text(strip=True)
                race_id = self._generate_race_id(course_name, date.today(), race_time)

                # Attempt to find runners
                runners = []
                runner_elements = container.select('[class*="runner"], [class*="horse"], [class*="entry"], tr') # Modified selector here
                for runner_el in runner_elements:
                    name_el = runner_el.select_one('[class*="horse-name"], [class*="runner-name"], strong, b')
                    odds_el = runner_el.select_one('[class*="odds"], [class*="price"]')
                    
                    if name_el:
                        runner_name = name_el.get_text(strip=True)
                        odds_str = odds_el.get_text(strip=True) if odds_el else "SP"
                        
                        runners.append({
                            'name': runner_name,
                            'odds_str': odds_str,
                            'odds_decimal': convert_odds_to_fractional_decimal(odds_str)
                        })
                
                valid_runners = sorted([r for r in runners if r['odds_decimal'] < 999.0], key=lambda x: x['odds_decimal'])

                race_data = {
                    'id': race_id,
                    'course': normalize_course_name(course_name),
                    'race_time': parse_hhmm_any(race_time),
                    'race_type': "Unknown Type",
                    'utc_datetime': None,
                    'local_time': parse_hhmm_any(race_time),
                    'timezone_name': "UTC",
                    'field_size': len(runners),
                    'country': "Unknown",
                    'discipline': "thoroughbred",
                    'source_file': source_file,
                    'race_url': "",
                    'runners': runners,
                    'favorite': valid_runners[0] if valid_runners else None,
                    'second_favorite': valid_runners[1] if len(valid_runners) > 1 else None,
                    'value_score': 0.0,
                    'data_sources': [source_file]
                }
                races_data.append(race_data)
            except Exception as e:
                logging.warning(f"Generic parser failed on a container: {e}")
                continue
                
        return races_data
