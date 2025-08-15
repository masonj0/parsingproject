#!/usr/bin/env python3
"""
Paddock Parser v1.2 (The Persistent Engine)

---
NOTE TO SELF (The Constitution - Preamble):
This is our "Deep Dive" tool. Its philosophy is absolute reliability and
analytical depth, achieved through a manual-first workflow. It is designed to
be an "always-on" analytical partner that builds a progressively richer and
more complete dataset throughout the day. It is the definitive source of
truth, immune to the flakiness of automated scraping. All architectural
decisions serve this core mission of reliability and intelligence.
---
"""

import json
import logging
import sys
import time
import argparse
from dataclasses import dataclass, field, asdict, fields
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Any
from tqdm import tqdm

# Import the advanced parser provided by the team
try:
    from racing_data_parser import RacingDataParser
except ImportError:
    print("FATAL: Could not import racing_data_parser.py. Ensure it's in the same directory.", file=sys.stderr)
    sys.exit(1)

# Shared Intelligence: Ensure all normalization is consistent
try:
    from normalizer import normalize_course_name
except ImportError:
    print("FATAL: Could not import normalizer.py. Ensure it's in the same directory.", file=sys.stderr)
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
class RaceData:
    """Represents a single race, enriched with runner data and a value score."""
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

# =============================================================================
# --- ENHANCED VALUE SCORER ---
# =============================================================================
class EnhancedValueScorer:
    """
    Analyzes Field Size, Race Type, and the shape of the
    Odds market to find structurally advantageous betting opportunities.
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

        # Ensure favorites have odds before calculating
        fav_odds = race.favorite.odds_decimal if race.favorite else 999.0
        sec_fav_odds = race.second_favorite.odds_decimal if race.second_favorite else 999.0

        base_score = (self._calculate_field_score(race.field_size) * self.weights["FIELD_SIZE_WEIGHT"] +
                      self._calculate_favorite_odds_score(fav_odds) * self.weights["FAVORITE_ODDS_WEIGHT"] +
                      self._calculate_odds_spread_score(fav_odds, sec_fav_odds) * self.weights["ODDS_SPREAD_WEIGHT"] +
                      self._calculate_data_quality_score(race) * self.weights["DATA_QUALITY_WEIGHT"])

        final_score = min(100.0, base_score)
        return round(final_score, 2)

    def _calculate_field_score(self, size: int) -> float:
        if 3 <= size <= 5: return 100.0
        if 6 <= size <= 8: return 85.0
        if 9 <= size <= 12: return 60.0
        return 20.0

    def _calculate_favorite_odds_score(self, odds: float) -> float:
        if odds == 999.0: return 20.0
        if 1.0 <= odds <= 1.5: return 100.0
        if 1.5 < odds <= 2.5: return 90.0
        if 2.5 < odds <= 4.0: return 75.0
        if 0.5 <= odds < 1.0: return 85.0
        if odds < 0.5: return 60.0
        return 40.0

    def _calculate_odds_spread_score(self, fav_odds: float, sec_odds: float) -> float:
        if fav_odds == 999.0 or sec_odds == 999.0: return 30.0
        spread = sec_odds - fav_odds
        if spread >= 2.0: return 100.0
        if spread >= 1.5: return 90.0
        if spread >= 1.0: return 80.0
        if spread >= 0.5: return 60.0
        return 40.0

    def _calculate_data_quality_score(self, race: RaceData) -> float:
        score = 0.0
        if race.runners and any(r.odds_str not in ["SP", "", "NR", "SCR"] for r in race.runners): score += 50.0
        if race.favorite and race.second_favorite: score += 30.0
        if race.race_url: score += 20.0
        return min(100.0, score)

# =============================================================================
# --- DATA MERGING & CACHE LOGIC ---
# =============================================================================
def smart_merge_race_data(existing_race: RaceData, new_race: RaceData) -> RaceData:
    """
    Intelligently merges new race data into existing data, prioritizing
    the most complete information and updating odds.
    """
    # Merge runners: Update odds if new odds are not 'SP' and old ones are.
    merged_runners = {runner.name: runner for runner in existing_race.runners}
    for new_runner in new_race.runners:
        if new_runner.name in merged_runners:
            existing_runner = merged_runners[new_runner.name]
            if new_runner.odds_str not in ["SP", "NR", "SCR", ""] and existing_runner.odds_str in ["SP", "NR", "SCR", ""]:
                merged_runners[new_runner.name] = new_runner
        else:
            merged_runners[new_runner.name] = new_runner
    existing_race.runners = list(merged_runners.values())

    # Update other fields if new data provides a value where old one was missing
    if not existing_race.race_url and new_race.race_url: existing_race.race_url = new_race.race_url
    if not existing_race.country or existing_race.country == "Unknown": existing_race.country = new_race.country
    if not existing_race.discipline or existing_race.discipline == "Unknown": existing_race.discipline = new_race.discipline
    if not existing_race.race_type or existing_race.race_type == "Unknown Type": existing_race.race_type = new_race.race_type
    
    # Combine data sources
    existing_race.data_sources = sorted(list(set(existing_race.data_sources + new_race.data_sources)))
    return existing_race

# =============================================================================
# --- PERSISTENT ENGINE ---
# =============================================================================
def run_persistent_engine(config: Dict, args: argparse.Namespace):
    """
    Runs the main, always-on loop for the Paddock Parser.
    It listens for clipboard data and merges it into a daily cache.
    """
    logging.info("Starting Persistent Deep Dive Engine...")
    scorer = EnhancedValueScorer(config)
    parser = RacingDataParser() # Use the new advanced parser

    # Cache file path setup
    cache_dir = Path(args.cache_dir) if args.cache_dir else Path(config["DEFAULT_OUTPUT_DIR"])
    cache_dir.mkdir(parents=True, exist_ok=True)
    today_str = date.today().strftime("%Y-%m-%d")
    cache_file = cache_dir / f"paddock_cache_{today_str}.json"

    races_by_id: Dict[str, RaceData] = {}
    if cache_file.exists() and not args.disable_cache_backup:
        restore = args.auto_restore or input(f"Cache file found for today. Restore? (Y/n): ").strip().lower() in ['y', '']
        if restore:
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    cached_data = json.load(f)
                    for race_dict in cached_data:
                        # Re-instantiate dataclasses from the loaded dict
                        runners = [Runner(**r) for r in race_dict.get('runners', [])]
                        race_dict['runners'] = runners
                        race_dict['favorite'] = Runner(**race_dict['favorite']) if race_dict.get('favorite') else None
                        race_dict['second_favorite'] = Runner(**race_dict['second_favorite']) if race_dict.get('second_favorite') else None
                        races_by_id[race_dict['id']] = RaceData(**race_dict)
                logging.info(f"Loaded {len(races_by_id)} races from cache: {cache_file}")
            except (json.JSONDecodeError, TypeError) as e:
                logging.warning(f"Cache file '{cache_file}' is corrupted or has an old format. Starting fresh. Error: {e}")

    logging.info(f"Engine is running. Paste data blocks followed by '{args.paste_sentinel}' on a new line.")
    logging.info("Press Ctrl+C to save and exit.")

    try:
        while True:
            print("\n" + "="*50)
            print(f" PASTE content, then type '{args.paste_sentinel}' and press Enter.")
            print("="*50)

            lines = []
            while True:
                line = sys.stdin.readline()
                if line.strip() == args.paste_sentinel:
                    break
                lines.append(line)
            pasted_content = "".join(lines)

            if not pasted_content.strip():
                logging.warning("No content detected. Waiting for next paste.")
                continue

            # Use the advanced parser to get a list of dictionaries
            parsed_races_dicts = parser.parse_racing_data(pasted_content, "Clipboard Paste")
            if not parsed_races_dicts:
                logging.warning("No races were parsed from the pasted content.")
                continue

            update_count = 0
            new_count = 0
            for race_dict in parsed_races_dicts:
                # Convert dicts to dataclasses for consistency and type safety
                runners = [Runner(**r) for r in race_dict.get('runners', [])]
                race_dict['runners'] = runners
                race_dict['favorite'] = Runner(**race_dict['favorite']) if race_dict.get('favorite') else None
                race_dict['second_favorite'] = Runner(**race_dict['second_favorite']) if race_dict.get('second_favorite') else None
                
                # Filter dict to only include keys that are fields in RaceData
                valid_keys = {f.name for f in fields(RaceData)}
                filtered_dict = {k: v for k, v in race_dict.items() if k in valid_keys}
                new_race = RaceData(**filtered_dict)

                if new_race.id in races_by_id:
                    existing_race = races_by_id[new_race.id]
                    races_by_id[new_race.id] = smart_merge_race_data(existing_race, new_race)
                    update_count += 1
                else:
                    races_by_id[new_race.id] = new_race
                    new_count += 1
            
            logging.info(f"Processed paste. Added {new_count} new races, updated {update_count} existing races.")
            
            # Rescore all races after a merge/update
            races_list = list(races_by_id.values())
            for race in races_list:
                race.value_score = scorer.calculate_score(race)

            # Save the updated cache
            if not args.disable_cache_backup:
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump([asdict(race) for race in races_list], f, indent=2, default=str)
                logging.info(f"Cache updated and saved to {cache_file}.")

    except KeyboardInterrupt:
        logging.info("\nCtrl+C detected. Saving final cache and exiting.")
        races_list = list(races_by_id.values())
        if not args.disable_cache_backup and races_list:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump([asdict(race) for race in races_list], f, indent=2, default=str)
            logging.info(f"Final cache of {len(races_list)} races saved to {cache_file}.")
        sys.exit(0)
    except Exception as e:
        logging.critical(f"A critical error occurred in the persistent engine: {e}", exc_info=True)
        sys.exit(1)

# =============================================================================
# --- BATCH PARSE MODE ---
# =============================================================================
# =============================================================================
# - BATCH PARSE MODE -
# =============================================================================
def run_batch_parse(config: Dict, args: Optional[argparse.Namespace]): # Allow Optional args
    """Processes all HTML files in the input directory."""
    # Handle case where args is None (e.g., called from interactive menu)
    input_dir_path = None
    if args is not None:
        # Check if args has the 'input_dir' attribute and it's set
        input_dir_path = getattr(args, 'input_dir', None)

    # If input_dir wasn't provided via args, use the default from config
    if input_dir_path is None:
        input_dir_path = config.get("INPUT_DIR", "html_input") # Fallback to "html_input" if not in config

    input_path = Path(input_dir_path)
    if not input_path.exists() or not input_path.is_dir():
        logging.error(f"Input directory '{input_path}' does not exist or is not a directory.")
        print(f"Error: Input directory '{input_path}' not found or is invalid.")
        return

    logging.info("Starting batch parse mode...")
    logging.info(f"Parsing files from directory: {input_path}")

    parser = RacingDataParser() # Use the new advanced parser
    scorer = EnhancedValueScorer(config)
    races_by_id: Dict[str, RaceData] = {}

    # Broaden file discovery to include both .html and .htm files
    html_files = list(input_path.glob("*.html")) + list(input_path.glob("*.htm"))
    if not html_files:
        logging.warning(f"No .html or .htm files found in '{input_path}'.")
        print(f"Warning: No HTML files (.html or .htm) found in '{input_path}'.")
        return

    # Use tqdm for a progress bar
    for file_path in tqdm(html_files, desc="Parsing Files"):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
            logging.info(f"Parsing file: {file_path.name}")
            # --- Use the new advanced parser ---
            races_batch = parser.parse_racing_data(html_content, source_file=file_path.name)
            # --- End parsing ---
            if races_batch:
                smart_merge_race_data(races_by_id, races_batch)
            else:
                logging.info(f"No races found in {file_path.name}")
        except Exception as e:
            logging.error(f"Error processing file {file_path.name}: {e}")

    if races_by_id:
        sorted_races = sorted(races_by_id.values(), key=lambda r: r.value_score, reverse=True)
        # Score the merged races using the shared intelligence module
        for race in sorted_races:
             scorer.calculate_value_score(race) # Recalculate score after merge

        # Re-sort after scoring if needed, or sort once after scoring
        # As scoring is done in calculate_value_score which modifies the object,
        # and we sorted before, we might not need to re-sort unless scoring changes order significantly
        # But it's safer to sort again after final scoring
        sorted_races.sort(key=lambda r: r.value_score, reverse=True)

        # Save the final output
        output_dir = Path(config["DEFAULT_OUTPUT_DIR"])
        output_dir.mkdir(parents=True, exist_ok=True)
        today_str = date.today().strftime("%Y-%m-%d")
        output_file = output_dir / f"paddock_report_{today_str}.json"
        try:
            # Convert dataclass instances to dictionaries for JSON serialization
            races_as_dicts = [asdict(race) for race in sorted_races]
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(races_as_dicts, f, indent=4)
            logging.info(f"✅ Final report saved to {output_file}")
            print(f"✅ Success! Final report saved to {output_file}")
            # --- Generate HTML Report ---
            try:
                template_dir = Path('.') # Assuming template is in the current directory
                env = Environment(
                    loader=FileSystemLoader(template_dir),
                    autoescape=select_autoescape(['html', 'xml'])
                )
                template = env.get_template(config["TEMPLATE_PADDOCK"])
                # Pass the sorted list of race dictionaries and the config
                html_output = template.render(races=sorted_races, config=config)
                html_output_file = output_dir / f"paddock_report_{today_str}.html"
                with open(html_output_file, 'w', encoding='utf-8') as f:
                    f.write(html_output)
                logging.info(f"✅ HTML report saved to {html_output_file}")
                print(f"✅ Success! HTML report saved to {html_output_file}")
            except Exception as e:
                logging.error(f"❌ Failed to generate HTML report: {e}")
                print(f"Warning: Could not generate HTML report: {e}")
            # --- End Generate HTML Report ---
        except Exception as e:
            logging.error(f"❌ Failed to save final report: {e}")
            print(f"Error: Failed to save report: {e}")
    else:
        logging.info("⚠️ No races were found or parsed successfully.")
        print("⚠️ No races were found or parsed successfully.")

    logging.info("-" * 50)
    logging.info("Batch parsing complete.")
    logging.info("-" * 50)

# This is a placeholder for the main script's CLI logic.
# The actual execution is handled by main.py
if __name__ == "__main__":
    print("This script is intended to be run via main.py")
    print("Example: python main.py persistent")
    print("Example: python main.py parse -i /path/to/files")
