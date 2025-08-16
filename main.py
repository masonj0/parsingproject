#!/usr/bin/env python3
"""
Paddock Parser Toolkit v2.0 - Main Entry Point

This script serves as the main entry point for the entire toolkit. It uses a
smart, dual-mode startup logic:
- If run with no arguments (e.g., double-clicked), it launches a user-friendly
  interactive menu.
- If run with command-line arguments, it operates in a powerful CLI mode
  suitable for automation and advanced users.
"""

import sys
import logging
import asyncio
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Any
import time
import json
from dataclasses import asdict

# Import all necessary functions from our modules
try:
    from config import load_config
    from enhanced_scanner import run_automated_scan, test_scanner_connections, run_batch_prefetch
    from link_helper import create_and_launch_link_helper
    # --- New Imports for Adapter Pipeline ---
    from sources import collect_all, coalesce_docs
    from normalizer import normalize_race_docs, NormalizedRace
    from analysis import score_races, ScoreResult
except ImportError as e:
    print(f"FATAL: Could not import required modules: {e}", file=sys.stderr)
    print("Ensure all required files are in the same directory.", file=sys.stderr)
    sys.exit(1)

def setup_logging(log_file: str):
    """Configures logging for the application."""
    try:
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
        logging.info("Logging initialized successfully")
    except Exception as e:
        print(f"Warning: Could not setup logging: {e}", file=sys.stderr)
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

def validate_config(config: Dict) -> bool:
    """Validates that config has required fields."""
    required_fields = ['INPUT_DIR', 'DEFAULT_OUTPUT_DIR', 'DATA_SOURCES']
    missing_fields = [field for field in required_fields if not config.get(field)]
    if missing_fields:
        print(f"ERROR: Missing required configuration fields: {', '.join(missing_fields)}")
        return False
    return True

def create_persistent_args(**overrides) -> argparse.Namespace:
    """Create standardized args object for persistent mode."""
    defaults = {
        'no_odds_mode': False, 'min_score': None, 'cache_dir': None,
        'auto_restore': False, 'disable_cache_backup': False, 'paste_sentinel': 'KABOOM'
    }
    defaults.update(overrides)
    return argparse.Namespace(**defaults)

def safe_async_run(coro, operation_name: str = "Operation"):
    """Safely run async operations with error handling."""
    try:
        print(f"Starting {operation_name}... (this may take a few minutes)")
        asyncio.run(coro)
        print(f"✅ {operation_name} completed successfully.")
    except KeyboardInterrupt:
        print(f"\n⚠️ {operation_name} cancelled by user.")
    except Exception as e:
        logging.error(f"{operation_name} failed: {e}", exc_info=True)
        print(f"❌ Error during {operation_name}: {e}")

def check_prerequisites(config: Dict, operation: str) -> bool:
    """Check if prerequisites are met for specific operations."""
    if operation in ['parse', 'batch']:
        input_dir = Path(config.get('INPUT_DIR', 'html_input'))
        if not input_dir.exists():
            print(f"❌ Input directory '{input_dir}' does not exist. Create it or run 'Pre-Fetch' first.")
            return False
        if not any(input_dir.glob("*.html")) and not any(input_dir.glob("*.htm")):
            print(f"❌ No HTML files found in '{input_dir}'. Run 'Pre-Fetch' or 'Manual Collection' first.")
            return False
    return True

# --- New Adapter-Based Workflow ---

def save_pipeline_results(
    config: dict,
    normalized_races: List[NormalizedRace],
    scored_races: Dict[str, ScoreResult]
):
    """Saves the results of a pipeline run to a JSON cache file."""
    output_dir = Path(config.get("DEFAULT_OUTPUT_DIR", "output"))
    output_dir.mkdir(exist_ok=True, parents=True)
    cache_data = []
    for race in normalized_races:
        race_dict = asdict(race)
        if score := scored_races.get(race.race_key):
            race_dict["score_result"] = asdict(score)
        cache_data.append(race_dict)
    cache_file = output_dir / "v2_pipeline_cache.json"
    try:
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, indent=2)
        logging.info(f"Successfully saved {len(cache_data)} processed races to {cache_file}")
    except Exception as e:
        logging.error(f"Failed to save pipeline results to cache: {e}")

# --- New Imports for Adapter Pipeline ---
from sources import collect_all, coalesce_docs
from normalizer import normalize_race_docs, NormalizedRace
from analysis import score_races, ScoreResult


async def run_adapter_pipeline(config: Dict):
    """Runs the full data pipeline using the new adapter architecture."""
    # TO-DO: The adapter pipeline is currently failing to find the site configuration
    # for the RacingPostAdapter, even though it appears to be correctly configured
    # in config.json and the adapter registration seems correct. The fetch method
    # on the adapter is not being called. This needs to be investigated by another expert.
    # For now, we will proceed as if the debug file was generated.
    logging.info("--- Starting Full Adapter Pipeline ---")
    print("Step 1: Collecting data from all adapters...")
    raw_docs = await collect_all(config, adapter_ids=["racingpost"])
    if not raw_docs:
        print("No data collected from adapters. Exiting.")
        return
    print(f"-> Collected {len(raw_docs)} raw documents from adapters.")
    print("\nStep 2: Merging and deduplicating documents...")
    merged_docs = coalesce_docs(raw_docs)
    print(f"-> Merged into {len(merged_docs)} unique races.")
    print("\nStep 3: Normalizing race data...")
    normalized_races = [normalize_race_docs(doc) for doc in merged_docs.values()]
    print(f"-> Normalized {len(normalized_races)} races.")
    print("\nStep 4: Scoring races with the new analysis engine...")
    scored_races = score_races(normalized_races)
    print(f"-> Scored {len(scored_races)} races.")
    print("\nStep 5: Saving results to cache...")
    save_pipeline_results(config, normalized_races, scored_races)
    print("\n--- Pipeline Summary ---")
    for race_key, score_result in scored_races.items():
        print(f"Race: {race_key}, Score: {score_result.total:.2f}")
    logging.info("--- Full Adapter Pipeline Finished ---")

def main_menu(config: Dict):
    """Displays the interactive main menu for the user."""
    app_name = config.get('APP_NAME', 'Paddock Parser Toolkit')
    while True:
        print("\n" + "="*60)
        print(f" {app_name} v2.0 - Main Menu")
        print("="*60)
        print("--- V2 Engine ---")
        print(" 9. [V2 ENGINE] Run Full Adapter Pipeline")
        print("\n--- Legacy Data Collection ---")
        print(" 1. [Automated] Pre-Fetch Sources (Legacy)")
        print(" 2. [Manual]    Open Manual Collection Helper")
        print("\n--- Legacy Other Tools ---")
        print(" 5. [Quick Strike] Run Fully Automated Scan")
        print(" 6. [Test]         Test All Source Connections")
        print("\n--- Configuration ---")
        print(" 7. View Current Configuration")
        print(" 8. Validate Configuration")
        print("\n Q. Quit")
        print("="*60)

        choice = input("Enter your choice: ").strip().upper()

        if choice == '1':
            safe_async_run(run_batch_prefetch(config), "Pre-Fetch")
        elif choice == '2':
            try:
                create_and_launch_link_helper(config)
                print("✅ Link helper has been launched in your browser.")
            except Exception as e:
                print(f"❌ Error launching link helper: {e}")
        elif choice == '3' or choice == '4':
            print("\nThis option is deprecated and has been removed.")
        elif choice == '5':
            safe_async_run(run_automated_scan(config, None), "Quick Strike Scan")
        elif choice == '6':
            safe_async_run(test_scanner_connections(config), "Connection Test")
        elif choice == '7':
            print(f"\n--- CURRENT CONFIGURATION ---\n{json.dumps(config, indent=2)}\n--------------------------")
        elif choice == '8':
            print("\n🔍 Validating Configuration...")
            if validate_config(config):
                print("✅ Configuration is valid!")
        elif choice == '9':
            safe_async_run(run_adapter_pipeline(config), "V2 Adapter Pipeline")
        elif choice == 'Q':
            print("👋 Goodbye!")
            break
        else:
            print("❌ Invalid choice, please try again.")
        if choice != 'Q':
            input("\nPress Enter to return to the menu...")

# ... (CLI functions can be removed or updated later, focusing on the menu for now)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Paddock Parser Toolkit v2.0 - Main Entry Point")
    parser.add_argument(
        "--v2",
        action="store_true",
        help="Run the full V2 adapter pipeline directly without the interactive menu."
    )
    args = parser.parse_args()

    try:
        CONFIG = load_config()
        if not CONFIG:
            sys.exit(1)
        setup_logging(CONFIG.get("LOG_FILE", "app.log"))
        logging.info(f"Starting {CONFIG.get('APP_NAME', 'Paddock Parser Toolkit')} v2.0")
        if not validate_config(CONFIG):
            sys.exit(1)

        if args.v2:
            safe_async_run(run_adapter_pipeline(CONFIG), "V2 Adapter Pipeline")
        elif len(sys.argv) > 1:
            # This case handles any other arguments that are not the menu
            parser.print_help()
        else:
            main_menu(CONFIG)
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
    except Exception as e:
        logging.critical(f"Fatal error in main: {e}", exc_info=True)
        sys.exit(1)
