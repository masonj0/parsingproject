#!/usr/bin/env python3
"""
Paddock Parser Toolkit v1.3 - Main Entry Point

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
import argparse  # FIXED: Moved import to top to prevent runtime errors
from pathlib import Path
from typing import Dict, List, Optional, Any
import time

# Import all necessary functions from our modules
try:
    from config import load_config
    # Only import the functions still used by the menu or the new pipeline
    from enhanced_scanner import run_automated_scan, test_scanner_connections, run_batch_prefetch
    # from paddock_parser import run_batch_parse, run_persistent_engine # <-- DECOMMISSIONED
    from link_helper import create_and_launch_link_helper
    # --- New Imports for Adapter Pipeline ---
    from sources import collect_all, coalesce_docs
    from normalizer import normalize_race_docs
    from analysis import score_races
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
        # Fallback to basic console logging
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

def validate_config(config: Dict) -> bool:
    """Validates that config has required fields."""
    required_fields = ['INPUT_DIR', 'DEFAULT_OUTPUT_DIR', 'DATA_SOURCES']
    missing_fields = []

    for field in required_fields:
        if not config.get(field):
            missing_fields.append(field)

    if missing_fields:
        print(f"ERROR: Missing required configuration fields: {', '.join(missing_fields)}")
        return False
    return True

def create_persistent_args(**overrides) -> argparse.Namespace:
    """Create standardized args object for persistent mode."""
    defaults = {
        'no_odds_mode': False,
        'min_score': None,
        'cache_dir': None,
        'auto_restore': False,
        'disable_cache_backup': False,
        'paste_sentinel': 'KABOOM'
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
        logging.error(f"{operation_name} failed: {e}")
        print(f"❌ Error during {operation_name}: {e}")

def check_prerequisites(config: Dict, operation: str) -> bool:
    """Check if prerequisites are met for specific operations."""
    if operation in ['parse', 'batch']:
        input_dir = Path(config.get('INPUT_DIR', 'html_input'))
        if not input_dir.exists():
            print(f"❌ Input directory '{input_dir}' does not exist.")
            print(f"   Create it or run 'Pre-Fetch' first to populate it.")
            return False

        html_files = list(input_dir.glob("*.html")) + list(input_dir.glob("*.htm"))
        if not html_files:
            print(f"❌ No HTML files found in '{input_dir}'.")
            print(f"   Run 'Pre-Fetch' or 'Manual Collection' first.")
            return False
    return True


# --- New Adapter-Based Workflow ---

async def run_adapter_pipeline(config: Dict):
    """
    Runs the full data pipeline using the new adapter architecture.
    Collect -> Coalesce -> Normalize -> Score
    """
    logging.info("--- Starting Full Adapter Pipeline ---")

    # 1. Collect raw data from all registered adapters
    print("Step 1: Collecting data from all adapters...")
    raw_docs = await collect_all(config)
    if not raw_docs:
        print("No data collected from adapters. Exiting.")
        return
    print(f"-> Collected {len(raw_docs)} raw documents from adapters.")

    # 2. Coalesce/merge documents from different sources
    print("\nStep 2: Merging and deduplicating documents...")
    merged_docs = coalesce_docs(raw_docs)
    print(f"-> Merged into {len(merged_docs)} unique races.")

    # 3. Normalize the race documents
    print("\nStep 3: Normalizing race data...")
    normalized_races = [normalize_race_docs(doc) for doc in merged_docs.values()]
    print(f"-> Normalized {len(normalized_races)} races.")

    # 4. Score the normalized races
    print("\nStep 4: Scoring races with the new analysis engine...")
    scored_races = score_races(normalized_races)
    print(f"-> Scored {len(scored_races)} races.")

    # 5. Display a summary of the results
    print("\n--- Pipeline Summary ---")
    for race_key, score_result in scored_races.items():
        print(f"Race: {race_key}, Score: {score_result.total:.2f}")
        # print("  Reasons:", "; ".join(score_result.reasons))

    logging.info("--- Full Adapter Pipeline Finished ---")

def main_menu(config: Dict):
    """Displays the interactive main menu for the user."""
    app_name = config.get('APP_NAME', 'Paddock Parser Toolkit')

    while True:
        print("\n" + "="*60)
        print(f" {app_name} v1.3 - Main Menu")
        print("="*60)
        print("--- Data Collection ---")
        print(" 1. [Automated] Pre-Fetch Accessible Sources")
        print(" 2. [Manual]    Open Manual Collection Helper")
        print()
        print("--- Data Processing (Legacy - Decommissioned) ---")
        print(" 3. [DEPRECATED] Parse All Local Files (Batch Mode)")
        print(" 4. [DEPRECATED] Launch Persistent Engine (Live Paste)")
        print()
        print("--- Other Tools ---")
        print(" 5. [Quick Strike] Run Fully Automated Scan")
        print(" 6. [Test]         Test All Source Connections")
        print()
        print("--- Configuration ---")
        print(" 7. View Current Configuration")
        print(" 8. Validate Configuration")
        print()
        print("--- NEW Adapter Pipeline ---")
        print(" 9. [V2 ENGINE] Run Full Adapter Pipeline")
        print()
        print(" Q. Quit")
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

        elif choice == '3':
            print("\nThis option is deprecated and will be removed. Please use the V2 Adapter Pipeline.")
            # print("\n⚙️ Chaining Pre-Fetch and Parse for reliability...")
            # safe_async_run(run_batch_prefetch(config), "Pre-Fetch")
            # print("--- Pre-fetch complete. Now parsing local files... ---")
            # if check_prerequisites(config, 'parse'):
            #     try:
            #         # run_batch_parse(config, None) # Decommissioned
            #         print("✅ Batch parsing completed successfully.")
            #     except Exception as e:
            #         print(f"❌ An error occurred during the parsing phase: {e}")
            #         logging.error(f"Batch parse failed: {e}")

        elif choice == '4':
            print("\nThis option is deprecated and will be removed. Please use the V2 Adapter Pipeline.")
            # print("\n📋 Launching Persistent Engine...")
            # print("   This will start the 'always-on' clipboard monitoring mode.")
            # print("   Press Ctrl+C in the engine to save and return to menu.")
            # confirm = input("   Continue? (Y/n): ").strip().lower()

            # if confirm in ['y', '', 'yes']:
            #     args = create_persistent_args()
            #     try:
            #         # run_persistent_engine(config, args) # Decommissioned
            #     except KeyboardInterrupt:
            #         print("\n✅ Persistent engine stopped. Returning to menu.")
            #     except Exception as e:
            #         print(f"❌ Error in persistent engine: {e}")
            #         logging.error(f"Persistent engine failed: {e}")
            # else:
            #     print("Operation cancelled.")

        elif choice == '5':
            safe_async_run(run_automated_scan(config, None), "Quick Strike Scan")

        elif choice == '6':
            safe_async_run(test_scanner_connections(config), "Connection Test")

        elif choice == '7':
            print("\n" + "="*50)
            print(" CURRENT CONFIGURATION")
            print("="*50)
            print(f"App Name: {config.get('APP_NAME', 'Not Set')}")
            print(f"Input Directory: {config.get('INPUT_DIR', 'Not Set')}")
            print(f"Output Directory: {config.get('DEFAULT_OUTPUT_DIR', 'Not Set')}")
            print(f"Log File: {config.get('LOG_FILE', 'Not Set')}")
            print(f"Data Sources Count: {len(config.get('DATA_SOURCES', []))}")
            print(f"HTTP Headers Configured: {'Yes' if config.get('HTTP_HEADERS') else 'No'}")
            print(f"Proxy Configured: {'Yes' if config.get('HTTP_CLIENT', {}).get('PROXIES') else 'No'}")
            print("="*50)

        elif choice == '8':
            print("\n🔍 Validating Configuration...")
            if validate_config(config):
                print("✅ Configuration is valid!")

                # Additional checks
                input_dir = Path(config['INPUT_DIR'])
                output_dir = Path(config['DEFAULT_OUTPUT_DIR'])

                if not input_dir.exists():
                    print(f"ℹ️  Input directory '{input_dir}' will be created when needed.")
                if not output_dir.exists():
                    print(f"ℹ️  Output directory '{output_dir}' will be created when needed.")

                data_sources = config.get('DATA_SOURCES', [])
                enabled_count = sum(1 for cat in data_sources
                                  for site in cat.get('sites', [])
                                  if site.get('enabled', True))
                print(f"ℹ️  Found {enabled_count} enabled data sources across {len(data_sources)} categories.")
            else:
                print("❌ Configuration validation failed!")

        elif choice == '9':
            safe_async_run(run_adapter_pipeline(config), "V2 Adapter Pipeline")

        elif choice == 'Q':
            print("👋 Goodbye!")
            break

        else:
            print("❌ Invalid choice, please try again.")

        if choice != 'Q':
            input("\nPress Enter to return to the menu...")

def main_cli(config: Dict, args: argparse.Namespace):
    """Handles command-line argument parsing and execution."""
    try:
        if args.command == 'scan':
            asyncio.run(run_automated_scan(config, args))

        elif args.command == 'parse':
            # This command now implicitly runs prefetch first to guarantee file availability
            logging.info("CLI 'parse' command initiated. Running pre-fetch first.")
            print("⚙️ Running pre-fetch before parsing to ensure data is fresh...")
            asyncio.run(run_batch_prefetch(config))
            print("--- Pre-fetch complete. Now parsing... ---")

            if not check_prerequisites(config, 'parse'):
                print("❌ Parsing cannot continue because prerequisites were not met after pre-fetch.")
                sys.exit(1)
            run_batch_parse(config, args)

        elif args.command == 'persistent':
            print("Starting Persistent Engine in CLI mode...")
            run_persistent_engine(config, args)

        elif args.command == 'collect':
            create_and_launch_link_helper(config)
            print("Link helper created and launched.")

        elif args.command == 'prefetch':
            asyncio.run(run_batch_prefetch(config))

        elif args.command == 'test':
            asyncio.run(test_scanner_connections(config))

        elif args.command == 'validate':
            success = validate_config(config)
            sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"CLI operation failed: {e}")
        print(f"Error: {e}")
        sys.exit(1)

def create_cli_parser(config: Dict) -> argparse.ArgumentParser:
    """Create and configure the CLI argument parser."""
    app_name = config.get('APP_NAME', 'Paddock Parser Toolkit')
    parser = argparse.ArgumentParser(
        description=f"{app_name} - A Unified Racing Intelligence Tool",
        epilog="Use '<command> --help' for command-specific options."
    )

    subparsers = parser.add_subparsers(dest='command', required=True, help="Available commands")

    # Scan command
    scan_parser = subparsers.add_parser('scan', help="Run the automated 'Quick Strike' scan")
    scan_parser.add_argument('--json-out', help="Path to export normalized JSON")
    scan_parser.add_argument('--date', help="Date to scan in YYYY-MM-DD format", default=None)

    # Parse command
    parse_parser = subparsers.add_parser('parse', help="Run the 'Deep Dive' parse on local files (Batch Mode)")
    parse_parser.add_argument('--input-dir', help="Input directory path (overrides config)")
    parse_parser.add_argument('--no-odds-mode', action='store_true', help="Bypass scoring and sort by time")
    parse_parser.add_argument('--min-score', type=float, help="Minimum value score to include")

    # Persistent command - FIXED: Added all required arguments
    persistent_parser = subparsers.add_parser('persistent', help="Launch the 'Always-On' persistent engine")
    persistent_parser.add_argument('--cache-dir', help="Directory for cache files (overrides config)")
    persistent_parser.add_argument('--auto-restore', action='store_true', help="Automatically restore cache on startup")
    persistent_parser.add_argument('--disable-cache-backup', action='store_true', help="Disable cache backup")
    persistent_parser.add_argument('--paste-sentinel', default='KABOOM', help="Sentinel string for paste detection")
    persistent_parser.add_argument('--no-odds-mode', action='store_true', help="Bypass scoring and sort by time")
    persistent_parser.add_argument('--min-score', type=float, help="Minimum value score to include")

    # Other commands
    collect_parser = subparsers.add_parser('collect', help="Generate and open the data collection helper page")

    prefetch_parser = subparsers.add_parser('prefetch', help="Pre-fetch all accessible data sources")
    prefetch_parser.add_argument('--date', help="Date to fetch in YYYY-MM-DD format", default=None)

    test_parser = subparsers.add_parser('test', help="Test all data source connections")

    validate_parser = subparsers.add_parser('validate', help="Validate configuration file")

    return parser

if __name__ == "__main__":
    # Load and validate configuration first
    try:
        CONFIG = load_config()
        if not CONFIG:
            print("FATAL: Could not load configuration file.", file=sys.stderr)
            sys.exit(1)
    except Exception as e:
        print(f"FATAL: Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # Setup logging
    setup_logging(CONFIG.get("LOG_FILE", "app.log"))
    logging.info(f"Starting {CONFIG.get('APP_NAME', 'Paddock Parser Toolkit')} v1.3")

    # Validate critical configuration
    if not validate_config(CONFIG):
        print("FATAL: Configuration validation failed. Please fix config.json.")
        sys.exit(1)

    # Determine mode based on command line arguments
    if len(sys.argv) > 1:
        # CLI mode
        parser = create_cli_parser(CONFIG)
        try:
            args = parser.parse_args()
            main_cli(CONFIG, args)
        except SystemExit:
            # argparse calls sys.exit() for --help and errors
            pass
    else:
        # Interactive menu mode
        try:
            main_menu(CONFIG)
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
        except Exception as e:
            logging.critical(f"Fatal error in main menu: {e}", exc_info=True)
            print(f"Fatal error: {e}")
            sys.exit(1)
