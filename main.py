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
from pathlib import Path
from typing import Dict, List, Optional, Any
import time

# Import all necessary functions from our modules
from config import load_config
from enhanced_scanner import run_automated_scan, test_scanner_connections, run_batch_prefetch
from paddock_parser import run_batch_parse, run_persistent_engine
from link_helper import create_and_launch_link_helper

def setup_logging(log_file: str):
    """Configures logging for the application."""
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

def main_menu(config):
    """Displays the interactive main menu for the user."""
    while True:
        print("\n" + "="*50)
        print(f" {config.get('APP_NAME', 'Paddock Parser Toolkit')} v1.3 - Main Menu")
        print("="*50)
        print("--- Data Collection ---")
        print(" 1. [Automated] Pre-Fetch Accessible Sources")
        print(" 2. [Manual]    Open Manual Collection Helper")
        print("\n" + "--- Data Processing ---")
        print(" 3. [Deep Dive] Parse All Local Files (Batch Mode)")
        print(" 4. [Deep Dive] Launch Persistent Engine (Live Paste)")
        print("\n" + "--- Other Tools ---")
        print(" 5. [Quick Strike] Run Fully Automated Scan")
        print(" 6. [Test]         Test All Source Connections")
        print(" Q. Quit")

        choice = input("Enter your choice: ").strip().upper()

        if choice == '1':
            asyncio.run(run_batch_prefetch(config))
            input("\nPress Enter to return to the menu...")
        elif choice == '2':
            create_and_launch_link_helper(config)
            print("\nLink helper has been launched in your browser.")
            input("\nPress Enter to return to the menu...")
        elif choice == '3':
            run_batch_parse(config, None) # Passes None instead of the wrong Argparse usage
            input("\nPress Enter to return to the menu...")
        elif choice == '4':
            # Create a placeholder args object for persistent mode
            args = argparse.Namespace( # Corrected: now imports argparse from here
                no_odds_mode=False, min_score=None, cache_dir=None,
                auto_restore=False, disable_cache_backup=False,
                paste_sentinel='KABOOM'
            )
            run_persistent_engine(config, args)
        elif choice == '5':
            asyncio.run(run_automated_scan(config, None))
            input("\nPress Enter to return to the menu...")
        elif choice == '6':
            asyncio.run(test_scanner_connections(config))
            input("\nPress Enter to return to the menu...")
        elif choice == 'Q':
            print("Goodbye! 👋")
            break
        else:
            print("Invalid choice, please try again.")

def main_cli(config, args):
    """Handles command-line argument parsing and execution."""
    if args.command == 'scan':
        asyncio.run(run_automated_scan(config, args))
    elif args.command == 'parse':
        run_batch_parse(config, args)
    elif args.command == 'persistent':
        run_persistent_engine(config, args)
    elif args.command == 'collect':
        create_and_launch_link_helper(config)
    elif args.command == 'prefetch': # Add new CLI command
        asyncio.run(run_batch_prefetch(config))

if __name__ == "__main__":
    import argparse 
    CONFIG = load_config()
    if not CONFIG:
        sys.exit(1)

    setup_logging(CONFIG.get("LOG_FILE", "app.log"))

    # Check how many command line arguments are passed
    if len(sys.argv) > 1:
        parser = argparse.ArgumentParser(description=f"{CONFIG.get('APP_NAME')} - A Unified Racing Intelligence Tool")
        subparsers = parser.add_subparsers(dest='command', required=True, help="Available commands")

        # Define subparsers
        scan_parser = subparsers.add_parser('scan', help="Run the automated 'Quick Strike' scan")
        parse_parser = subparsers.add_parser('parse', help="Run the 'Deep Dive' parse on local files (Batch Mode)")
        persistent_parser = subparsers.add_parser('persistent', help="Launch the 'Always-On' persistent engine")
        collect_parser = subparsers.add_parser('collect', help="Generate and open the data collection helper page")
        prefetch_parser = subparsers.add_parser('prefetch', help="Pre-fetches all accessible data sources into the input directory")

        # Add arguments for commands
        scan_parser.add_argument('--json-out', help="Path to export normalized JSON.")
        scan_parser.add_argument('--date', help="Date to scan in YYYY-MM-DD format (for scan)", default=None) 

        for p in [parse_parser, persistent_parser]:
             p.add_argument('--no-odds-mode', action='store_true', help="Bypass scoring and sort by time")
             p.add_argument('--min-score', type=float, help="Minimum value score to include")
        
        args = parser.parse_args()
        main_cli(CONFIG, args)
    else:
        # Fall back to main menu if no arguments are provided
        main_menu(CONFIG)
        
