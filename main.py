#!/usr/bin/env python3
"""
Paddock Parser Toolkit v1.2 - Main Entry Point

This script serves as the main entry point for the entire toolkit. It uses a
smart, dual-mode startup logic:
- If run with no arguments (e.g., double-clicked), it launches a user-friendly
  interactive menu.
- If run with command-line arguments, it operates in a powerful CLI mode
  suitable for automation and advanced users.

This is a direct implementation of the user's brilliant architectural design.
"""

import sys
import argparse
import logging
import asyncio
from pathlib import Path

# We will create these files next
from config import load_config
from enhanced_scanner import run_automated_scan, test_scanner_connections
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
    """Displays an interactive main menu for the user."""
    while True:
        print("\n" + "="*45)
        print(f" {config.get('APP_NAME', 'Racing Tool')} v{config.get('VERSION', '1.2')} - Main Menu")
        print("="*45)
        print("1. [Quick Strike] Automated Scan for Today")
        print("2. [Deep Dive] Launch Persistent Engine")
        print("3. [Deep Dive] Parse Local Files Once (Batch Mode)")
        print("---------------------------------------------")
        print("4. [Collect] Open Data Collection Helper")
        print("5. [Test] Test Scanner Connections")
        print("Q. Quit")
        
        choice = input("Enter your choice: ").strip().upper()

        if choice == '1':
            asyncio.run(run_automated_scan(config))
            input("\nPress Enter to return to the menu...")
        elif choice == '2':
            # This will start the persistent loop
            run_persistent_engine(config, None) # Args are not needed for menu-driven persistence
        elif choice == '3':
            run_batch_parse(config)
            input("\nPress Enter to return to the menu...")
        elif choice == '4':
            create_and_launch_link_helper(config)
            print("\nLink helper has been launched in your browser.")
            time.sleep(2)
        elif choice == '5':
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

if __name__ == "__main__":
    # Load configuration first
    CONFIG = load_config()
    if not CONFIG:
        sys.exit(1)

    # Setup logging
    setup_logging(CONFIG.get("LOG_FILE", "app.log"))
    
    # Decide between interactive menu or CLI
    if len(sys.argv) > 1:
        # Arguments were passed, use CLI mode
        parser = argparse.ArgumentParser(description=f"{CONFIG.get('APP_NAME')} - A Unified Racing Intelligence Tool")
        subparsers = parser.add_subparsers(dest='command', required=True, help="Available commands")

        # Define subparsers for each command
        scan_parser = subparsers.add_parser('scan', help="Run the automated 'Quick Strike' scan")
        parse_parser = subparsers.add_parser('parse', help="Run the 'Deep Dive' parse on local files")
        persistent_parser = subparsers.add_parser('persistent', help="Launch the 'Always-On' persistent engine")
        collect_parser = subparsers.add_parser('collect', help="Generate and open the data collection helper page")
        
        # Add arguments specific to scan command
        scan_parser.add_argument('--date', help="Target date (YYYY-MM-DD). Default: today")
        scan_parser.add_argument('--json-out', help="Path to export normalized schedule JSON (bridge to Paddock)")
        
        # Add arguments specific to paddock parser modes
        for p in [parse_parser, persistent_parser]:
             p.add_argument('--no-odds-mode', action='store_true', help="Bypass scoring and sort by time")
             p.add_argument('--min-score', type=float, help="Minimum value score to include")
        
        args = parser.parse_args()
        main_cli(CONFIG, args)
    else:
        # No arguments, use interactive menu (double-click scenario)
        main_menu(CONFIG)