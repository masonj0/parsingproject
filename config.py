#!/usr/bin/env python3

"""
Paddock Parser Toolkit - Configuration Loader (config.py)

This module is responsible for loading the central `config.json` file.
By centralizing this logic, we ensure that every part of the application,
from the main entry point to the individual parsers, uses the exact same
set of configurations.
"""

import json
import sys
from pathlib import Path
from typing import Dict, Any

def load_config(path: str = 'config.json') -> Dict[str, Any]:
    """
    Loads the main configuration from an external JSON file.

    Args:
        path (str): The path to the config.json file.

    Returns:
        Dict[str, Any]: A dictionary containing the application's configuration.
    """
    try:
        with open(path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            
            # --- Performance Optimization from Jules ---
            # Convert the list of Canadian tracks to a set for faster lookups.
            # This is done once at load time for maximum efficiency.
            if 'CANADIAN_TRACKS' in config:
                config['CANADIAN_TRACKS'] = set(config['CANADIAN_TRACKS'])
                
            return config
            
    except FileNotFoundError:
        print(f"FATAL: Configuration file not found at '{path}'.", file=sys.stderr)
        print("Please ensure 'config.json' is in the same directory as the main script.", file=sys.stderr)
        return {}
    except json.JSONDecodeError as e:
        print(f"FATAL: Could not parse '{path}'. It may be invalid JSON. Error: {e}", file=sys.stderr)
        return {}
    except Exception as e:
        print(f"FATAL: An unexpected error occurred while loading the config file: {e}", file=sys.stderr)
        return {}
