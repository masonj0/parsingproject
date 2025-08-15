#!/usr/bin/env python3
"""
Paddock Parser Toolkit - Configuration Loader (config.py)

This module is responsible for loading the central `config.json` file.
By centralizing this logic, we ensure that every part of the application,
from the main entry point to the individual parsers, uses the exact same
set of configurations.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any

def load_config(path: str = 'config.json') -> Dict[str, Any]:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.critical(f"FATAL: '{path}' not found.")
    except json.JSONDecodeError as e:
        logging.critical(f"FATAL: Could not parse '{path}': {e}")
        print(f"FATAL: Could not parse '{path}'. It may be invalid JSON. Error: {e}", file=sys.stderr)
        return {}
    except Exception as e:
        print(f"FATAL: An unexpected error occurred while loading the config file: {e}", file=sys.stderr)
        return {}
