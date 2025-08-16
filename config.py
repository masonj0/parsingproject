#!/usr/bin/env python3
import json
import logging
import sys
from pathlib import Path
from typing import Dict, Any

def load_config(path: str = 'config.json') -> Dict[str, Any]:
    """
    Loads the main configuration file.
    On critical errors (file not found, parse error), it logs the error
    and exits the application to prevent running in a broken state.
    """
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.critical(f"FATAL: Configuration file '{path}' not found. Application cannot continue.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        logging.critical(f"FATAL: Could not parse configuration file '{path}': {e}. Application cannot continue.")
        sys.exit(1)
    return {} # Should not be reached in error cases