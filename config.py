#!/usr/bin/env python3
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
    return {}
