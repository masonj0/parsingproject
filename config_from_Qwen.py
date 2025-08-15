#!/usr/bin/env python3
import json
import logging
from pathlib import Path
from typing import Dict, Any

def load_config(path: str = 'config.json') -> Dict[str, Any]:
    """Loads the configuration from a JSON file."""
    try:
        config_path = Path(path)
        if not config_path.exists():
            logging.critical(f"FATAL: Configuration file '{path}' not found.")
            return {}
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        logging.info(f"Configuration loaded successfully from '{path}'.")
        return config_data
    except json.JSONDecodeError as e:
        logging.critical(f"FATAL: Could not parse '{path}': {e}")
    except Exception as e:
        logging.critical(f"FATAL: Unexpected error loading config from '{path}': {e}")
    return {}
