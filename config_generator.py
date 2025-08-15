#!/usr/bin/env python3
"""
Configuration Generator & Validator for Paddock Parser Toolkit
Provides intelligent config generation, validation, and management.
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

class ConfigGenerator:
    """Generates and validates configuration files for the Paddock Parser toolkit."""
    
    def __init__(self):
        self.config_template = self._get_config_template()
    
    def _get_config_template(self) -> Dict[str, Any]:
        """Returns the default configuration template."""
        return {
            "SCHEMA_VERSION": "1.2",
            "APP_NAME": "Paddock Parser Toolkit",
            "INPUT_DIR": "html_input",
            "DEFAULT_OUTPUT_DIR": "output",
            "LOG_FILE": "app.log",
            "TEMPLATE_SCANNER": "template_scanner.html",
            "TEMPLATE_PADDOCK": "template_paddock.html",
            
            "HTTP_CLIENT": {
                "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "REQUEST_TIMEOUT": 10,
                "RETRY_ATTEMPTS": 3,
                "RETRY_DELAY_SECONDS": 5,
                "VERIFY_SSL": True
            },
            
            "PROXY": {
                "ENABLED": False,
                "HTTP_PROXY": "",
                "HTTPS_PROXY": ""
            },
            
            "PROXY_VIEWERS": [
                {
                    "ENABLED": True,
                    "TOOL_URL": "https://codebeautify.org/source-code-viewer?url={target_url}",
                    "LINK_TEXT": "Proxy (CodeBeautify)"
                }
            ],
            
            "DATA_SOURCES": [
                {
                    "title": "🌟 Premium Global Sources",
                    "sites": [
                        {"name": "Timeform (UK/IRE/INTL)", "url": "https://www.timeform.com/horse-racing"},
                        {"name": "At The Races (UK/IRE)", "url": "https://www.attheraces.com/racecards"},
                        {"name": "Betfair Exchange (Global)", "url": "https://www.betfair.com/exchange/plus/horse-racing"},
                        {"name": "Sporting Life (UK/Global)", "url": "https://www.sportinglife.com/racing"},
                        {"name": "Sky Sports Racing (UK/Global)", "url": "https://www.skysports.com/racing/racecards"}
                    ]
                }
            ],
            
            "TIMEZONES": {
                "TRACKS": {
                    "assiniboia-downs": "America/Winnipeg",
                    "belmont-park": "America/New_York",
                    "woodbine": "America/Toronto"
                },
                "COUNTRIES": {
                    "GB": "Europe/London",
                    "IE": "Europe/Dublin",
                    "US": "America/New_York",
                    "FR": "Europe/Paris",
                    "AU": "Australia/Sydney",
                    "CA": "America/Toronto"
                },
                "ABBREVIATIONS": {
                    "ET": "America/New_York",
                    "CT": "America/Chicago"
                }
            },
            
            "CANADIAN_TRACKS": ["fort erie", "assiniboia downs", "woodbine"],
            
            "SCORER_WEIGHTS": {
                "FIELD_SIZE_WEIGHT": 0.35,
                "FAVORITE_ODDS_WEIGHT": 0.45,
                "ODDS_SPREAD_WEIGHT": 0.15,
                "DATA_QUALITY_WEIGHT": 0.05
            },
            
            "FILTERS": {
                "MIN_FIELD_SIZE": 4,
                "MAX_FIELD_SIZE": 12
            },
            
            "OUTPUT": {
                "AUTO_OPEN_BROWSER": True,
                "OUTPUT_FORMATS": ["html", "json", "csv"]
            }
        }
    
    def generate_default_config(self, output_path: Path = Path("config.json")) -> Dict[str, Any]:
        """Generate a comprehensive default configuration file."""
        config = self.config_template.copy()
        
        # Add generation metadata
        config["_generated"] = {
            "timestamp": datetime.now().isoformat(),
            "generator_version": "1.0"
        }
        
        # Write to file
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Generated default configuration: {output_path}")
        print("📝 You can now customize the generated config.json file")
        return config
    
    def validate_config(self, config_path: Path) -> Tuple[bool, List[str]]:
        """Validate an existing configuration file."""
        if not config_path.exists():
            return False, [f"Configuration file not found: {config_path}"]
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
        except json.JSONDecodeError as e:
            return False, [f"Invalid JSON in config file: {e}"]
        except Exception as e:
            return False, [f"Error reading config file: {e}"]
        
        errors = []
        warnings = []
        
        # Validate required keys
        required_keys = [
            "APP_NAME", "SCHEMA_VERSION", "INPUT_DIR", "DEFAULT_OUTPUT_DIR",
            "TIMEZONES", "SCORER_WEIGHTS", "OUTPUT", "FILTERS"
        ]
        
        for key in required_keys:
            if key not in config:
                errors.append(f"Missing required key: {key}")
        
        # Validate data structures
        if "TIMEZONES" in config:
            tz_config = config["TIMEZONES"]
            if not isinstance(tz_config, dict):
                errors.append("TIMEZONES must be a dictionary")
            else:
                required_tz_keys = ["COUNTRIES", "TRACKS", "ABBREVIATIONS"]
                for tz_key in required_tz_keys:
                    if tz_key not in tz_config:
                        errors.append(f"Missing TIMEZONES.{tz_key}")
        
        # Validate scorer weights
        if "SCORER_WEIGHTS" in config:
            weights = config["SCORER_WEIGHTS"]
            if not isinstance(weights, dict):
                errors.append("SCORER_WEIGHTS must be a dictionary")
            else:
                required_weights = [
                    "FIELD_SIZE_WEIGHT", "FAVORITE_ODDS_WEIGHT", 
                    "ODDS_SPREAD_WEIGHT", "DATA_QUALITY_WEIGHT"
                ]
                total_weight = 0
                for weight in required_weights:
                    if weight not in weights:
                        errors.append(f"Missing SCORER_WEIGHTS.{weight}")
                    elif not isinstance(weights[weight], (int, float)):
                        errors.append(f"SCORER_WEIGHTS.{weight} must be a number")
                    else:
                        total_weight += weights[weight]
                
                # Check if weights sum to approximately 1.0
                if abs(total_weight - 1.0) > 0.01:
                    warnings.append(f"Scorer weights sum to {total_weight:.3f}, should be close to 1.0")
        
        # Validate output formats
        if "OUTPUT" in config and "OUTPUT_FORMATS" in config["OUTPUT"]:
            formats = config["OUTPUT"]["OUTPUT_FORMATS"]
            if not isinstance(formats, list):
                errors.append("OUTPUT.OUTPUT_FORMATS must be a list")
            else:
                valid_formats = ["html", "json", "csv", "excel"]
                for fmt in formats:
                    if fmt not in valid_formats:
                        warnings.append(f"Unknown output format: {fmt}")
        
        # Validate DATA_SOURCES structure
        if "DATA_SOURCES" in config:
            sources = config["DATA_SOURCES"]
            if not isinstance(sources, list):
                errors.append("DATA_SOURCES must be a list")
            else:
                for i, category in enumerate(sources):
                    if not isinstance(category, dict):
                        errors.append(f"DATA_SOURCES[{i}] must be a dictionary")
                        continue
                    if "title" not in category:
                        warnings.append(f"DATA_SOURCES[{i}] missing title")
                    if "sites" not in category:
                        errors.append(f"DATA_SOURCES[{i}] missing sites")
                    elif not isinstance(category["sites"], list):
                        errors.append(f"DATA_SOURCES[{i}].sites must be a list")
        
        # Validate directories exist or can be created
        for dir_key in ["INPUT_DIR", "DEFAULT_OUTPUT_DIR"]:
            if dir_key in config:
                dir_path = Path(config[dir_key])
                try:
                    dir_path.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    errors.append(f"Cannot create directory {dir_key} ({dir_path}): {e}")
        
        is_valid = len(errors) == 0
        all_messages = errors + warnings
        
        return is_valid, all_messages
    
    def update_config(self, config_path: Path, updates: Dict[str, Any]) -> bool:
        """Update an existing configuration file with new values."""
        try:
            if config_path.exists():
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
            else:
                config = self.config_template.copy()
            
            # Deep merge updates into config
            def deep_update(base_dict: Dict, update_dict: Dict):
                for key, value in update_dict.items():
                    if key in base_dict and isinstance(base_dict[key], dict) and isinstance(value, dict):
                        deep_update(base_dict[key], value)
                    else:
                        base_dict[key] = value
            
            deep_update(config, updates)
            
            # Validate before saving
            temp_path = config_path.with_suffix('.tmp')
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            
            is_valid, messages = self.validate_config(temp_path)
            
            if is_valid:
                temp_path.replace(config_path)
                print(f"✅ Configuration updated successfully: {config_path}")
                if messages:  # Show warnings
                    for msg in messages:
                        print(f"⚠️  {msg}")
                return True
            else:
                temp_path.unlink()  # Remove invalid temp file
                print("❌ Configuration update failed:")
                for msg in messages:
                    print(f"   {msg}")
                return False
                
        except Exception as e:
            print(f"❌ Error updating configuration: {e}")
            return False
    
    def interactive_setup(self) -> Dict[str, Any]:
        """Interactive configuration setup wizard."""
        print("🎯 Paddock Parser Configuration Wizard")
        print("=" * 50)
        
        config = self.config_template.copy()
        
        # Basic settings
        app_name = input(f"App Name [{config['APP_NAME']}]: ").strip()
        if app_name:
            config['APP_NAME'] = app_name
        
        output_dir = input(f"Output Directory [{config['DEFAULT_OUTPUT_DIR']}]: ").strip()
        if output_dir:
            config['DEFAULT_OUTPUT_DIR'] = output_dir
        
        # Proxy configuration
        use_proxy = input("Do you need proxy support? (y/N): ").strip().lower()
        if use_proxy in ['y', 'yes']:
            config['PROXY']['ENABLED'] = True
            http_proxy = input("HTTP Proxy (e.g., http://proxy:8080): ").strip()
            if http_proxy:
                config['PROXY']['HTTP_PROXY'] = http_proxy
            https_proxy = input("HTTPS Proxy (e.g., http://proxy:8080): ").strip()
            if https_proxy:
                config['PROXY']['HTTPS_PROXY'] = https_proxy
        
        # Scorer weights
        print("\n📊 Scorer Weights (must sum to 1.0)")
        weights = config['SCORER_WEIGHTS']
        for weight_name, default_value in weights.items():
            user_input = input(f"{weight_name} [{default_value}]: ").strip()
            if user_input:
                try:
                    weights[weight_name] = float(user_input)
                except ValueError:
                    print(f"⚠️  Invalid number, keeping default: {default_value}")
        
        return config

def main():
    """CLI interface for the configuration generator."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Paddock Parser Configuration Manager")
    parser.add_argument('action', choices=['generate', 'validate', 'interactive'], 
                       help='Action to perform')
    parser.add_argument('--config', '-c', type=Path, default=Path('config.json'),
                       help='Configuration file path')
    parser.add_argument('--force', '-f', action='store_true',
                       help='Force overwrite existing configuration')
    
    args = parser.parse_args()
    
    generator = ConfigGenerator()
    
    if args.action == 'generate':
        if args.config.exists() and not args.force:
            print(f"❌ Configuration file already exists: {args.config}")
            print("Use --force to overwrite")
            sys.exit(1)
        
        config = generator.generate_default_config(args.config)
        
    elif args.action == 'validate':
        is_valid, messages = generator.validate_config(args.config)
        
        if is_valid:
            print(f"✅ Configuration is valid: {args.config}")
        else:
            print(f"❌ Configuration validation failed: {args.config}")
        
        for msg in messages:
            symbol = "⚠️ " if is_valid else "❌ "
            print(f"{symbol} {msg}")
        
        sys.exit(0 if is_valid else 1)
        
    elif args.action == 'interactive':
        config = generator.interactive_setup()
        
        # Save the configuration
        with open(args.config, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        
        print(f"\n✅ Configuration saved: {args.config}")
        
        # Validate the result
        is_valid, messages = generator.validate_config(args.config)
        if not is_valid:
            print("⚠️  Generated configuration has issues:")
            for msg in messages:
                print(f"   {msg}")

if __name__ == '__main__':
    main()