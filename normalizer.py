#!/usr/bin/env python3
"""
Paddock Parser Toolkit - Shared Normalizer Module (normalizer.py)

NOTE TO SELF (The Constitution - The "Shared Intelligence" Gem):
This module is the single source of truth for all data cleaning and
normalization logic. Both the automated scanner and the manual parser MUST use
these functions to ensure that data is 100% consistent across the entire
toolkit. This prevents logical drift between the two tools and is a
cornerstone of our professional architecture.
"""

import re
from typing import Optional

def normalize_course_name(name: str) -> str:
    """
    Cleans and standardizes a racetrack name.
    Handles common suffixes, parenthetical text, and special cases like "at".
    """
    if not name:
        return ""
    name = name.lower().strip()
    name = re.sub(r' at .*$', '', name)  # Handles "Woodbine at Mohawk"
    name = re.sub(r'\s*\([^)]*\)', '', name)  # Removes text in parentheses
    replacements = {
        'park': '', 'raceway': '', 'racecourse': '', 'track': '',
        'stadium': '', 'greyhound': '', 'harness': ''
    }
    for old, new in replacements.items():
        name = name.replace(old, new)
    return " ".join(name.split())

def map_discipline(discipline_name: str) -> str:
    """
    Maps a raw discipline string to a standardized category.
    Handles various terms for thoroughbred, harness, and greyhound racing.
    """
    if not discipline_name:
        return "thoroughbred"
    d_lower = discipline_name.lower()
    if "greyhound" in d_lower or "dog" in d_lower:
        return "greyhound"
    if "harness" in d_lower or "trot" in d_lower or "standardbred" in d_lower:
        return "harness"
    if "jump" in d_lower or "chase" in d_lower or "hurdle" in d_lower:
        return "jump"
    return "thoroughbred"

def parse_hhmm_any(time_text: str) -> Optional[str]:
    """
    Parses a time string from various common formats (e.g., '7:30 PM', '19.30')
    into a standardized 24-hour 'HH:MM' format.
    """
    if not time_text:
        return None
    
    match = re.search(r'(\d{1,2})[:.](\d{2})', str(time_text))
    if not match:
        return None
    
    hour, minute = int(match.group(1)), int(match.group(2))
    
    text_lower = str(time_text).lower()
    if 'pm' in text_lower and hour != 12:
        hour += 12
    if 'am' in text_lower and hour == 12:  # Handle midnight case (12:xx AM)
        hour = 0
        
    return f"{hour:02d}:{minute:02d}"

def convert_odds_to_fractional_decimal(odds_str: str) -> float:
    """
    Converts various odds formats ('5/2', '7-5', 'EVS', 'SP') into a
    single, comparable decimal float of the fraction (e.g., 5/2 -> 2.5).
    """
    if not isinstance(odds_str, str) or not odds_str.strip():
        return 999.0
    s = odds_str.strip().upper().replace("-", "/")
    if s in {"SP", "NR", "SCR", "VOID"}:
        return 999.0
    if s in {"EVS", "EVENS"}:
        return 1.0
    if "/" in s:
        try:
            num, den = map(float, s.split("/", 1))
            return num / den if den > 0 else 999.0
        except (ValueError, ZeroDivisionError):
            return 999.0
    try:
        dec = float(s)
        return dec - 1.0 if dec > 1.0 else 999.0
    except ValueError:
        return 999.0
