#!/usr/bin/env python3
"""
Paddock Parser Toolkit - Shared Analysis Module (analysis.py)

NOTE TO SELF (The Constitution - The "Shared Intelligence" Gem):
This module is a single source of truth for high-level analytical logic,
like value scoring. By centralizing it here, both the desktop toolkit and
the mobile alerting engine can use the exact same analytical brain, ensuring
100% consistency and preventing logical drift.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

# =============================================================================
# DATA CLASSES (Required for the Scorer's type hints)
# =============================================================================

@dataclass
class Runner:
    """Represents a single runner in a race with its odds."""
    name: str
    odds_str: str
    odds_decimal: float

@dataclass
class RaceData:
    """Represents a single race, enriched with runner data and a value score."""
    id: str
    course: str
    race_time: str
    race_type: str
    field_size: int
    runners: List[Runner] = field(default_factory=list)
    favorite: Optional[Runner] = None
    second_favorite: Optional[Runner] = None
    value_score: float = 0.0
    # Add other fields as necessary if the scorer evolves
    discipline: str = ""
    race_url: str = ""
    data_sources: List[str] = field(default_factory=list)


# =============================================================================
# --- ENHANCED VALUE SCORER ---
# =============================================================================

class EnhancedValueScorer:
    """
    Analyzes Field Size, Race Type, and the shape of the
    Odds market to find structurally advantageous betting opportunities.
    """
    def __init__(self, config: Dict):
        self.config = config
        self.weights = config.get("SCORER_WEIGHTS", {
            "FIELD_SIZE_WEIGHT": 0.35, "FAVORITE_ODDS_WEIGHT": 0.45,
            "ODDS_SPREAD_WEIGHT": 0.15, "DATA_QUALITY_WEIGHT": 0.05
        })

    def calculate_score(self, race: RaceData) -> float:
        """Calculates the final value score for a race."""
        if not race.runners or not race.favorite:
            return 0.0

        fav_odds = race.favorite.odds_decimal if race.favorite else 999.0
        sec_fav_odds = race.second_favorite.odds_decimal if race.second_favorite else 999.0

        base_score = (self._calculate_field_score(race.field_size) * self.weights["FIELD_SIZE_WEIGHT"] +
                      self._calculate_favorite_odds_score(fav_odds) * self.weights["FAVORITE_ODDS_WEIGHT"] +
                      self._calculate_odds_spread_score(fav_odds, sec_fav_odds) * self.weights["ODDS_SPREAD_WEIGHT"] +
                      self._calculate_data_quality_score(race) * self.weights["DATA_QUALITY_WEIGHT"])

        final_score = min(100.0, base_score)
        return round(final_score, 2)

    def _calculate_field_score(self, size: int) -> float:
        if 3 <= size <= 5: return 100.0
        if 6 <= size <= 8: return 85.0
        if 9 <= size <= 12: return 60.0
        return 20.0

    def _calculate_favorite_odds_score(self, odds: float) -> float:
        if odds == 999.0: return 20.0
        if 1.0 <= odds <= 1.5: return 100.0
        if 1.5 < odds <= 2.5: return 90.0
        if 2.5 < odds <= 4.0: return 75.0
        if 0.5 <= odds < 1.0: return 85.0
        if odds < 0.5: return 60.0
        return 40.0

    def _calculate_odds_spread_score(self, fav_odds: float, sec_odds: float) -> float:
        if fav_odds == 999.0 or sec_odds == 999.0: return 30.0
        spread = sec_odds - fav_odds
        if spread >= 2.0: return 100.0
        if spread >= 1.5: return 90.0
        if spread >= 1.0: return 80.0
        if spread >= 0.5: return 60.0
        return 40.0

    def _calculate_data_quality_score(self, race: RaceData) -> float:
        score = 0.0
        if race.runners and any(r.odds_str not in ["SP", "", "NR", "SCR"] for r in race.runners): score += 50.0
        if race.favorite and race.second_favorite: score += 30.0
        if race.race_url: score += 20.0
        return min(100.0, score)