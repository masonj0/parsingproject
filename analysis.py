# analysis.py
from dataclasses import dataclass
from typing import Dict, List, Any
from statistics import mean
from normalizer import NormalizedRace # Import the new data structure

@dataclass
class ScoreResult:
    """Contains the final score and the individual signals that produced it."""
    total: float
    signals: Dict[str, float]
    reasons: List[str]

# Default weights for combining signals into a final score.
# These can be tuned over time.
DEFAULT_WEIGHTS = {
    "value_vs_sp": 0.35,
    "steam_move": 0.25,
    "market_consensus": 0.15,
    "trainer_form": 0.10,
    "jockey_uplift": 0.05,
    "overlay_confidence": 0.10,
}

# "Track Personas" - allows for tuning signal weights based on the track.
# For example, some tracks might be more sensitive to market steam.
TRACK_PROFILES = {
    "default": {"steam_sensitivity": 1.0, "min_conf": 0.5},
    "ascot": {"steam_sensitivity": 1.2, "min_conf": 0.6},
    # Add more track-specific profiles here over time
}

def compute_signals(race: NormalizedRace, history=None) -> Dict[str, float]:
    """
    Computes a dictionary of raw signals for a given race.
    This is where the core analytical logic lives.

    (Currently uses placeholder logic as per the proposal)
    """
    signals = {}

    # Example signal: Average "overlay" confidence.
    # An overlay is when the odds seem higher than the fair price.
    overlays = []
    if race.runners:
        for runner in race.runners:
            if runner.odds_decimal and runner.odds_decimal > 1.0:
                # Simplistic fair price proxy based on odds
                fair_price = 1.0 / runner.odds_decimal
                overlay = runner.odds_decimal - (1.0 / fair_price if fair_price > 0 else runner.odds_decimal)
                overlays.append(overlay)

    signals["overlay_confidence"] = mean(overlays) if overlays else 0.0

    # Placeholder signals that would require historical data
    signals["steam_move"] = 0.0  # Needs price history
    signals["value_vs_sp"] = 0.0 # Needs Starting Price history
    # Calculate market consensus via overround
    implied_probabilities = []
    if race.runners:
        for runner in race.runners:
            if runner.odds_decimal and runner.odds_decimal > 0:
                implied_probabilities.append(1 / runner.odds_decimal)

    if implied_probabilities:
        overround = sum(implied_probabilities)
        # A lower overround suggests a more competitive/confident market.
        # We'll define the signal as (1 / overround), so higher is better.
        signals["market_consensus"] = (1 / overround) if overround > 0 else 0.0
    else:
        signals["market_consensus"] = 0.0
    signals["trainer_form"] = 0.0
    signals["jockey_uplift"] = 0.0

    return signals

def apply_profile(track_key: str) -> Dict[str, Any]:
    """
    Applies a track-specific profile, falling back to the default.
    """
    for key, profile in TRACK_PROFILES.items():
        if key != "default" and key in track_key:
            return {**TRACK_PROFILES["default"], **profile}
    return TRACK_PROFILES["default"]

def score_race(race: NormalizedRace) -> ScoreResult:
    """
    Scores a single normalized race by computing signals and applying weights.
    """
    profile = apply_profile(race.track_key)
    signals = compute_signals(race)
    reasons = []
    total_score = 0.0

    for signal_name, weight in DEFAULT_WEIGHTS.items():
        signal_value = signals.get(signal_name, 0.0)

        # Apply track-specific modifications
        if signal_name == "steam_move":
            signal_value *= profile["steam_sensitivity"]

        total_score += weight * signal_value
        reasons.append(f"{signal_name}={signal_value:.3f} * w={weight:.2f}")

    return ScoreResult(total=total_score, signals=signals, reasons=reasons)

def score_races(races: List[NormalizedRace]) -> Dict[str, ScoreResult]:
    """
    Takes a list of normalized races and returns a dictionary of their scores,
    keyed by race_key.
    """
    return {race.race_key: score_race(race) for race in races}