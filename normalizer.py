# normalizer.py
from dataclasses import dataclass, field
from typing import Dict, Any, List
from sources import RawRaceDocument, RunnerDoc, FieldConfidence

SCHEMA_VERSION = "2.0"

def canonical_track_key(raw: str) -> str:
    """Creates a standardized, URL-safe key for a track name."""
    if not raw:
        return "unknown_track"
    return raw.strip().lower().replace(" ", "_").replace("(", "").replace(")", "")

def canonical_race_key(track: str, race_no: str | int) -> str:
    """Creates a unique, standardized key for a specific race."""
    return f"{canonical_track_key(track)}::r{str(race_no).zfill(2)}"

@dataclass
class NormalizedRunner:
    """A runner with standardized and cleaned data fields."""
    runner_id: str
    name: str
    odds_decimal: float | None
    features: Dict[str, Any] = field(default_factory=dict)

@dataclass
class NormalizedRace:
    """A race with all data normalized and ready for analysis."""
    schema_version: str
    track_key: str
    race_key: str
    start_time_iso: str | None
    runners: List[NormalizedRunner]
    provenance: Dict[str, Any] = field(default_factory=dict)

def _parse_odds(value: str | float | None) -> float | None:
    """
    Converts various odds formats (e.g., '7/2', 'SP', 3.5) into a decimal float.
    Returns None if parsing fails.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    
    v = str(value).strip().upper()
    if v in {"SP", "NR", "SCR", "VOID"}:
        return None
    if v in {"EVS", "EVENS"}:
        return 2.0  # Standard decimal representation of evens

    if "/" in v:
        try:
            num, den = v.split("/", 1)
            return 1.0 + (float(num) / float(den))
        except (ValueError, ZeroDivisionError):
            return None
    try:
        # Assumes decimal odds if not fractional
        return float(v)
    except (ValueError, TypeError):
        return None

def normalize_race_docs(doc: RawRaceDocument) -> NormalizedRace:
    """
    Transforms a RawRaceDocument from a source adapter into a clean,
    standardized NormalizedRace object ready for the analysis engine.
    """
    runners = []
    for r in doc.runners:
        odds = _parse_odds(r.odds.value if r.odds else None)

        # Pass through any extra data from the source, along with confidence scores
        features = {
            "jockey": r.jockey.value if r.jockey else None,
            "trainer": r.trainer.value if r.trainer else None,
            "extras": {k: v.value for k, v in r.extras.items()},
            "field_confidence": {
                "name": r.name.confidence if r.name else 0.0,
                "odds": r.odds.confidence if r.odds else 0.0,
                "jockey": r.jockey.confidence if r.jockey else 0.0,
                "trainer": r.trainer.confidence if r.trainer else 0.0,
            }
        }

        runners.append(NormalizedRunner(
            runner_id=r.runner_id,
            name=r.name.value if r.name else "UNKNOWN",
            odds_decimal=odds,
            features=features,
        ))

    provenance = {"source_id": doc.source_id, "fetched_at": doc.fetched_at}

    return NormalizedRace(
        schema_version=SCHEMA_VERSION,
        track_key=doc.track_key,
        race_key=doc.race_key,
        start_time_iso=doc.start_time_iso,
        runners=runners,
        provenance=provenance,
    )
