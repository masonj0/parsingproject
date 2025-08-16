# sources.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Protocol, List, Dict, Any, Iterable
import datetime as dt

@dataclass
class FieldConfidence:
    value: Any
    confidence: float  # 0..1
    provenance: str | None = None  # e.g., "DOM: #price span"

@dataclass
class RunnerDoc:
    runner_id: str
    name: FieldConfidence
    number: FieldConfidence | None = None
    odds: FieldConfidence | None = None
    jockey: FieldConfidence | None = None
    trainer: FieldConfidence | None = None
    extras: Dict[str, FieldConfidence] = field(default_factory=dict)

@dataclass
class RawRaceDocument:
    source_id: str
    fetched_at: str  # ISO
    track_key: str
    race_key: str
    start_time_iso: str | None
    runners: List[RunnerDoc]
    extras: Dict[str, FieldConfidence] = field(default_factory=dict)

class SourceAdapter(Protocol):
    source_id: str
    async def fetch(self, config: dict) -> List[RawRaceDocument]: ...

# --- Adapter Registry & Merging Logic ---

import asyncio
from typing import Tuple

ADAPTERS: List[SourceAdapter] = []

def register_adapter(adapter_cls):
    """Decorator to register a new source adapter."""
    ADAPTERS.append(adapter_cls())
    return adapter_cls

async def collect_all(config: dict, adapter_ids: List[str] = None) -> List[RawRaceDocument]:
    """
    Fetches data from all registered source adapters concurrently.
    If adapter_ids is provided, only fetches from those adapters.
    """
    if adapter_ids:
        adapters_to_run = [adapter for adapter in ADAPTERS if adapter.source_id in adapter_ids]
    else:
        adapters_to_run = ADAPTERS

    print(f"DEBUG: Adapters to run: {[adapter.source_id for adapter in adapters_to_run]}")

    tasks = [adapter.fetch(config) for adapter in adapters_to_run]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_docs = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"[ERROR] Adapter '{adapters_to_run[i].source_id}' failed: {result}")
        else:
            all_docs.extend(result)

    return all_docs

def merge_field(a: FieldConfidence | None, b: FieldConfidence | None) -> FieldConfidence | None:
    """Merges two FieldConfidence objects, preferring the one with higher confidence."""
    if a and not b: return a
    if b and not a: return b
    if not a and not b: return None
    # In case of a tie in confidence, 'a' is preferred (e.g., the existing doc)
    return a if a.confidence >= b.confidence else b

def merge_runner(a: RunnerDoc, b: RunnerDoc) -> RunnerDoc:
    """Merges two RunnerDoc objects field by field."""
    # This assumes runner_id is the same
    return RunnerDoc(
        runner_id=a.runner_id,
        name=merge_field(a.name, b.name) or a.name, # Name is required, so fallback to 'a'
        number=merge_field(a.number, b.number),
        odds=merge_field(a.odds, b.odds),
        jockey=merge_field(a.jockey, b.jockey),
        trainer=merge_field(a.trainer, b.trainer),
        extras={**b.extras, **a.extras},  # Simple dict merge, 'a' overwrites 'b'
    )

def coalesce_docs(docs: List[RawRaceDocument]) -> Dict[Tuple[str, str], RawRaceDocument]:
    """
    Merges a list of RawRaceDocument objects into a dictionary, keyed by
    (track_key, race_key). Runners within races are merged by runner_id.
    """
    by_key: Dict[Tuple[str, str], RawRaceDocument] = {}
    for doc in docs:
        key = (doc.track_key, doc.race_key)
        if key not in by_key:
            by_key[key] = doc
            continue

        # Merge with existing document
        base_doc = by_key[key]

        # Create a dictionary of runners from the base document for efficient merging
        merged_runners: Dict[str, RunnerDoc] = {r.runner_id: r for r in base_doc.runners}

        for new_runner in doc.runners:
            if new_runner.runner_id in merged_runners:
                # Merge with existing runner
                existing_runner = merged_runners[new_runner.runner_id]
                merged_runners[new_runner.runner_id] = merge_runner(existing_runner, new_runner)
            else:
                # Add new runner
                merged_runners[new_runner.runner_id] = new_runner

        base_doc.runners = list(merged_runners.values())

        # Optional: Merge 'extras' from the main doc if needed, similar to runner extras
        base_doc.extras = {**doc.extras, **base_doc.extras}

    return by_key
