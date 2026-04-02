from __future__ import annotations

from typing import Protocol

from signals.core.dto import CombinedResult, NormalizedTransaction, SignalResult


class IngestionService(Protocol):
    def ingest(self, source_path: str) -> dict: ...


class ParsingService(Protocol):
    def parse(self, source_path: str) -> dict: ...


class NormalizationService(Protocol):
    def normalize(self, parsed: dict, run_id: str) -> list[NormalizedTransaction]: ...


class EntityResolutionService(Protocol):
    def resolve(self, *args, **kwargs) -> dict: ...


class ScoringService(Protocol):
    def score(self, normalized: list[NormalizedTransaction], run_id: str) -> list[SignalResult]: ...


class ReportingService(Protocol):
    def render_text(self, payload: dict) -> str: ...
    def render_json(self, payload: dict) -> dict: ...


class CombinedService(Protocol):
    def combine(self, *args, **kwargs) -> list[CombinedResult]: ...

