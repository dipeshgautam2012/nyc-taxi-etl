"""Shared types for pipeline runs and validation."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum


class RunStatus(StrEnum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


@dataclass
class ValidationResult:
    passed: bool
    checks: list[CheckResult] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "passed": self.passed,
            "checks": [
                {"name": c.name, "passed": c.passed, "detail": c.detail}
                for c in self.checks
            ],
        }


@dataclass
class PartitionCounts:
    bronze_rows: int = 0
    silver_rows: int = 0
    quarantine_rows: int = 0
    gold_rows: int = 0


@dataclass
class PipelineRunRecord:
    run_id: str
    year: int
    month: int
    status: RunStatus
    started_at: datetime
    finished_at: datetime | None = None
    counts: PartitionCounts = field(default_factory=PartitionCounts)
    validation_json: str | None = None
    error_message: str | None = None
