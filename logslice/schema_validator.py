"""Schema validation for structured log entries."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Iterator, List, Optional


@dataclass
class FieldSpec:
    """Specification for a single log field."""
    name: str
    required: bool = False
    expected_type: Optional[type] = None
    allowed_values: Optional[List[Any]] = None


@dataclass
class SchemaViolation:
    """Describes a single schema violation found in a log entry."""
    entry: Dict[str, Any]
    field: str
    reason: str

    def __str__(self) -> str:
        return f"[{self.field}] {self.reason}"


@dataclass
class Schema:
    """A collection of FieldSpecs that define the expected shape of a log entry."""
    fields: List[FieldSpec] = field(default_factory=list)

    def add_field(self, spec: FieldSpec) -> "Schema":
        self.fields.append(spec)
        return self

    def validate(self, entry: Dict[str, Any]) -> List[SchemaViolation]:
        """Return a list of violations for the given entry (empty means valid)."""
        violations: List[SchemaViolation] = []
        for spec in self.fields:
            value = entry.get(spec.name)
            if value is None:
                if spec.required:
                    violations.append(
                        SchemaViolation(entry, spec.name, "required field is missing")
                    )
                continue
            if spec.expected_type is not None and not isinstance(value, spec.expected_type):
                violations.append(
                    SchemaViolation(
                        entry,
                        spec.name,
                        f"expected {spec.expected_type.__name__}, got {type(value).__name__}",
                    )
                )
            if spec.allowed_values is not None and value not in spec.allowed_values:
                violations.append(
                    SchemaViolation(
                        entry,
                        spec.name,
                        f"value {value!r} not in allowed set {spec.allowed_values!r}",
                    )
                )
        return violations


def validate_entries(
    entries: Iterable[Dict[str, Any]],
    schema: Schema,
    on_violation: str = "drop",
) -> Iterator[Dict[str, Any]]:
    """Filter or tag entries based on schema validation.

    Args:
        entries: Iterable of log entry dicts.
        schema: Schema to validate against.
        on_violation: One of 'drop' (skip invalid), 'tag' (add '_violations' key),
                      or 'passthrough' (yield all regardless).
    Yields:
        Log entries according to the chosen violation strategy.
    """
    for entry in entries:
        violations = schema.validate(entry)
        if not violations:
            yield entry
        elif on_violation == "drop":
            continue
        elif on_violation == "tag":
            tagged = dict(entry)
            tagged["_violations"] = [str(v) for v in violations]
            yield tagged
        else:  # passthrough
            yield entry
