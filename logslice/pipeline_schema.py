"""Pipeline stages for schema validation."""
from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional

from logslice.pipeline import stage
from logslice.schema_validator import FieldSpec, Schema, SchemaViolation, validate_entries


def _build_schema(
    required_fields: Optional[List[str]] = None,
    typed_fields: Optional[Dict[str, type]] = None,
    enum_fields: Optional[Dict[str, List[Any]]] = None,
) -> Schema:
    schema = Schema()
    all_names: set = set()
    for name in (required_fields or []):
        all_names.add(name)
    for name in (typed_fields or {}):
        all_names.add(name)
    for name in (enum_fields or {}):
        all_names.add(name)
    for name in all_names:
        spec = FieldSpec(
            name=name,
            required=name in (required_fields or []),
            expected_type=(typed_fields or {}).get(name),
            allowed_values=(enum_fields or {}).get(name),
        )
        schema.add_field(spec)
    return schema


@stage
def validate_stage(
    entries: Iterable[Dict[str, Any]],
    *,
    required_fields: Optional[List[str]] = None,
    typed_fields: Optional[Dict[str, type]] = None,
    enum_fields: Optional[Dict[str, List[Any]]] = None,
    on_violation: str = "drop",
) -> Iterator[Dict[str, Any]]:
    """Pipeline stage: validate entries against a schema.

    Args:
        required_fields: Field names that must be present.
        typed_fields: Mapping of field name -> expected Python type.
        enum_fields: Mapping of field name -> list of allowed values.
        on_violation: 'drop', 'tag', or 'passthrough'.
    """
    schema = _build_schema(required_fields, typed_fields, enum_fields)
    yield from validate_entries(entries, schema, on_violation=on_violation)


@stage
def reject_invalid_stage(
    entries: Iterable[Dict[str, Any]],
    *,
    required_fields: Optional[List[str]] = None,
    typed_fields: Optional[Dict[str, type]] = None,
    enum_fields: Optional[Dict[str, List[Any]]] = None,
) -> Iterator[Dict[str, Any]]:
    """Convenience stage that drops all entries that fail schema validation."""
    schema = _build_schema(required_fields, typed_fields, enum_fields)
    yield from validate_entries(entries, schema, on_violation="drop")


@stage
def tag_invalid_stage(
    entries: Iterable[Dict[str, Any]],
    *,
    required_fields: Optional[List[str]] = None,
    typed_fields: Optional[Dict[str, type]] = None,
    enum_fields: Optional[Dict[str, List[Any]]] = None,
) -> Iterator[Dict[str, Any]]:
    """Convenience stage that tags entries with '_violations' instead of dropping them."""
    schema = _build_schema(required_fields, typed_fields, enum_fields)
    yield from validate_entries(entries, schema, on_violation="tag")
