"""Field redaction and masking utilities for sensitive log data."""

import re
import hashlib
from typing import Any, Dict, Iterable, Iterator, List, Optional


_DEFAULT_SENSITIVE_FIELDS = [
    "password", "passwd", "secret", "token", "api_key",
    "authorization", "credit_card", "ssn",
]

_MASK = "***REDACTED***"


def _mask_value(value: Any, mask: str = _MASK) -> str:
    """Replace a value with a redaction mask."""
    return mask


def _hash_value(value: Any, salt: str = "") -> str:
    """One-way hash a value for pseudonymisation."""
    raw = salt + str(value)
    return "sha256:" + hashlib.sha256(raw.encode()).hexdigest()[:16]


def _partial_mask(value: Any, visible: int = 4, mask_char: str = "*") -> str:
    """Show only the last *visible* characters; mask the rest."""
    text = str(value)
    if len(text) <= visible:
        return mask_char * len(text)
    return mask_char * (len(text) - visible) + text[-visible:]


def redact_entry(
    entry: Dict[str, Any],
    fields: Optional[List[str]] = None,
    strategy: str = "mask",
    salt: str = "",
    visible: int = 4,
) -> Dict[str, Any]:
    """Return a copy of *entry* with sensitive fields redacted.

    strategy:
        ``mask``    – replace with ``***REDACTED***``
        ``hash``    – one-way SHA-256 hash (pseudonymise)
        ``partial`` – show last *visible* chars, mask the rest
    """
    target_fields = set(f.lower() for f in (fields or _DEFAULT_SENSITIVE_FIELDS))
    result = {}
    for key, value in entry.items():
        if key.lower() in target_fields:
            if strategy == "hash":
                result[key] = _hash_value(value, salt=salt)
            elif strategy == "partial":
                result[key] = _partial_mask(value, visible=visible)
            else:
                result[key] = _mask_value(value)
        else:
            result[key] = value
    return result


def redact_entries(
    entries: Iterable[Dict[str, Any]],
    fields: Optional[List[str]] = None,
    strategy: str = "mask",
    salt: str = "",
    visible: int = 4,
) -> Iterator[Dict[str, Any]]:
    """Lazily apply :func:`redact_entry` to each entry in *entries*."""
    for entry in entries:
        yield redact_entry(entry, fields=fields, strategy=strategy,
                           salt=salt, visible=visible)
