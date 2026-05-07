"""Route log entries to different output streams based on field conditions."""

from typing import Any, Callable, Dict, Generator, Iterable, List, Optional, Tuple

Entry = Dict[str, Any]
Sink = Callable[[Entry], None]


class Route:
    """A named route: a predicate and an associated sink."""

    def __init__(self, name: str, predicate: Callable[[Entry], bool], sink: Sink) -> None:
        self.name = name
        self.predicate = predicate
        self.sink = sink


def field_equals(field: str, value: Any) -> Callable[[Entry], bool]:
    """Return a predicate that matches entries where field == value."""
    return lambda entry: entry.get(field) == value


def field_contains(field: str, substring: str) -> Callable[[Entry], bool]:
    """Return a predicate that matches entries where field contains substring."""
    return lambda entry: substring in str(entry.get(field, ""))


def field_in(field: str, values: List[Any]) -> Callable[[Entry], bool]:
    """Return a predicate that matches entries where field value is in the given list."""
    value_set = set(values)
    return lambda entry: entry.get(field) in value_set


def route_entries(
    entries: Iterable[Entry],
    routes: List[Route],
    fallback_sink: Optional[Sink] = None,
) -> Generator[Entry, None, None]:
    """Route each entry to matching sinks.

    An entry may match multiple routes (fan-out). If no route matches and a
    fallback_sink is provided, the entry is sent there. Each entry is also
    yielded downstream so the pipeline can continue.
    """
    for entry in entries:
        matched = False
        for route in routes:
            if route.predicate(entry):
                route.sink(entry)
                matched = True
        if not matched and fallback_sink is not None:
            fallback_sink(entry)
        yield entry


def collect_sink(bucket: List[Entry]) -> Sink:
    """Return a sink that appends entries to a list (useful for testing)."""
    return lambda entry: bucket.append(entry)
