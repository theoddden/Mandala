"""URN-style identifiers used as ``subject`` on every CloudEvent.

Format: ``urn:mandala:<entity>:<scope>:<id>``

Examples:
    ``urn:mandala:truck:samsara:281474976710656``
    ``urn:mandala:shipment:descartes:DES-2026-001234``
    ``urn:mandala:bol:descartes:BOL-987654``
    ``urn:mandala:customs-entry:cbp:316-1234567-9``
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Self

_URN_RE = re.compile(r"^urn:mandala:(?P<entity>[a-z][a-z0-9-]*):(?P<scope>[a-z0-9-]+):(?P<id>.+)$")


@dataclass(frozen=True, slots=True)
class URN:
    """Parsed Mandala URN."""

    entity: str
    scope: str
    id: str

    def __str__(self) -> str:  # noqa: D401
        return f"urn:mandala:{self.entity}:{self.scope}:{self.id}"

    @classmethod
    def truck(cls, scope: str, id: str) -> Self:
        return cls(entity="truck", scope=scope, id=id)

    @classmethod
    def shipment(cls, scope: str, id: str) -> Self:
        return cls(entity="shipment", scope=scope, id=id)

    @classmethod
    def bol(cls, scope: str, id: str) -> Self:
        return cls(entity="bol", scope=scope, id=id)

    @classmethod
    def customs_entry(cls, scope: str, id: str) -> Self:
        return cls(entity="customs-entry", scope=scope, id=id)

    @classmethod
    def party(cls, scope: str, id: str) -> Self:
        return cls(entity="party", scope=scope, id=id)


def parse_urn(value: str) -> URN:
    """Parse a Mandala URN string into a :class:`URN` dataclass."""
    m = _URN_RE.match(value)
    if not m:
        raise ValueError(f"invalid Mandala URN: {value!r}")
    return URN(entity=m["entity"], scope=m["scope"], id=m["id"])
