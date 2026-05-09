"""Base classes for connectors.

A connector has up to four facets, all optional:

* a **webhook router** — FastAPI ``APIRouter`` that receives inbound events
  from the vendor (HMAC-verified, idempotent, fast 2xx).
* a **client** — async outbound REST/SOAP/EDI shim (rate-limited, retrying).
* a **poller** — when no webhooks are available, a coroutine that polls
  the vendor on a schedule and emits the same normalized events.
* a **normalizer** — pure functions ``vendor_payload -> MandalaEvent[]``.

Connectors must not depend on each other. Each must run usefully with only
its own credentials configured. See ``RISKS.md`` #1 (Descartes API
fragmentation) for the rationale.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar


class BaseConnector(ABC):
    """Abstract base for all Mandala connectors."""

    #: Stable connector slug, used in CloudEvents ``source`` and config keys.
    slug: ClassVar[str]
    #: Human-readable name.
    name: ClassVar[str]
    #: Vendor-defined product name (e.g. ``"Samsara Connected Operations"``).
    vendor: ClassVar[str]

    @property
    def source_uri(self) -> str:
        """Value used as the CloudEvents ``source`` for events from this connector."""
        return f"mandala/connector/{self.slug}"

    @abstractmethod
    def is_configured(self) -> bool:
        """Return ``True`` when credentials/secrets needed to operate are present."""
