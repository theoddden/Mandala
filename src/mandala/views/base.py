"""Base protocol for materialized views."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from mandala.core.events.envelope import MandalaEvent


class MaterializedView(ABC):
    """A read-model projection of the Mandala event stream.

    Implementations must be:

    * **Idempotent** — applying the same event twice yields the same state.
      (The view runner uses Redis Streams consumer groups, which provide
      at-least-once delivery; duplicate applies are expected and must be safe.)
    * **Monotonic** — later events override earlier state for the same key.
    * **Pure-ish** — reads the event only; optional sidecar reads from state
      store are allowed but writes to the canonical StateStore are forbidden.
    """

    #: Short name used in metrics labels and CLI output.
    name: str = "unknown"

    @abstractmethod
    async def apply(self, event: MandalaEvent) -> None:
        """Apply a single event to the view. Must be idempotent."""

    async def health(self) -> dict[str, Any]:
        """Optional: return a small dict describing the view's state.

        Used by ``mandala views --health`` and the MCP health tool. Default
        implementation returns just the name.
        """
        return {"name": self.name, "ok": True}
