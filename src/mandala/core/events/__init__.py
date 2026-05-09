"""Canonical event model.

Mandala wraps every fact about the world in a `CloudEvents 1.0
<https://cloudevents.io/>`_ envelope. The ``type`` is drawn from the
``mandala.*`` registry in :mod:`mandala.core.events.types` and the
``data`` payload is one of the canonical schema models.
"""
from mandala.core.events.envelope import MandalaEvent, new_event
from mandala.core.events.idempotency import IdempotencyStore
from mandala.core.events.types import EventType

__all__ = ["MandalaEvent", "new_event", "EventType", "IdempotencyStore"]
