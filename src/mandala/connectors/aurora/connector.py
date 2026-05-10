"""Aurora autonomous truck connector (stub).

This is a stub documenting the integration pattern for Aurora autonomous trucks.
Integration requires partnership via the Aurora Partner Program.

See docs/integrations/aurora.md for full integration pattern.
"""
from __future__ import annotations

from mandala.connectors.base import BaseConnector
from mandala.settings import get_settings


class AuroraConnector(BaseConnector):
    """Aurora autonomous truck integration (partnership required).

    This connector is a stub. When Aurora partnership becomes available,
    implement:
    1. Webhook ingestion (POST /webhooks/aurora)
    2. Event normalization (Aurora → MandalaEvent)
    3. Outbound API client (Aurora Beacon platform)
    4. Intelligence sharing (Aurora → Samsara trucks)

    See docs/integrations/aurora.md for full integration pattern.
    """

    def is_configured(self) -> bool:
        """Check if Aurora integration is configured.

        Aurora requires partnership (webhook_secret + api_key).
        This connector is disabled by default until partnership available.
        """
        s = get_settings()
        # Aurora config will be added to settings.py when partnership available
        # For now, always return False (stub)
        return False

    async def start(self) -> None:
        """Start Aurora connector (stub).

        When partnership available, this will:
        1. Register webhook router in app.py
        2. Start outbound polling (Aurora Beacon platform)
        3. Initialize intelligence sharing (Aurora → Samsara)
        """
        # Stub - implement when partnership available
        pass

    async def stop(self) -> None:
        """Stop Aurora connector (stub)."""
        # Stub - implement when partnership available
        pass
