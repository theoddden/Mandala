"""FLEETCOR fuel card connector (Comdata, Fuelman).

FLEETCOR provides fleet fuel card management with multiple brands:
- Comdata
- Fuelman
- FleetONE
- etc.
"""

from mandala.connectors.fleetcor.connector import FleetcorConnector

__all__ = ["FleetcorConnector"]
