"""EFS fuel card connector.

EFS (Electronic Fuel Systems) provides fleet fuel card management with a REST API for transaction data.
"""
from mandala.connectors.efs.connector import EfsConnector

__all__ = ["EfsConnector"]
