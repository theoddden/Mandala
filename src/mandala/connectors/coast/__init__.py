"""Coast fuel card connector.

Coast provides fuel card management with a REST API for transaction data.
https://www.coastpay.com/
"""
from mandala.connectors.coast.connector import CoastConnector

__all__ = ["CoastConnector"]
