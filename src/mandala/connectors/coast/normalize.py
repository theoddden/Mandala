"""Normalize Coast API payloads into MandalaEvent objects."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from mandala.core.events.envelope import MandalaEvent, new_event
from mandala.core.events.types import EventType
from mandala.core.schema.geo import GeoPoint
from mandala.core.schema.identifiers import URN
from mandala.core.schema.truck import FuelTransaction, FuelType

SOURCE = "mandala/connector/coast"


def _parse_ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _truck_urn(vehicle_id: Any) -> str:
    return str(URN.truck(scope="coast", id=str(vehicle_id)))


def normalize_transaction(transaction: dict[str, Any]) -> MandalaEvent:
    """Convert a Coast transaction into a mandala.truck.fueled event.

    Args:
        transaction: Coast API transaction response

    Returns:
        MandalaEvent for the fuel transaction
    """
    truck_id = transaction.get("truckId") or transaction.get("vehicle_id")
    if not truck_id:
        raise ValueError("Transaction missing truckId")

    # Parse location if available
    location = None
    lat = transaction.get("latitude") or transaction.get("location", {}).get("lat")
    lon = transaction.get("longitude") or transaction.get("location", {}).get("lon")
    if lat and lon:
        location = GeoPoint(
            lat=float(lat),
            lon=float(lon),
            captured_at=_parse_ts(transaction.get("transactionDate") or transaction.get("date")),
        )

    # Map fuel type
    fuel_type_str = transaction.get("fuelType") or transaction.get("product")
    fuel_type = None
    if fuel_type_str:
        fuel_type_lower = fuel_type_str.lower()
        if "diesel" in fuel_type_lower:
            fuel_type = FuelType.DIESEL
        elif "gas" in fuel_type_lower:
            fuel_type = FuelType.GASOLINE
        elif "electric" in fuel_type_lower:
            fuel_type = FuelType.ELECTRIC

    fuel_txn = FuelTransaction(
        truck_id=str(truck_id),
        transaction_id=str(transaction.get("id") or transaction.get("transactionId")),
        transaction_date=_parse_ts(transaction.get("transactionDate") or transaction.get("date")),
        location=location,
        station_name=transaction.get("stationName") or transaction.get("merchant"),
        station_address=transaction.get("stationAddress") or transaction.get("address"),
        gallons=float(transaction.get("gallons") or transaction.get("quantity") or 0),
        cost_usd=float(transaction.get("amount") or transaction.get("cost") or 0),
        price_per_gallon=float(transaction.get("pricePerGallon")) if transaction.get("pricePerGallon") else None,
        fuel_type=fuel_type,
        odometer_km=float(transaction.get("odometerKm")) if transaction.get("odometerKm") else None,
        driver_id=transaction.get("driverId"),
        card_number=transaction.get("cardNumber"),
        vendor="coast",
        metadata={
            k: str(v)
            for k, v in transaction.items()
            if k not in {"truckId", "vehicle_id", "id", "transactionId", "transactionDate", "date"}
            and v is not None
        },
    )

    return new_event(
        type=EventType.TRUCK_FUELED,
        source=SOURCE,
        subject=_truck_urn(truck_id),
        data=fuel_txn,
        ingest_id=str(transaction.get("id") or transaction.get("transactionId")),
    )


def normalize_transactions(transactions: list[dict[str, Any]]) -> list[MandalaEvent]:
    """Convert multiple Coast transactions into MandalaEvent objects.

    Args:
        transactions: List of Coast API transaction responses

    Returns:
        List of MandalaEvent objects for fuel transactions
    """
    events = []
    for txn in transactions:
        try:
            events.append(normalize_transaction(txn))
        except (KeyError, ValueError, TypeError):
            # Skip malformed transactions
            continue
    return events
