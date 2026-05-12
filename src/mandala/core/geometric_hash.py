"""Geometric hashing for spatial idempotency and event-time determinism.

Uses H3 (Uber's hexagonal hierarchical spatial index) or S2 (Google's
spherical geometry library) to derive deterministic geometric hashes from
coordinates. These hashes are bound to event timestamps at the source to
prevent state-machine corruption from out-of-order spatial data.

The "Geometric Idempotency" principle:
- Standard idempotency prevents duplicate records
- Geometric idempotency prevents state-machine corruption from out-of-order spatial data
- If Ping A (Location: Laredo) arrives after Ping B (Location: San Antonio) due to network lag,
  a naive system triggers false "Speeding Alert" or "Route Deviation"
- Geometric hashing + event-time determinism solves this at the network layer
"""

from __future__ import annotations

import hashlib
import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timezone
from enum import Enum
from typing import Any

import structlog

from mandala.settings import get_settings

# Try to import Rust-accelerated implementation
try:
    from mandala_rust_ext import h3_hash as rust_h3_hash
    from mandala_rust_ext import h3_hash_time_bound as rust_h3_hash_time_bound

    _RUST_EXT_AVAILABLE = True
except ImportError:
    _RUST_EXT_AVAILABLE = False

log = structlog.get_logger(__name__)


# S2CellId placeholder for compatibility with tests
class S2CellId:
    """Placeholder for S2 cell ID compatibility."""

    def __init__(self, id: str) -> None:
        self.id = id

    def __str__(self) -> str:
        return self.id


class GeometricHashProvider(str, Enum):
    """Supported geometric hashing providers."""

    H3 = "h3"
    S2 = "s2"
    NONE = "none"  # Disable geometric hashing


class GeometricHashService:
    """Service for computing geometric hashes from coordinates.

    Supports H3 (Uber) and S2 (Google) spatial indexing systems.
    Falls back to simple geohash if libraries are not available.
    """

    def __init__(self, provider: GeometricHashProvider | None = None, resolution: int = 9) -> None:
        """Initialize the geometric hash service.

        Args:
            provider: Hashing provider (h3, s2, or none). Defaults to settings.
            resolution: Spatial resolution (H3: 0-15, S2: 0-30, Geohash: 1-12).
                       Higher = more precise, larger hash strings.
        """
        s = get_settings()
        self._provider = provider or GeometricHashProvider(getattr(s, "geometric_hash_provider", "h3"))
        self._resolution = resolution or getattr(s, "geometric_hash_resolution", 9)
        self._h3_lib = None
        self._s2_lib = None

        # Lazy-load libraries to avoid hard dependencies
        if self._provider == GeometricHashProvider.H3:
            try:
                import h3 as h3_lib

                self._h3_lib = h3_lib
            except ImportError:
                # Fallback to simple geohash
                self._provider = GeometricHashProvider.NONE
        elif self._provider == GeometricHashProvider.S2:
            try:
                import s2 as s2_lib

                self._s2_lib = s2_lib
            except ImportError:
                # Fallback to simple geohash
                self._provider = GeometricHashProvider.NONE

    def compute_hash(
        self,
        latitude: float,
        longitude: float,
        event_time: datetime | None = None,
    ) -> str:
        """Compute a geometric hash for the given coordinates.

        Args:
            latitude: Latitude in decimal degrees
            longitude: Longitude in decimal degrees
            event_time: Event timestamp for temporal binding. If None, uses current time.

        Returns:
            Geometric hash string (deterministic for same lat/lon/time)
        """
        if self._provider == GeometricHashProvider.H3 and self._h3_lib:
            return self._h3_hash(latitude, longitude, event_time)
        if self._provider == GeometricHashProvider.S2 and self._s2_lib:
            return self._s2_hash(latitude, longitude, event_time)
        return self._geohash_fallback(latitude, longitude, event_time)

    def _h3_hash(
        self,
        latitude: float,
        longitude: float,
        event_time: datetime | None = None,
    ) -> str:
        """Compute H3 hexagonal hash."""
        # Use Rust implementation if available
        if _RUST_EXT_AVAILABLE:
            if event_time:
                event_time_ms = int(event_time.timestamp() * 1000)
                return rust_h3_hash_time_bound(latitude, longitude, self._resolution, event_time_ms)
            return rust_h3_hash(latitude, longitude, self._resolution)

        # Convert lat/lon to H3 cell at resolution
        h3_index = self._h3_lib.latlng_to_cell(latitude, longitude, self._resolution)
        h3_str = self._h3_lib.cell_to_string(h3_index)

        # Bind to event time for temporal determinism
        if event_time:
            time_binding = event_time.isoformat()
            combined = f"{h3_str}:{time_binding}"
            return hashlib.sha256(combined.encode()).hexdigest()[:16]

        return h3_str

    def _s2_hash(
        self,
        latitude: float,
        longitude: float,
        event_time: datetime | None = None,
    ) -> str:
        """Compute S2 cell ID hash.

        NOTE: This is a simplified implementation that falls back to geohash encoding
        when the s2geometry library is not available. For production use with actual
        S2 geometry, install the s2geometry library and this will use it.
        """

        # Try to use s2geometry library if available
        try:
            from s2 import s2, s2cellid
            from s2.geometry import S2LatLng

            # Convert lat/lon to S2LatLng
            lat_lng = S2LatLng.FromDegrees(latitude, longitude)

            # Get S2 cell ID at resolution
            cell_id = lat_lng.ToPoint().ToS2CellId(self._resolution)

            # Use cell ID as hash
            s2_cell_str = str(cell_id)

            # Bind to event time
            if event_time:
                time_binding = event_time.isoformat()
                combined = f"{s2_cell_str}:{time_binding}"
                return hashlib.sha256(combined.encode()).hexdigest()[:16]

            return hashlib.sha256(s2_cell_str.encode()).hexdigest()[:16]

        except ImportError:
            # Fallback to geohash-like encoding when s2geometry not available
            log.warning(
                "s2_library_not_available",
                message="s2geometry library not installed, using fallback encoding",
            )
            return self._geohash_fallback(latitude, longitude, event_time)

    def _geohash_fallback(
        self,
        latitude: float,
        longitude: float,
        event_time: datetime | None = None,
    ) -> str:
        """Fallback to simple geohash-based encoding."""
        # Simple geohash-like encoding
        lat_bits = _float_to_bits(latitude, 32)
        lon_bits = _float_to_bits(longitude, 32)
        combined = f"{lat_bits:08x}{lon_bits:08x}"

        # Bind to event time
        if event_time:
            time_binding = event_time.isoformat()
            combined = f"{combined}:{time_binding}"

        return hashlib.sha256(combined.encode()).hexdigest()[:16]

    def compute_delta_t_vector(
        self,
        current_hash: str,
        previous_hash: str | None,
        current_time: datetime,
        previous_time: datetime | None,
    ) -> dict[str, Any]:
        """Compute a vector of Delta-T for trajectory analysis.

        The "Clever" Bit: Mandala doesn't just store the location; it stores
        a Vector of Delta-T. If an event arrives out of sequence, the "Stator"
        uses a Re-ordering Buffer to "re-wind" the state of the asset, insert
        the missing data point, and re-calculate the trajectory.

        Args:
            current_hash: Current geometric hash
            previous_hash: Previous geometric hash (if available)
            current_time: Current event timestamp
            previous_time: Previous event timestamp (if available)

        Returns:
            Dictionary with delta_t_seconds, hash_changed, and spatial_delta
        """
        result = {
            "delta_t_seconds": 0,
            "hash_changed": False,
            "spatial_delta": None,
            "velocity_mps": None,  # meters per second
        }

        if previous_time:
            result["delta_t_seconds"] = (current_time - previous_time).total_seconds()

        if previous_hash and previous_hash != current_hash:
            result["hash_changed"] = True
            result["spatial_delta"] = _hash_distance_estimate(current_hash, previous_hash)

            # Estimate velocity if we have time delta
            if result["delta_t_seconds"] > 0 and result["spatial_delta"]:
                result["velocity_mps"] = result["spatial_delta"] / result["delta_t_seconds"]

        return result

    def is_spatially_coherent(
        self,
        delta_t_seconds: float,
        velocity_mps: float | None,
        max_velocity_mps: float = 150.0,  # ~335 mph, generous for trucks
    ) -> bool:
        """Check if spatial movement is coherent with time delta.

        Detects "hallucinated" teleportation from out-of-order events.
        If a truck appears to travel 150 miles in 1 second, this returns False.

        Args:
            delta_t_seconds: Time between events in seconds
            velocity_mps: Computed velocity in meters per second
            max_velocity_mps: Maximum plausible velocity (default: 150 mps ~335 mph)

        Returns:
            True if spatially coherent, False if likely out-of-order or corrupted
        """
        if velocity_mps is None:
            return True  # Can't determine, assume coherent

        if delta_t_seconds <= 0:
            return False  # Time travel detected

        if velocity_mps > max_velocity_mps:
            return False  # Impossibly fast movement

        return True


def _float_to_bits(value: float, bits: int) -> int:
    """Convert a float to an integer representation for hashing."""
    import struct

    packed = struct.pack(">d", value)
    return int.from_bytes(packed, byteorder="big") >> (64 - bits)


def _hash_distance_estimate(hash1: str, hash2: str) -> float | None:
    """Estimate spatial distance between two geometric hashes.

    This is a rough estimate. For precise distances, use the full H3/S2 APIs.
    Returns distance in meters (approximate).
    """
    # Simple Hamming distance as a proxy for spatial distance
    # For production, use H3's h3_distance or S2's GetDistance
    if len(hash1) != len(hash2):
        return None

    hamming = sum(c1 != c2 for c1, c2 in zip(hash1, hash2))

    # Rough conversion: each hex character ~4 bits, approximate distance
    # This is a heuristic - real implementations would use the library's distance functions
    return hamming * 1000.0  # Very rough estimate in meters


def generate_geometric_idempotency_key(
    source_id: str,
    event_time: datetime,
    latitude: float | None = None,
    longitude: float | None = None,
    provider: GeometricHashProvider = GeometricHashProvider.H3,
) -> str:
    """Generate a deterministic geometric idempotency key.

    Combines source ID, event time, and optional geometric hash for
    geometric idempotency. This prevents state-machine corruption from
    out-of-order spatial data.

    Args:
        source_id: Entity identifier (e.g., truck ID, shipment URN)
        event_time: Event timestamp (when the event occurred at the source)
        latitude: Optional latitude for geometric binding
        longitude: Optional longitude for geometric binding
        provider: Geometric hash provider to use

    Returns:
        Deterministic idempotency key string
    """
    service = GeometricHashService(provider=provider)

    # Base key from source ID and event time
    base_key = f"{source_id}:{event_time.isoformat()}"

    # Add geometric component if coordinates provided
    if latitude is not None and longitude is not None:
        geo_hash = service.compute_hash(latitude, longitude, event_time)
        base_key = f"{base_key}:{geo_hash}"

    return hashlib.sha256(base_key.encode()).hexdigest()


# Standalone functions for test compatibility
def compute_delta_t_vector(positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Compute delta-t vectors for a series of positions.

    Args:
        positions: List of position dicts with lat, lng, time

    Returns:
        List of delta-t vectors
    """
    if len(positions) < 2:
        return []

    service = GeometricHashService()
    results = []

    for i in range(1, len(positions)):
        prev = positions[i - 1]
        curr = positions[i]

        prev_hash = service.compute_hash(prev["lat"], prev["lng"], prev.get("time"))
        curr_hash = service.compute_hash(curr["lat"], curr["lng"], curr.get("time"))

        delta_t = service.compute_delta_t_vector(
            curr_hash,
            prev_hash,
            curr.get("time") or datetime.now(UTC),
            prev.get("time") or datetime.now(UTC),
        )
        results.append(delta_t)

    return results


def check_spatial_coherence(positions: list[dict[str, Any]], max_speed: float = 150.0) -> bool:
    """Check if spatial movement is coherent across positions.

    Args:
        positions: List of position dicts with lat, lng, time
        max_speed: Maximum plausible velocity in m/s

    Returns:
        True if spatially coherent, False otherwise
    """
    if len(positions) < 2:
        return True

    service = GeometricHashService()

    for i in range(1, len(positions)):
        prev = positions[i - 1]
        curr = positions[i]

        prev_hash = service.compute_hash(prev["lat"], prev["lng"], prev.get("time"))
        curr_hash = service.compute_hash(curr["lat"], curr["lng"], curr.get("time"))

        delta_t = service.compute_delta_t_vector(
            curr_hash,
            prev_hash,
            curr.get("time") or datetime.now(UTC),
            prev.get("time") or datetime.now(UTC),
        )

        if not service.is_spatially_coherent(
            delta_t.get("delta_t_seconds", 0),
            delta_t.get("velocity_mps"),
            max_speed,
        ):
            return False

    return True
