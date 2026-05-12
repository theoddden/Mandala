"""Extended comprehensive tests for geometric hash module."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock, patch

import pytest

from mandala.core.geometric_hash import (
    GeometricHashService,
    S2CellId,
    check_spatial_coherence,
    compute_delta_t_vector,
    generate_geometric_idempotency_key,
)


class TestGeometricHashServiceExtended:
    """Extended test cases for GeometricHashService."""

    @pytest.fixture
    def hash_service(self):
        """Create a GeometricHashService instance."""
        return GeometricHashService()

    def test_service_initialization(self, hash_service):
        """Test that GeometricHashService initializes correctly."""
        assert hash_service is not None
        assert hash_service._use_s2library is not None

    def test_geohash_basic(self, hash_service):
        """Test basic geohash computation."""
        lat = 40.7128
        lng = -74.0060
        cell_id = hash_service.compute_geohash(lat, lng, level=10)
        assert cell_id is not None
        assert isinstance(cell_id, str)

    def test_geohash_different_levels(self, hash_service):
        """Test geohash computation at different precision levels."""
        lat = 40.7128
        lng = -74.0060

        cell_id_5 = hash_service.compute_geohash(lat, lng, level=5)
        cell_id_10 = hash_service.compute_geohash(lat, lng, level=10)
        cell_id_15 = hash_service.compute_geohash(lat, lng, level=15)

        assert cell_id_5 != cell_id_10
        assert cell_id_10 != cell_id_15
        assert len(cell_id_5) < len(cell_id_10)

    def test_geohash_same_location_same_hash(self, hash_service):
        """Test that same location produces same hash."""
        lat = 40.7128
        lng = -74.0060

        cell_id1 = hash_service.compute_geohash(lat, lng, level=10)
        cell_id2 = hash_service.compute_geohash(lat, lng, level=10)

        assert cell_id1 == cell_id2

    def test_geohash_different_locations_different_hashes(self, hash_service):
        """Test that different locations produce different hashes."""
        lat1, lng1 = 40.7128, -74.0060  # NYC
        lat2, lng2 = 34.0522, -118.2437  # LA

        cell_id1 = hash_service.compute_geohash(lat1, lng1, level=10)
        cell_id2 = hash_service.compute_geohash(lat2, lng2, level=10)

        assert cell_id1 != cell_id2

    def test_geohash_nearby_locations_similar_hashes(self, hash_service):
        """Test that nearby locations have similar hashes."""
        lat1, lng1 = 40.7128, -74.0060
        lat2, lng2 = 40.7130, -74.0062  # Very close

        cell_id1 = hash_service.compute_geohash(lat1, lng1, level=10)
        cell_id2 = hash_service.compute_geohash(lat2, lng2, level=10)

        # At lower precision, they should be the same
        cell_id1_low = hash_service.compute_geohash(lat1, lng1, level=5)
        cell_id2_low = hash_service.compute_geohash(lat2, lng2, level=5)
        assert cell_id1_low == cell_id2_low

    def test_geohash_invalid_latitude(self, hash_service):
        """Test geohash with invalid latitude."""
        with pytest.raises(ValueError):
            hash_service.compute_geohash(91.0, -74.0060, level=10)

    def test_geohash_invalid_longitude(self, hash_service):
        """Test geohash with invalid longitude."""
        with pytest.raises(ValueError):
            hash_service.compute_geohash(40.7128, 181.0, level=10)

    def test_geohash_invalid_level(self, hash_service):
        """Test geohash with invalid precision level."""
        with pytest.raises(ValueError):
            hash_service.compute_geohash(40.7128, -74.0060, level=31)

    def test_compute_distance(self, hash_service):
        """Test computing distance between two points."""
        lat1, lng1 = 40.7128, -74.0060
        lat2, lng2 = 34.0522, -118.2437

        distance = hash_service.compute_distance(lat1, lng1, lat2, lng2)
        assert distance > 0
        assert distance < 5000  # Should be less than 5000 km

    def test_compute_distance_same_point(self, hash_service):
        """Test distance to same point is zero."""
        lat, lng = 40.7128, -74.0060
        distance = hash_service.compute_distance(lat, lng, lat, lng)
        assert distance == 0

    def test_compute_delta_t_vector(self):
        """Test computing delta t vector."""
        positions = [
            {"lat": 40.7128, "lng": -74.0060, "time": datetime(2026, 5, 12, 12, 0, 0, tzinfo=timezone.utc)},
            {"lat": 40.7130, "lng": -74.0062, "time": datetime(2026, 5, 12, 12, 1, 0, tzinfo=timezone.utc)},
            {"lat": 40.7132, "lng": -74.0064, "time": datetime(2026, 5, 12, 12, 2, 0, tzinfo=timezone.utc)},
        ]

        delta_t = compute_delta_t_vector(positions)
        assert delta_t is not None
        assert len(delta_t) == len(positions) - 1

    def test_compute_delta_t_vector_single_point(self):
        """Test delta t vector with single point."""
        positions = [
            {"lat": 40.7128, "lng": -74.0060, "time": datetime(2026, 5, 12, 12, 0, 0, tzinfo=timezone.utc)}
        ]
        delta_t = compute_delta_t_vector(positions)
        assert delta_t == []

    def test_generate_geometric_idempotency_key(self):
        """Test generating geometric idempotency key."""
        lat = 40.7128
        lng = -74.0060
        event_type = "position.update"
        source_id = "truck-123"

        key = generate_geometric_idempotency_key(
            lat, lng, event_type, source_id, level=10
        )
        assert key is not None
        assert isinstance(key, str)

    def test_generate_geometric_idempotency_key_deterministic(self):
        """Test that key generation is deterministic."""
        lat = 40.7128
        lng = -74.0060
        event_type = "position.update"
        source_id = "truck-123"

        key1 = generate_geometric_idempotency_key(
            lat, lng, event_type, source_id, level=10
        )
        key2 = generate_geometric_idempotency_key(
            lat, lng, event_type, source_id, level=10
        )

        assert key1 == key2

    def test_generate_geometric_idempotency_key_different_inputs(self):
        """Test that different inputs produce different keys."""
        key1 = generate_geometric_idempotency_key(
            40.7128, -74.0060, "position.update", "truck-123", level=10
        )
        key2 = generate_geometric_idempotency_key(
            34.0522, -118.2437, "position.update", "truck-123", level=10
        )

        assert key1 != key2

    def test_check_spatial_coherence(self):
        """Test checking spatial coherence."""
        positions = [
            {"lat": 40.7128, "lng": -74.0060, "time": datetime(2026, 5, 12, 12, 0, 0, tzinfo=timezone.utc)},
            {"lat": 40.7130, "lng": -74.0062, "time": datetime(2026, 5, 12, 12, 1, 0, tzinfo=timezone.utc)},
        ]

        is_coherent = check_spatial_coherence(positions, max_speed=100)
        assert is_coherent is True

    def test_check_spatial_coherence_incoherent(self):
        """Test detecting incoherent movement."""
        positions = [
            {"lat": 40.7128, "lng": -74.0060, "time": datetime(2026, 5, 12, 12, 0, 0, tzinfo=timezone.utc)},
            {"lat": 34.0522, "lng": -118.2437, "time": datetime(2026, 5, 12, 12, 1, 0, tzinfo=timezone.utc)},  # Impossible in 1 minute
        ]

        is_coherent = check_spatial_coherence(positions, max_speed=100)
        assert is_coherent is False

    def test_check_spatial_coherence_single_point(self):
        """Test spatial coherence with single point."""
        positions = [
            {"lat": 40.7128, "lng": -74.0060, "time": datetime(2026, 5, 12, 12, 0, 0, tzinfo=timezone.utc)}
        ]
        is_coherent = check_spatial_coherence(positions, max_speed=100)
        assert is_coherent is True

    def test_check_spatial_coherence_empty(self):
        """Test spatial coherence with empty list."""
        is_coherent = check_spatial_coherence([], max_speed=100)
        assert is_coherent is True

    def test_get_cell_neighbors(self, hash_service):
        """Test getting neighboring cells."""
        lat = 40.7128
        lng = -74.0060
        neighbors = hash_service.get_cell_neighbors(lat, lng, level=10)
        assert neighbors is not None
        assert len(neighbors) > 0

    def test_get_cell_center(self, hash_service):
        """Test getting cell center point."""
        lat = 40.7128
        lng = -74.0060
        center = hash_service.get_cell_center(lat, lng, level=10)
        assert center is not None
        assert "lat" in center
        assert "lng" in center

    def test_get_cell_area(self, hash_service):
        """Test getting cell area."""
        lat = 40.7128
        lng = -74.0060
        area = hash_service.get_cell_area(lat, lng, level=10)
        assert area is not None
        assert area > 0

    def test_cell_id_to_lat_lng(self, hash_service):
        """Test converting cell ID back to lat/lng."""
        lat = 40.7128
        lng = -74.0060
        cell_id = hash_service.compute_geohash(lat, lng, level=10)

        recovered_lat, recovered_lng = hash_service.cell_id_to_lat_lng(cell_id)
        assert recovered_lat is not None
        assert recovered_lng is not None

        # Should be close to original
        assert abs(recovered_lat - lat) < 0.1
        assert abs(recovered_lng - lng) < 0.1

    def test_get_parent_cell(self, hash_service):
        """Test getting parent cell at lower precision."""
        lat = 40.7128
        lng = -74.0060
        cell_id = hash_service.compute_geohash(lat, lng, level=10)
        parent = hash_service.get_parent_cell(cell_id)
        assert parent is not None
        assert parent != cell_id

    def test_get_child_cells(self, hash_service):
        """Test getting child cells at higher precision."""
        lat = 40.7128
        lng = -74.0060
        cell_id = hash_service.compute_geohash(lat, lng, level=5)
        children = hash_service.get_child_cells(cell_id)
        assert children is not None
        assert len(children) > 0

    def test_compute_bearing(self, hash_service):
        """Test computing bearing between two points."""
        lat1, lng1 = 40.7128, -74.0060
        lat2, lng2 = 40.7130, -74.0062

        bearing = hash_service.compute_bearing(lat1, lng1, lat2, lng2)
        assert bearing is not None
        assert 0 <= bearing < 360

    def test_is_point_in_cell(self, hash_service):
        """Test checking if point is in cell."""
        lat = 40.7128
        lng = -74.0060
        cell_id = hash_service.compute_geohash(lat, lng, level=10)

        is_in_cell = hash_service.is_point_in_cell(lat, lng, cell_id)
        assert is_in_cell is True

    def test_is_point_in_cell_false(self, hash_service):
        """Test checking if point is not in cell."""
        lat1, lng1 = 40.7128, -74.0060
        lat2, lng2 = 34.0522, -118.2437
        cell_id = hash_service.compute_geohash(lat1, lng1, level=10)

        is_in_cell = hash_service.is_point_in_cell(lat2, lng2, cell_id)
        assert is_in_cell is False

    def test_get_cell_boundary(self, hash_service):
        """Test getting cell boundary polygon."""
        lat = 40.7128
        lng = -74.0060
        boundary = hash_service.get_cell_boundary(lat, lng, level=10)
        assert boundary is not None
        assert len(boundary) >= 4  # At least 4 points for a polygon

    def test_compute_area_of_polygon(self, hash_service):
        """Test computing area of polygon."""
        polygon = [
            (40.7128, -74.0060),
            (40.7130, -74.0060),
            (40.7130, -74.0062),
            (40.7128, -74.0062),
        ]
        area = hash_service.compute_area_of_polygon(polygon)
        assert area is not None
        assert area > 0

    def test_geohash_precision_to_meters(self, hash_service):
        """Test converting precision level to approximate meters."""
        meters_5 = hash_service.precision_to_meters(5)
        meters_10 = hash_service.precision_to_meters(10)
        meters_15 = hash_service.precision_to_meters(15)

        assert meters_5 > meters_10 > meters_15

    def test_meters_to_precision(self, hash_service):
        """Test converting meters to precision level."""
        precision_1000m = hash_service.meters_to_precision(1000)
        precision_100m = hash_service.meters_to_precision(100)
        precision_10m = hash_service.meters_to_precision(10)

        assert precision_1000m < precision_100m < precision_10m

    def test_get_covering_cells(self, hash_service):
        """Test getting cells covering a region."""
        min_lat, min_lng = 40.7120, -74.0070
        max_lat, max_lng = 40.7130, -74.0050

        cells = hash_service.get_covering_cells(
            min_lat, min_lng, max_lat, max_lng, level=10
        )
        assert cells is not None
        assert len(cells) > 0

    def test_interpolate_position(self, hash_service):
        """Test interpolating position between two points."""
        lat1, lng1 = 40.7128, -74.0060
        lat2, lng2 = 40.7130, -74.0062
        time1 = datetime(2026, 5, 12, 12, 0, 0, tzinfo=timezone.utc)
        time2 = datetime(2026, 5, 12, 12, 2, 0, tzinfo=timezone.utc)
        time_mid = datetime(2026, 5, 12, 12, 1, 0, tzinfo=timezone.utc)

        interpolated = hash_service.interpolate_position(
            lat1, lng1, time1, lat2, lng2, time2, time_mid
        )
        assert interpolated is not None
        assert "lat" in interpolated
        assert "lng" in interpolated
