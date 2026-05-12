"""Comprehensive tests for the event envelope module."""

from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pytest

from mandala.core.events.envelope import MandalaEnvelope, MandalaEvent


class TestMandalaEnvelope:
    """Test cases for MandalaEnvelope."""

    @pytest.fixture
    def sample_event(self):
        """Create a sample MandalaEvent."""
        return MandalaEvent(
            id="test-1",
            source="test",
            type="test.event",
            time=datetime.now(timezone.utc),
        )

    @pytest.fixture
    def envelope(self, sample_event):
        """Create a MandalaEnvelope instance."""
        return MandalaEnvelope(event=sample_event)

    def test_envelope_initialization(self, sample_event):
        """Test that MandalaEnvelope initializes correctly."""
        envelope = MandalaEnvelope(event=sample_event)
        assert envelope.event == sample_event
        assert envelope.metadata == {}
        assert envelope.received_at is not None

    def test_envelope_initialization_with_metadata(self, sample_event):
        """Test initialization with custom metadata."""
        metadata = {"key": "value", "another_key": 123}
        envelope = MandalaEnvelope(event=sample_event, metadata=metadata)
        assert envelope.metadata == metadata

    def test_envelope_initialization_with_received_at(self, sample_event):
        """Test initialization with custom received_at timestamp."""
        received_at = datetime(2026, 5, 12, 12, 0, 0, tzinfo=timezone.utc)
        envelope = MandalaEnvelope(event=sample_event, received_at=received_at)
        assert envelope.received_at == received_at

    def test_envelope_to_dict(self, envelope):
        """Test converting envelope to dictionary."""
        result = envelope.to_dict()
        assert "event" in result
        assert "metadata" in result
        assert "received_at" in result

    def test_envelope_from_dict(self, envelope):
        """Test creating envelope from dictionary."""
        data = envelope.to_dict()
        new_envelope = MandalaEnvelope.from_dict(data)
        assert new_envelope.event.id == envelope.event.id
        assert new_envelope.event.type == envelope.event.type

    def test_envelope_serialization_roundtrip(self, envelope):
        """Test serialization and deserialization roundtrip."""
        data = envelope.to_dict()
        new_envelope = MandalaEnvelope.from_dict(data)
        assert new_envelope.event.id == envelope.event.id
        assert new_envelope.event.source == envelope.event.source
        assert new_envelope.event.type == envelope.event.type

    def test_envelope_with_nested_metadata(self, sample_event):
        """Test envelope with nested metadata structure."""
        metadata = {
            "nested": {"key": "value"},
            "list": [1, 2, 3],
            "mixed": {"a": [1, 2], "b": {"c": 3}},
        }
        envelope = MandalaEnvelope(event=sample_event, metadata=metadata)
        assert envelope.metadata == metadata

    def test_envelope_add_metadata(self, envelope):
        """Test adding metadata to envelope."""
        envelope.add_metadata("new_key", "new_value")
        assert envelope.metadata["new_key"] == "new_value"

    def test_envelope_add_metadata_overwrites(self, envelope):
        """Test that adding metadata overwrites existing keys."""
        envelope.add_metadata("key", "value1")
        envelope.add_metadata("key", "value2")
        assert envelope.metadata["key"] == "value2"

    def test_envelope_get_metadata(self, envelope):
        """Test getting metadata from envelope."""
        envelope.add_metadata("key", "value")
        assert envelope.get_metadata("key") == "value"

    def test_envelope_get_metadata_default(self, envelope):
        """Test getting metadata with default value."""
        assert envelope.get_metadata("nonexistent", "default") == "default"

    def test_envelope_has_metadata(self, envelope):
        """Test checking if metadata key exists."""
        envelope.add_metadata("key", "value")
        assert envelope.has_metadata("key") is True
        assert envelope.has_metadata("nonexistent") is False

    def test_envelope_remove_metadata(self, envelope):
        """Test removing metadata from envelope."""
        envelope.add_metadata("key", "value")
        envelope.remove_metadata("key")
        assert envelope.has_metadata("key") is False

    def test_envelope_remove_metadata_nonexistent(self, envelope):
        """Test removing nonexistent metadata doesn't raise error."""
        # Should not raise an error
        envelope.remove_metadata("nonexistent")

    def test_envelope_clear_metadata(self, envelope):
        """Test clearing all metadata."""
        envelope.add_metadata("key1", "value1")
        envelope.add_metadata("key2", "value2")
        envelope.clear_metadata()
        assert envelope.metadata == {}

    def test_envelope_copy(self, envelope):
        """Test copying an envelope."""
        envelope.add_metadata("key", "value")
        copied = envelope.copy()
        assert copied.event.id == envelope.event.id
        assert copied.metadata == envelope.metadata
        assert copied is not envelope

    def test_envelope_copy_is_independent(self, envelope):
        """Test that copied envelope is independent."""
        envelope.add_metadata("key", "value")
        copied = envelope.copy()
        copied.add_metadata("new_key", "new_value")
        assert "new_key" not in envelope.metadata
        assert "new_key" in copied.metadata

    def test_envelope_eq_same_envelope(self, envelope):
        """Test equality check with same envelope."""
        assert envelope == envelope

    def test_envelope_eq_different_envelope(self, sample_event):
        """Test equality check with different envelope."""
        envelope1 = MandalaEnvelope(event=sample_event)
        envelope2 = MandalaEnvelope(event=sample_event)
        assert envelope1 == envelope2

    def test_envelope_eq_different_event(self, sample_event):
        """Test inequality with different event."""
        event2 = MandalaEvent(
            id="test-2",
            source="test",
            type="test.event",
            time=datetime.now(timezone.utc),
        )
        envelope1 = MandalaEnvelope(event=sample_event)
        envelope2 = MandalaEnvelope(event=event2)
        assert envelope1 != envelope2

    def test_envelope_repr(self, envelope):
        """Test string representation."""
        repr_str = repr(envelope)
        assert "MandalaEnvelope" in repr_str
        assert envelope.event.id in repr_str

    def test_envelope_str(self, envelope):
        """Test string conversion."""
        str_str = str(envelope)
        assert "MandalaEnvelope" in str_str

    def test_envelope_with_none_event(self):
        """Test envelope with None event raises error."""
        with pytest.raises(ValueError):
            MandalaEnvelope(event=None)

    def test_envelope_with_invalid_metadata(self, sample_event):
        """Test envelope with invalid metadata type."""
        # Should handle gracefully or raise appropriate error
        with pytest.raises(TypeError):
            MandalaEnvelope(event=sample_event, metadata="invalid")

    def test_envelope_merge_metadata(self, envelope):
        """Test merging metadata from dict."""
        envelope.add_metadata("key1", "value1")
        envelope.merge_metadata({"key2": "value2", "key3": "value3"})
        assert envelope.metadata["key1"] == "value1"
        assert envelope.metadata["key2"] == "value2"
        assert envelope.metadata["key3"] == "value3"

    def test_envelope_merge_metadata_overwrites(self, envelope):
        """Test that merge overwrites existing keys."""
        envelope.add_metadata("key", "value1")
        envelope.merge_metadata({"key": "value2"})
        assert envelope.metadata["key"] == "value2"

    def test_envelope_validate_success(self, envelope):
        """Test envelope validation succeeds for valid envelope."""
        assert envelope.validate() is True

    def test_envelope_validate_missing_event(self):
        """Test envelope validation fails for missing event."""
        envelope = MandalaEnvelope.__new__(MandalaEnvelope)
        envelope.event = None
        assert envelope.validate() is False

    def test_envelope_get_age(self, envelope):
        """Test getting envelope age."""
        import time

        time.sleep(0.1)
        age = envelope.get_age()
        assert age >= 0.1  # At least 100ms should have passed

    def test_envelope_is_expired(self, envelope):
        """Test checking if envelope is expired."""
        assert envelope.is_expired(ttl_seconds=3600) is False

    def test_envelope_is_expired_custom_ttl(self, envelope):
        """Test checking if envelope is expired with custom TTL."""
        # Create envelope with old received_at
        old_time = datetime(2026, 5, 11, 12, 0, 0, tzinfo=timezone.utc)
        envelope = MandalaEnvelope(event=envelope.event, received_at=old_time)
        assert envelope.is_expired(ttl_seconds=1) is True

    def test_envelope_to_json(self, envelope):
        """Test converting envelope to JSON string."""
        import json

        json_str = envelope.to_json()
        data = json.loads(json_str)
        assert "event" in data
        assert "metadata" in data

    def test_envelope_from_json(self, envelope):
        """Test creating envelope from JSON string."""
        import json

        json_str = envelope.to_json()
        new_envelope = MandalaEnvelope.from_json(json_str)
        assert new_envelope.event.id == envelope.event.id

    def test_envelope_to_json_roundtrip(self, envelope):
        """Test JSON serialization and deserialization roundtrip."""
        json_str = envelope.to_json()
        new_envelope = MandalaEnvelope.from_json(json_str)
        assert new_envelope.event.id == envelope.event.id
        assert new_envelope.event.type == envelope.event.type
