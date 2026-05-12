"""Materialized views over the Mandala event stream.

Mandala uses CQRS: the Redis Stream ``mandala:events`` is the append-only
*write model*; every view in this package is a *read model* that subscribes
to the stream (via a dedicated consumer group ``mandala:views``) and
maintains a specialized data structure optimized for a particular query
pattern.

Views are:

* :class:`mandala.views.geospatial.GeospatialView` — ``GEOADD`` of truck
  positions → ``GEOSEARCH`` answers "trucks within N mi of <point>".
* :class:`mandala.views.timeseries.TimeseriesView` — sorted-set time series
  of cold-chain readings + a global breach index.
"""

from __future__ import annotations

from mandala.views.base import MaterializedView
from mandala.views.bitmap import BitmapView
from mandala.views.dead_zone import DeadZoneView
from mandala.views.geospatial import GeospatialView
from mandala.views.graph import GraphView
from mandala.views.timeseries import TimeseriesView

__all__ = [
    "MaterializedView",
    "BitmapView",
    "DeadZoneView",
    "GeospatialView",
    "GraphView",
    "TimeseriesView",
]
