"""Descartes connectors — each Descartes product is a separate sub-package.

Mandala intentionally does **not** treat "Descartes" as one thing. The
DSGX portfolio (~50 acquired products) has fragmented APIs, auth schemes,
and commercial agreements. Each sub-package may be installed and
configured independently; Mandala degrades gracefully when only some
are configured (see ``RISKS.md`` #1).

v1 ships a public-docs MacroPoint carrier connector. Datamyne and
Visual Compliance scaffolds exist but are stubs until commercial
partnerships are in place.
"""
