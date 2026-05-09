"""Connectors translate vendor formats to and from the canonical Mandala schema.

Every connector subclasses :class:`mandala.connectors.base.BaseConnector`,
ships a webhook router (if applicable), and a thin async client for outbound
REST calls. Connectors are independent — Mandala must run usefully with only
the Samsara connector configured (see ``RISKS.md`` #1).
"""
