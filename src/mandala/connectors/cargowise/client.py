"""Outbound client for the CargoWise eAdaptor inbound endpoint.

Used to push Mandala-derived events back into CargoWise — typically a
border-crossing arrival timestamp, a measured cold-chain breach, or an
ETA refinement that originated in Samsara.

CargoWise eAdaptor accepts XML messages via HTTP POST with HTTP Basic
auth. The XML body is wrapped in a Universal* document. We construct the
minimum-viable Universal Event document for status pushes; for full
shipment payloads, build the XML upstream and pass it via :meth:`post_xml`.
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from mandala.settings import get_settings

NS = "http://www.cargowise.com/Schemas/Universal/2011/11"


def build_universal_event_xml(
    *,
    data_source_type: str,
    data_source_key: str,
    event_type: str,
    event_time: datetime | None = None,
    event_reference: str | None = None,
    company_code: str | None = None,
) -> str:
    """Compose a minimal Universal Event XML document.

    Args:
        data_source_type: e.g. ``"ForwardingShipment"`` or ``"CustomsDeclaration"``.
        data_source_key: the CargoWise reference (e.g. shipment number).
        event_type: the 3-letter CargoWise status code (``"DIM"``, ``"CDR"``, …).
        event_time: defaults to now.
        event_reference: free-text description.
        company_code: CargoWise OrganizationCode (defaults to the configured one).
    """
    s = get_settings()
    when = (event_time or datetime.now(UTC)).strftime("%Y-%m-%dT%H:%M:%S")
    company = company_code or s.cargowise_organization_code or ""

    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<UniversalEvent xmlns="{NS}">',
        "<Event>",
        "<DataContext>",
        "<DataSourceCollection>",
        "<DataSource>",
        f"<Type>{_xml_escape(data_source_type)}</Type>",
        f"<Key>{_xml_escape(data_source_key)}</Key>",
        "</DataSource>",
        "</DataSourceCollection>",
    ]
    if company:
        parts += [
            "<Company>",
            f"<Code>{_xml_escape(company)}</Code>",
            "</Company>",
        ]
    parts += [
        "</DataContext>",
        f"<EventTime>{when}</EventTime>",
        f"<EventType>{_xml_escape(event_type)}</EventType>",
    ]
    if event_reference:
        parts.append(f"<EventReference>{_xml_escape(event_reference)}</EventReference>")
    parts += ["</Event>", "</UniversalEvent>"]
    return "".join(parts)


def _xml_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


class CargoWiseClient:
    """Async eAdaptor client. Use as an async context manager."""

    def __init__(
        self,
        *,
        url: str | None = None,
        username: str | None = None,
        password: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        s = get_settings()
        self._url = (url or s.cargowise_eadaptor_url).rstrip("/")
        self._auth = (username or s.cargowise_username, password or s.cargowise_password)
        self._http = httpx.AsyncClient(
            timeout=timeout,
            headers={
                "Content-Type": "application/xml; charset=utf-8",
                "Accept": "application/xml",
                "User-Agent": "mandala/0.1 (+https://github.com/mandala-bridge/mandala)",
            },
            auth=self._auth,
        )

    async def __aenter__(self) -> CargoWiseClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def close(self) -> None:
        await self._http.aclose()

    async def post_xml(self, xml_body: str) -> httpx.Response:
        """POST a pre-built XML document to the eAdaptor inbound URL."""
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(4),
            wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
            retry=retry_if_exception_type((httpx.TransportError, httpx.HTTPStatusError)),
            reraise=True,
        ):
            with attempt:
                resp = await self._http.post(self._url, content=xml_body.encode("utf-8"))
                if resp.status_code in (408, 429) or 500 <= resp.status_code < 600:
                    raise httpx.HTTPStatusError(
                        f"CargoWise transient {resp.status_code}",
                        request=resp.request,
                        response=resp,
                    )
                resp.raise_for_status()
                return resp
        raise RuntimeError("unreachable")

    async def post_event(
        self,
        *,
        data_source_type: str,
        data_source_key: str,
        event_type: str,
        event_time: datetime | None = None,
        event_reference: str | None = None,
        company_code: str | None = None,
    ) -> dict[str, Any]:
        """Convenience wrapper: build + POST a Universal Event."""
        xml_body = build_universal_event_xml(
            data_source_type=data_source_type,
            data_source_key=data_source_key,
            event_type=event_type,
            event_time=event_time,
            event_reference=event_reference,
            company_code=company_code,
        )
        resp = await self.post_xml(xml_body)
        return {"status_code": resp.status_code, "body": resp.text}
