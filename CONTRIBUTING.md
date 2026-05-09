# Contributing to Mandala

Thanks for your interest. This is a small project; a few rules keep it
small and useful.

## Ground rules

- **Apache 2.0** for everything. Contributions are accepted under the
  Apache License 2.0, with the [DCO](https://developercertificate.org/)
  sign-off (`git commit -s`). No CLA.
- **Conventional commits.** `feat:`, `fix:`, `docs:`, `chore:` etc.
- **Schema discipline.** The `mandala.*` event registry and the canonical
  Pydantic models in `src/mandala/core/schema/` are a public contract.
  Additive changes are minor bumps; breaking changes require a major
  bump and a migration note in `SCHEMA.md`.
- **Connectors are independent.** A new connector must run usefully on
  its own and degrade gracefully when others aren't configured. See
  `RISKS.md` #1.

## Adding a connector

1. Create `src/mandala/connectors/<vendor>/__init__.py` with a one-line
   summary and the public surface.
2. Add `connector.py` (subclass `BaseConnector`, implement
   `is_configured()`).
3. Add `normalize.py` — pure functions
   `vendor_payload -> list[MandalaEvent]`. No I/O. No config reads.
4. Add `webhook.py` if the vendor sends inbound events
   (HMAC-verify the body via `mandala.core.hmac.verify_hmac_sha256`,
   publish events to `app.state.bus`).
5. Add `client.py` if Mandala needs to call the vendor's API.
6. Wire the webhook into `src/mandala/app.py` inside a `try/except
   ImportError` block — if the connector isn't installed/configured,
   the app must still boot.
7. Add the connector's settings to `src/mandala/settings.py`
   (`<vendor>_*`).

## Adding a detector

Detectors live in `src/mandala/alerts.py` (alerts) or
`src/mandala/loadboard.py` (capacity flows). Each is a plain async
function:

```python
async def my_detector(event, state, redis) -> list[MandalaEvent]:
    if event.type != "mandala.something":
        return []
    # ... read state, decide ...
    return [new_event(...)]
```

Append it to the module's `DETECTORS` tuple. The single worker picks it
up automatically on next restart.

## Reporting security issues

Please **do not** open public issues for security concerns. Email
`security@mandala-bridge.dev` (or use GitHub's private vulnerability
reporting on the repository).
