# Risks & mitigations

The known project-kill risks for Mandala, ranked by impact, with the
mitigations baked into v0.1.

## 1. Descartes API fragmentation

> Descartes is 50+ acquired products. If Mandala requires a full Descartes
> subscription stack to do anything useful, adoption dies before it starts.

**Mitigation in v0.1:**
- Every Descartes connector is its own sub-package
  (`mandala.connectors.descartes.macropoint`, `…datamyne`, `…compliance`).
- Each is **optional**. The webhook app lazy-imports them and logs
  `mandala.connector.<x>.disabled` if not present.
- The default v0.1 install ships only the **MacroPoint carrier-docs**
  connector — no commercial agreement required.
- Mandala is fully useful with **only Samsara configured** (the
  cross-border alert engine still fires; it just defaults to "no linked
  shipment" reasoning).

## 2. Driver / vehicle data GDPR exposure

> Vehicle position and driver-behaviour data is PII. Cross-border
> forwarding can create transfer-compliance issues.

**Mitigation in v0.1:**
- `DATA_PRIVACY.md` documents Mandala's pass-through posture and the
  operator's responsibilities under GDPR, CCPA, and Quebec law.
- TTL-based Redis state (default 14 days), not a persistent database.
- No phone-home. No telemetry endpoint.

**Planned for v0.2:**
- `MANDALA_ANONYMIZE=1` mode strips driver/VIN PII before bus emission.
- `mandala admin redact <urn>` for fast deletion-request handling.

## 3. Schema breaking changes

> If `ShipmentEvent` v0.2 breaks v0.1, the community fragments.

**Mitigation in v0.1:**
- `SCHEMA.md` is versioned independently of the codebase.
- Every CloudEvent carries `mandalaschemaversion`.
- Strict semver: minor bumps are additive only. Breaking changes go in
  major bumps with a one-minor-version deprecation window.
- The dbt-mandala package is versioned to track Mandala's *minor*
  versions, never the patch.

## 4. Connector drift (vendor API changes)

Samsara and MacroPoint update their schemas. v0.1 mitigation:

- Normalizers are pure functions in `connectors/<vendor>/normalize.py`,
  unit-tested against fixture payloads in `tests/fixtures/`.
- Unknown event types are logged and ignored, never crashed on.
- Webhook receivers ack-and-skip malformed payloads instead of poisoning
  the consumer group.

## 5. Operational complexity for a small team

> A bridge that needs Postgres, Kafka, and Kubernetes won't get adopted by
> the one-person ops team that needs it most.

**Mitigation in v0.1:**
- Single binary, single Redis dependency, single worker process.
- `docker compose up` is the entire production deployment story.
- No Postgres. No Kafka. No Helm. Add them later if you need them.

## 6. AI-agent hallucination on logistics data

> An LLM with MCP tools may invent shipment IDs or hallucinate customs
> statuses, then take actions on bad data.

**Mitigation in v0.1:**
- MCP tools are read-only. They return raw canonical objects, not
  paraphrased summaries.
- `check_customs_status` returns `"unknown"` when no data is in state —
  never a guess.
- Outbound actions (sending driver alerts, filing amendments) are
  intentionally not exposed via MCP in v0.1.

## 7. Auto-posting blast radius

> A bug in the load-board auto-poster could publish hundreds of phantom
> truck postings to DAT/Truckstop, damaging the operator's broker
> relationships and potentially incurring per-post fees.

**Mitigation in v0.1:**
- `MANDALA_LOADBOARD_ENABLED=0` is the default. Auto-posting is
  strictly opt-in; flipping it on is a deliberate operator decision.
- Per-truck 6-hour debounce prevents repost storms even if the upstream
  delivery event repeats.
- Postings carry an explicit TTL (default 24h) so phantom posts age out.
- Each board call is independent: if one fails the others still
  succeed, and a `mandala.loadboard.post_failed` audit event is emitted.
- `external_reference` is a Mandala-generated id stamped on every
  posting so operators can bulk-expire by prefix if needed.

## 8. Brand / trademark risk

> "Samsara" and "Descartes" are trademarks. We use them only for
> interoperability description.

**Mitigation in v0.1:**
- `NOTICE` file disclaims affiliation.
- `README.md` repeats the disclaimer.
- The package and project name is "Mandala", which is not a trademark of
  either company.
