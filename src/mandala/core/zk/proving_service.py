"""Async ZK proof generation service for Mandala.

Background service for async ZK proof generation. Proof generation is slow
(seconds to minutes), so we run it in a dedicated background task queue.
Events that need proofs are queued, proofs are generated asynchronously,
and results are stored in Iceberg.

.. warning:: PRODUCTION SAFETY — ZK MPC Ceremony

    The ``zk_mpc_simulate_ceremony`` helper in ``mandala.core.zk.circuits`` creates
    a trusted setup using **locally generated, known randomness**. This means any
    party who ran the simulation can forge proofs.

    **Simulated ceremony keys MUST NOT be used for production customs clearance,
    insurance claims, or any legally binding verification.**

    For production use, run a real multi-party computation ceremony with at least
    three independent participants, each contributing secret randomness that is
    immediately discarded. Set ``MANDALA_ZK_ENABLED=0`` (the default) until a
    production ceremony has been completed and the output keys have been
    independently verified.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any

import structlog

from mandala.core.events.envelope import MandalaEvent
from mandala.core.zk.circuits import ColdChainBreachProof, ZKCircuit

log = structlog.get_logger(__name__)

_QUEUE_KEY = "mandala:zk:proof:queue"
_STATUS_HASH = "mandala:zk:proof:status"


class AsyncProvingService:
    """
    Background service for async ZK proof generation.

    Proof generation is slow (seconds to minutes), so we run it in a
    dedicated background task queue. Events that need proofs are queued,
    proofs are generated asynchronously, and results are stored in Iceberg.

    When ``redis`` is supplied the queue and status map are persisted in Redis
    (LIST ``mandala:zk:proof:queue`` and HASH ``mandala:zk:proof:status``) so
    pending proofs survive worker restarts. Without Redis the previous
    in-memory behaviour is preserved.
    """

    PROOF_STATUS_PENDING = "pending"
    PROOF_STATUS_COMPLETE = "complete"
    PROOF_STATUS_FAILED = "failed"

    def __init__(self, max_concurrent_proofs: int = 4, redis: object | None = None):
        self._redis = redis
        self._queue: asyncio.Queue[tuple[str, MandalaEvent, dict[str, Any]]] = asyncio.Queue()
        self._max_concurrent = max_concurrent_proofs
        self._workers: list[asyncio.Task] = []
        self._running = False
        self._proof_status: dict[str, str] = {}  # in-memory; mirrored to Redis when available

    async def start(self) -> None:
        """Start background proof generation workers."""
        if self._running:
            return

        self._running = True

        if self._redis is not None:
            pending = await self._redis.llen(_QUEUE_KEY)  # type: ignore[union-attr]
            if pending > 0:
                log.info("zk.proving_service.resuming_pending", count=pending)

        for i in range(self._max_concurrent):
            worker = asyncio.create_task(self._worker(f"worker-{i}"))
            self._workers.append(worker)

        log.info("zk.proving_service.started", workers=self._max_concurrent,
                 backend="redis" if self._redis else "in_memory")

    async def stop(self) -> None:
        """Graceful shutdown of proof generation workers."""
        self._running = False
        for worker in self._workers:
            worker.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        log.info("zk.proving_service.stopped")

    async def enqueue_proof_request(
        self,
        event: MandalaEvent,
        proof_params: dict[str, Any],
    ) -> str:
        """
        Enqueue event for async proof generation.

        Returns proof_id for tracking. Proof will be generated in background
        and stored in Iceberg when complete.
        """
        proof_id = str(uuid.uuid4())
        self._proof_status[proof_id] = self.PROOF_STATUS_PENDING

        if self._redis is not None:
            payload = json.dumps(
                {
                    "proof_id": proof_id,
                    "event": json.loads(event.to_json()),
                    "params": proof_params,
                },
                default=str,
            )
            await self._redis.hset(_STATUS_HASH, proof_id, self.PROOF_STATUS_PENDING)  # type: ignore[union-attr]
            await self._redis.lpush(_QUEUE_KEY, payload)  # type: ignore[union-attr]
        else:
            await self._queue.put((proof_id, event, proof_params))

        log.info("zk.proof.enqueued", proof_id=proof_id, event_id=event.id)
        return proof_id

    def get_proof_status(self, proof_id: str) -> str | None:
        """Return status of a proof request, or None if unknown.

        Note: For Redis-backed mode the in-memory dict is updated by the worker
        on completion, providing a fast local read without a round-trip.
        """
        return self._proof_status.get(proof_id)

    async def _worker(self, name: str) -> None:
        """Background worker that generates proofs from queue."""
        while self._running:
            proof_id: str | None = None
            event: MandalaEvent | None = None
            params: dict[str, Any] = {}

            if self._redis is not None:
                # Blocking pop from Redis list with 1 s timeout
                try:
                    result = await asyncio.wait_for(
                        self._redis.brpop(_QUEUE_KEY, timeout=0),  # type: ignore[union-attr]
                        timeout=1.0,
                    )
                except TimeoutError:
                    continue
                if result is None:
                    continue
                _, payload_bytes = result
                payload_str = payload_bytes.decode() if isinstance(payload_bytes, bytes) else payload_bytes
                try:
                    payload = json.loads(payload_str)
                except json.JSONDecodeError:
                    log.exception("zk.proof.queue.decode_failed", worker=name)
                    continue
                proof_id = payload["proof_id"]
                event = MandalaEvent.model_validate(payload["event"])
                params = payload["params"]
            else:
                try:
                    proof_id, event, params = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                except TimeoutError:
                    continue

            try:
                await self._generate_and_store(proof_id, event, params)  # type: ignore[arg-type]
                status = self.PROOF_STATUS_COMPLETE
            except Exception:
                status = self.PROOF_STATUS_FAILED
                log.exception(
                    "zk.proof.generation_failed",
                    worker=name,
                    proof_id=proof_id,
                    event_id=event.id if event else None,  # type: ignore[union-attr]
                )
            finally:
                if proof_id:
                    self._proof_status[proof_id] = status  # type: ignore[possibly-undefined]
                    if self._redis is not None:
                        await self._redis.hset(_STATUS_HASH, proof_id, status)  # type: ignore[union-attr]
                if self._redis is None:
                    self._queue.task_done()

    async def _generate_and_store(self, proof_id: str, event: MandalaEvent, params: dict[str, Any]) -> None:
        """Generate proof and store in Iceberg."""
        circuit = ZKCircuit(event)

        # Generate proof (async)
        proof = await circuit.build_cold_chain_circuit(
            declared_min_c=params["declared_min_c"],
            declared_max_c=params["declared_max_c"],
            breach_timestamp=params["breach_timestamp"],
        )

        # Store in Iceberg (async)
        await self._store_proof_in_iceberg(proof_id, event, proof)

        log.info(
            "zk.proof.stored",
            proof_id=proof_id,
            circuit_proof_id=proof.proof_id,
            event_id=event.id,
        )

    async def _store_proof_in_iceberg(self, proof_id: str, event: MandalaEvent, proof: ColdChainBreachProof) -> None:
        """Store proof in Iceberg event log table."""
        # Import here to avoid circular dependency
        from mandala.core.event_log import get_event_log
        from mandala.core.events.envelope import new_event

        # Append to Iceberg as new event type
        proof_event = new_event(
            type="mandala.zk.proof.generated",
            source="mandala/zk/proving_service",
            subject=event.subject,
            data={
                "proof_id": proof_id,
                "circuit_proof_id": proof.proof_id,
                "event_id": event.id,
                "event_type": event.type,
                "public_inputs": proof.public_inputs,
                "generated_at": proof.generated_at.isoformat(),
            },
        )

        # Append to Iceberg (async fire-and-forget)
        event_log = get_event_log()
        if event_log:
            asyncio.create_task(event_log.append(proof_event))
