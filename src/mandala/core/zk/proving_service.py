"""Async ZK proof generation service for Mandala.

Background service for async ZK proof generation. Proof generation is slow
(seconds to minutes), so we run it in a dedicated background task queue.
Events that need proofs are queued, proofs are generated asynchronously,
and results are stored in Iceberg.
"""
from __future__ import annotations

import asyncio
import uuid
from typing import Any

import structlog

from mandala.core.events.envelope import MandalaEvent
from mandala.core.zk.circuits import ColdChainBreachProof, ZKCircuit

log = structlog.get_logger(__name__)


class AsyncProvingService:
    """
    Background service for async ZK proof generation.
    
    Proof generation is slow (seconds to minutes), so we run it in a
    dedicated background task queue. Events that need proofs are queued,
    proofs are generated asynchronously, and results are stored in Iceberg.
    """
    
    def __init__(self, max_concurrent_proofs: int = 4):
        self._queue: asyncio.Queue[tuple[MandalaEvent, dict[str, Any]]] = asyncio.Queue()
        self._max_concurrent = max_concurrent_proofs
        self._workers: list[asyncio.Task] = []
        self._running = False
    
    async def start(self) -> None:
        """Start background proof generation workers."""
        if self._running:
            return
        
        self._running = True
        for i in range(self._max_concurrent):
            worker = asyncio.create_task(self._worker(f"worker-{i}"))
            self._workers.append(worker)
        
        log.info("zk.proving_service.started", workers=self._max_concurrent)
    
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
        await self._queue.put((event, proof_params))
        log.info("zk.proof.enqueued", proof_id=proof_id, event_id=event.id)
        return proof_id
    
    async def _worker(self, name: str) -> None:
        """Background worker that generates proofs from queue."""
        while self._running:
            try:
                event, params = await asyncio.wait_for(
                    self._queue.get(), timeout=1.0
                )
            except TimeoutError:
                continue
            
            try:
                await self._generate_and_store(event, params)
            except Exception:
                log.exception(
                    "zk.proof.generation_failed",
                    worker=name,
                    event_id=event.id,
                )
            finally:
                self._queue.task_done()
    
    async def _generate_and_store(
        self, event: MandalaEvent, params: dict[str, Any]
    ) -> None:
        """Generate proof and store in Iceberg."""
        circuit = ZKCircuit(event)
        
        # Generate proof (async)
        proof = await circuit.build_cold_chain_circuit(
            declared_min_c=params["declared_min_c"],
            declared_max_c=params["declared_max_c"],
            breach_timestamp=params["breach_timestamp"],
        )
        
        # Store in Iceberg (async)
        await self._store_proof_in_iceberg(event, proof)
        
        log.info(
            "zk.proof.stored",
            proof_id=proof.proof_id,
            event_id=event.id,
        )
    
    async def _store_proof_in_iceberg(
        self, event: MandalaEvent, proof: ColdChainBreachProof
    ) -> None:
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
                "proof_id": proof.proof_id,
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
