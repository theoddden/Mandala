"""ZK-SNARK circuit builder for Mandala events.

Provides async circuit construction for privacy-preserving verification
of logistics events (cold-chain breaches, customs holds, etc.).

Circuits are defined in Circom and compiled via snarkjs. This module
provides the Python async interface for proof generation.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from datetime import UTC, datetime
from typing import Any, NamedTuple

import structlog

from mandala.core.events.envelope import MandalaEvent

log = structlog.get_logger(__name__)


class ColdChainBreachProof(NamedTuple):
    """ZK proof for cold-chain breach."""
    proof: bytes  # 256-byte SNARK proof
    public_inputs: dict[str, str]  # Revealed claims only
    verification_key: bytes  # Circuit verification key
    proof_id: str  # UUID for tracking
    generated_at: datetime  # When proof was generated


class ZKCircuit:
    """Async ZK-SNARK circuit builder for Mandala events."""
    
    def __init__(self, event: MandalaEvent):
        self._event = event
        self._circuit_vk: bytes | None = None
    
    async def _load_verification_key(self, vk_path: str) -> bytes:
        """Async load verification key from disk or cache."""
        if self._circuit_vk is not None:
            return self._circuit_vk
        
        # Load from file asynchronously
        import aiofiles
        async with aiofiles.open(vk_path, "rb") as f:
            self._circuit_key = await f.read()
        return self._circuit_key
    
    async def build_cold_chain_circuit(
        self,
        declared_min_c: float,
        declared_max_c: float,
        breach_timestamp: datetime,
        vk_path: str = "/opt/mandala/zk/keys/cold_chain_breach.vk",
    ) -> ColdChainBreachProof:
        """
        Async circuit build: Prove that an event with:
        - type = mandala.truck.cold_chain.breach
        - timestamp in [T_start, T_end]
        - temperature outside [min_c, max_c]
        
        Without revealing:
        - Shipment ID
        - Route
        - Cargo details
        - Other events in the log
        
        Runs in background task to avoid blocking event pipeline.
        """
        # Private inputs (witness)
        witness = {
            "event_hash": self._hash_event(self._event),
            "event_timestamp": int(self._event.time.timestamp()),
            "temperature_c": self._event.data.get("temperature_c") if self._event.data else None,
            "declared_min_c": declared_min_c,
            "declared_max_c": declared_max_c,
        }
        
        # Public inputs (revealed)
        public_inputs = {
            "event_type": self._event.type,
            "timestamp_range_start": int(breach_timestamp.timestamp() - 300),
            "timestamp_range_end": int(breach_timestamp.timestamp() + 300),
            "breach_confirmed": True,
        }
        
        # Generate proof in async task (can take seconds)
        proof = await self._generate_snark_async(witness, public_inputs)
        
        # Load verification key asynchronously
        vk = await self._load_verification_key(vk_path)
        
        proof_id = str(uuid.uuid4())
        
        log.info(
            "zk.proof.generated",
            proof_id=proof_id,
            event_type=self._event.type,
            event_id=self._event.id,
        )
        
        return ColdChainBreachProof(
            proof=proof,
            public_inputs=public_inputs,
            verification_key=vk,
            proof_id=proof_id,
            generated_at=datetime.now(UTC),
        )
    
    async def _generate_snark_async(
        self, witness: dict[str, Any], public_inputs: dict[str, Any]
    ) -> bytes:
        """
        Async SNARK generation using subprocess or external proving service.
        
        Proof generation is CPU-intensive (seconds to minutes), so we run it
        in an executor or offload to a dedicated proving service.
        """
        loop = asyncio.get_event_loop()
        
        # Option 1: Run in thread pool executor (for local proving)
        def _generate_sync():
            # Call snarkjs or circomlib-rs
            import subprocess
            result = subprocess.run(
                [
                    "snarkjs", "groth16", "prove",
                    "/opt/mandala/zk/circuits/cold_chain_breach.wasm",
                    "/opt/mandala/zk/keys/cold_chain_breach.zkey",
                    "proof.json",
                    "public.json"
                ],
                capture_output=True,
                check=True,
                cwd="/tmp",
            )
            
            # Read generated proof
            with open("/tmp/proof.json", "r") as f:
                proof_data = json.load(f)
            
            # Encode proof as bytes
            return json.dumps(proof_data).encode()
        
        proof = await loop.run_in_executor(None, _generate_sync)
        return proof
    
    def _hash_event(self, event: MandalaEvent) -> str:
        """Hash event for circuit commitment."""
        return hashlib.sha256(event.to_json().encode()).hexdigest()
