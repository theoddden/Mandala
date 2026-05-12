"""ZK-SNARK verifier for Mandala event proofs.

Provides async verification of ZK proofs without learning witness data.
Supports both local verification and remote verification via HTTP endpoints.
"""
from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

import httpx
import structlog

from mandala.core.zk.circuits import ColdChainBreachProof

log = structlog.get_logger(__name__)


class ZKVerifier:
    """Async ZK proof verifier without learning witness data."""
    
    def __init__(self, verification_key: bytes | None = None):
        self._vk = verification_key
    
    async def verify_cold_chain_proof(
        self,
        proof: ColdChainBreachProof,
        expected_timestamp_range: tuple[datetime, datetime] | None = None,
    ) -> bool:
        """
        Async verify that:
        1. Proof is valid against verification key
        2. Public inputs match claimed breach
        3. Timestamp is in claimed range
        4. Event type is cold_chain_breach
        """
        # Verify SNARK in executor (CPU-bound)
        snark_valid = await self._verify_snark_async(
            proof.proof, proof.public_inputs, proof.verification_key
        )
        if not snark_valid:
            log.warning("zk.verification.snark_failed", proof_id=proof.proof_id)
            return False
        
        # Verify public inputs
        if proof.public_inputs["event_type"] != "mandala.truck.cold_chain.breach":
            log.warning("zk.verification.event_type_mismatch", proof_id=proof.proof_id)
            return False
        
        # Verify timestamp range (if provided by verifier)
        if expected_timestamp_range:
            ts_start, ts_end = expected_timestamp_range
            proof_ts_start = proof.public_inputs["timestamp_range_start"]
            proof_ts_end = proof.public_inputs["timestamp_range_end"]
            
            if not (ts_start.timestamp() <= proof_ts_start and proof_ts_end <= ts_end.timestamp()):
                log.warning("zk.verification.timestamp_range_mismatch", proof_id=proof.proof_id)
                return False
        
        log.info("zk.verification.success", proof_id=proof.proof_id)
        return True
    
    async def _verify_snark_async(
        self, proof: bytes, public_inputs: dict[str, Any], verification_key: bytes
    ) -> bool:
        """Async SNARK verification in executor."""
        loop = asyncio.get_event_loop()
        
        def _verify_sync():
            # Call snarkjs or circomlib-rs
            import json
            import subprocess
            import tempfile
            
            # Write proof and public inputs to temp files
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
                json.dump({"proof": proof.hex()}, f)
                proof_file = f.name
            
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
                json.dump(public_inputs, f)
                public_file = f.name
            
            result = subprocess.run(
                [
                    "snarkjs", "groth16", "verify",
                    verification_key.decode() if isinstance(verification_key, bytes) else verification_key,
                    public_file,
                    proof_file
                ],
                capture_output=True,
                check=False,
            )
            
            return result.returncode == 0
        
        return await loop.run_in_executor(None, _verify_sync)


class RemoteZKVerifier:
    """Async verifier that calls external HTTP endpoint (insurer, customs)."""
    
    def __init__(self, endpoint: str, timeout: float = 30.0):
        self._endpoint = endpoint
        self._timeout = timeout
    
    async def verify_remote(self, proof: ColdChainBreachProof) -> dict[str, Any]:
        """
        Async POST proof to external verifier endpoint.
        
        Returns verification result from external party.
        """
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.post(
                f"{self._endpoint}/api/v1/zk/verify",
                json={
                    "proof": proof.proof.hex() if isinstance(proof.proof, bytes) else proof.proof,
                    "public_inputs": proof.public_inputs,
                    "proof_id": proof.proof_id,
                },
            )
            response.raise_for_status()
            return response.json()
