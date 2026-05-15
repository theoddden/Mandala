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

# Try to import Rust backend, fall back to subprocess
try:
    from mandala_rust_ext.zk import (
        zk_verify_cold_chain_proof,
        zk_verify_cold_chain_proof_with_timestamp_check,
    )

    RUST_BACKEND_AVAILABLE = True
    log.info("zk.rust_backend.enabled")
except ImportError:
    RUST_BACKEND_AVAILABLE = False
    log.warning("zk.rust_backend.unavailable", message="Falling back to subprocess calls")


class ZKVerifier:
    """Async ZK proof verifier without learning witness data."""

    def __init__(self, verification_key: bytes | None = None, verification_key_path: str | None = None):
        self._vk = verification_key
        self._vk_path = verification_key_path

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
        # Use Rust backend if available
        if RUST_BACKEND_AVAILABLE:
            return await self._verify_with_rust(proof, expected_timestamp_range)
        else:
            return await self._verify_with_subprocess(proof, expected_timestamp_range)

    async def _verify_with_rust(
        self,
        proof: ColdChainBreachProof,
        expected_timestamp_range: tuple[datetime, datetime] | None = None,
    ) -> bool:
        """Verify using Rust backend via FFI."""
        loop = asyncio.get_event_loop()

        def _verify_sync():
            # Determine verification key source
            if self._vk_path:
                verification_key = self._vk_path
            elif self._vk:
                verification_key = self._vk
            else:
                verification_key = proof.verification_key

            # Convert public inputs to JSON
            import json

            public_inputs_json = json.dumps(proof.public_inputs)

            # Call Rust verification
            if expected_timestamp_range:
                ts_start = expected_timestamp_range[0].isoformat()
                ts_end = expected_timestamp_range[1].isoformat()
                return zk_verify_cold_chain_proof_with_timestamp_check(
                    proof.proof,
                    public_inputs_json,
                    verification_key,
                    ts_start,
                    ts_end,
                )
            else:
                return zk_verify_cold_chain_proof(
                    proof.proof,
                    public_inputs_json,
                    verification_key,
                )

        try:
            is_valid = await loop.run_in_executor(None, _verify_sync)
            if not is_valid:
                log.warning("zk.verification.snark_failed", proof_id=proof.proof_id)
                return False

            # Verify public inputs (still needed even with Rust backend)
            if proof.public_inputs["event_type"] != "mandala.truck.cold_chain.breach":
                log.warning("zk.verification.event_type_mismatch", proof_id=proof.proof_id)
                return False

            log.info("zk.verification.success", proof_id=proof.proof_id, backend="rust")
            return True
        except Exception as e:
            log.exception("zk.verification.rust_error", proof_id=proof.proof_id, error=str(e))
            # Fall back to subprocess on error
            return await self._verify_with_subprocess(proof, expected_timestamp_range)

    async def _verify_with_subprocess(
        self,
        proof: ColdChainBreachProof,
        expected_timestamp_range: tuple[datetime, datetime] | None = None,
    ) -> bool:
        """Verify using subprocess calls to snarkjs (fallback)."""
        # Verify SNARK in executor (CPU-bound)
        snark_valid = await self._verify_snark_async(proof.proof, proof.public_inputs, proof.verification_key)
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

        log.info("zk.verification.success", proof_id=proof.proof_id, backend="subprocess")
        return True

    async def _verify_snark_async(self, proof: bytes, public_inputs: dict[str, Any], verification_key: bytes) -> bool:
        """Async SNARK verification in executor (subprocess fallback)."""
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
                    "snarkjs",
                    "groth16",
                    "verify",
                    verification_key.decode() if isinstance(verification_key, bytes) else verification_key,
                    public_file,
                    proof_file,
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
