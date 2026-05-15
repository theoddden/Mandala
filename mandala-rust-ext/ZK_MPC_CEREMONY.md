# MPC Ceremony Documentation

## Overview

This document explains how to participate in a Multi-Party Computation (MPC) ceremony for generating proving and verification keys for the Mandala ZK-SNARK system.

## What is an MPC Ceremony?

An MPC ceremony is a cryptographic protocol where multiple independent participants contribute randomness to generate proving and verification keys. This ensures that no single participant knows the "toxic waste" (the random seed used for key generation), preventing any one party from being able to forge proofs.

## Why is it Required?

In Groth16 and other SNARK systems, the proving key is generated using a random seed. If a single party generates both the proving and verification keys, they could use the secret seed to forge proofs for any statement. An MPC ceremony with multiple participants ensures the secret is distributed and destroyed.

## Participation Guide

### Prerequisites

- Rust toolchain installed
- Mandala Rust extension built with ZK support
- At least 3-5 independent participants recommended for production

### Step 1: Ceremony Coordinator Setup

The coordinator initializes the ceremony:

```python
from mandala_rust_ext.zk import zk_mpc_ceremony_new

# Create ceremony requiring 5 participants
ceremony = zk_mpc_ceremony_new(required_participants=5)
```

### Step 2: Participant Contribution Generation

Each participant generates their random contribution:

```python
from mandala_rust_ext.zk import zk_mpc_generate_contribution

# Generate your contribution
contribution = zk_mpc_generate_contribution()
```

**Important:**
- Generate contributions on a secure, offline machine
- Use a hardware RNG if available
- Never share your contribution before the ceremony
- Destroy your contribution after the ceremony

### Step 3: Submit Contribution

Participants submit their contributions to the coordinator:

```python
participant_id = "participant_1"  # Your unique identifier
ceremony.add_contribution(participant_id, contribution)
```

### Step 4: Verification

After all participants have contributed, the coordinator generates the keys:

```python
# Example event data
event_json = '''
{
    "id": "event_123",
    "time": "2024-01-01T12:00:00Z",
    "data": {"temperature_c": -5.0}
}
'''

ceremony.generate_keys(
    event_json=event_json,
    declared_min_c=0.0,
    declared_max_c=10.0,
    breach_timestamp="2024-01-01T12:05:00Z",
    pk_path="/opt/mandala/zk/keys/cold_chain_breach.pk",
    vk_path="/opt/mandala/zk/keys/cold_chain_breach.vk"
)
```

### Step 5: Key Publication

The coordinator publishes:
- The proving key (`.pk`)
- The verification key (`.vk`)
- A transcript of all participant contributions
- Hash of the final ceremony state

Participants verify:
- Their contribution is in the transcript
- The final keys match the transcript
- No participant contributed twice

## Security Requirements

### For Participants

1. **Secure Generation**: Generate contributions on an air-gapped machine
2. **Hardware RNG**: Use a hardware random number generator if possible
3. **Verification**: Verify the final keys against the transcript
4. **Destruction**: Destroy your contribution after the ceremony

### For Coordinator

1. **Transparency**: Publish all contributions publicly
2. **Verification**: Allow participants to verify their contributions
3. **Integrity**: Ensure no participant contributes twice
4. **Documentation**: Maintain a complete audit trail

## Recommended Participants

For production use, include participants from:
- Mandala development team
- Independent security auditors
- Insurance company representatives
- Logistics company representatives
- Trusted third-party auditors

## Alternative: Perpetual Powers of Tau

For larger-scale or more robust ceremonies, consider using the established Perpetual Powers of Tau framework:

- GitHub: https://github.com/privacy-scaling-explorations/perpetualpowersoftau
- Battle-tested with millions of participants
- Supports multiple SNARK systems
- Provides verification tools

## Testing

For development/testing, you can simulate a ceremony:

```python
from mandala_rust_ext.zk import zk_mpc_simulate_ceremony

# Simulate ceremony with 3 participants
zk_mpc_simulate_ceremony(
    num_participants=3,
    event_json=event_json,
    declared_min_c=0.0,
    declared_max_c=10.0,
    breach_timestamp="2024-01-01T12:05:00Z",
    pk_path="/tmp/test.pk",
    vk_path="/tmp/test.vk"
)
```

**WARNING**: Simulated ceremonies are for testing only. Do not use generated keys for production.

## Troubleshooting

### Contribution Rejected

If your contribution is rejected:
- Check that your participant ID is unique
- Verify the ceremony is not already complete
- Ensure the coordinator has not reached the participant limit

### Key Generation Failed

If key generation fails:
- Verify all required participants have contributed
- Check that the ceremony state is complete
- Ensure the event data is valid

## Contact

For questions about the MPC ceremony, contact the Mandala security team.
