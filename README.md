# Mysticeti

[![rustc](https://img.shields.io/badge/rustc-1.72+-blue?style=flat-square&logo=rust)](https://www.rust-lang.org)
[![license](https://img.shields.io/badge/license-Apache-blue.svg?style=flat-square)](LICENSE)

The code in this branch is a prototype of Mysticeti. It supplements the paper [Mysticeti: Low-Latency DAG Consensus with Fast Commit Path](https://arxiv.org/abs/2310.14821) enabling reproducible results. There are no plans to maintain this branch.

## Iroh transport and node discovery

The optional **Iroh** feature uses [Iroh](https://github.com/n0-computer/iroh) for P2P networking (QUIC, relay/NAT traversal) and static node addressing. When enabled, validators can use Iroh identities instead of raw TCP addresses.

### Building with Iroh

```bash
cargo build --features iroh
```

### Running a network over Iroh

1. **Generate Iroh keys** for each validator (one key per node, 32-byte secret; the public part is the EndpointId):

   ```bash
   cargo run --features iroh -- gen-iroh-key --out genesis/0-iroh.key
   cargo run --features iroh -- gen-iroh-key --out genesis/1-iroh.key
   # ... one per authority
   ```

   Each run prints an `iroh_endpoint_id` (hex). Use that in `parameters.yaml`.

2. **Configure parameters** so every identifier has `iroh_endpoint_id` set (hex string, 64 chars). Optionally set `iroh_relay_url` per peer (e.g. `https://relay.iroh.computer`). Keep `network_address` for metrics and for direct-addr hints.

3. **Private config**: set `iroh_secret_key_path` to the path of the key file for this node so its EndpointId is stable across restarts.

4. **Start validators** with the same binary built with `--features iroh`. If all identifiers have `iroh_endpoint_id`, the validator will use Iroh transport automatically.

### Config fields (Iroh)

- **parameters / identifiers**: `iroh_endpoint_id` (optional, hex), `iroh_relay_url` (optional).
- **private config**: `iroh_secret_key_path` (optional, path to 32-byte key file).

## License

This software is licensed as [Apache 2.0](LICENSE).
