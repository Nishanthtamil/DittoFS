# DittoFS

DittoFS is a CRDT-backed distributed filesystem that synchronizes file changes across multiple nodes in real time over a local network. Files written on one mount point automatically appear on all other connected nodes without a central server.

## Architecture

| Layer | Technology | Role |
|-------|-----------|------|
| CRDT State | [Loro](https://loro.dev) v1.10 | Tree structure + file metadata (names, types, timestamps) |
| Blob Storage | [Sled](https://docs.rs/sled) v0.34 | Raw file content bytes, persisted to disk |
| Sync | [libp2p](https://libp2p.io) v0.54 | Gossipsub for data broadcast, mDNS for peer discovery |
| OS Interface | [fuse3](https://docs.rs/fuse3) v0.8.1 | FUSE filesystem — mount as a regular directory |

## Prerequisites

- **Linux** (FUSE3 required)
- **Rust nightly** toolchain
- **FUSE3 libraries:**

```bash
sudo apt install fuse3 libfuse3-dev
```

## Build and Run

```bash
# Build
cargo build --release

# Start Node A
./target/release/dittofs --mount /tmp/ditto_a --db /tmp/db_a &

# Start Node B (on same or different machine on the LAN)
./target/release/dittofs --mount /tmp/ditto_b --db /tmp/db_b &

# Wait ~30s for mDNS discovery, then:
echo "hello from node A" > /tmp/ditto_a/test.txt
cat /tmp/ditto_b/test.txt
# → hello from node A
```

Or run the automated demo:

```bash
chmod +x demo.sh && ./demo.sh
```

## Test Output

```
$ cargo test -- --nocapture

running 0 tests
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

running 1 test
test git_init_succeeds_on_mount ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

running 3 tests
test concurrent_conflict_resolves_cleanly ... ok
test truncate_then_write_produces_correct_content ... ok
test two_nodes_sync_via_wire_format ... ok
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```
