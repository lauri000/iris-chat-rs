# nostr-double-ratchet

Minimal synchronous Double Ratchet core.

This crate is the domain layer only. It does not expose Nostr events, invite URLs, relay code,
storage, or background orchestration.

## Public API

High-level API:

- `SessionManager`
- `GroupManager`
- `RosterEditor`

Lower-level primitives:

- `Session`
- `Invite`

Core data types:

- `MessageEnvelope`
- `InviteResponseEnvelope`
- `DeviceRoster`
- `AuthorizedDevice`
- `ProtocolContext`
- `SessionManagerSnapshot`
- `GroupManagerSnapshot`
- `SessionState`
- `DevicePubkey`
- `OwnerPubkey`
- `UnixSeconds`

## Design

- Payloads are opaque `Vec<u8>`.
- State transitions are explicit call/return operations.
- `ProtocolContext` carries time and randomness per call.
- Device authorization is represented by `DeviceRoster`.
- `SessionManager` owns multi-device Sesame-style state for decentralized relay input, but stays
  pure domain logic.
- `GroupManager` owns group semantics above `SessionManager` and uses pairwise fanout for v1 group
  control and group messages.
- `GroupSnapshot::protocol` selects the group semantics, currently `pairwise_fanout_v1`.
- `wire_format_version` is the inner group payload schema version inside pairwise message bytes.
- `RosterEditor` is the supported helper for building and editing full roster snapshots outside
  `SessionManager`.

No compatibility layer is kept for the removed APIs.

## Basic `SessionManager` Flow

1. Create a separate local device secret key and derive its public key with
   `DevicePubkey::from_secret_bytes(...)`.
2. Construct `SessionManager::new(local_owner_pubkey, local_device_secret_key)`.
3. Build the authoritative local roster with `RosterEditor`.
4. Publish the local public invite with `ensure_local_invite(...)`.
5. Apply the local roster and observe peer rosters.
6. Observe peer device invites.
7. Call `prepare_send(...)` to produce:
   - `deliveries`
   - `invite_responses`
   - `relay_gaps`
8. Feed invite responses and incoming messages back with:
   - `observe_invite_response(...)`
   - `receive(...)`
9. Persist and restore with `snapshot()` / `SessionManager::from_snapshot(...)`.

## Device Roster CRUD

Use `RosterEditor` for authoritative roster editing:

- `RosterEditor::new()` or `RosterEditor::from_roster(...)`
- `authorize_device(...)`
- `revoke_device(...)`
- `build(created_at)`

This is snapshot CRUD:

- add device = build and publish a newer full roster including it
- remove device = build and publish a newer full roster omitting it
- `SessionManager` consumes those snapshots with `apply_local_roster(...)` /
  `observe_peer_roster(...)`

## Pairwise-Fanout Groups

Use `GroupManager` above `SessionManager` when you want Signal-shaped group semantics without
adding a separate group ratchet yet:

- group membership is expressed in owner pubkeys
- `GroupManager` returns merged `deliveries`, `invite_responses`, and `relay_gaps`
- `SessionManager` remains pairwise-only and unchanged in role
- group control and group chat payloads are carried inside ordinary pairwise payload bytes

## Lower-Level Device-to-Device Usage

If you do not want the multi-device manager, use:

- `Invite` to bootstrap a pairwise session
- `Session::new_initiator(...)`
- `Session::new_responder(...)`
- `Session::plan_send(...)` / `apply_send(...)`
- `Session::plan_receive(...)` / `apply_receive(...)`

Those APIs operate only on opaque byte payloads and `MessageEnvelope`.

## Nostr Integration

Use the sibling adapter crate for Nostr conversions:

- [nostr-double-ratchet-nostr](../nostr-double-ratchet-nostr)

That crate translates:

- Nostr DM events <-> `MessageEnvelope`
- invite URLs/events <-> `Invite`
- invite response events <-> `InviteResponseEnvelope`
- roster events <-> `DeviceRoster`

## Testing

```bash
cargo test -p nostr-double-ratchet --manifest-path rust/Cargo.toml
```

See [./docs/TUTORIAL.md](./docs/TUTORIAL.md) for the user-facing integration guide.
See [./docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md) for the current owner/device architecture, invite
semantics, and recommended integration flow.
See [./docs/MASTER_PARITY.md](./docs/MASTER_PARITY.md) for the optional interop payload and
sender-key protocol formats layered above the core ratchet.
See [./docs/SOURCES.md](./docs/SOURCES.md) for the compact source index behind the current group-architecture
comparisons and tradeoff discussion.
