# Owner / Device Architecture

This document describes the current Rust architecture after the hard cut away from the old
`owner == device` convenience model.

For a user-facing integration guide, start with [./TUTORIAL.md](./TUTORIAL.md).
For a side-by-side comparison of the old `master` boundary and the current `experimental`
boundary, see [./ARCHITECTURE_COMPARISON.md](./ARCHITECTURE_COMPARISON.md).
For the roadmap to match `htree/master` behavior while preserving these boundaries, see
[./MASTER_PARITY.md](./MASTER_PARITY.md).
For a compact evidence index behind the group-design comparisons, see [./SOURCES.md](./SOURCES.md).

## Hard-Cut Summary

- `DevicePubkey::as_owner()` is gone.
- `OwnerPubkey::as_device()` is gone.
- `Invite` is now explicitly device-authored.
- `Invite` carries an optional owner claim instead of pretending the device key is the owner key.
- `SessionManager` always creates invites from the local device pubkey and always includes the
  local owner claim.
- `RosterEditor` stays separate from `SessionManager` and remains the supported roster-CRUD helper.
- `GroupManager` now sits above `SessionManager` for pairwise-fanout group metadata and chat.

## Topology

```mermaid
flowchart LR
  App["App code"] --> RE["RosterEditor"]
  App --> SM["SessionManager"]
  App --> GM["GroupManager"]
  App --> D2D["Session + Invite"]
  App --> Adapter["nostr-double-ratchet-nostr"]

  RE --> Roster["DeviceRoster snapshot"]
  Roster --> SM

  SM --> InviteResponse["InviteResponseEnvelope"]
  SM --> Message["MessageEnvelope"]
  GM --> GroupPayload["group control / group message payload bytes"]
  GroupPayload --> SM
  D2D --> Message

  Adapter --> Nostr["Nostr events / URLs"]
```

## Identity Roles

```mermaid
flowchart TD
  Owner["Owner identity"] --> Roster["Owner-authored roster snapshot"]
  Roster --> DeviceA["Authorized device A"]
  Roster --> DeviceB["Authorized device B"]
  DeviceA --> Invite["Device-authored public invite"]
  DeviceB --> Invite
  Invite --> Session["Per-device Session bootstrap"]
```

Rules:

- `Session` and `Invite` are direct device-to-device primitives.
- `SessionManager` is the owner/device orchestration layer.
- `GroupManager` is a separate group-state layer above `SessionManager`.
- When using `SessionManager`, always generate a separate local device key, even if the roster has
  only one device.

## Current Event Authority Split

```mermaid
flowchart LR
  Device["Device secret key"] --> InviteEvent["Invite event / invite URL"]
  Owner["Owner secret key"] --> RosterEvent["Roster event"]
```

This is intentional in the current adapter:

- invite publication is device-authored because the invite belongs to one device
- roster publication remains owner-authored because the roster event is the authoritative
  owner-level replaceable snapshot

That keeps roster replacement semantics stable without requiring multiple devices to co-author the
same replaceable record.

## Core Data Shapes

```rust
pub struct Invite {
    pub inviter_device_pubkey: DevicePubkey,
    pub inviter_ephemeral_public_key: DevicePubkey,
    pub shared_secret: [u8; 32],
    pub inviter_ephemeral_private_key: Option<[u8; 32]>,
    pub max_uses: Option<usize>,
    pub used_by: Vec<DevicePubkey>,
    pub created_at: UnixSeconds,
    pub inviter_owner_pubkey: Option<OwnerPubkey>,
}
```

```rust
pub struct SessionManagerSnapshot {
    pub local_owner_pubkey: OwnerPubkey,
    pub local_device_pubkey: DevicePubkey,
    pub local_invite: Option<Invite>,
    pub users: Vec<UserRecordSnapshot>,
}
```

```rust
pub struct PreparedSend {
    pub recipient_owner: OwnerPubkey,
    pub payload: Vec<u8>,
    pub deliveries: Vec<Delivery>,
    pub invite_responses: Vec<InviteResponseEnvelope>,
    pub relay_gaps: Vec<RelayGap>,
}
```

## SessionManager Flow

```mermaid
sequenceDiagram
  participant App
  participant RE as RosterEditor
  participant SM as SessionManager
  participant Adapter
  participant Relay

  App->>RE: authorize_device(...) / revoke_device(...)
  RE-->>App: DeviceRoster
  App->>SM: apply_local_roster(roster)
  App->>SM: ensure_local_invite(...)
  SM-->>App: Invite
  App->>Adapter: roster_unsigned_event(...)
  App->>Adapter: invite_unsigned_event(...)
  App->>Relay: sign + publish

  Relay-->>App: peer roster / peer invite / invite response / message
  App->>Adapter: parse_*_event(...)
  App->>SM: observe_peer_roster(...) / observe_device_invite(...)
  App->>SM: observe_invite_response(...) / receive(...)
```

## GroupManager Flow

```mermaid
sequenceDiagram
  participant App
  participant GM as GroupManager
  participant SM as SessionManager
  participant Relay

  App->>GM: create_group / update_name / add_members / send_message
  GM->>SM: prepare_send(...) once per remote owner
  SM-->>GM: PreparedSend
  GM-->>App: GroupPreparedSend
  App->>Relay: publish deliveries / invite responses

  Relay-->>App: pairwise group payload bytes
  App->>SM: receive(...)
  SM-->>App: decrypted payload + sender owner
  App->>GM: handle_incoming(sender_owner, payload)
  GM-->>App: MetadataUpdated / Message
```

Current v1 group rules:

- group membership is in owner pubkeys
- group control is revision-based
- admins are enforced in the core crate
- v1 uses pairwise fanout, not a separate group ratchet or epoch key
- `GroupSnapshot::protocol` selects the group semantics, currently `pairwise_fanout_v1`
- `wire_format_version` is the inner group payload schema version carried inside pairwise payload
  bytes

## How To Use It

### 1. SessionManager path

```rust
use nostr_double_ratchet::{
    DevicePubkey, OwnerPubkey, ProtocolContext, RosterEditor, SessionManager, UnixSeconds,
};

let owner_pubkey: OwnerPubkey = /* account identity */;
let local_device_secret_key: [u8; 32] = /* installation key */;
let local_device_pubkey = DevicePubkey::from_secret_bytes(local_device_secret_key)?;

let mut roster_editor = RosterEditor::new();
roster_editor.authorize_device(local_device_pubkey, UnixSeconds(1_850_000_000));
let local_roster = roster_editor.build(UnixSeconds(1_850_000_001));

let mut manager = SessionManager::new(owner_pubkey, local_device_secret_key);
manager.apply_local_roster(local_roster);

let mut ctx = ProtocolContext::new(UnixSeconds(1_850_000_002), &mut rng);
let local_invite = manager.ensure_local_invite(&mut ctx)?.clone();
```

Then:

1. encode and publish the owner-authored roster event
2. encode and publish the device-authored invite event
3. observe peer rosters and peer invites
4. call `prepare_send(...)`
5. publish `deliveries` and `invite_responses`
6. feed inbound invite responses and message envelopes back to the manager

### 2. Direct device-to-device path

```rust
use nostr_double_ratchet::{Invite, ProtocolContext, Session, UnixSeconds};

let mut invite_ctx = ProtocolContext::new(UnixSeconds(1_850_000_010), &mut rng);
let invite = Invite::create_new(&mut invite_ctx, alice_device_pubkey, None, None)?;

let mut accept_ctx = ProtocolContext::new(UnixSeconds(1_850_000_011), &mut rng);
let (bob_session, response_envelope) =
    invite.accept(&mut accept_ctx, bob_device_pubkey, bob_device_secret_key)?;
```

This path does not need `SessionManager`, `DeviceRoster`, or an owner claim.

## Internal Review Notes

```mermaid
flowchart TD
  A["Invite response arrives"] --> B["claimed owner already verified?"]
  B -->|yes| C["attach session under owner"]
  B -->|no| D["park under provisional device-derived owner bucket"]
  D --> E["migrate when roster proof arrives"]
```

Current verification staging:

- unverified owner claims are parked under a provisional owner bucket derived from the device
  pubkey bytes
- once a roster proves that device belongs to the claimed owner, the record is migrated

That mechanism is not a public API shim. It is the current internal way to stage unverified owner
claims until roster proof arrives.

## Remaining Constraint

The main unresolved architectural constraint is unchanged:

- roster events are still modeled as owner-authored replaceable snapshots

If the project later wants secondary devices to publish authoritative roster updates without access
to the owner signing key, that will require a larger event-authority redesign rather than another
cleanup pass.
