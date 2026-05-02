# Sources

Concise evidence index for the current group-architecture discussion.

For the side-by-side architecture comparison and rewrite rationale, see
[./ARCHITECTURE_COMPARISON.md](./ARCHITECTURE_COMPARISON.md).
For the main external Signal and sender-key references, see [./PRIOR_WORK.md](./PRIOR_WORK.md).

## Current Implementations

### `iris` current Rust group model

- [`rust/README.md`](/Users/l/Projects/iris/nostr-double-ratchet/rust/README.md)
  States the current Rust stack's group model: metadata and sender-key distributions over 1:1 sessions, one-to-many outer messages, and shared-channel bootstrap.
- [`src/group.rs`](/Users/l/Projects/iris/nostr-double-ratchet/rust/crates/nostr-double-ratchet/src/group.rs)
  Defines the current group event kinds: metadata `40`, sender-key distribution `10446`, and sender-key message `10447`.
- [`src/group_manager.rs`](/Users/l/Projects/iris/nostr-double-ratchet/rust/crates/nostr-double-ratchet/src/group_manager.rs)
  Shows the hybrid design in code: pairwise metadata and sender-key distribution, then one-to-many steady-state chat, plus pending outer-event recovery.
- [`src/session_manager.rs`](/Users/l/Projects/iris/nostr-double-ratchet/rust/crates/nostr-double-ratchet/src/session_manager.rs)
  Stores sender-event routing and sender-key state, subscribes to sender-event pubkeys, and decrypts queued one-to-many outers after authenticated distribution arrives.
- [`src/one_to_many.rs`](/Users/l/Projects/iris/nostr-double-ratchet/rust/crates/nostr-double-ratchet/src/one_to_many.rs)
  Defines the one-to-many outer wire shape and makes the sender split explicit: sender-event signing key plus sender-key ciphertext state.
- [`src/runtime.rs`](/Users/l/Projects/iris/nostr-double-ratchet/rust/crates/nostr-double-ratchet/src/runtime.rs)
  Shows how the higher-level runtime composes `SessionManager` and `GroupManager`; useful for understanding integration complexity in `iris`.

### `iris-fork` current Rust group model

- [`README.md`](../README.md)
  States the current fork split and that `GroupManager` is the pairwise-fanout group layer above `SessionManager`.
- [`ARCHITECTURE.md`](./ARCHITECTURE.md)
  Documents the owner/device architecture and the current pairwise group flow used by the Rust core.
- [`src/group_manager.rs`](../src/group_manager.rs)
  Holds the local revisioned group state machine: create, sync, membership/admin changes, and `GroupMessage` over pairwise payload bytes.
- [`src/session_manager.rs`](../src/session_manager.rs)
  Shows the pairwise delivery surface used by groups: `deliveries`, `invite_responses`, and `relay_gaps`.

### App-core recovery evidence

- [`core.rs`](/Users/l/Projects/iris-fork/ndr-demo-app/core/src/core.rs)
  Shows the current recovery behavior that matters to the relay-loss discussion: pending inbound queues, retry on `unknown group` / `revision mismatch`, and sender-specific lookback fetches.

## Relay-Only Group-State Options

- [NIP-29: Relay-based Groups](https://nips.nostr.com/29)
  Canonical relay-managed group state on Nostr; strongest relay-only consistency option, but with a different privacy and trust model.
- [NIP-77: Negentropy Syncing](https://nips.nostr.com/77)
  Repair and reconciliation mechanism for missing events; useful as a sync primitive, not a complete group-state model.
- [NIP-EE: E2EE Messaging using MLS and Nostr](https://nips.nostr.com/EE)
  Nostr MLS draft; relevant because it surfaces the same ordering and commit-race problems on relays. It is marked unrecommended.

## Sender-Key vs Pairwise Security Tradeoffs

- [`iris` `src/one_to_many.rs`](/Users/l/Projects/iris/nostr-double-ratchet/rust/crates/nostr-double-ratchet/src/one_to_many.rs)
  Best local reference for the sender-key message plane: one outer event signed by a sender-event key and encrypted with sender-key state.
- [`iris` `src/session_manager.rs`](/Users/l/Projects/iris/nostr-double-ratchet/rust/crates/nostr-double-ratchet/src/session_manager.rs)
  Best local reference for sender-key attribution and recovery: authenticated sender-key distribution, sender-event mapping, and queued outer decrypt.
- [`iris-fork` `src/group_manager.rs`](../src/group_manager.rs)
  Best local reference for the pairwise alternative: every group control and message payload still rides ordinary pairwise transport.
- [Signal 2014: Private Group Messaging](https://signal.org/blog/private-groups/)
  Useful prior art for pairwise-fanout groups and for why that model initially appealed.
- [Signal 2019: Technology Preview: Signal Private Group System](https://signal.org/blog/signal-private-group-system/)
  Signal's own explanation of why the old distributed group-state model led to race conditions and divergence, motivating a canonical encrypted state service.
- [A Security Analysis of the Signal Protocol's Group Messaging Capabilities](https://kr-labs.com.ua/books/JansenMatthewL2020.pdf)
  Useful external discussion of sender-key tradeoffs, especially weaker compromise containment and recovery than pairwise ratchets.

## Standards and Prior Art

- [The Signal Private Group System paper](https://signal.org/blog/pdfs/signal_private_group_system.pdf)
  Detailed write-up of Signal's private canonical group-state design and its anonymous-credential approach.
- [RFC 9420: MLS Protocol](https://www.rfc-editor.org/rfc/rfc9420)
  Primary standard for MLS; useful when comparing sender-key and pairwise designs with formal group-keying approaches.
- [RFC 9750: MLS Architecture](https://datatracker.ietf.org/doc/html/rfc9750)
  Explains the delivery-service assumptions MLS still needs, which matters when evaluating raw-relay deployments.
- [Matrix room/state specification](https://spec.matrix.org/v1.7/)
  Useful federation prior art for separating state events from timeline messages and for the complexity that comes with distributed state resolution.
