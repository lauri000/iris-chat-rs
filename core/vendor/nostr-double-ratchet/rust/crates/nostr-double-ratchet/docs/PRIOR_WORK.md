# Prior Work

This document collects the main external references behind the current architecture and protocol
discussion.

The goal is not to claim that Iris should copy Signal exactly. The goal is to keep the relevant
prior art in one place when evaluating:

- pairwise session-management boundaries
- sender-key group messaging
- private group-state systems
- migration tradeoffs between the old `master` design and the rewritten `experimental` design

For the local code evidence and tradeoff discussion, see [./SOURCES.md](./SOURCES.md) and
[./ARCHITECTURE_COMPARISON.md](./ARCHITECTURE_COMPARISON.md).

## Signal Protocol Specs

1. [Signal protocol specs index](https://signal.org/docs/)
   Primary index for Signal protocol specifications, including X3DH, PQXDH, Double Ratchet,
   Sesame, and related work.

2. [Sesame specification](https://signal.org/docs/specifications/sesame/sesame.pdf)
   Most relevant spec for the current pairwise multi-device architecture discussion. It describes
   active/inactive session handling, stale-device state, sender copies to linked devices, and
   recovery in asynchronous multi-device messaging.

## Signal Group-System References

1. [The Signal Private Group System](https://signal.org/blog/pdfs/signal_private_group_system.pdf)
   Main official and academic reference for Signal's private group-state system. Covers private
   membership, encrypted group state, anonymous credentials, and the service-assisted model used by
   modern Signal groups.

2. [Technology Preview: Signal Private Group System](https://signal.org/blog/signal-private-group-system/)
   Readable product-facing explanation of Groups v2 and the private group system.

3. [New Features Coming to Signal Groups](https://signal.org/blog/new-groups/)
   Official rollout post stating that new Signal groups are built on the private group system.

4. [Signal Support: Group chats](https://support.signal.org/hc/en-us/articles/360007319331-Group-chats)
   Current user-facing production statement that Signal groups are built on the private group
   system and that the service does not keep normal plaintext group metadata such as memberships,
   titles, avatars, or attributes.

## Signal Implementations

1. [signalapp/libsignal](https://github.com/signalapp/libsignal)
   Current public Signal protocol and crypto implementation repository. Useful for real API and
   layering choices, not only paper/spec descriptions.

2. [libsignal sender-key implementation](https://github.com/signalapp/libsignal/blob/main/rust/protocol/src/group_cipher.rs)
   Current Rust sender-key encrypt/decrypt implementation. Useful when evaluating how sender-key
   state is keyed, advanced, persisted, and validated in a production-grade library.

3. [Signal-Server](https://github.com/signalapp/Signal-Server)
   Public server repository. Useful when reasoning about service-assisted flows, with the normal
   caveat that public code does not prove every production deployment detail.

## Historical Sender-Key References

1. [Archived `libsignal-protocol-c` group session builder](https://github.com/signalapp/libsignal-protocol-c/blob/master/src/group_session_builder.h)
   Older but still clear reference for classic sender-key semantics, especially the
   group/sender/device-shaped state model.

2. [Analysis and Improvements of the Sender Keys Protocol for Group Messaging](https://arxiv.org/abs/2301.07045)
   Third-party formal analysis of Sender Keys as used by Signal and WhatsApp. Useful when weighing
   sender-key efficiency against compromise-recovery and attribution tradeoffs.

## Historical Group-Messaging Background

1. [Private Group Messaging, 2014](https://signal.org/blog/private-groups/)
   Historical TextSecure and early Signal group design based on pairwise fanout. Useful background
   for why pairwise groups are simple and attractive, but not representative of current production
   Signal groups.

## How To Use These References

- Use the Sesame spec when reasoning about pairwise session and linked-device state machines.
- Use the private group system references when discussing modern Signal group-state privacy
  properties.
- Use `libsignal` and sender-key implementation code when discussing practical library boundaries
  and sender-key state handling.
- Use the historical pairwise and sender-key references as background, not as a claim about current
  Signal production behavior.
