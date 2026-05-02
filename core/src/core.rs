use crate::actions::AppAction;
use crate::state::{
    AccountSnapshot, AppState, ChatKind, ChatMessageSnapshot, ChatThreadSnapshot,
    CurrentChatSnapshot, DeliveryState, DeviceAuthorizationState, DeviceEntrySnapshot,
    DeviceRosterSnapshot, GroupDetailsSnapshot, GroupMemberSnapshot, Router, Screen,
};
use crate::updates::{AppUpdate, CoreMsg, InternalEvent};
use flume::Sender;
use nostr::EventBuilder;
use nostr_double_ratchet::{
    DevicePubkey, DeviceRoster, DomainError, Error, GroupIncomingEvent, GroupManager,
    GroupManagerSnapshot, GroupSnapshot, MessageEnvelope, OwnerPubkey, ProtocolContext, RelayGap,
    RosterEditor, SessionManager, SessionManagerSnapshot, SessionState, UnixSeconds,
};
use nostr_double_ratchet_nostr::nostr as codec;
use nostr_double_ratchet_pairwise_codec as pairwise_codec;
use nostr_double_ratchet_pairwise_codec::PairwiseRumorKind;
use nostr_sdk::prelude::{
    Client, Event, Filter, Keys, Kind, PublicKey, RelayPoolNotification, RelayUrl, SubscriptionId,
    Timestamp, ToBech32,
};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Duration};

const FALLBACK_DEFAULT_RELAYS: &[&str] = &[
    "wss://relay.damus.io",
    "wss://nos.lol",
    "wss://relay.primal.net",
];
const APP_VERSION: &str = env!("NDR_APP_VERSION");
const BUILD_CHANNEL: &str = env!("NDR_BUILD_CHANNEL");
const BUILD_GIT_SHA: &str = env!("NDR_BUILD_GIT_SHA");
const BUILD_TIMESTAMP_UTC: &str = env!("NDR_BUILD_TIMESTAMP_UTC");
const COMPILED_DEFAULT_RELAYS_CSV: &str = env!("NDR_DEFAULT_RELAYS");
const RELAY_SET_ID: &str = env!("NDR_RELAY_SET_ID");
const TRUSTED_TEST_BUILD: &str = env!("NDR_TRUSTED_TEST_BUILD");
const MAX_SEEN_EVENT_IDS: usize = 2048;
const RECENT_HANDSHAKE_TTL_SECS: u64 = 10 * 60;
const PENDING_RETRY_DELAY_SECS: u64 = 2;
const FIRST_CONTACT_STAGE_DELAY_MS: u64 = 1500;
const FIRST_CONTACT_RETRY_DELAY_SECS: u64 = 5;
const CATCH_UP_LOOKBACK_SECS: u64 = 30;
const UNKNOWN_GROUP_RECOVERY_LOOKBACK_SECS: u64 = 24 * 60 * 60;
const DEVICE_INVITE_DISCOVERY_LOOKBACK_SECS: u64 = 30 * 24 * 60 * 60;
const DEVICE_INVITE_DISCOVERY_LIMIT: usize = 256;
const DEVICE_INVITE_DISCOVERY_POLL_SECS: u64 = 5;
const RELAY_CONNECT_TIMEOUT_SECS: u64 = 5;
const RESUBSCRIBE_CATCH_UP_DELAY_SECS: u64 = 5;
const PROTOCOL_SUBSCRIPTION_ID: &str = "ndr-protocol";
const APP_DIRECT_MESSAGE_PAYLOAD_VERSION: u8 = 1;
const APP_GROUP_MESSAGE_PAYLOAD_VERSION: u8 = 1;
const GROUP_CHAT_PREFIX: &str = "group:";
const DEBUG_SNAPSHOT_FILENAME: &str = "ndr_demo_runtime_debug.json";
const MAX_DEBUG_LOG_ENTRIES: usize = 128;
const PERSISTED_STATE_VERSION: u32 = 11;

pub struct AppCore {
    update_tx: Sender<AppUpdate>,
    core_sender: Sender<CoreMsg>,
    shared_state: Arc<RwLock<AppState>>,
    runtime: tokio::runtime::Runtime,
    data_dir: PathBuf,
    state: AppState,
    logged_in: Option<LoggedInState>,
    threads: BTreeMap<String, ThreadRecord>,
    active_chat_id: Option<String>,
    screen_stack: Vec<Screen>,
    next_message_id: u64,
    pending_inbound: Vec<PendingInbound>,
    pending_outbound: Vec<PendingOutbound>,
    pending_group_controls: Vec<PendingGroupControl>,
    owner_profiles: BTreeMap<String, OwnerProfileRecord>,
    recent_handshake_peers: BTreeMap<String, RecentHandshakePeer>,
    seen_event_ids: HashSet<String>,
    seen_event_order: VecDeque<String>,
    device_invite_poll_token: u64,
    protocol_subscription_runtime: ProtocolSubscriptionRuntime,
    debug_log: VecDeque<DebugLogEntry>,
    debug_event_counters: DebugEventCounters,
}

struct LoggedInState {
    owner_pubkey: OwnerPubkey,
    owner_keys: Option<Keys>,
    device_keys: Keys,
    client: Client,
    relay_urls: Vec<RelayUrl>,
    session_manager: SessionManager,
    group_manager: GroupManager,
    authorization_state: LocalAuthorizationState,
}

#[derive(Clone)]
struct ThreadRecord {
    chat_id: String,
    unread_count: u64,
    updated_at_secs: u64,
    messages: Vec<ChatMessageSnapshot>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
enum PendingInbound {
    Envelope {
        envelope: MessageEnvelope,
    },
    Decrypted {
        sender_owner_hex: String,
        payload: Vec<u8>,
        created_at_secs: u64,
    },
}

impl PendingInbound {
    fn envelope(envelope: MessageEnvelope) -> Self {
        Self::Envelope { envelope }
    }

    fn decrypted(sender_owner: OwnerPubkey, payload: Vec<u8>, created_at_secs: u64) -> Self {
        Self::Decrypted {
            sender_owner_hex: sender_owner.to_string(),
            payload,
            created_at_secs,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PreparedPublishBatch {
    #[serde(default)]
    invite_events: Vec<Event>,
    #[serde(default)]
    message_events: Vec<Event>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
enum OutboundPublishMode {
    FirstContactStaged,
    OrdinaryFirstAck,
    #[default]
    WaitForPeer,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PendingOutbound {
    message_id: String,
    chat_id: String,
    body: String,
    #[serde(default)]
    prepared_publish: Option<PreparedPublishBatch>,
    #[serde(default)]
    publish_mode: OutboundPublishMode,
    #[serde(default)]
    reason: PendingSendReason,
    #[serde(default)]
    next_retry_at_secs: u64,
    #[serde(default)]
    in_flight: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
enum PendingGroupControlKind {
    Create {
        name: String,
        member_owner_hexes: Vec<String>,
    },
    Rename {
        name: String,
    },
    AddMembers {
        member_owner_hexes: Vec<String>,
    },
    RemoveMember {
        owner_pubkey_hex: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PendingGroupControl {
    operation_id: String,
    group_id: String,
    target_owner_hexes: Vec<String>,
    #[serde(default)]
    prepared_publish: Option<PreparedPublishBatch>,
    #[serde(default)]
    reason: PendingSendReason,
    #[serde(default)]
    next_retry_at_secs: u64,
    #[serde(default)]
    in_flight: bool,
    kind: PendingGroupControlKind,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct AppDirectMessagePayload {
    version: u8,
    chat_id: String,
    body: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct AppGroupMessagePayload {
    version: u8,
    body: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
struct OwnerProfileRecord {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    updated_at_secs: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
struct NostrProfileMetadata {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    display_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RoutedChatMessage {
    chat_id: String,
    body: String,
    is_outgoing: bool,
    author: Option<String>,
}

#[derive(Clone, Debug)]
struct RecentHandshakePeer {
    owner_hex: String,
    device_hex: String,
    observed_at_secs: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LocalAuthorizationState {
    Authorized,
    AwaitingApproval,
    Revoked,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
enum PendingSendReason {
    #[default]
    MissingRoster,
    MissingDeviceInvite,
    PublishingFirstContact,
    PublishRetry,
}

#[derive(Debug, Clone)]
struct StagedOutboundSend {
    message_id: String,
    chat_id: String,
    invite_events: Vec<Event>,
    message_events: Vec<Event>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProtocolSubscriptionPlan {
    roster_authors: Vec<String>,
    invite_authors: Vec<String>,
    invite_response_recipient: Option<String>,
    message_authors: Vec<String>,
}

#[derive(Clone, Debug, Default)]
struct ProtocolSubscriptionRuntime {
    current_plan: Option<ProtocolSubscriptionPlan>,
    applying_plan: Option<ProtocolSubscriptionPlan>,
    refresh_in_flight: bool,
    refresh_dirty: bool,
    force_refresh_dirty: bool,
    refresh_token: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct DebugEventCounters {
    roster_events: u64,
    invite_events: u64,
    invite_response_events: u64,
    message_events: u64,
    other_events: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DebugLogEntry {
    timestamp_secs: u64,
    category: String,
    detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RuntimeDebugSnapshot {
    generated_at_secs: u64,
    local_owner_pubkey_hex: Option<String>,
    local_device_pubkey_hex: Option<String>,
    authorization_state: Option<String>,
    active_chat_id: Option<String>,
    current_protocol_plan: Option<RuntimeProtocolPlanDebug>,
    tracked_owner_hexes: Vec<String>,
    known_users: Vec<RuntimeKnownUserDebug>,
    pending_outbound: Vec<RuntimePendingOutboundDebug>,
    pending_group_controls: Vec<RuntimePendingGroupControlDebug>,
    recent_handshake_peers: Vec<RuntimeRecentHandshakeDebug>,
    event_counts: DebugEventCounters,
    recent_log: Vec<DebugLogEntry>,
    toast: Option<String>,
    current_chat_list: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct SupportBuildMetadata {
    app_version: String,
    build_channel: String,
    git_sha: String,
    build_timestamp_utc: String,
    relay_set_id: String,
    trusted_test_build: bool,
}

#[derive(Clone, Debug, Serialize)]
struct SupportBundle {
    generated_at_secs: u64,
    build: SupportBuildMetadata,
    relay_urls: Vec<String>,
    authorization_state: Option<String>,
    active_chat_id: Option<String>,
    current_screen: String,
    chat_count: usize,
    direct_chat_count: usize,
    group_chat_count: usize,
    unread_chat_count: usize,
    pending_outbound: Vec<RuntimePendingOutboundDebug>,
    pending_group_controls: Vec<RuntimePendingGroupControlDebug>,
    protocol: Option<RuntimeProtocolPlanDebug>,
    tracked_owner_hexes: Vec<String>,
    known_users: Vec<RuntimeKnownUserDebug>,
    recent_handshake_peers: Vec<RuntimeRecentHandshakeDebug>,
    event_counts: DebugEventCounters,
    recent_log: Vec<DebugLogEntry>,
    current_chat_list: Vec<String>,
    latest_toast: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RuntimeProtocolPlanDebug {
    roster_authors: Vec<String>,
    invite_authors: Vec<String>,
    invite_response_recipient: Option<String>,
    message_authors: Vec<String>,
    refresh_in_flight: bool,
    refresh_dirty: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RuntimeKnownUserDebug {
    owner_pubkey_hex: String,
    has_roster: bool,
    roster_device_count: usize,
    device_count: usize,
    authorized_device_count: usize,
    active_session_device_count: usize,
    inactive_session_count: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RuntimePendingOutboundDebug {
    message_id: String,
    chat_id: String,
    reason: String,
    publish_mode: String,
    in_flight: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RuntimePendingGroupControlDebug {
    operation_id: String,
    group_id: String,
    target_owner_hexes: Vec<String>,
    reason: String,
    in_flight: bool,
    kind: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RuntimeRecentHandshakeDebug {
    owner_hex: String,
    device_hex: String,
    observed_at_secs: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedState {
    version: u32,
    #[serde(alias = "active_peer_hex")]
    active_chat_id: Option<String>,
    next_message_id: u64,
    session_manager: Option<SessionManagerSnapshot>,
    #[serde(default)]
    group_manager: Option<GroupManagerSnapshot>,
    #[serde(default)]
    owner_profiles: BTreeMap<String, OwnerProfileRecord>,
    threads: Vec<PersistedThread>,
    #[serde(default)]
    pending_inbound: Vec<PendingInbound>,
    #[serde(default)]
    pending_outbound: Vec<PendingOutbound>,
    #[serde(default)]
    pending_group_controls: Vec<PendingGroupControl>,
    #[serde(default)]
    seen_event_ids: Vec<String>,
    #[serde(default)]
    authorization_state: Option<PersistedAuthorizationState>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedThread {
    #[serde(alias = "peer_hex")]
    chat_id: String,
    unread_count: u64,
    #[serde(default)]
    updated_at_secs: u64,
    messages: Vec<PersistedMessage>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedMessage {
    id: String,
    #[serde(alias = "peer_input")]
    chat_id: String,
    author: String,
    body: String,
    is_outgoing: bool,
    created_at_secs: u64,
    delivery: PersistedDeliveryState,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum PersistedDeliveryState {
    Pending,
    Sent,
    Received,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum PersistedAuthorizationState {
    Authorized,
    AwaitingApproval,
    Revoked,
}

impl From<PersistedDeliveryState> for DeliveryState {
    fn from(value: PersistedDeliveryState) -> Self {
        match value {
            PersistedDeliveryState::Pending => DeliveryState::Pending,
            PersistedDeliveryState::Sent => DeliveryState::Sent,
            PersistedDeliveryState::Received => DeliveryState::Received,
            PersistedDeliveryState::Failed => DeliveryState::Failed,
        }
    }
}

impl From<&DeliveryState> for PersistedDeliveryState {
    fn from(value: &DeliveryState) -> Self {
        match value {
            DeliveryState::Pending => Self::Pending,
            DeliveryState::Sent => Self::Sent,
            DeliveryState::Received => Self::Received,
            DeliveryState::Failed => Self::Failed,
        }
    }
}

impl From<LocalAuthorizationState> for PersistedAuthorizationState {
    fn from(value: LocalAuthorizationState) -> Self {
        match value {
            LocalAuthorizationState::Authorized => Self::Authorized,
            LocalAuthorizationState::AwaitingApproval => Self::AwaitingApproval,
            LocalAuthorizationState::Revoked => Self::Revoked,
        }
    }
}

impl From<PersistedAuthorizationState> for LocalAuthorizationState {
    fn from(value: PersistedAuthorizationState) -> Self {
        match value {
            PersistedAuthorizationState::Authorized => Self::Authorized,
            PersistedAuthorizationState::AwaitingApproval => Self::AwaitingApproval,
            PersistedAuthorizationState::Revoked => Self::Revoked,
        }
    }
}

impl AppCore {
    pub fn new(
        update_tx: Sender<AppUpdate>,
        core_sender: Sender<CoreMsg>,
        data_dir: String,
        shared_state: Arc<RwLock<AppState>>,
    ) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");

        let state = AppState::empty();
        match shared_state.write() {
            Ok(mut slot) => *slot = state.clone(),
            Err(poison) => *poison.into_inner() = state.clone(),
        }

        Self {
            update_tx,
            core_sender,
            shared_state,
            runtime,
            data_dir: PathBuf::from(data_dir),
            state,
            logged_in: None,
            threads: BTreeMap::new(),
            active_chat_id: None,
            screen_stack: Vec::new(),
            next_message_id: 1,
            pending_inbound: Vec::new(),
            pending_outbound: Vec::new(),
            pending_group_controls: Vec::new(),
            owner_profiles: BTreeMap::new(),
            recent_handshake_peers: BTreeMap::new(),
            seen_event_ids: HashSet::new(),
            seen_event_order: VecDeque::new(),
            device_invite_poll_token: 0,
            protocol_subscription_runtime: ProtocolSubscriptionRuntime::default(),
            debug_log: VecDeque::new(),
            debug_event_counters: DebugEventCounters::default(),
        }
    }

    pub fn handle_message(&mut self, msg: CoreMsg) -> bool {
        match msg {
            CoreMsg::Action(action) => self.handle_action(action),
            CoreMsg::Internal(event) => self.handle_internal(*event),
            CoreMsg::ExportSupportBundle(reply_tx) => {
                let _ = reply_tx.send(self.export_support_bundle_json());
            }
            CoreMsg::Shutdown(reply_tx) => {
                self.shutdown();
                if let Some(reply_tx) = reply_tx {
                    let _ = reply_tx.send(());
                }
                return false;
            }
        }
        true
    }

    fn shutdown(&mut self) {
        self.push_debug_log("app.shutdown", "stopping core");
        self.device_invite_poll_token = self.device_invite_poll_token.saturating_add(1);
        if let Some(existing) = self.logged_in.take() {
            self.runtime.block_on(async {
                existing.client.unsubscribe_all().await;
                let _ = existing.client.shutdown().await;
            });
        }
    }

    fn handle_action(&mut self, action: AppAction) {
        self.state.toast = None;
        match action {
            AppAction::CreateAccount { name } => self.create_account(&name),
            AppAction::RestoreSession { owner_nsec } => self.restore_primary_session(&owner_nsec),
            AppAction::RestoreAccountBundle {
                owner_nsec,
                owner_pubkey_hex,
                device_nsec,
            } => self.restore_account_bundle(owner_nsec, &owner_pubkey_hex, &device_nsec),
            AppAction::StartLinkedDevice { owner_input } => self.start_linked_device(&owner_input),
            AppAction::AppForegrounded => self.handle_app_foregrounded(),
            AppAction::Logout => self.logout(),
            AppAction::CreateChat { peer_input } => self.create_chat(&peer_input),
            AppAction::CreateGroup {
                name,
                member_inputs,
            } => self.create_group(&name, &member_inputs),
            AppAction::OpenChat { chat_id } => self.open_chat(&chat_id),
            AppAction::SendMessage { chat_id, text } => self.send_message(&chat_id, &text),
            AppAction::UpdateGroupName { group_id, name } => {
                self.update_group_name(&group_id, &name)
            }
            AppAction::AddGroupMembers {
                group_id,
                member_inputs,
            } => self.add_group_members(&group_id, &member_inputs),
            AppAction::RemoveGroupMember {
                group_id,
                owner_pubkey_hex,
            } => self.remove_group_member(&group_id, &owner_pubkey_hex),
            AppAction::AddAuthorizedDevice { device_input } => {
                self.add_authorized_device(&device_input)
            }
            AppAction::RemoveAuthorizedDevice { device_pubkey_hex } => {
                self.remove_authorized_device(&device_pubkey_hex)
            }
            AppAction::AcknowledgeRevokedDevice => self.acknowledge_revoked_device(),
            AppAction::PushScreen { screen } => self.push_screen(screen),
            AppAction::UpdateScreenStack { stack } => self.update_screen_stack(stack),
        }
    }

    fn handle_internal(&mut self, event: InternalEvent) {
        match event {
            InternalEvent::RelayEvent(event) => {
                self.handle_relay_event(event);
            }
            InternalEvent::RetryPendingOutbound => {
                let now = unix_now();
                self.retry_pending_outbound(now);
                self.retry_pending_group_controls(now);
                self.rebuild_state();
                self.persist_best_effort();
                self.emit_state();
            }
            InternalEvent::FetchTrackedPeerCatchUp => {
                let now = unix_now();
                self.push_debug_log("protocol.catch_up.schedule", "fetch tracked peers");
                self.fetch_recent_protocol_state();
                self.fetch_recent_messages_for_tracked_peers(now);
                if self.is_device_roster_open() {
                    self.fetch_pending_device_invites_for_local_owner();
                }
            }
            InternalEvent::PollPendingDeviceInvites { token } => {
                if token != self.device_invite_poll_token || !self.can_poll_pending_device_invites()
                {
                    return;
                }
                self.fetch_pending_device_invites_for_local_owner();
                self.schedule_pending_device_invite_poll(Duration::from_secs(
                    DEVICE_INVITE_DISCOVERY_POLL_SECS,
                ));
            }
            InternalEvent::FetchCatchUpEvents(events) => {
                for event in events {
                    self.handle_relay_event(event);
                }
            }
            InternalEvent::FetchPendingDeviceInvites(events) => {
                self.handle_pending_device_invite_events(events);
            }
            InternalEvent::DebugLog { category, detail } => {
                self.push_debug_log(&category, detail);
                self.persist_debug_snapshot_best_effort();
            }
            InternalEvent::PublishFinished {
                message_id,
                chat_id,
                success,
            } => {
                if success {
                    self.pending_outbound
                        .retain(|pending| pending.message_id != message_id);
                    self.update_message_delivery(&chat_id, &message_id, DeliveryState::Sent);
                } else if let Some(pending) = self
                    .pending_outbound
                    .iter_mut()
                    .find(|pending| pending.message_id == message_id)
                {
                    pending.in_flight = false;
                    pending.reason = PendingSendReason::PublishRetry;
                    let retry_after_secs = retry_delay_for_publish_mode(&pending.publish_mode);
                    pending.next_retry_at_secs = unix_now().get().saturating_add(retry_after_secs);
                    self.schedule_pending_outbound_retry(Duration::from_secs(retry_after_secs));
                }
                self.schedule_next_pending_retry(unix_now().get());
                self.rebuild_state();
                self.persist_best_effort();
                self.emit_state();
            }
            InternalEvent::GroupControlPublishFinished {
                operation_id,
                success,
            } => {
                if success {
                    self.pending_group_controls
                        .retain(|pending| pending.operation_id != operation_id);
                } else if let Some(pending) = self
                    .pending_group_controls
                    .iter_mut()
                    .find(|pending| pending.operation_id == operation_id)
                {
                    pending.in_flight = false;
                    pending.reason = PendingSendReason::PublishRetry;
                    pending.next_retry_at_secs =
                        unix_now().get().saturating_add(PENDING_RETRY_DELAY_SECS);
                    self.schedule_pending_outbound_retry(Duration::from_secs(
                        PENDING_RETRY_DELAY_SECS,
                    ));
                }
                self.schedule_next_pending_retry(unix_now().get());
                self.rebuild_state();
                self.persist_best_effort();
                self.emit_state();
            }
            InternalEvent::ProtocolSubscriptionRefreshCompleted {
                token,
                applied,
                plan,
            } => {
                if token != self.protocol_subscription_runtime.refresh_token {
                    return;
                }
                self.protocol_subscription_runtime.refresh_in_flight = false;
                self.protocol_subscription_runtime.applying_plan = None;
                if applied {
                    self.push_debug_log(
                        "protocol.subscription.applied",
                        summarize_protocol_plan(plan.as_ref()),
                    );
                    self.protocol_subscription_runtime.current_plan = plan;
                    self.fetch_recent_protocol_state();
                    self.persist_best_effort();
                } else {
                    self.push_debug_log("protocol.subscription.failed", "apply returned false");
                }
                if self.protocol_subscription_runtime.refresh_dirty {
                    let force_refresh = self.protocol_subscription_runtime.force_refresh_dirty;
                    self.protocol_subscription_runtime.refresh_dirty = false;
                    self.protocol_subscription_runtime.force_refresh_dirty = false;
                    self.request_protocol_subscription_refresh_inner(force_refresh);
                }
            }
            InternalEvent::SyncComplete => {
                self.state.busy.syncing_network = false;
                self.emit_state();
            }
            InternalEvent::Toast(message) => {
                self.state.toast = Some(message);
                self.emit_state();
            }
        }
    }

    fn create_account(&mut self, name: &str) {
        self.state.busy.creating_account = true;
        self.emit_state();

        let owner_keys = Keys::generate();
        let device_keys = Keys::generate();
        let trimmed_name = name.trim().to_string();

        if let Err(error) = self.start_primary_session(owner_keys, device_keys, false, false) {
            self.state.toast = Some(error.to_string());
        } else if !trimmed_name.is_empty() {
            self.set_local_profile_name(&trimmed_name);
            self.republish_local_identity_artifacts();
        }

        self.state.busy.creating_account = false;
        self.rebuild_state();
        self.emit_state();
    }

    fn handle_app_foregrounded(&mut self) {
        if self.logged_in.is_none() {
            return;
        }

        let now = unix_now();
        self.push_debug_log("app.foreground", "refresh relay session");
        self.schedule_session_connect();
        self.request_protocol_subscription_refresh_forced();
        self.fetch_recent_protocol_state();
        self.fetch_recent_messages_for_tracked_peers(now);
        if self.can_poll_pending_device_invites() {
            self.fetch_pending_device_invites_for_local_owner();
            self.schedule_pending_device_invite_poll(Duration::from_secs(
                DEVICE_INVITE_DISCOVERY_POLL_SECS,
            ));
        }

        for pending in &mut self.pending_outbound {
            if !pending.in_flight {
                pending.next_retry_at_secs = now.get();
            }
        }
        for pending in &mut self.pending_group_controls {
            if !pending.in_flight {
                pending.next_retry_at_secs = now.get();
            }
        }
        self.retry_pending_outbound(now);
        self.retry_pending_group_controls(now);
        self.schedule_next_pending_retry(now.get());
        self.state.busy.syncing_network = true;
        self.rebuild_state();
        self.persist_best_effort();
        self.emit_state();
    }

    fn restore_primary_session(&mut self, owner_nsec: &str) {
        self.state.busy.restoring_session = true;
        self.emit_state();

        let result = Keys::parse(owner_nsec.trim())
            .map_err(|error| anyhow::anyhow!(error.to_string()))
            .and_then(|owner_keys| {
                self.start_primary_session(owner_keys, Keys::generate(), true, false)
            });

        if let Err(error) = result {
            self.state.toast = Some(error.to_string());
        }

        self.state.busy.restoring_session = false;
        self.rebuild_state();
        self.emit_state();
    }

    fn restore_account_bundle(
        &mut self,
        owner_nsec: Option<String>,
        owner_pubkey_hex: &str,
        device_nsec: &str,
    ) {
        self.push_debug_log(
            "session.restore_bundle",
            format!(
                "owner_pubkey_hex={} has_owner_nsec={}",
                owner_pubkey_hex.trim(),
                owner_nsec
                    .as_ref()
                    .map(|value| !value.trim().is_empty())
                    .unwrap_or(false),
            ),
        );
        self.state.busy.restoring_session = true;
        self.emit_state();

        let result = (|| -> anyhow::Result<()> {
            let owner_pubkey = parse_owner_input(owner_pubkey_hex)?;
            let owner_keys = match owner_nsec {
                Some(secret) => {
                    let keys = Keys::parse(secret.trim())
                        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
                    let derived_owner = OwnerPubkey::from_bytes(keys.public_key().to_bytes());
                    if derived_owner != owner_pubkey {
                        return Err(anyhow::anyhow!(
                            "stored owner secret does not match stored owner pubkey"
                        ));
                    }
                    Some(keys)
                }
                None => None,
            };
            let device_keys = Keys::parse(device_nsec.trim())
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            self.start_session(owner_pubkey, owner_keys, device_keys, true, true)
        })();

        if let Err(error) = result {
            self.state.toast = Some(error.to_string());
        }

        self.state.busy.restoring_session = false;
        self.rebuild_state();
        self.emit_state();
    }

    fn start_linked_device(&mut self, owner_input: &str) {
        self.push_debug_log(
            "session.start_linked",
            format!("owner_input={}", owner_input.trim()),
        );
        self.state.busy.linking_device = true;
        self.emit_state();

        let result = parse_owner_input(owner_input).and_then(|owner_pubkey| {
            self.start_session(owner_pubkey, None, Keys::generate(), false, false)
        });
        if let Err(error) = result {
            self.state.toast = Some(error.to_string());
        }

        self.state.busy.linking_device = false;
        self.rebuild_state();
        self.emit_state();
    }

    fn logout(&mut self) {
        self.push_debug_log("session.logout", "clearing runtime state");
        let previous_rev = self.state.rev;
        self.device_invite_poll_token = self.device_invite_poll_token.saturating_add(1);
        if let Some(logged_in) = self.logged_in.take() {
            let client = logged_in.client.clone();
            self.runtime.spawn(async move {
                client.unsubscribe_all().await;
                let _ = client.shutdown().await;
            });
        }

        self.threads.clear();
        self.active_chat_id = None;
        self.screen_stack.clear();
        self.pending_inbound.clear();
        self.pending_outbound.clear();
        self.pending_group_controls.clear();
        self.owner_profiles.clear();
        self.recent_handshake_peers.clear();
        self.seen_event_ids.clear();
        self.seen_event_order.clear();
        self.protocol_subscription_runtime = ProtocolSubscriptionRuntime::default();
        self.next_message_id = 1;
        self.state = AppState::empty();
        self.state.rev = previous_rev;
        self.clear_persistence_best_effort();
        self.emit_state();
    }

    fn create_chat(&mut self, peer_input: &str) {
        if self.logged_in.is_none() {
            self.state.toast = Some("Create or restore an account first.".to_string());
            self.emit_state();
            return;
        }
        if !self.can_use_chats() {
            self.state.toast = Some(chat_unavailable_message(self.logged_in.as_ref()).to_string());
            self.emit_state();
            return;
        }

        self.state.busy.creating_chat = true;
        self.emit_state();

        let Ok((chat_id, _pubkey)) = parse_peer_input(peer_input) else {
            self.state.toast = Some("Invalid peer key.".to_string());
            self.state.busy.creating_chat = false;
            self.emit_state();
            return;
        };
        self.push_debug_log(
            "chat.create",
            format!("peer_input={} chat_id={chat_id}", peer_input.trim()),
        );

        let now = unix_now().get();
        self.prune_recent_handshake_peers(now);
        self.ensure_thread_record(&chat_id, now).unread_count = 0;

        self.active_chat_id = Some(chat_id.clone());
        self.screen_stack = vec![Screen::Chat { chat_id }];
        self.republish_local_identity_artifacts();
        self.rebuild_state();
        self.persist_best_effort();
        self.request_protocol_subscription_refresh();
        self.schedule_tracked_peer_catch_up(Duration::from_secs(RESUBSCRIBE_CATCH_UP_DELAY_SECS));
        self.state.busy.creating_chat = false;
        self.emit_state();
    }

    fn create_group(&mut self, name: &str, member_inputs: &[String]) {
        if self.logged_in.is_none() {
            self.state.toast = Some("Create or restore an account first.".to_string());
            self.emit_state();
            return;
        }
        if !self.can_use_chats() {
            self.state.toast = Some(chat_unavailable_message(self.logged_in.as_ref()).to_string());
            self.emit_state();
            return;
        }

        let trimmed_name = name.trim();
        if trimmed_name.is_empty() {
            self.state.toast = Some("Group name is required.".to_string());
            self.emit_state();
            return;
        }

        let Some(local_owner) = self
            .logged_in
            .as_ref()
            .map(|logged_in| logged_in.owner_pubkey)
        else {
            self.state.toast = Some("Create or restore an account first.".to_string());
            self.emit_state();
            return;
        };

        let member_owners = match parse_owner_inputs(member_inputs, local_owner) {
            Ok(member_owners) if !member_owners.is_empty() => member_owners,
            Ok(_) => {
                self.state.toast = Some("Groups need at least one other member.".to_string());
                self.emit_state();
                return;
            }
            Err(error) => {
                self.state.toast = Some(error.to_string());
                self.emit_state();
                return;
            }
        };
        let target_owner_hexes = sorted_owner_hexes(&member_owners);

        self.state.busy.creating_group = true;
        self.emit_state();

        let now = unix_now();
        let create_result = {
            let logged_in = self.logged_in.as_mut().expect("checked above");
            let mut rng = OsRng;
            let mut ctx = ProtocolContext::new(now, &mut rng);
            let (session_manager, group_manager) =
                (&mut logged_in.session_manager, &mut logged_in.group_manager);
            group_manager.create_group(
                session_manager,
                &mut ctx,
                trimmed_name.to_string(),
                member_owners,
            )
        };

        match create_result {
            Ok(result) => {
                let create_kind = PendingGroupControlKind::Create {
                    name: trimmed_name.to_string(),
                    member_owner_hexes: target_owner_hexes.clone(),
                };
                let chat_id = group_chat_id(&result.group.group_id);
                self.apply_group_snapshot_to_threads(&result.group, now.get());
                self.active_chat_id = Some(chat_id.clone());
                self.screen_stack = vec![Screen::Chat {
                    chat_id: chat_id.clone(),
                }];
                self.publish_group_local_sibling_best_effort(&result.prepared);

                if let Some(reason) = pending_reason_from_group_prepared(&result.prepared) {
                    let operation_id = self.allocate_message_id();
                    self.queue_pending_group_control(
                        operation_id,
                        result.group.group_id.clone(),
                        target_owner_hexes,
                        None,
                        reason.clone(),
                        now.get().saturating_add(PENDING_RETRY_DELAY_SECS),
                        create_kind,
                    );
                    self.nudge_protocol_state_for_pending_reason(&reason);
                } else {
                    match build_group_prepared_publish_batch(&result.prepared) {
                        Ok(Some(batch)) => {
                            let operation_id = self.allocate_message_id();
                            let publish_mode = publish_mode_for_batch(&batch);
                            self.queue_pending_group_control(
                                operation_id.clone(),
                                result.group.group_id.clone(),
                                target_owner_hexes,
                                Some(batch.clone()),
                                pending_reason_for_publish_mode(&publish_mode),
                                retry_deadline_for_publish_mode(now.get(), &publish_mode),
                                create_kind.clone(),
                            );
                            self.set_pending_group_control_in_flight(&operation_id, true);
                            self.start_group_control_publish(operation_id, publish_mode, batch);
                        }
                        Ok(None) => {}
                        Err(error) => self.state.toast = Some(error.to_string()),
                    }
                }

                self.request_protocol_subscription_refresh();
                self.schedule_tracked_peer_catch_up(Duration::from_secs(
                    RESUBSCRIBE_CATCH_UP_DELAY_SECS,
                ));
            }
            Err(error) => {
                self.state.toast = Some(error.to_string());
            }
        }

        self.schedule_next_pending_retry(now.get());
        self.state.busy.creating_group = false;
        self.rebuild_state();
        self.persist_best_effort();
        self.emit_state();
    }

    fn ensure_thread_record(&mut self, chat_id: &str, updated_at_secs: u64) -> &mut ThreadRecord {
        let thread = self
            .threads
            .entry(chat_id.to_string())
            .or_insert_with(|| ThreadRecord {
                chat_id: chat_id.to_string(),
                unread_count: 0,
                updated_at_secs,
                messages: Vec::new(),
            });
        if thread.updated_at_secs == 0 {
            thread.updated_at_secs = updated_at_secs;
        }
        thread
    }

    fn normalize_chat_id(&self, chat_id: &str) -> Option<String> {
        if is_group_chat_id(chat_id) {
            let group_id = parse_group_id_from_chat_id(chat_id)?;
            let group_chat_id = group_chat_id(&group_id);
            let known_group = self
                .logged_in
                .as_ref()
                .and_then(|logged_in| logged_in.group_manager.group(&group_id))
                .is_some();
            if known_group || self.threads.contains_key(&group_chat_id) {
                return Some(group_chat_id);
            }
            return None;
        }

        parse_peer_input(chat_id)
            .ok()
            .map(|(normalized, _)| normalized)
    }

    fn prepare_group_control(
        &mut self,
        group_id: &str,
        kind: &PendingGroupControlKind,
        now: UnixSeconds,
    ) -> anyhow::Result<(
        GroupSnapshot,
        Vec<String>,
        nostr_double_ratchet::GroupPreparedSend,
    )> {
        let logged_in = self.logged_in.as_mut().expect("logged in checked above");
        let mut rng = OsRng;
        let mut ctx = ProtocolContext::new(now, &mut rng);
        let (session_manager, group_manager) =
            (&mut logged_in.session_manager, &mut logged_in.group_manager);

        match kind {
            PendingGroupControlKind::Create {
                name,
                member_owner_hexes,
            } => {
                let members = owner_pubkeys_from_hexes(member_owner_hexes)?;
                let result =
                    group_manager.create_group(session_manager, &mut ctx, name.clone(), members)?;
                return Ok((
                    result.group.clone(),
                    member_owner_hexes.clone(),
                    result.prepared,
                ));
            }
            PendingGroupControlKind::Rename { name } => {
                let prepared =
                    group_manager.update_name(session_manager, &mut ctx, group_id, name.clone())?;
                let snapshot = group_manager
                    .group(group_id)
                    .ok_or_else(|| anyhow::anyhow!("Unknown group."))?;
                return Ok((
                    snapshot.clone(),
                    sorted_owner_hexes(
                        &snapshot
                            .members
                            .iter()
                            .copied()
                            .filter(|member| *member != logged_in.owner_pubkey)
                            .collect::<Vec<_>>(),
                    ),
                    prepared,
                ));
            }
            PendingGroupControlKind::AddMembers { member_owner_hexes } => {
                let members = owner_pubkeys_from_hexes(member_owner_hexes)?;
                let prepared =
                    group_manager.add_members(session_manager, &mut ctx, group_id, members)?;
                let snapshot = group_manager
                    .group(group_id)
                    .ok_or_else(|| anyhow::anyhow!("Unknown group."))?;
                return Ok((
                    snapshot.clone(),
                    sorted_owner_hexes(
                        &snapshot
                            .members
                            .iter()
                            .copied()
                            .filter(|member| *member != logged_in.owner_pubkey)
                            .collect::<Vec<_>>(),
                    ),
                    prepared,
                ));
            }
            PendingGroupControlKind::RemoveMember { owner_pubkey_hex } => {
                let owner = parse_owner_input(owner_pubkey_hex)?;
                let prepared = group_manager.remove_members(
                    session_manager,
                    &mut ctx,
                    group_id,
                    vec![owner],
                )?;
                let snapshot = group_manager
                    .group(group_id)
                    .ok_or_else(|| anyhow::anyhow!("Unknown group."))?;
                return Ok((
                    snapshot.clone(),
                    sorted_owner_hexes(
                        &snapshot
                            .members
                            .iter()
                            .copied()
                            .filter(|member| *member != logged_in.owner_pubkey)
                            .collect::<Vec<_>>(),
                    ),
                    prepared,
                ));
            }
        }
    }

    fn rebuild_group_control(
        &mut self,
        group_id: &str,
        kind: &PendingGroupControlKind,
        now: UnixSeconds,
    ) -> anyhow::Result<(
        GroupSnapshot,
        Vec<String>,
        nostr_double_ratchet::GroupPreparedSend,
    )> {
        let logged_in = self.logged_in.as_mut().expect("logged in checked above");
        let mut rng = OsRng;
        let mut ctx = ProtocolContext::new(now, &mut rng);
        let (session_manager, group_manager) =
            (&mut logged_in.session_manager, &mut logged_in.group_manager);

        match kind {
            PendingGroupControlKind::Create {
                member_owner_hexes, ..
            } => {
                let members = owner_pubkeys_from_hexes(member_owner_hexes)?;
                let prepared = group_manager.retry_create_group(
                    session_manager,
                    &mut ctx,
                    group_id,
                    members,
                )?;
                let snapshot = group_manager
                    .group(group_id)
                    .ok_or_else(|| anyhow::anyhow!("Unknown group."))?;
                Ok((snapshot, member_owner_hexes.clone(), prepared))
            }
            PendingGroupControlKind::Rename { .. } => {
                let prepared =
                    group_manager.retry_update_name(session_manager, &mut ctx, group_id)?;
                let snapshot = group_manager
                    .group(group_id)
                    .ok_or_else(|| anyhow::anyhow!("Unknown group."))?;
                let target_owner_hexes = sorted_owner_hexes(
                    &snapshot
                        .members
                        .iter()
                        .copied()
                        .filter(|member| *member != logged_in.owner_pubkey)
                        .collect::<Vec<_>>(),
                );
                Ok((snapshot, target_owner_hexes, prepared))
            }
            PendingGroupControlKind::AddMembers { member_owner_hexes } => {
                let members = owner_pubkeys_from_hexes(member_owner_hexes)?;
                let prepared = group_manager.retry_add_members(
                    session_manager,
                    &mut ctx,
                    group_id,
                    members,
                )?;
                let snapshot = group_manager
                    .group(group_id)
                    .ok_or_else(|| anyhow::anyhow!("Unknown group."))?;
                let target_owner_hexes = sorted_owner_hexes(
                    &snapshot
                        .members
                        .iter()
                        .copied()
                        .filter(|member| *member != logged_in.owner_pubkey)
                        .collect::<Vec<_>>(),
                );
                Ok((snapshot, target_owner_hexes, prepared))
            }
            PendingGroupControlKind::RemoveMember { owner_pubkey_hex } => {
                let owner = parse_owner_input(owner_pubkey_hex)?;
                let prepared = group_manager.retry_remove_members(
                    session_manager,
                    &mut ctx,
                    group_id,
                    vec![owner],
                )?;
                let snapshot = group_manager
                    .group(group_id)
                    .ok_or_else(|| anyhow::anyhow!("Unknown group."))?;
                let mut targets = snapshot
                    .members
                    .iter()
                    .copied()
                    .filter(|member| *member != logged_in.owner_pubkey)
                    .map(|member| member.to_string())
                    .collect::<HashSet<_>>();
                if owner != logged_in.owner_pubkey {
                    targets.insert(owner.to_string());
                }
                Ok((snapshot, sorted_hexes(targets), prepared))
            }
        }
    }

    fn apply_group_snapshot_to_threads(&mut self, group: &GroupSnapshot, updated_at_secs: u64) {
        let chat_id = group_chat_id(&group.group_id);
        let thread = self.ensure_thread_record(&chat_id, updated_at_secs);
        thread.updated_at_secs = thread.updated_at_secs.max(updated_at_secs);
    }

    fn queue_pending_group_control(
        &mut self,
        operation_id: String,
        group_id: String,
        target_owner_hexes: Vec<String>,
        prepared_publish: Option<PreparedPublishBatch>,
        reason: PendingSendReason,
        next_retry_at_secs: u64,
        kind: PendingGroupControlKind,
    ) {
        self.pending_group_controls.push(PendingGroupControl {
            operation_id,
            group_id,
            target_owner_hexes,
            prepared_publish,
            reason,
            next_retry_at_secs,
            in_flight: false,
            kind,
        });
    }

    fn set_pending_group_control_in_flight(&mut self, operation_id: &str, in_flight: bool) {
        if let Some(pending) = self
            .pending_group_controls
            .iter_mut()
            .find(|pending| pending.operation_id == operation_id)
        {
            pending.in_flight = in_flight;
        }
    }

    fn start_group_control_publish(
        &mut self,
        operation_id: String,
        publish_mode: OutboundPublishMode,
        batch: PreparedPublishBatch,
    ) {
        let Some((client, relay_urls)) = self
            .logged_in
            .as_ref()
            .map(|logged_in| (logged_in.client.clone(), logged_in.relay_urls.clone()))
        else {
            return;
        };

        for event in batch
            .invite_events
            .iter()
            .chain(batch.message_events.iter())
        {
            self.remember_event(event.id.to_string());
        }

        let tx = self.core_sender.clone();
        match publish_mode {
            OutboundPublishMode::OrdinaryFirstAck => {
                self.runtime.spawn(async move {
                    let success = publish_events_first_ack(
                        &client,
                        &relay_urls,
                        &batch.message_events,
                        "group control",
                    )
                    .await
                    .is_ok();
                    let _ = tx.send(CoreMsg::Internal(Box::new(
                        InternalEvent::GroupControlPublishFinished {
                            operation_id,
                            success,
                        },
                    )));
                });
            }
            OutboundPublishMode::FirstContactStaged => {
                self.runtime.spawn(async move {
                    let invite_publish = publish_events_with_retry(
                        &client,
                        &relay_urls,
                        batch.invite_events,
                        "group control",
                    )
                    .await;
                    if invite_publish.is_err() {
                        let _ = tx.send(CoreMsg::Internal(Box::new(
                            InternalEvent::GroupControlPublishFinished {
                                operation_id,
                                success: false,
                            },
                        )));
                        return;
                    }

                    sleep(Duration::from_millis(FIRST_CONTACT_STAGE_DELAY_MS)).await;
                    let success = publish_events_with_retry(
                        &client,
                        &relay_urls,
                        batch.message_events,
                        "group control",
                    )
                    .await
                    .is_ok();
                    let _ = tx.send(CoreMsg::Internal(Box::new(
                        InternalEvent::GroupControlPublishFinished {
                            operation_id,
                            success,
                        },
                    )));
                });
            }
            OutboundPublishMode::WaitForPeer => {}
        }
    }

    fn start_best_effort_publish(&mut self, label: &'static str, batch: PreparedPublishBatch) {
        let Some((client, relay_urls)) = self
            .logged_in
            .as_ref()
            .map(|logged_in| (logged_in.client.clone(), logged_in.relay_urls.clone()))
        else {
            return;
        };
        if batch.message_events.is_empty() {
            return;
        }

        for event in batch
            .invite_events
            .iter()
            .chain(batch.message_events.iter())
        {
            self.remember_event(event.id.to_string());
        }

        let tx = self.core_sender.clone();
        match publish_mode_for_batch(&batch) {
            OutboundPublishMode::OrdinaryFirstAck => {
                self.runtime.spawn(async move {
                    let success = publish_events_first_ack(
                        &client,
                        &relay_urls,
                        &batch.message_events,
                        label,
                    )
                    .await
                    .is_ok();
                    let _ = tx.send(CoreMsg::Internal(Box::new(InternalEvent::DebugLog {
                        category: "publish.best_effort".to_string(),
                        detail: format!("label={label} success={success}"),
                    })));
                });
            }
            OutboundPublishMode::FirstContactStaged => {
                self.runtime.spawn(async move {
                    let success = if publish_events_with_retry(
                        &client,
                        &relay_urls,
                        batch.invite_events,
                        label,
                    )
                    .await
                    .is_ok()
                    {
                        sleep(Duration::from_millis(FIRST_CONTACT_STAGE_DELAY_MS)).await;
                        publish_events_with_retry(&client, &relay_urls, batch.message_events, label)
                            .await
                            .is_ok()
                    } else {
                        false
                    };
                    let _ = tx.send(CoreMsg::Internal(Box::new(InternalEvent::DebugLog {
                        category: "publish.best_effort".to_string(),
                        detail: format!("label={label} success={success}"),
                    })));
                });
            }
            OutboundPublishMode::WaitForPeer => {}
        }
    }

    fn publish_group_local_sibling_best_effort(
        &mut self,
        prepared: &nostr_double_ratchet::GroupPreparedSend,
    ) {
        match build_group_local_sibling_publish_batch(prepared) {
            Ok(Some(batch)) => self.start_best_effort_publish("group sibling sync", batch),
            Ok(None) => {}
            Err(error) => self.state.toast = Some(error.to_string()),
        }
    }

    fn open_chat(&mut self, chat_id: &str) {
        if !self.can_use_chats() {
            self.state.toast = Some(chat_unavailable_message(self.logged_in.as_ref()).to_string());
            self.emit_state();
            return;
        }

        let Some(chat_id) = self.normalize_chat_id(chat_id) else {
            self.state.toast = Some("Invalid chat id.".to_string());
            self.emit_state();
            return;
        };

        let now = unix_now().get();
        self.prune_recent_handshake_peers(now);
        self.ensure_thread_record(&chat_id, now).unread_count = 0;
        self.active_chat_id = Some(chat_id.clone());
        self.screen_stack = vec![Screen::Chat {
            chat_id: chat_id.clone(),
        }];
        self.republish_local_identity_artifacts();
        self.rebuild_state();
        self.persist_best_effort();
        self.request_protocol_subscription_refresh();
        self.schedule_tracked_peer_catch_up(Duration::from_secs(RESUBSCRIBE_CATCH_UP_DELAY_SECS));
        self.emit_state();
    }

    fn send_message(&mut self, chat_id: &str, text: &str) {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return;
        }

        if self.logged_in.is_none() {
            self.state.toast = Some("Create or restore an account first.".to_string());
            self.emit_state();
            return;
        }
        if !self.can_use_chats() {
            self.state.toast = Some(chat_unavailable_message(self.logged_in.as_ref()).to_string());
            self.emit_state();
            return;
        }

        let Some(normalized_chat_id) = self.normalize_chat_id(chat_id) else {
            self.state.toast = Some("Invalid chat id.".to_string());
            self.emit_state();
            return;
        };
        self.push_debug_log(
            "chat.send",
            format!(
                "chat_id={} is_group={}",
                normalized_chat_id,
                is_group_chat_id(&normalized_chat_id)
            ),
        );

        let now = unix_now();
        self.prune_recent_handshake_peers(now.get());
        self.active_chat_id = Some(normalized_chat_id.clone());
        self.screen_stack = vec![Screen::Chat {
            chat_id: normalized_chat_id.clone(),
        }];
        self.ensure_thread_record(&normalized_chat_id, now.get());
        self.state.busy.sending_message = true;
        self.rebuild_state();
        self.emit_state();

        if is_group_chat_id(&normalized_chat_id) {
            self.send_group_message(&normalized_chat_id, trimmed, now);
        } else {
            self.send_direct_message(&normalized_chat_id, trimmed, now);
        }

        self.schedule_next_pending_retry(now.get());
        self.state.busy.sending_message = false;
        self.rebuild_state();
        self.persist_best_effort();
        self.emit_state();
    }

    fn send_direct_message(&mut self, chat_id: &str, text: &str, now: UnixSeconds) {
        let Ok((normalized_chat_id, peer_pubkey)) = parse_peer_input(chat_id) else {
            self.state.toast = Some("Invalid peer key.".to_string());
            return;
        };

        let owner = OwnerPubkey::from_bytes(peer_pubkey.to_bytes());
        let prepared = {
            let logged_in = self.logged_in.as_mut().expect("logged in checked above");
            prepare_direct_send_payloads(logged_in, owner, &normalized_chat_id, text, now)
        };

        self.handle_prepared_direct_send(&normalized_chat_id, text, now, prepared);
    }

    fn send_group_message(&mut self, chat_id: &str, text: &str, now: UnixSeconds) {
        let Some(group_id) = parse_group_id_from_chat_id(chat_id) else {
            self.state.toast = Some("Invalid group id.".to_string());
            return;
        };
        let payload = match encode_app_group_message_payload(text) {
            Ok(payload) => payload,
            Err(error) => {
                self.state.toast = Some(error.to_string());
                return;
            }
        };

        let prepared = {
            let logged_in = self.logged_in.as_mut().expect("logged in checked above");
            let mut rng = OsRng;
            let mut ctx = ProtocolContext::new(now, &mut rng);
            let (session_manager, group_manager) =
                (&mut logged_in.session_manager, &mut logged_in.group_manager);
            group_manager.send_message(session_manager, &mut ctx, &group_id, payload)
        };

        match prepared {
            Ok(prepared) => {
                self.publish_group_local_sibling_best_effort(&prepared);
                if let Some(reason) = pending_reason_from_group_prepared(&prepared) {
                    self.push_debug_log(
                        "group.send.pending",
                        format!(
                            "chat_id={} reason={reason:?} gaps={}",
                            chat_id,
                            summarize_relay_gaps(&prepared.remote.relay_gaps)
                        ),
                    );
                    let pending_reason = reason.clone();
                    let message = self.push_outgoing_message(
                        chat_id,
                        text.to_string(),
                        now.get(),
                        DeliveryState::Pending,
                    );
                    self.queue_pending_outbound(
                        message.id,
                        chat_id.to_string(),
                        text.to_string(),
                        None,
                        OutboundPublishMode::WaitForPeer,
                        pending_reason.clone(),
                        now.get().saturating_add(PENDING_RETRY_DELAY_SECS),
                    );
                    self.nudge_protocol_state_for_pending_reason(&pending_reason);
                    self.request_protocol_subscription_refresh();
                    self.schedule_pending_outbound_retry(Duration::from_secs(
                        PENDING_RETRY_DELAY_SECS,
                    ));
                } else {
                    match build_group_prepared_publish_batch(&prepared) {
                        Ok(Some(batch)) => {
                            let publish_mode = publish_mode_for_batch(&batch);
                            let message = self.push_outgoing_message(
                                chat_id,
                                text.to_string(),
                                now.get(),
                                DeliveryState::Pending,
                            );
                            self.queue_pending_outbound(
                                message.id.clone(),
                                chat_id.to_string(),
                                text.to_string(),
                                Some(batch.clone()),
                                publish_mode.clone(),
                                pending_reason_for_publish_mode(&publish_mode),
                                retry_deadline_for_publish_mode(now.get(), &publish_mode),
                            );
                            self.set_pending_outbound_in_flight(&message.id, true);
                            self.start_publish_for_pending(
                                message.id,
                                chat_id.to_string(),
                                publish_mode,
                                batch,
                            );
                        }
                        Ok(None) => {
                            let message = self.push_outgoing_message(
                                chat_id,
                                text.to_string(),
                                now.get(),
                                DeliveryState::Failed,
                            );
                            self.update_message_delivery(
                                chat_id,
                                &message.id,
                                DeliveryState::Failed,
                            );
                        }
                        Err(error) => self.state.toast = Some(error.to_string()),
                    }
                }
            }
            Err(error) => {
                self.state.toast = Some(error.to_string());
            }
        }
    }

    fn handle_prepared_direct_send(
        &mut self,
        chat_id: &str,
        text: &str,
        now: UnixSeconds,
        prepared: anyhow::Result<nostr_double_ratchet::PreparedSend>,
    ) {
        match prepared {
            Ok(prepared) => {
                if let Some(reason) = pending_reason_from_prepared(&prepared) {
                    self.push_debug_log(
                        "direct.send.pending",
                        format!(
                            "chat_id={} reason={reason:?} gaps={}",
                            chat_id,
                            summarize_relay_gaps(&prepared.relay_gaps)
                        ),
                    );
                    let pending_reason = reason.clone();
                    let message = self.push_outgoing_message(
                        chat_id,
                        text.to_string(),
                        now.get(),
                        DeliveryState::Pending,
                    );
                    self.queue_pending_outbound(
                        message.id,
                        chat_id.to_string(),
                        text.to_string(),
                        None,
                        OutboundPublishMode::WaitForPeer,
                        pending_reason.clone(),
                        now.get().saturating_add(PENDING_RETRY_DELAY_SECS),
                    );
                    self.nudge_protocol_state_for_pending_reason(&pending_reason);
                    self.request_protocol_subscription_refresh();
                    self.schedule_pending_outbound_retry(Duration::from_secs(
                        PENDING_RETRY_DELAY_SECS,
                    ));
                } else {
                    match build_prepared_publish_batch(&prepared) {
                        Ok(Some(batch)) => {
                            let publish_mode = publish_mode_for_batch(&batch);
                            let message = self.push_outgoing_message(
                                chat_id,
                                text.to_string(),
                                now.get(),
                                DeliveryState::Pending,
                            );
                            self.queue_pending_outbound(
                                message.id.clone(),
                                chat_id.to_string(),
                                text.to_string(),
                                Some(batch.clone()),
                                publish_mode.clone(),
                                pending_reason_for_publish_mode(&publish_mode),
                                retry_deadline_for_publish_mode(now.get(), &publish_mode),
                            );
                            self.set_pending_outbound_in_flight(&message.id, true);
                            self.start_publish_for_pending(
                                message.id,
                                chat_id.to_string(),
                                publish_mode,
                                batch,
                            );
                        }
                        Ok(None) => {
                            let message = self.push_outgoing_message(
                                chat_id,
                                text.to_string(),
                                now.get(),
                                DeliveryState::Failed,
                            );
                            self.update_message_delivery(
                                chat_id,
                                &message.id,
                                DeliveryState::Failed,
                            );
                        }
                        Err(error) => self.state.toast = Some(error.to_string()),
                    }
                }
            }
            Err(error) => {
                self.state.toast = Some(error.to_string());
            }
        }
    }

    fn update_group_name(&mut self, group_id: &str, name: &str) {
        self.run_group_control(
            group_id,
            PendingGroupControlKind::Rename {
                name: name.trim().to_string(),
            },
        );
    }

    fn add_group_members(&mut self, group_id: &str, member_inputs: &[String]) {
        let Some(local_owner) = self
            .logged_in
            .as_ref()
            .map(|logged_in| logged_in.owner_pubkey)
        else {
            self.state.toast = Some("Create or restore an account first.".to_string());
            self.emit_state();
            return;
        };
        let member_owners = match parse_owner_inputs(member_inputs, local_owner) {
            Ok(member_owners) if !member_owners.is_empty() => member_owners,
            Ok(_) => {
                self.state.toast = Some("Pick at least one member to add.".to_string());
                self.emit_state();
                return;
            }
            Err(error) => {
                self.state.toast = Some(error.to_string());
                self.emit_state();
                return;
            }
        };
        self.run_group_control(
            group_id,
            PendingGroupControlKind::AddMembers {
                member_owner_hexes: sorted_owner_hexes(&member_owners),
            },
        );
    }

    fn remove_group_member(&mut self, group_id: &str, owner_pubkey_hex: &str) {
        let Ok((owner_pubkey_hex, _)) = parse_peer_input(owner_pubkey_hex) else {
            self.state.toast = Some("Invalid member key.".to_string());
            self.emit_state();
            return;
        };
        self.run_group_control(
            group_id,
            PendingGroupControlKind::RemoveMember { owner_pubkey_hex },
        );
    }

    fn run_group_control(&mut self, group_id: &str, kind: PendingGroupControlKind) {
        if self.logged_in.is_none() {
            self.state.toast = Some("Create or restore an account first.".to_string());
            self.emit_state();
            return;
        }
        if !self.can_use_chats() {
            self.state.toast = Some(chat_unavailable_message(self.logged_in.as_ref()).to_string());
            self.emit_state();
            return;
        }

        let Some(group_id) = normalize_group_id(group_id) else {
            self.state.toast = Some("Unknown group.".to_string());
            self.emit_state();
            return;
        };
        self.state.busy.updating_group = true;
        self.emit_state();

        let now = unix_now();
        let control_result = self.prepare_group_control(&group_id, &kind, now);
        match control_result {
            Ok((snapshot, target_owner_hexes, prepared)) => {
                self.apply_group_snapshot_to_threads(&snapshot, now.get());
                self.publish_group_local_sibling_best_effort(&prepared);
                if let Some(reason) = pending_reason_from_group_prepared(&prepared) {
                    let operation_id = self.allocate_message_id();
                    self.queue_pending_group_control(
                        operation_id,
                        group_id,
                        target_owner_hexes,
                        None,
                        reason.clone(),
                        now.get().saturating_add(PENDING_RETRY_DELAY_SECS),
                        kind,
                    );
                    self.nudge_protocol_state_for_pending_reason(&reason);
                } else {
                    match build_group_prepared_publish_batch(&prepared) {
                        Ok(Some(batch)) => {
                            let operation_id = self.allocate_message_id();
                            let publish_mode = publish_mode_for_batch(&batch);
                            self.queue_pending_group_control(
                                operation_id.clone(),
                                group_id.clone(),
                                target_owner_hexes,
                                Some(batch.clone()),
                                pending_reason_for_publish_mode(&publish_mode),
                                retry_deadline_for_publish_mode(now.get(), &publish_mode),
                                kind.clone(),
                            );
                            self.set_pending_group_control_in_flight(&operation_id, true);
                            self.start_group_control_publish(operation_id, publish_mode, batch);
                        }
                        Ok(None) => {}
                        Err(error) => self.state.toast = Some(error.to_string()),
                    }
                }

                self.request_protocol_subscription_refresh();
                self.schedule_tracked_peer_catch_up(Duration::from_secs(
                    RESUBSCRIBE_CATCH_UP_DELAY_SECS,
                ));
            }
            Err(error) => self.state.toast = Some(error.to_string()),
        }

        self.schedule_next_pending_retry(now.get());
        self.state.busy.updating_group = false;
        self.rebuild_state();
        self.persist_best_effort();
        self.emit_state();
    }

    fn push_screen(&mut self, screen: Screen) {
        if self.state.account.is_none() {
            match screen {
                Screen::Welcome => {
                    self.screen_stack.clear();
                    self.active_chat_id = None;
                }
                Screen::CreateAccount | Screen::RestoreAccount | Screen::AddDevice => {
                    self.screen_stack = vec![screen];
                    self.active_chat_id = None;
                }
                _ => return,
            }

            self.rebuild_state();
            self.persist_best_effort();
            self.emit_state();
            return;
        }

        match screen {
            Screen::CreateAccount | Screen::RestoreAccount | Screen::AddDevice => return,
            Screen::ChatList => {
                self.screen_stack.clear();
                self.active_chat_id = None;
            }
            Screen::NewChat => {
                if !self.can_use_chats() {
                    self.state.toast =
                        Some(chat_unavailable_message(self.logged_in.as_ref()).to_string());
                    self.emit_state();
                    return;
                }
                self.screen_stack = vec![Screen::NewChat];
                self.active_chat_id = None;
            }
            Screen::NewGroup => {
                if !self.can_use_chats() {
                    self.state.toast =
                        Some(chat_unavailable_message(self.logged_in.as_ref()).to_string());
                    self.emit_state();
                    return;
                }
                self.screen_stack = vec![Screen::NewGroup];
                self.active_chat_id = None;
            }
            Screen::Chat { chat_id } => {
                self.open_chat(&chat_id);
                return;
            }
            Screen::GroupDetails { group_id } => {
                let Some(group_id) = normalize_group_id(&group_id) else {
                    return;
                };
                let group_chat_id = group_chat_id(&group_id);
                if self.active_chat_id.as_deref() != Some(group_chat_id.as_str()) {
                    self.open_chat(&group_chat_id);
                }
                if !matches!(
                    self.screen_stack.last(),
                    Some(Screen::GroupDetails { group_id: current }) if current == &group_id
                ) {
                    self.screen_stack.push(Screen::GroupDetails { group_id });
                }
            }
            Screen::DeviceRoster => {
                self.screen_stack = vec![Screen::DeviceRoster];
                self.active_chat_id = None;
                self.fetch_pending_device_invites_for_local_owner();
            }
            Screen::AwaitingDeviceApproval | Screen::DeviceRevoked | Screen::Welcome => return,
        }

        self.rebuild_state();
        self.persist_best_effort();
        self.emit_state();
    }

    fn update_screen_stack(&mut self, stack: Vec<Screen>) {
        if self.state.account.is_none() {
            self.screen_stack = stack
                .into_iter()
                .filter(|screen| {
                    matches!(
                        screen,
                        Screen::CreateAccount | Screen::RestoreAccount | Screen::AddDevice
                    )
                })
                .collect();
            self.active_chat_id = None;
            self.rebuild_state();
            self.persist_best_effort();
            self.emit_state();
            return;
        }

        let mut normalized_stack = Vec::new();
        for screen in stack {
            match screen {
                Screen::Welcome
                | Screen::CreateAccount
                | Screen::RestoreAccount
                | Screen::AddDevice
                | Screen::ChatList
                | Screen::AwaitingDeviceApproval
                | Screen::DeviceRevoked => {}
                Screen::NewChat => {
                    if self.can_use_chats() {
                        normalized_stack.push(Screen::NewChat);
                    }
                }
                Screen::NewGroup => {
                    if self.can_use_chats() {
                        normalized_stack.push(Screen::NewGroup);
                    }
                }
                Screen::DeviceRoster => normalized_stack.push(Screen::DeviceRoster),
                Screen::Chat { chat_id } => {
                    if self.can_use_chats() {
                        if let Some(chat_id) = self.normalize_chat_id(&chat_id) {
                            normalized_stack.push(Screen::Chat { chat_id });
                        }
                    }
                }
                Screen::GroupDetails { group_id } => {
                    if self.can_use_chats() {
                        if let Some(group_id) = normalize_group_id(&group_id) {
                            normalized_stack.push(Screen::GroupDetails { group_id });
                        }
                    }
                }
            }
        }

        self.screen_stack = normalized_stack;
        self.sync_active_chat_from_router();
        self.rebuild_state();
        self.persist_best_effort();
        self.emit_state();
    }

    fn is_device_roster_open(&self) -> bool {
        matches!(self.screen_stack.last(), Some(Screen::DeviceRoster))
    }

    fn add_authorized_device(&mut self, device_input: &str) {
        let Some(logged_in) = self.logged_in.as_ref() else {
            self.state.toast = Some("Create or restore an account first.".to_string());
            self.emit_state();
            return;
        };
        if logged_in.owner_keys.is_none() {
            self.state.toast = Some("Only the primary device can manage devices.".to_string());
            self.emit_state();
            return;
        }

        let Ok(device_pubkey) = parse_device_input(device_input) else {
            self.state.toast = Some("Invalid device key.".to_string());
            self.emit_state();
            return;
        };
        if device_pubkey == local_device_from_keys(&logged_in.device_keys) {
            self.state.toast = Some("The current device is already authorized.".to_string());
            self.emit_state();
            return;
        }

        self.state.busy.updating_roster = true;
        self.emit_state();

        let now = unix_now();
        let updated_roster = {
            let logged_in = self.logged_in.as_mut().expect("checked above");
            let current_roster = local_roster_from_session_manager(&logged_in.session_manager);
            let mut editor = RosterEditor::from_roster(current_roster.as_ref());
            editor.authorize_device(device_pubkey, now);
            let roster = editor.build(now);
            logged_in.session_manager.apply_local_roster(roster.clone());
            logged_in.authorization_state = derive_local_authorization_state(
                logged_in.owner_keys.is_some(),
                logged_in.owner_pubkey,
                local_device_from_keys(&logged_in.device_keys),
                &logged_in.session_manager,
                Some(logged_in.authorization_state),
            );
            roster
        };

        self.publish_roster_update(updated_roster);
        self.request_protocol_subscription_refresh();
        self.persist_best_effort();
        self.state.busy.updating_roster = false;
        self.rebuild_state();
        self.emit_state();
    }

    fn remove_authorized_device(&mut self, device_pubkey_hex: &str) {
        let Some(logged_in) = self.logged_in.as_ref() else {
            self.state.toast = Some("Create or restore an account first.".to_string());
            self.emit_state();
            return;
        };
        if logged_in.owner_keys.is_none() {
            self.state.toast = Some("Only the primary device can manage devices.".to_string());
            self.emit_state();
            return;
        }

        let Ok(device_pubkey) = parse_device_input(device_pubkey_hex) else {
            self.state.toast = Some("Invalid device key.".to_string());
            self.emit_state();
            return;
        };
        if device_pubkey == local_device_from_keys(&logged_in.device_keys) {
            self.state.toast = Some("The current device cannot remove itself.".to_string());
            self.emit_state();
            return;
        }

        self.state.busy.updating_roster = true;
        self.emit_state();

        let now = unix_now();
        let updated_roster = {
            let logged_in = self.logged_in.as_mut().expect("checked above");
            let current_roster = local_roster_from_session_manager(&logged_in.session_manager);
            let mut editor = RosterEditor::from_roster(current_roster.as_ref());
            editor.revoke_device(device_pubkey);
            let roster = editor.build(now);
            logged_in.session_manager.apply_local_roster(roster.clone());
            logged_in.authorization_state = derive_local_authorization_state(
                logged_in.owner_keys.is_some(),
                logged_in.owner_pubkey,
                local_device_from_keys(&logged_in.device_keys),
                &logged_in.session_manager,
                Some(logged_in.authorization_state),
            );
            roster
        };

        self.publish_roster_update(updated_roster);
        self.request_protocol_subscription_refresh();
        self.persist_best_effort();
        self.state.busy.updating_roster = false;
        self.rebuild_state();
        self.emit_state();
    }

    fn acknowledge_revoked_device(&mut self) {
        if matches!(
            self.logged_in
                .as_ref()
                .map(|logged_in| logged_in.authorization_state),
            Some(LocalAuthorizationState::Revoked)
        ) {
            self.screen_stack.clear();
            self.rebuild_state();
            self.emit_state();
        }
    }

    fn handle_relay_event(&mut self, event: Event) {
        let event_id = event.id.to_string();
        if self.has_seen_event(&event_id) {
            return;
        }

        if self.logged_in.is_none() {
            return;
        }

        let kind = event.kind.as_u16() as u32;
        self.push_debug_log("relay.event", format!("kind_raw={} id={event_id}", kind));
        let now = unix_now();
        self.prune_recent_handshake_peers(now.get());
        match kind {
            0 => {
                if self.apply_profile_metadata_event(&event) {
                    self.remember_event(event_id);
                    self.persist_best_effort();
                    self.rebuild_state();
                    self.emit_state();
                    return;
                }
                self.remember_event(event_id);
            }
            codec::ROSTER_EVENT_KIND => {
                if let Ok(decoded) = codec::parse_roster_event(&event) {
                    self.debug_event_counters.roster_events += 1;
                    let is_local_owner = self
                        .logged_in
                        .as_ref()
                        .map(|logged_in| decoded.owner_pubkey == logged_in.owner_pubkey)
                        .unwrap_or(false);

                    let mut roster_log: Option<(&'static str, String)> = None;
                    {
                        let logged_in = self.logged_in.as_mut().expect("checked above");
                        if is_local_owner {
                            logged_in.session_manager.apply_local_roster(decoded.roster);
                            let previous = logged_in.authorization_state;
                            logged_in.authorization_state = derive_local_authorization_state(
                                logged_in.owner_keys.is_some(),
                                logged_in.owner_pubkey,
                                local_device_from_keys(&logged_in.device_keys),
                                &logged_in.session_manager,
                                Some(previous),
                            );
                            match (previous, logged_in.authorization_state) {
                                (
                                    LocalAuthorizationState::AwaitingApproval,
                                    LocalAuthorizationState::Authorized,
                                ) => {
                                    roster_log = Some((
                                        "relay.roster.local",
                                        "local device transitioned to Authorized".to_string(),
                                    ));
                                    self.state.toast =
                                        Some("This device has been approved.".to_string());
                                }
                                (_, LocalAuthorizationState::Revoked) => {
                                    roster_log = Some((
                                        "relay.roster.local",
                                        "local device transitioned to Revoked".to_string(),
                                    ));
                                    self.state.toast = Some(
                                        "This device was removed from the roster.".to_string(),
                                    );
                                    self.active_chat_id = None;
                                    self.screen_stack.clear();
                                    self.pending_inbound.clear();
                                    self.pending_outbound.clear();
                                    self.pending_group_controls.clear();
                                }
                                _ => {}
                            }
                        } else {
                            roster_log = Some((
                                "relay.roster.peer",
                                format!("observed roster for {}", decoded.owner_pubkey),
                            ));
                            logged_in
                                .session_manager
                                .observe_peer_roster(decoded.owner_pubkey, decoded.roster);
                        }
                    }
                    if let Some((category, detail)) = roster_log {
                        self.push_debug_log(category, detail);
                    }

                    let migrated_owner_hexes = self.reconcile_recent_handshake_peers();
                    self.apply_owner_migrations(&migrated_owner_hexes);
                    self.remember_event(event_id);
                    self.retry_pending_inbound(now);
                    self.retry_pending_outbound(now);
                    self.request_protocol_subscription_refresh();
                    if is_local_owner
                        && matches!(
                            self.logged_in
                                .as_ref()
                                .map(|logged_in| logged_in.authorization_state),
                            Some(LocalAuthorizationState::Authorized)
                        )
                    {
                        self.schedule_tracked_peer_catch_up(Duration::from_secs(
                            RESUBSCRIBE_CATCH_UP_DELAY_SECS,
                        ));
                    }
                    for (_, owner_hex) in migrated_owner_hexes {
                        if let Ok(pubkey) = PublicKey::parse(&owner_hex) {
                            self.fetch_recent_messages_for_owner(
                                OwnerPubkey::from_bytes(pubkey.to_bytes()),
                                now,
                            );
                        }
                    }
                    self.persist_best_effort();
                    self.rebuild_state();
                    self.emit_state();
                    return;
                }

                if let Ok(invite) = codec::parse_invite_event(&event) {
                    self.debug_event_counters.invite_events += 1;
                    let invite_owner = invite.inviter_owner_pubkey.unwrap_or_else(|| {
                        OwnerPubkey::from_bytes(invite.inviter_device_pubkey.to_bytes())
                    });
                    let local_device = {
                        let logged_in = self.logged_in.as_ref().expect("checked above");
                        local_device_from_keys(&logged_in.device_keys)
                    };
                    // Pending linked-device invites for the local owner arrive before the
                    // device has been added to the owner-signed roster. Observe every
                    // non-self invite so the primary can render it as "Pending".
                    let should_observe = invite.inviter_device_pubkey != local_device;
                    if should_observe {
                        if let Err(error) = self
                            .logged_in
                            .as_mut()
                            .expect("checked above")
                            .session_manager
                            .observe_device_invite(invite_owner, invite)
                        {
                            self.state.toast = Some(error.to_string());
                        } else {
                            self.push_debug_log(
                                "relay.invite",
                                format!("observed invite for owner {}", invite_owner),
                            );
                            self.remember_event(event_id.clone());
                            self.retry_pending_inbound(now);
                            self.retry_pending_outbound(now);
                            self.request_protocol_subscription_refresh();
                            self.persist_best_effort();
                        }
                        self.rebuild_state();
                        self.emit_state();
                        return;
                    }
                }
                self.remember_event(event_id);
            }
            codec::INVITE_RESPONSE_KIND => {
                let Some(local_invite_recipient) = self
                    .logged_in
                    .as_ref()
                    .expect("checked above")
                    .session_manager
                    .snapshot()
                    .local_invite
                    .as_ref()
                    .map(|invite| invite.inviter_ephemeral_public_key)
                else {
                    self.remember_event(event_id);
                    return;
                };

                let Ok(envelope) = codec::parse_invite_response_event(&event) else {
                    self.remember_event(event_id);
                    return;
                };
                self.debug_event_counters.invite_response_events += 1;
                if envelope.recipient != local_invite_recipient {
                    self.remember_event(event_id);
                    return;
                }

                let mut rng = OsRng;
                let mut ctx = ProtocolContext::new(now, &mut rng);
                let invite_response = self
                    .logged_in
                    .as_mut()
                    .expect("checked above")
                    .session_manager
                    .observe_invite_response(&mut ctx, &envelope);
                match invite_response {
                    Ok(Some(processed)) => {
                        self.push_debug_log(
                            "relay.invite_response",
                            format!(
                                "processed owner={} device={}",
                                processed.owner_pubkey, processed.device_pubkey
                            ),
                        );
                        let owner_hex = processed.owner_pubkey.to_string();
                        self.remember_recent_handshake_peer(
                            owner_hex.clone(),
                            processed.device_pubkey.to_string(),
                            now.get(),
                        );
                        let migrated_owner_hexes = self.reconcile_recent_handshake_peers();
                        self.apply_owner_migrations(&migrated_owner_hexes);
                        self.retry_pending_inbound(now);
                        self.retry_pending_outbound(now);
                        self.request_protocol_subscription_refresh();
                        self.fetch_recent_messages_for_owner(processed.owner_pubkey, now);
                        for (_, migrated_owner_hex) in migrated_owner_hexes {
                            if migrated_owner_hex != owner_hex {
                                if let Ok(pubkey) = PublicKey::parse(&migrated_owner_hex) {
                                    self.fetch_recent_messages_for_owner(
                                        OwnerPubkey::from_bytes(pubkey.to_bytes()),
                                        now,
                                    );
                                }
                            }
                        }
                        self.persist_best_effort();
                    }
                    Ok(None) => {}
                    Err(error) => {
                        let should_ignore = matches!(
                            error,
                            Error::Domain(DomainError::InviteAlreadyUsed)
                                | Error::Domain(DomainError::InviteExhausted)
                        );
                        if !should_ignore {
                            self.state.toast = Some(error.to_string());
                        }
                    }
                }
                self.remember_event(event_id);
                self.rebuild_state();
                self.emit_state();
            }
            codec::MESSAGE_EVENT_KIND => {
                let Ok(envelope) = codec::parse_message_event(&event) else {
                    self.remember_event(event_id);
                    return;
                };
                self.debug_event_counters.message_events += 1;

                let sender_owner = self.logged_in.as_ref().and_then(|logged_in| {
                    resolve_message_sender_owner(&logged_in.session_manager, &envelope, now)
                });
                let Some(sender_owner) = sender_owner else {
                    self.push_debug_log(
                        "relay.message.pending",
                        "sender owner unresolved; queued as pending inbound",
                    );
                    self.remember_event(event_id.clone());
                    self.pending_inbound
                        .push(PendingInbound::envelope(envelope));
                    self.persist_best_effort();
                    return;
                };

                let mut rng = OsRng;
                let mut ctx = ProtocolContext::new(now, &mut rng);
                match self
                    .logged_in
                    .as_mut()
                    .expect("checked above")
                    .session_manager
                    .receive(&mut ctx, sender_owner, &envelope)
                {
                    Ok(Some(message)) => {
                        self.push_debug_log(
                            "relay.message.received",
                            format!(
                                "owner={} bytes={}",
                                message.owner_pubkey,
                                message.payload.len()
                            ),
                        );
                        self.remember_event(event_id);
                        let owner_hex = message.owner_pubkey.to_string();
                        self.clear_recent_handshake_peer(&owner_hex);
                        if let Err(error) = self.apply_decrypted_payload(
                            message.owner_pubkey,
                            &message.payload,
                            now.get(),
                        ) {
                            if is_retryable_group_payload_error(&error) {
                                if is_unknown_group_payload_error(&error) {
                                    self.fetch_recent_messages_for_owner_with_lookback(
                                        message.owner_pubkey,
                                        now,
                                        UNKNOWN_GROUP_RECOVERY_LOOKBACK_SECS,
                                    );
                                }
                                self.push_debug_log(
                                    "relay.message.pending",
                                    format!("payload apply deferred: {error}"),
                                );
                                self.pending_inbound.push(PendingInbound::decrypted(
                                    message.owner_pubkey,
                                    message.payload,
                                    now.get(),
                                ));
                            } else {
                                self.state.toast = Some(error.to_string());
                            }
                        } else {
                            self.retry_pending_inbound(now);
                        }
                        self.request_protocol_subscription_refresh();
                        self.persist_best_effort();
                        self.rebuild_state();
                        self.emit_state();
                    }
                    Ok(None) => {
                        self.push_debug_log(
                            "relay.message.pending",
                            "session_manager returned None; queued as pending inbound",
                        );
                        self.remember_event(event_id.clone());
                        self.pending_inbound
                            .push(PendingInbound::envelope(envelope));
                        self.persist_best_effort();
                    }
                    Err(error) => {
                        self.remember_event(event_id);
                        self.state.toast = Some(error.to_string());
                        self.emit_state();
                    }
                }
            }
            _ => {
                self.debug_event_counters.other_events += 1;
            }
        }
    }

    fn start_primary_session(
        &mut self,
        owner_keys: Keys,
        device_keys: Keys,
        allow_restore: bool,
        allow_protocol_restore: bool,
    ) -> anyhow::Result<()> {
        self.push_debug_log(
            "session.start_primary",
            format!(
                "owner_pubkey={} allow_restore={} allow_protocol_restore={}",
                owner_keys.public_key().to_hex(),
                allow_restore,
                allow_protocol_restore,
            ),
        );
        let owner_pubkey = OwnerPubkey::from_bytes(owner_keys.public_key().to_bytes());
        self.start_session(
            owner_pubkey,
            Some(owner_keys),
            device_keys,
            allow_restore,
            allow_protocol_restore,
        )
    }

    fn start_session(
        &mut self,
        owner_pubkey: OwnerPubkey,
        owner_keys: Option<Keys>,
        device_keys: Keys,
        allow_restore: bool,
        allow_protocol_restore: bool,
    ) -> anyhow::Result<()> {
        self.push_debug_log(
            "session.start",
            format!(
                "owner={} has_owner_keys={} allow_restore={} allow_protocol_restore={}",
                owner_pubkey,
                owner_keys.is_some(),
                allow_restore,
                allow_protocol_restore,
            ),
        );
        if let Some(existing) = self.logged_in.take() {
            let client = existing.client;
            self.runtime.spawn(async move {
                client.unsubscribe_all().await;
                let _ = client.shutdown().await;
            });
        }

        self.threads.clear();
        self.pending_inbound.clear();
        self.active_chat_id = None;
        self.screen_stack.clear();
        self.pending_outbound.clear();
        self.pending_group_controls.clear();
        self.owner_profiles.clear();
        self.recent_handshake_peers.clear();
        self.seen_event_ids.clear();
        self.seen_event_order.clear();
        self.protocol_subscription_runtime = ProtocolSubscriptionRuntime::default();
        self.debug_log.clear();
        self.debug_event_counters = DebugEventCounters::default();
        self.next_message_id = 1;

        let device_secret_bytes = device_keys.secret_key().to_secret_bytes();
        let local_device = DevicePubkey::from_bytes(device_keys.public_key().to_bytes());
        let now = unix_now();

        let persisted = if allow_restore {
            match self.load_persisted() {
                Ok(persisted) => persisted,
                Err(error) => {
                    self.push_debug_log(
                        "session.restore_state",
                        format!("ignored_invalid_persistence={error}"),
                    );
                    None
                }
            }
        } else {
            None
        };
        self.push_debug_log(
            "session.restore_state",
            format!("persisted_present={}", persisted.is_some()),
        );
        let persisted_authorization_state = persisted
            .as_ref()
            .and_then(|persisted| persisted.authorization_state.clone())
            .map(Into::into);

        if let Some(persisted) = &persisted {
            self.active_chat_id = persisted.active_chat_id.clone();
            self.next_message_id = persisted.next_message_id.max(1);
            self.owner_profiles = persisted.owner_profiles.clone();
            if allow_protocol_restore {
                self.pending_outbound = persisted.pending_outbound.clone();
                for pending in &mut self.pending_outbound {
                    pending.publish_mode = migrate_publish_mode(
                        pending.publish_mode.clone(),
                        pending.prepared_publish.as_ref(),
                    );
                    if pending.in_flight {
                        pending.in_flight = false;
                        pending.next_retry_at_secs = now.get();
                    }
                }
                self.pending_group_controls = persisted.pending_group_controls.clone();
                for pending in &mut self.pending_group_controls {
                    if pending.in_flight {
                        pending.in_flight = false;
                        pending.next_retry_at_secs = now.get();
                    }
                }
                self.pending_inbound = persisted.pending_inbound.clone();
                self.seen_event_order = persisted
                    .seen_event_ids
                    .iter()
                    .rev()
                    .take(MAX_SEEN_EVENT_IDS)
                    .cloned()
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect();
                self.seen_event_ids = self.seen_event_order.iter().cloned().collect();
            }
            self.threads = persisted
                .threads
                .iter()
                .map(|thread| {
                    let updated_at_secs = thread.updated_at_secs.max(
                        thread
                            .messages
                            .iter()
                            .map(|message| message.created_at_secs)
                            .max()
                            .unwrap_or(0),
                    );
                    (
                        thread.chat_id.clone(),
                        ThreadRecord {
                            chat_id: thread.chat_id.clone(),
                            unread_count: thread.unread_count,
                            updated_at_secs,
                            messages: thread
                                .messages
                                .iter()
                                .map(|message| ChatMessageSnapshot {
                                    id: message.id.clone(),
                                    chat_id: message.chat_id.clone(),
                                    author: message.author.clone(),
                                    body: message.body.clone(),
                                    is_outgoing: message.is_outgoing,
                                    created_at_secs: message.created_at_secs,
                                    delivery: message.delivery.clone().into(),
                                })
                                .collect(),
                        },
                    )
                })
                .collect();
        }

        let persisted_session_manager = persisted.as_ref().and_then(|persisted| {
            if allow_protocol_restore {
                persisted.session_manager.clone()
            } else {
                None
            }
        });

        let mut session_manager = persisted_session_manager
            .filter(|snapshot| {
                snapshot.local_owner_pubkey == owner_pubkey
                    && snapshot.local_device_pubkey == local_device
            })
            .map(|snapshot| SessionManager::from_snapshot(snapshot, device_secret_bytes))
            .transpose()?
            .unwrap_or_else(|| SessionManager::new(owner_pubkey, device_secret_bytes));

        let group_manager = persisted
            .as_ref()
            .and_then(|persisted| persisted.group_manager.clone())
            .filter(|snapshot| snapshot.local_owner_pubkey == owner_pubkey)
            .map(GroupManager::from_snapshot)
            .transpose()?
            .unwrap_or_else(|| GroupManager::new(owner_pubkey));

        let existing_local_roster = session_manager
            .snapshot()
            .users
            .into_iter()
            .find(|user| user.owner_pubkey == owner_pubkey)
            .and_then(|user| user.roster);
        if owner_keys.is_some() && existing_local_roster.is_none() {
            let mut roster_editor = RosterEditor::new();
            roster_editor.authorize_device(local_device, now);
            session_manager.apply_local_roster(roster_editor.build(now));
        }

        let authorization_state = derive_local_authorization_state(
            owner_keys.is_some(),
            owner_pubkey,
            local_device,
            &session_manager,
            persisted_authorization_state,
        );
        self.push_debug_log(
            "session.authorization",
            format!("state={authorization_state:?} owner={owner_pubkey} device={local_device}"),
        );

        if authorization_state != LocalAuthorizationState::Revoked {
            let mut rng = OsRng;
            let mut ctx = ProtocolContext::new(now, &mut rng);
            session_manager.ensure_local_invite(&mut ctx)?;
        }

        if authorization_state != LocalAuthorizationState::Authorized {
            self.active_chat_id = None;
            self.screen_stack.clear();
            self.pending_inbound.clear();
            self.pending_outbound.clear();
            self.pending_group_controls.clear();
        } else if let Some(chat_id) = self.active_chat_id.clone() {
            self.screen_stack = vec![Screen::Chat { chat_id }];
        }

        let client = Client::new(device_keys.clone());
        let relay_urls = configured_relay_urls();
        self.runtime
            .block_on(ensure_session_relays_configured(&client, &relay_urls));
        self.start_notifications_loop(client.clone());

        self.logged_in = Some(LoggedInState {
            owner_pubkey,
            owner_keys: owner_keys.clone(),
            device_keys: device_keys.clone(),
            client,
            relay_urls,
            session_manager,
            group_manager,
            authorization_state,
        });
        self.schedule_pending_device_invite_poll(Duration::from_secs(
            DEVICE_INVITE_DISCOVERY_POLL_SECS,
        ));
        self.schedule_session_connect();

        self.emit_account_bundle_update(owner_keys.as_ref(), &device_keys);
        self.republish_local_identity_artifacts();
        self.reconcile_recent_handshake_peers();
        self.retry_pending_inbound(now);
        self.retry_pending_outbound(now);
        self.retry_pending_group_controls(now);
        self.schedule_next_pending_retry(now.get());
        self.state.busy.syncing_network = true;
        self.rebuild_state();
        self.persist_best_effort();
        self.request_protocol_subscription_refresh();
        if authorization_state != LocalAuthorizationState::Revoked {
            self.schedule_tracked_peer_catch_up(Duration::from_secs(
                RESUBSCRIBE_CATCH_UP_DELAY_SECS,
            ));
        }
        self.emit_state();
        Ok(())
    }

    fn retry_pending_inbound(&mut self, now: UnixSeconds) {
        if self.logged_in.is_none() {
            return;
        }

        let mut pending = std::mem::take(&mut self.pending_inbound);
        loop {
            let mut still_pending = Vec::new();
            let mut made_progress = false;

            for item in pending {
                if let PendingInbound::Decrypted {
                    sender_owner_hex,
                    payload,
                    created_at_secs,
                } = item.clone()
                {
                    let Ok(sender_pubkey) = PublicKey::parse(&sender_owner_hex) else {
                        still_pending.push(item);
                        continue;
                    };
                    match self.apply_decrypted_payload(
                        OwnerPubkey::from_bytes(sender_pubkey.to_bytes()),
                        &payload,
                        created_at_secs,
                    ) {
                        Ok(()) => {
                            made_progress = true;
                        }
                        Err(error) if is_retryable_group_payload_error(&error) => {
                            if is_unknown_group_payload_error(&error) {
                                self.fetch_recent_messages_for_owner_with_lookback(
                                    OwnerPubkey::from_bytes(sender_pubkey.to_bytes()),
                                    now,
                                    UNKNOWN_GROUP_RECOVERY_LOOKBACK_SECS,
                                );
                            }
                            still_pending.push(item);
                        }
                        Err(error) => {
                            self.state.toast = Some(error.to_string());
                            made_progress = true;
                        }
                    }
                    continue;
                }

                let PendingInbound::Envelope { envelope } = &item else {
                    continue;
                };

                let sender_owner = self.logged_in.as_ref().and_then(|logged_in| {
                    resolve_message_sender_owner(&logged_in.session_manager, envelope, now)
                });
                let Some(sender_owner) = sender_owner else {
                    still_pending.push(item);
                    continue;
                };
                let receive_result = {
                    let logged_in = self.logged_in.as_mut().expect("checked above");
                    let mut rng = OsRng;
                    let mut ctx = ProtocolContext::new(now, &mut rng);
                    logged_in
                        .session_manager
                        .receive(&mut ctx, sender_owner, envelope)
                };
                match receive_result {
                    Ok(Some(message)) => match self.apply_decrypted_payload(
                        message.owner_pubkey,
                        &message.payload,
                        envelope.created_at.get(),
                    ) {
                        Ok(()) => {
                            made_progress = true;
                        }
                        Err(error) if is_retryable_group_payload_error(&error) => {
                            if is_unknown_group_payload_error(&error) {
                                self.fetch_recent_messages_for_owner_with_lookback(
                                    message.owner_pubkey,
                                    now,
                                    UNKNOWN_GROUP_RECOVERY_LOOKBACK_SECS,
                                );
                            }
                            still_pending.push(PendingInbound::decrypted(
                                message.owner_pubkey,
                                message.payload,
                                envelope.created_at.get(),
                            ));
                        }
                        Err(error) => {
                            self.state.toast = Some(error.to_string());
                            made_progress = true;
                        }
                    },
                    Ok(None) | Err(_) => {
                        // If the owner is now resolvable but the real session manager can no
                        // longer receive this envelope, the payload was already consumed earlier.
                        // Keeping the raw envelope would wedge the queue forever.
                        made_progress = true;
                    }
                }
            }

            if still_pending.is_empty() || !made_progress {
                self.pending_inbound = still_pending;
                break;
            }
            pending = still_pending;
        }
    }

    fn retry_pending_outbound(&mut self, now: UnixSeconds) {
        if self.logged_in.is_none() || self.pending_outbound.is_empty() {
            return;
        }

        self.prune_recent_handshake_peers(now.get());
        let pending = std::mem::take(&mut self.pending_outbound);
        let mut still_pending = Vec::new();

        for mut pending_message in pending {
            if pending_message.next_retry_at_secs > now.get() {
                still_pending.push(pending_message);
                continue;
            }

            if pending_message.in_flight {
                still_pending.push(pending_message);
                continue;
            }

            if let Some(batch) = pending_message.prepared_publish.clone() {
                pending_message.publish_mode =
                    migrate_publish_mode(pending_message.publish_mode.clone(), Some(&batch));
                pending_message.reason =
                    pending_reason_for_publish_mode(&pending_message.publish_mode);
                pending_message.next_retry_at_secs =
                    retry_deadline_for_publish_mode(now.get(), &pending_message.publish_mode);
                pending_message.in_flight = true;
                self.start_publish_for_pending(
                    pending_message.message_id.clone(),
                    pending_message.chat_id.clone(),
                    pending_message.publish_mode.clone(),
                    batch,
                );
                still_pending.push(pending_message);
                continue;
            }

            if is_group_chat_id(&pending_message.chat_id) {
                let Some(group_id) = parse_group_id_from_chat_id(&pending_message.chat_id) else {
                    self.update_message_delivery(
                        &pending_message.chat_id,
                        &pending_message.message_id,
                        DeliveryState::Failed,
                    );
                    continue;
                };
                let payload = match encode_app_group_message_payload(&pending_message.body) {
                    Ok(payload) => payload,
                    Err(error) => {
                        self.state.toast = Some(error.to_string());
                        self.update_message_delivery(
                            &pending_message.chat_id,
                            &pending_message.message_id,
                            DeliveryState::Failed,
                        );
                        continue;
                    }
                };
                let prepared = {
                    let logged_in = self.logged_in.as_mut().expect("checked above");
                    let mut rng = OsRng;
                    let mut ctx = ProtocolContext::new(now, &mut rng);
                    let (session_manager, group_manager) =
                        (&mut logged_in.session_manager, &mut logged_in.group_manager);
                    group_manager.send_message(session_manager, &mut ctx, &group_id, payload)
                };

                match prepared {
                    Ok(prepared) => {
                        self.publish_group_local_sibling_best_effort(&prepared);
                        if let Some(reason) = pending_reason_from_group_prepared(&prepared) {
                            self.push_debug_log(
                                "retry.group.pending",
                                format!(
                                    "chat_id={} reason={reason:?} gaps={}",
                                    pending_message.chat_id,
                                    summarize_relay_gaps(&prepared.remote.relay_gaps)
                                ),
                            );
                            pending_message.reason = reason.clone();
                            pending_message.next_retry_at_secs =
                                now.get().saturating_add(PENDING_RETRY_DELAY_SECS);
                            self.nudge_protocol_state_for_pending_reason(&reason);
                            pending_message.publish_mode = OutboundPublishMode::WaitForPeer;
                            still_pending.push(pending_message);
                        } else {
                            match build_group_prepared_publish_batch(&prepared) {
                                Ok(Some(batch)) => {
                                    pending_message.publish_mode = publish_mode_for_batch(&batch);
                                    pending_message.prepared_publish = Some(batch.clone());
                                    pending_message.reason = pending_reason_for_publish_mode(
                                        &pending_message.publish_mode,
                                    );
                                    pending_message.next_retry_at_secs =
                                        retry_deadline_for_publish_mode(
                                            now.get(),
                                            &pending_message.publish_mode,
                                        );
                                    pending_message.in_flight = true;
                                    self.start_publish_for_pending(
                                        pending_message.message_id.clone(),
                                        pending_message.chat_id.clone(),
                                        pending_message.publish_mode.clone(),
                                        batch,
                                    );
                                    still_pending.push(pending_message);
                                }
                                Ok(None) => {
                                    pending_message.publish_mode = OutboundPublishMode::WaitForPeer;
                                    pending_message.reason = PendingSendReason::MissingDeviceInvite;
                                    pending_message.next_retry_at_secs =
                                        now.get().saturating_add(PENDING_RETRY_DELAY_SECS);
                                    self.push_debug_log(
                                        "retry.group.pending",
                                        format!(
                                            "chat_id={} reason={:?}",
                                            pending_message.chat_id, pending_message.reason
                                        ),
                                    );
                                    self.nudge_protocol_state_for_pending_reason(
                                        &pending_message.reason,
                                    );
                                    still_pending.push(pending_message);
                                }
                                Err(error) => {
                                    self.state.toast = Some(error.to_string());
                                    self.update_message_delivery(
                                        &pending_message.chat_id,
                                        &pending_message.message_id,
                                        DeliveryState::Failed,
                                    );
                                }
                            }
                        }
                    }
                    Err(error) => {
                        self.state.toast = Some(error.to_string());
                        self.update_message_delivery(
                            &pending_message.chat_id,
                            &pending_message.message_id,
                            DeliveryState::Failed,
                        );
                    }
                }
                continue;
            }

            let prepared = {
                let owner = match parse_peer_input(&pending_message.chat_id) {
                    Ok((_, peer_pubkey)) => OwnerPubkey::from_bytes(peer_pubkey.to_bytes()),
                    Err(_) => {
                        self.update_message_delivery(
                            &pending_message.chat_id,
                            &pending_message.message_id,
                            DeliveryState::Failed,
                        );
                        continue;
                    }
                };

                let logged_in = self.logged_in.as_mut().expect("checked above");
                prepare_direct_send_payloads(
                    logged_in,
                    owner,
                    &pending_message.chat_id,
                    &pending_message.body,
                    now,
                )
            };

            match prepared {
                Ok(prepared) => {
                    if let Some(reason) = pending_reason_from_prepared(&prepared) {
                        self.push_debug_log(
                            "retry.direct.pending",
                            format!(
                                "chat_id={} reason={reason:?} gaps={}",
                                pending_message.chat_id,
                                summarize_relay_gaps(&prepared.relay_gaps)
                            ),
                        );
                        pending_message.reason = reason.clone();
                        pending_message.next_retry_at_secs =
                            now.get().saturating_add(PENDING_RETRY_DELAY_SECS);
                        self.nudge_protocol_state_for_pending_reason(&reason);
                        pending_message.publish_mode = OutboundPublishMode::WaitForPeer;
                        still_pending.push(pending_message);
                    } else {
                        match build_prepared_publish_batch(&prepared) {
                            Ok(Some(batch)) => {
                                pending_message.publish_mode = publish_mode_for_batch(&batch);
                                pending_message.prepared_publish = Some(batch.clone());
                                pending_message.reason =
                                    pending_reason_for_publish_mode(&pending_message.publish_mode);
                                pending_message.next_retry_at_secs =
                                    retry_deadline_for_publish_mode(
                                        now.get(),
                                        &pending_message.publish_mode,
                                    );
                                pending_message.in_flight = true;
                                self.start_publish_for_pending(
                                    pending_message.message_id.clone(),
                                    pending_message.chat_id.clone(),
                                    pending_message.publish_mode.clone(),
                                    batch,
                                );
                                still_pending.push(pending_message);
                            }
                            Ok(None) => {
                                pending_message.publish_mode = OutboundPublishMode::WaitForPeer;
                                pending_message.reason = PendingSendReason::MissingDeviceInvite;
                                pending_message.next_retry_at_secs =
                                    now.get().saturating_add(PENDING_RETRY_DELAY_SECS);
                                self.push_debug_log(
                                    "retry.direct.pending",
                                    format!(
                                        "chat_id={} reason={:?}",
                                        pending_message.chat_id, pending_message.reason
                                    ),
                                );
                                self.nudge_protocol_state_for_pending_reason(
                                    &pending_message.reason,
                                );
                                still_pending.push(pending_message);
                            }
                            Err(error) => {
                                self.state.toast = Some(error.to_string());
                                self.update_message_delivery(
                                    &pending_message.chat_id,
                                    &pending_message.message_id,
                                    DeliveryState::Failed,
                                );
                            }
                        }
                    }
                }
                Err(error) => {
                    self.state.toast = Some(error.to_string());
                    self.update_message_delivery(
                        &pending_message.chat_id,
                        &pending_message.message_id,
                        DeliveryState::Failed,
                    );
                }
            }
        }

        self.pending_outbound = still_pending;
        self.schedule_next_pending_retry(now.get());
    }

    fn retry_pending_group_controls(&mut self, now: UnixSeconds) {
        if self.logged_in.is_none() || self.pending_group_controls.is_empty() {
            return;
        }

        let pending = std::mem::take(&mut self.pending_group_controls);
        let mut still_pending = Vec::new();

        for mut control in pending {
            if control.next_retry_at_secs > now.get() || control.in_flight {
                still_pending.push(control);
                continue;
            }

            if let Some(batch) = control.prepared_publish.clone() {
                control.in_flight = true;
                let publish_mode = publish_mode_for_batch(&batch);
                self.start_group_control_publish(control.operation_id.clone(), publish_mode, batch);
                still_pending.push(control);
                continue;
            }

            match self.rebuild_group_control(&control.group_id, &control.kind, now) {
                Ok((snapshot, target_owner_hexes, prepared)) => {
                    self.apply_group_snapshot_to_threads(&snapshot, now.get());
                    control.target_owner_hexes = target_owner_hexes;
                    self.publish_group_local_sibling_best_effort(&prepared);
                    if let Some(reason) = pending_reason_from_group_prepared(&prepared) {
                        self.push_debug_log(
                            "retry.group_control.pending",
                            format!(
                                "group_id={} reason={reason:?} gaps={}",
                                control.group_id,
                                summarize_relay_gaps(&prepared.remote.relay_gaps)
                            ),
                        );
                        control.reason = reason.clone();
                        control.next_retry_at_secs =
                            now.get().saturating_add(PENDING_RETRY_DELAY_SECS);
                        self.nudge_protocol_state_for_pending_reason(&reason);
                        still_pending.push(control);
                    } else {
                        match build_group_prepared_publish_batch(&prepared) {
                            Ok(Some(batch)) => {
                                control.prepared_publish = Some(batch.clone());
                                control.reason = pending_reason_for_publish_mode(
                                    &publish_mode_for_batch(&batch),
                                );
                                control.next_retry_at_secs = retry_deadline_for_publish_mode(
                                    now.get(),
                                    &publish_mode_for_batch(&batch),
                                );
                                control.in_flight = true;
                                self.start_group_control_publish(
                                    control.operation_id.clone(),
                                    publish_mode_for_batch(&batch),
                                    batch,
                                );
                                still_pending.push(control);
                            }
                            Ok(None) => {
                                control.next_retry_at_secs =
                                    now.get().saturating_add(PENDING_RETRY_DELAY_SECS);
                                self.nudge_protocol_state_for_pending_reason(
                                    &PendingSendReason::MissingDeviceInvite,
                                );
                                still_pending.push(control);
                            }
                            Err(error) => self.state.toast = Some(error.to_string()),
                        }
                    }
                }
                Err(error) => self.state.toast = Some(error.to_string()),
            }
        }

        self.pending_group_controls = still_pending;
        self.schedule_next_pending_retry(now.get());
    }

    fn queue_pending_outbound(
        &mut self,
        message_id: String,
        chat_id: String,
        body: String,
        prepared_publish: Option<PreparedPublishBatch>,
        publish_mode: OutboundPublishMode,
        reason: PendingSendReason,
        next_retry_at_secs: u64,
    ) {
        self.pending_outbound.push(PendingOutbound {
            message_id,
            chat_id,
            body,
            prepared_publish,
            publish_mode,
            reason,
            next_retry_at_secs,
            in_flight: false,
        });
    }

    fn set_pending_outbound_in_flight(&mut self, message_id: &str, in_flight: bool) {
        if let Some(pending) = self
            .pending_outbound
            .iter_mut()
            .find(|pending| pending.message_id == message_id)
        {
            pending.in_flight = in_flight;
        }
    }

    fn prune_recent_handshake_peers(&mut self, now_secs: u64) {
        self.reconcile_recent_handshake_peers();
        self.recent_handshake_peers.retain(|_, peer| {
            let within_ttl =
                now_secs.saturating_sub(peer.observed_at_secs) <= RECENT_HANDSHAKE_TTL_SECS;
            within_ttl && !self.threads.contains_key(&peer.owner_hex)
        });
    }

    fn remember_recent_handshake_peer(
        &mut self,
        owner_hex: String,
        device_hex: String,
        now_secs: u64,
    ) {
        if self.threads.contains_key(&owner_hex) {
            self.recent_handshake_peers
                .retain(|_, peer| peer.owner_hex != owner_hex);
            return;
        }
        self.recent_handshake_peers.insert(
            device_hex.clone(),
            RecentHandshakePeer {
                owner_hex,
                device_hex,
                observed_at_secs: now_secs,
            },
        );
    }

    fn clear_recent_handshake_peer(&mut self, owner_hex: &str) {
        self.recent_handshake_peers
            .retain(|_, peer| peer.owner_hex != owner_hex);
    }

    fn tracked_peer_owner_hexes(&self) -> HashSet<String> {
        let mut owners = self
            .threads
            .keys()
            .filter(|chat_id| !is_group_chat_id(chat_id))
            .cloned()
            .collect::<HashSet<_>>();
        if let Some(chat_id) = self.active_chat_id.as_ref() {
            if !is_group_chat_id(chat_id) {
                owners.insert(chat_id.clone());
            }
        }
        for pending in &self.pending_outbound {
            if !is_group_chat_id(&pending.chat_id) {
                owners.insert(pending.chat_id.clone());
            }
        }
        for pending in &self.pending_group_controls {
            owners.extend(pending.target_owner_hexes.iter().cloned());
        }
        for pending in &self.pending_inbound {
            if let PendingInbound::Decrypted {
                sender_owner_hex, ..
            } = pending
            {
                owners.insert(sender_owner_hex.clone());
            }
        }
        if let Some(logged_in) = self.logged_in.as_ref() {
            for group in logged_in.group_manager.groups() {
                for member in group.members {
                    if member != logged_in.owner_pubkey {
                        owners.insert(member.to_string());
                    }
                }
            }
        }
        owners
    }

    fn protocol_owner_hexes(&self) -> HashSet<String> {
        let mut owners = self.tracked_peer_owner_hexes();
        owners.extend(
            self.recent_handshake_peers
                .values()
                .map(|peer| peer.owner_hex.clone()),
        );
        if let Some(logged_in) = self.logged_in.as_ref() {
            for user in logged_in.session_manager.snapshot().users {
                for device in user.devices {
                    if let Some(claimed_owner_pubkey) = device.claimed_owner_pubkey {
                        owners.insert(claimed_owner_pubkey.to_string());
                    }
                }
            }
        }
        owners
    }

    fn schedule_pending_outbound_retry(&self, after: Duration) {
        let tx = self.core_sender.clone();
        self.runtime.spawn(async move {
            sleep(after).await;
            let _ = tx.send(CoreMsg::Internal(Box::new(
                InternalEvent::RetryPendingOutbound,
            )));
        });
    }

    fn schedule_tracked_peer_catch_up(&self, after: Duration) {
        let tx = self.core_sender.clone();
        self.runtime.spawn(async move {
            sleep(after).await;
            let _ = tx.send(CoreMsg::Internal(Box::new(
                InternalEvent::FetchTrackedPeerCatchUp,
            )));
        });
    }

    fn schedule_pending_device_invite_poll(&mut self, after: Duration) {
        if !self.can_poll_pending_device_invites() {
            return;
        }
        self.device_invite_poll_token = self.device_invite_poll_token.saturating_add(1);
        let token = self.device_invite_poll_token;
        let tx = self.core_sender.clone();
        self.runtime.spawn(async move {
            sleep(after).await;
            let _ = tx.send(CoreMsg::Internal(Box::new(
                InternalEvent::PollPendingDeviceInvites { token },
            )));
        });
    }

    fn schedule_next_pending_retry(&self, now_secs: u64) {
        let next_retry_at_secs = self
            .pending_outbound
            .iter()
            .map(|pending| pending.next_retry_at_secs)
            .chain(
                self.pending_group_controls
                    .iter()
                    .map(|pending| pending.next_retry_at_secs),
            )
            .min();
        let Some(next_retry_at_secs) = next_retry_at_secs else {
            return;
        };
        let delay_secs = next_retry_at_secs.saturating_sub(now_secs).max(1);
        self.schedule_pending_outbound_retry(Duration::from_secs(delay_secs));
    }

    fn start_pending_message_publish(
        &mut self,
        message_id: String,
        chat_id: String,
        message_events: Vec<Event>,
    ) {
        self.start_ordinary_publish(message_id, chat_id, message_events);
    }

    fn start_publish_for_pending(
        &mut self,
        message_id: String,
        chat_id: String,
        publish_mode: OutboundPublishMode,
        batch: PreparedPublishBatch,
    ) {
        self.request_protocol_subscription_refresh();
        if batch.message_events.is_empty() {
            return;
        }

        match publish_mode {
            OutboundPublishMode::OrdinaryFirstAck => {
                self.start_pending_message_publish(message_id, chat_id, batch.message_events);
            }
            OutboundPublishMode::FirstContactStaged => {
                self.start_staged_first_contact_send(StagedOutboundSend {
                    message_id,
                    chat_id,
                    invite_events: batch.invite_events,
                    message_events: batch.message_events,
                });
            }
            OutboundPublishMode::WaitForPeer => {}
        }
    }

    fn reconcile_recent_handshake_peers(&mut self) -> Vec<(String, String)> {
        let Some(logged_in) = self.logged_in.as_ref() else {
            return Vec::new();
        };

        let mut session_owners_by_device = BTreeMap::new();
        for user in logged_in.session_manager.snapshot().users {
            let owner_hex = user.owner_pubkey.to_string();
            for device in user.devices {
                if device.active_session.is_none()
                    && device.inactive_sessions.is_empty()
                    && device.claimed_owner_pubkey.is_none()
                {
                    continue;
                }
                session_owners_by_device
                    .insert(device.device_pubkey.to_string(), owner_hex.clone());
            }
        }

        let mut migrated_owner_hexes = Vec::new();
        for peer in self.recent_handshake_peers.values_mut() {
            let Some(owner_hex) = session_owners_by_device.get(&peer.device_hex) else {
                continue;
            };
            if *owner_hex != peer.owner_hex {
                let previous_owner_hex = peer.owner_hex.clone();
                peer.owner_hex = owner_hex.clone();
                migrated_owner_hexes.push((previous_owner_hex, owner_hex.clone()));
            }
        }

        migrated_owner_hexes
    }

    fn apply_owner_migrations(&mut self, migrations: &[(String, String)]) {
        for (old_owner_hex, new_owner_hex) in migrations {
            if old_owner_hex == new_owner_hex {
                continue;
            }

            if let Some(mut old_thread) = self.threads.remove(old_owner_hex) {
                old_thread.chat_id = new_owner_hex.clone();
                for message in &mut old_thread.messages {
                    message.chat_id = new_owner_hex.clone();
                }

                match self.threads.get_mut(new_owner_hex) {
                    Some(existing) => {
                        existing.unread_count = existing
                            .unread_count
                            .saturating_add(old_thread.unread_count);
                        existing.updated_at_secs =
                            existing.updated_at_secs.max(old_thread.updated_at_secs);
                        existing.messages.extend(old_thread.messages);
                        existing.messages.sort_by(|left, right| {
                            left.created_at_secs
                                .cmp(&right.created_at_secs)
                                .then_with(|| left.id.cmp(&right.id))
                        });
                    }
                    None => {
                        self.threads.insert(new_owner_hex.clone(), old_thread);
                    }
                }
            }

            if self.active_chat_id.as_deref() == Some(old_owner_hex.as_str()) {
                self.active_chat_id = Some(new_owner_hex.clone());
            }
            for pending in &mut self.pending_outbound {
                if pending.chat_id == *old_owner_hex {
                    pending.chat_id = new_owner_hex.clone();
                }
            }
            for screen in &mut self.screen_stack {
                if let Screen::Chat { chat_id } = screen {
                    if *chat_id == *old_owner_hex {
                        *chat_id = new_owner_hex.clone();
                    }
                }
            }
        }
    }

    fn start_staged_first_contact_send(&mut self, staged: StagedOutboundSend) {
        let Some((client, relay_urls)) = self
            .logged_in
            .as_ref()
            .map(|logged_in| (logged_in.client.clone(), logged_in.relay_urls.clone()))
        else {
            return;
        };

        for event in staged
            .invite_events
            .iter()
            .chain(staged.message_events.iter())
        {
            self.remember_event(event.id.to_string());
        }

        let tx = self.core_sender.clone();
        self.runtime.spawn(async move {
            let invite_publish = publish_events_with_retry(
                &client,
                &relay_urls,
                staged.invite_events,
                "invite response",
            )
            .await;
            if invite_publish.is_err() {
                let _ = tx.send(CoreMsg::Internal(Box::new(
                    InternalEvent::PublishFinished {
                        message_id: staged.message_id,
                        chat_id: staged.chat_id,
                        success: false,
                    },
                )));
                return;
            }

            sleep(Duration::from_millis(FIRST_CONTACT_STAGE_DELAY_MS)).await;

            let success =
                publish_events_with_retry(&client, &relay_urls, staged.message_events, "message")
                    .await
                    .is_ok();
            let _ = tx.send(CoreMsg::Internal(Box::new(
                InternalEvent::PublishFinished {
                    message_id: staged.message_id,
                    chat_id: staged.chat_id,
                    success,
                },
            )));
        });
    }

    fn start_ordinary_publish(&mut self, message_id: String, chat_id: String, events: Vec<Event>) {
        let Some((client, relay_urls)) = self
            .logged_in
            .as_ref()
            .map(|logged_in| (logged_in.client.clone(), logged_in.relay_urls.clone()))
        else {
            return;
        };

        for event in &events {
            self.remember_event(event.id.to_string());
        }

        let tx = self.core_sender.clone();
        self.runtime.spawn(async move {
            let success = publish_events_first_ack(&client, &relay_urls, &events, "message")
                .await
                .is_ok();
            let _ = tx.send(CoreMsg::Internal(Box::new(
                InternalEvent::PublishFinished {
                    message_id,
                    chat_id,
                    success,
                },
            )));
        });
    }

    fn fetch_recent_messages_for_owner(&self, owner_pubkey: OwnerPubkey, now: UnixSeconds) {
        self.fetch_recent_messages_for_owner_with_lookback(
            owner_pubkey,
            now,
            CATCH_UP_LOOKBACK_SECS,
        );
    }

    fn fetch_recent_messages_for_owner_with_lookback(
        &self,
        owner_pubkey: OwnerPubkey,
        now: UnixSeconds,
        lookback_secs: u64,
    ) {
        let Some(client) = self
            .logged_in
            .as_ref()
            .map(|logged_in| logged_in.client.clone())
        else {
            return;
        };

        let filters = self.message_filters_for_owner(owner_pubkey, now, lookback_secs);
        if filters.is_empty() {
            return;
        }

        let tx = self.core_sender.clone();
        self.runtime.spawn(async move {
            client.connect_with_timeout(Duration::from_secs(5)).await;
            if let Ok(events) = client
                .fetch_events(filters, Some(Duration::from_secs(5)))
                .await
            {
                let collected = events.into_iter().collect::<Vec<_>>();
                if !collected.is_empty() {
                    let _ = tx.send(CoreMsg::Internal(Box::new(
                        InternalEvent::FetchCatchUpEvents(collected),
                    )));
                }
            }
        });
    }

    fn fetch_recent_protocol_state(&mut self) {
        let Some(client) = self
            .logged_in
            .as_ref()
            .map(|logged_in| logged_in.client.clone())
        else {
            return;
        };

        let Some(plan) = self.compute_protocol_subscription_plan() else {
            return;
        };
        self.push_debug_log(
            "protocol.catch_up.fetch",
            summarize_protocol_plan(Some(&plan)),
        );

        let filters = build_protocol_state_catch_up_filters(&plan, unix_now());
        if filters.is_empty() {
            return;
        }

        let tx = self.core_sender.clone();
        let plan_summary = summarize_protocol_plan(Some(&plan));
        self.runtime.spawn(async move {
            client.connect_with_timeout(Duration::from_secs(5)).await;
            match client
                .fetch_events(filters, Some(Duration::from_secs(5)))
                .await
            {
                Ok(events) => {
                    let collected = events.into_iter().collect::<Vec<_>>();
                    let _ = tx.send(CoreMsg::Internal(Box::new(InternalEvent::DebugLog {
                        category: "protocol.catch_up.result".to_string(),
                        detail: format!("{} events={}", plan_summary, collected.len(),),
                    })));
                    if !collected.is_empty() {
                        let _ = tx.send(CoreMsg::Internal(Box::new(
                            InternalEvent::FetchCatchUpEvents(collected),
                        )));
                    }
                }
                Err(error) => {
                    let _ = tx.send(CoreMsg::Internal(Box::new(InternalEvent::DebugLog {
                        category: "protocol.catch_up.error".to_string(),
                        detail: format!("{plan_summary} error={error}"),
                    })));
                }
            }
        });
    }

    fn fetch_pending_device_invites_for_local_owner(&mut self) {
        let Some(logged_in) = self.logged_in.as_ref() else {
            return;
        };
        if logged_in.owner_keys.is_none() {
            return;
        }

        let owner_pubkey = logged_in.owner_pubkey;
        let device_keys = logged_in.device_keys.clone();
        let relay_urls = logged_in.relay_urls.clone();
        let since = unix_now()
            .get()
            .saturating_sub(DEVICE_INVITE_DISCOVERY_LOOKBACK_SECS);
        self.push_debug_log(
            "device.invite.fetch",
            format!(
                "owner={} since={} limit={}",
                owner_pubkey, since, DEVICE_INVITE_DISCOVERY_LIMIT
            ),
        );
        let tx = self.core_sender.clone();
        let filters = vec![Filter::new()
            .kind(Kind::from(codec::INVITE_EVENT_KIND as u16))
            .since(Timestamp::from(since))
            .limit(DEVICE_INVITE_DISCOVERY_LIMIT)];

        self.runtime.spawn(async move {
            let client = Client::new(device_keys);
            ensure_session_relays_configured(&client, &relay_urls).await;
            client.connect_with_timeout(Duration::from_secs(5)).await;
            match client
                .fetch_events(filters, Some(Duration::from_secs(5)))
                .await
            {
                Ok(events) => {
                    let collected = events.into_iter().collect::<Vec<_>>();
                    let _ = tx.send(CoreMsg::Internal(Box::new(
                        InternalEvent::FetchPendingDeviceInvites(collected),
                    )));
                }
                Err(error) => {
                    let _ = tx.send(CoreMsg::Internal(Box::new(InternalEvent::DebugLog {
                        category: "device.invite.fetch.error".to_string(),
                        detail: error.to_string(),
                    })));
                }
            }
            let _ = client.shutdown().await;
        });
    }

    fn handle_pending_device_invite_events(&mut self, events: Vec<Event>) {
        let Some((local_owner, local_device)) = self.logged_in.as_ref().and_then(|logged_in| {
            logged_in.owner_keys.as_ref().map(|_| {
                (
                    logged_in.owner_pubkey,
                    local_device_from_keys(&logged_in.device_keys),
                )
            })
        }) else {
            return;
        };

        let mut observed = 0usize;
        let mut last_error = None;

        for event in events {
            let event_id = event.id.to_string();
            if self.has_seen_event(&event_id) {
                continue;
            }

            let Ok(invite) = codec::parse_invite_event(&event) else {
                continue;
            };
            if invite.inviter_owner_pubkey != Some(local_owner)
                || invite.inviter_device_pubkey == local_device
            {
                continue;
            }

            match self
                .logged_in
                .as_mut()
                .expect("logged-in state checked above")
                .session_manager
                .observe_device_invite(local_owner, invite)
            {
                Ok(()) => {
                    observed += 1;
                    self.remember_event(event_id);
                }
                Err(error) => {
                    last_error = Some(error.to_string());
                }
            }
        }

        if let Some(error) = last_error {
            self.push_debug_log("device.invite.observe.error", error);
        }

        self.push_debug_log(
            "device.invite.observe",
            format!("owner={} observed={}", local_owner, observed),
        );

        if observed > 0 {
            self.persist_best_effort();
            self.rebuild_state();
            self.emit_state();
        }
    }

    fn nudge_protocol_state_for_pending_reason(&mut self, reason: &PendingSendReason) {
        self.push_debug_log("protocol.nudge", format!("reason={reason:?}"));
        match reason {
            PendingSendReason::MissingRoster => {
                self.republish_local_identity_artifacts();
                self.request_protocol_subscription_refresh();
                self.fetch_recent_protocol_state();
            }
            PendingSendReason::MissingDeviceInvite => {
                self.request_protocol_subscription_refresh();
                self.fetch_recent_protocol_state();
            }
            PendingSendReason::PublishingFirstContact | PendingSendReason::PublishRetry => {}
        }
    }

    fn fetch_recent_messages_for_tracked_peers(&self, now: UnixSeconds) {
        for owner_hex in self.tracked_peer_owner_hexes() {
            let Ok(pubkey) = PublicKey::parse(&owner_hex) else {
                continue;
            };
            self.fetch_recent_messages_for_owner(OwnerPubkey::from_bytes(pubkey.to_bytes()), now);
        }
    }

    fn message_filters_for_owner(
        &self,
        owner_pubkey: OwnerPubkey,
        now: UnixSeconds,
        lookback_secs: u64,
    ) -> Vec<Filter> {
        let Some(logged_in) = self.logged_in.as_ref() else {
            return Vec::new();
        };

        let Some(user) = logged_in
            .session_manager
            .snapshot()
            .users
            .into_iter()
            .find(|user| user.owner_pubkey == owner_pubkey)
        else {
            return Vec::new();
        };

        let authors = user
            .devices
            .into_iter()
            .flat_map(|device| {
                let mut senders = HashSet::new();
                if let Some(session) = device.active_session.as_ref() {
                    collect_expected_senders(session, &mut senders);
                }
                for session in &device.inactive_sessions {
                    collect_expected_senders(session, &mut senders);
                }
                senders.into_iter().collect::<Vec<_>>()
            })
            .filter_map(|hex| PublicKey::parse(&hex).ok())
            .collect::<Vec<_>>();

        if authors.is_empty() {
            return Vec::new();
        }

        vec![Filter::new()
            .kind(Kind::from(codec::MESSAGE_EVENT_KIND as u16))
            .authors(authors)
            .since(Timestamp::from(now.get().saturating_sub(lookback_secs)))]
    }

    fn update_message_delivery(
        &mut self,
        chat_id: &str,
        message_id: &str,
        delivery: DeliveryState,
    ) {
        let Some(thread) = self.threads.get_mut(chat_id) else {
            return;
        };
        if let Some(message) = thread
            .messages
            .iter_mut()
            .find(|message| message.id == message_id)
        {
            message.delivery = delivery;
        }
    }

    fn push_outgoing_message(
        &mut self,
        chat_id: &str,
        body: String,
        created_at_secs: u64,
        delivery: DeliveryState,
    ) -> ChatMessageSnapshot {
        let message = ChatMessageSnapshot {
            id: self.allocate_message_id(),
            chat_id: chat_id.to_string(),
            author: self
                .state
                .account
                .as_ref()
                .map(|account| account.display_name.clone())
                .unwrap_or_else(|| "me".to_string()),
            body,
            is_outgoing: true,
            created_at_secs,
            delivery,
        };
        self.threads
            .entry(chat_id.to_string())
            .or_insert_with(|| ThreadRecord {
                chat_id: chat_id.to_string(),
                unread_count: 0,
                updated_at_secs: created_at_secs,
                messages: Vec::new(),
            })
            .messages
            .push(message.clone());
        if let Some(thread) = self.threads.get_mut(chat_id) {
            thread.updated_at_secs = created_at_secs;
        }
        message
    }

    fn push_incoming_message_from(
        &mut self,
        chat_id: &str,
        body: String,
        created_at_secs: u64,
        author: Option<String>,
    ) {
        let message_id = self.allocate_message_id();
        let author = author.unwrap_or_else(|| self.owner_display_label(chat_id));
        let thread = self
            .threads
            .entry(chat_id.to_string())
            .or_insert_with(|| ThreadRecord {
                chat_id: chat_id.to_string(),
                unread_count: 0,
                updated_at_secs: created_at_secs,
                messages: Vec::new(),
            });
        if self.active_chat_id.as_deref() != Some(chat_id) {
            thread.unread_count = thread.unread_count.saturating_add(1);
        }
        thread.updated_at_secs = created_at_secs;
        thread.messages.push(ChatMessageSnapshot {
            id: message_id,
            chat_id: chat_id.to_string(),
            author,
            body,
            is_outgoing: false,
            created_at_secs,
            delivery: DeliveryState::Received,
        });
    }

    fn apply_routed_chat_message(&mut self, routed: RoutedChatMessage, created_at_secs: u64) {
        if routed.is_outgoing {
            self.push_outgoing_message(
                &routed.chat_id,
                routed.body,
                created_at_secs,
                DeliveryState::Sent,
            );
        } else {
            self.push_incoming_message_from(
                &routed.chat_id,
                routed.body,
                created_at_secs,
                routed.author,
            );
        }
    }

    fn route_received_direct_message(
        &self,
        local_owner: OwnerPubkey,
        sender_owner: OwnerPubkey,
        payload: &[u8],
    ) -> Option<RoutedChatMessage> {
        if let Some(decoded) = decode_app_direct_message_payload(payload) {
            if sender_owner == local_owner {
                if let Ok((chat_id, _)) = parse_peer_input(&decoded.chat_id) {
                    if chat_id != local_owner.to_string() {
                        return Some(RoutedChatMessage {
                            chat_id,
                            body: decoded.body,
                            is_outgoing: true,
                            author: Some(self.owner_display_label(&local_owner.to_string())),
                        });
                    }
                }
            }

            return Some(RoutedChatMessage {
                chat_id: sender_owner.to_string(),
                body: decoded.body,
                is_outgoing: false,
                author: Some(self.owner_display_label(&sender_owner.to_string())),
            });
        }

        if let Ok(decoded) = pairwise_codec::decode(payload) {
            let PairwiseRumorKind::Message { body, .. } = decoded.kind else {
                return None;
            };
            let is_outgoing = sender_owner == local_owner;
            let chat_id = if is_outgoing {
                local_owner.to_string()
            } else {
                sender_owner.to_string()
            };
            let author_owner = if is_outgoing {
                local_owner
            } else {
                sender_owner
            };
            return Some(RoutedChatMessage {
                chat_id,
                body,
                is_outgoing,
                author: Some(self.owner_display_label(&author_owner.to_string())),
            });
        }

        Some(RoutedChatMessage {
            chat_id: sender_owner.to_string(),
            body: String::from_utf8_lossy(payload).into_owned(),
            is_outgoing: false,
            author: Some(self.owner_display_label(&sender_owner.to_string())),
        })
    }

    fn apply_group_metadata_update(&mut self, group: GroupSnapshot, created_at_secs: u64) {
        self.apply_group_snapshot_to_threads(&group, created_at_secs.max(group.updated_at.get()));
    }

    fn apply_decrypted_payload(
        &mut self,
        sender_owner: OwnerPubkey,
        payload: &[u8],
        created_at_secs: u64,
    ) -> anyhow::Result<()> {
        let local_owner = self.logged_in.as_ref().expect("logged in").owner_pubkey;

        let group_event = {
            let logged_in = self.logged_in.as_mut().expect("logged in");
            logged_in
                .group_manager
                .handle_incoming(sender_owner, payload)?
        };

        match group_event {
            Some(GroupIncomingEvent::MetadataUpdated(group)) => {
                self.apply_group_metadata_update(group, created_at_secs);
            }
            Some(GroupIncomingEvent::Message(group_message)) => {
                let decoded = decode_app_group_message_payload(&group_message.body)
                    .ok_or_else(|| anyhow::anyhow!("Invalid group message payload."))?;
                self.apply_routed_chat_message(
                    RoutedChatMessage {
                        chat_id: group_chat_id(&group_message.group_id),
                        body: decoded.body,
                        is_outgoing: group_message.sender_owner == local_owner,
                        author: Some(
                            self.owner_display_label(&group_message.sender_owner.to_string()),
                        ),
                    },
                    created_at_secs,
                );
            }
            None => {
                if let Some(routed) =
                    self.route_received_direct_message(local_owner, sender_owner, payload)
                {
                    self.apply_routed_chat_message(routed, created_at_secs);
                }
            }
        }

        Ok(())
    }

    fn allocate_message_id(&mut self) -> String {
        let id = self.next_message_id;
        self.next_message_id = self.next_message_id.saturating_add(1);
        id.to_string()
    }

    fn rebuild_state(&mut self) {
        self.state.account = self.build_account_snapshot();
        self.state.device_roster = self.build_device_roster_snapshot();

        let default_screen = match self
            .logged_in
            .as_ref()
            .map(|logged_in| logged_in.authorization_state)
        {
            None => Screen::Welcome,
            Some(LocalAuthorizationState::Authorized) => Screen::ChatList,
            Some(LocalAuthorizationState::AwaitingApproval) => Screen::AwaitingDeviceApproval,
            Some(LocalAuthorizationState::Revoked) => Screen::DeviceRevoked,
        };

        let mut threads: Vec<&ThreadRecord> = self.threads.values().collect();
        threads.sort_by_key(|thread| std::cmp::Reverse(thread.updated_at_secs));

        self.state.chat_list = threads
            .iter()
            .map(|thread| {
                let last_message = thread.messages.last();
                let thread_kind = chat_kind_for_id(&thread.chat_id);
                let group_snapshot = self.group_snapshot_for_chat_id(&thread.chat_id);
                let display_name = group_snapshot
                    .as_ref()
                    .map(|group| group.name.clone())
                    .unwrap_or_else(|| self.owner_display_label(&thread.chat_id));
                let subtitle = group_snapshot
                    .as_ref()
                    .map(|group| format!("{} members", group.members.len()))
                    .or_else(|| self.owner_secondary_identifier(&thread.chat_id));
                let member_count = group_snapshot
                    .as_ref()
                    .map(|group| group.members.len() as u64)
                    .unwrap_or(0);
                ChatThreadSnapshot {
                    chat_id: thread.chat_id.clone(),
                    kind: thread_kind,
                    display_name,
                    subtitle,
                    member_count,
                    last_message_preview: last_message.map(|message| message.body.clone()),
                    last_message_at_secs: last_message.map(|message| message.created_at_secs),
                    last_message_is_outgoing: last_message.map(|message| message.is_outgoing),
                    last_message_delivery: last_message.map(|message| message.delivery.clone()),
                    unread_count: thread.unread_count,
                }
            })
            .collect();

        self.state.current_chat = self
            .active_chat_id
            .as_ref()
            .and_then(|chat_id| self.threads.get(chat_id))
            .map(|thread| {
                let group_snapshot = self.group_snapshot_for_chat_id(&thread.chat_id);
                CurrentChatSnapshot {
                    chat_id: thread.chat_id.clone(),
                    kind: chat_kind_for_id(&thread.chat_id),
                    display_name: group_snapshot
                        .as_ref()
                        .map(|group| group.name.clone())
                        .unwrap_or_else(|| self.owner_display_label(&thread.chat_id)),
                    subtitle: group_snapshot
                        .as_ref()
                        .map(|group| format!("{} members", group.members.len()))
                        .or_else(|| self.owner_secondary_identifier(&thread.chat_id)),
                    group_id: group_snapshot.as_ref().map(|group| group.group_id.clone()),
                    member_count: group_snapshot
                        .as_ref()
                        .map(|group| group.members.len() as u64)
                        .unwrap_or(0),
                    messages: thread.messages.clone(),
                }
            });

        self.state.group_details = self.screen_stack.last().and_then(|screen| match screen {
            Screen::GroupDetails { group_id } => self.build_group_details_snapshot(group_id),
            _ => None,
        });

        self.state.router = Router {
            default_screen,
            screen_stack: self.screen_stack.clone(),
        };
    }

    fn build_account_snapshot(&self) -> Option<AccountSnapshot> {
        let logged_in = self.logged_in.as_ref()?;
        let owner_public_key_hex = logged_in.owner_pubkey.to_string();
        let owner_npub = owner_npub_from_owner(logged_in.owner_pubkey)
            .unwrap_or_else(|| owner_public_key_hex.clone());
        let display_name = self
            .owner_display_name(&owner_public_key_hex)
            .unwrap_or_else(|| owner_npub.clone());
        let device_public_key_hex = logged_in.device_keys.public_key().to_hex();
        let device_npub = logged_in
            .device_keys
            .public_key()
            .to_bech32()
            .unwrap_or_else(|_| device_public_key_hex.clone());

        Some(AccountSnapshot {
            public_key_hex: owner_public_key_hex,
            npub: owner_npub,
            display_name,
            device_public_key_hex,
            device_npub,
            has_owner_signing_authority: logged_in.owner_keys.is_some(),
            authorization_state: public_authorization_state(logged_in.authorization_state),
        })
    }

    fn set_local_profile_name(&mut self, name: &str) {
        let Some(local_owner_hex) = self
            .logged_in
            .as_ref()
            .map(|logged_in| logged_in.owner_pubkey.to_string())
        else {
            return;
        };

        let Some(record) = build_owner_profile_record(name) else {
            return;
        };

        self.owner_profiles.insert(local_owner_hex.clone(), record);
        self.push_debug_log("profile.local.set", format!("owner={local_owner_hex}"));
        self.persist_best_effort();
    }

    fn apply_profile_metadata_event(&mut self, event: &Event) -> bool {
        let owner_hex = event.pubkey.to_hex();
        let Some(record) = parse_owner_profile_record(&event.content, event.created_at.as_u64())
        else {
            return false;
        };

        if let Some(existing) = self.owner_profiles.get(&owner_hex) {
            if existing.updated_at_secs > record.updated_at_secs {
                return false;
            }
        }

        self.owner_profiles.insert(owner_hex.clone(), record);
        self.push_debug_log("relay.metadata", format!("owner={owner_hex}"));
        true
    }

    fn owner_display_name(&self, owner_hex: &str) -> Option<String> {
        self.owner_profiles
            .get(owner_hex)
            .and_then(OwnerProfileRecord::preferred_label)
    }

    fn owner_display_label(&self, owner_hex: &str) -> String {
        self.owner_display_name(owner_hex)
            .or_else(|| owner_npub(owner_hex))
            .unwrap_or_else(|| owner_hex.to_string())
    }

    fn owner_secondary_identifier(&self, owner_hex: &str) -> Option<String> {
        let npub = owner_npub(owner_hex)?;
        match self.owner_display_name(owner_hex) {
            Some(label) if label != npub => Some(npub),
            Some(_) => None,
            None => Some(npub),
        }
    }

    fn build_device_roster_snapshot(&self) -> Option<DeviceRosterSnapshot> {
        let logged_in = self.logged_in.as_ref()?;
        let account = self.build_account_snapshot()?;
        let current_device_pubkey_hex = account.device_public_key_hex.clone();
        let current_device_npub = account.device_npub.clone();
        let mut entries = BTreeMap::<String, DeviceEntrySnapshot>::new();

        if let Some(user) = logged_in
            .session_manager
            .snapshot()
            .users
            .into_iter()
            .find(|user| user.owner_pubkey == logged_in.owner_pubkey)
        {
            if let Some(roster) = user.roster.as_ref() {
                for authorized_device in roster.devices() {
                    let device_pubkey_hex = authorized_device.device_pubkey.to_string();
                    entries
                        .entry(device_pubkey_hex.clone())
                        .or_insert(DeviceEntrySnapshot {
                            device_pubkey_hex: device_pubkey_hex.clone(),
                            device_npub: device_npub(&device_pubkey_hex)
                                .unwrap_or_else(|| device_pubkey_hex.clone()),
                            is_current_device: device_pubkey_hex == current_device_pubkey_hex,
                            is_authorized: true,
                            is_stale: false,
                            last_activity_secs: None,
                        });
                }
            }

            for device in user.devices {
                let device_pubkey_hex = device.device_pubkey.to_string();
                let entry =
                    entries
                        .entry(device_pubkey_hex.clone())
                        .or_insert(DeviceEntrySnapshot {
                            device_pubkey_hex: device_pubkey_hex.clone(),
                            device_npub: device_npub(&device_pubkey_hex)
                                .unwrap_or_else(|| device_pubkey_hex.clone()),
                            is_current_device: device_pubkey_hex == current_device_pubkey_hex,
                            is_authorized: device.authorized,
                            is_stale: device.is_stale,
                            last_activity_secs: device.last_activity.map(UnixSeconds::get),
                        });
                entry.is_authorized = device.authorized;
                entry.is_stale = device.is_stale;
                entry.last_activity_secs = device.last_activity.map(UnixSeconds::get);
            }
        }

        entries
            .entry(current_device_pubkey_hex.clone())
            .or_insert(DeviceEntrySnapshot {
                device_pubkey_hex: current_device_pubkey_hex.clone(),
                device_npub: current_device_npub.clone(),
                is_current_device: true,
                is_authorized: matches!(
                    logged_in.authorization_state,
                    LocalAuthorizationState::Authorized
                ),
                is_stale: matches!(
                    logged_in.authorization_state,
                    LocalAuthorizationState::Revoked
                ),
                last_activity_secs: None,
            });

        let mut devices = entries.into_values().collect::<Vec<_>>();
        devices.sort_by(|left, right| {
            right
                .is_current_device
                .cmp(&left.is_current_device)
                .then_with(|| left.device_pubkey_hex.cmp(&right.device_pubkey_hex))
        });

        Some(DeviceRosterSnapshot {
            owner_public_key_hex: account.public_key_hex,
            owner_npub: account.npub,
            current_device_public_key_hex: current_device_pubkey_hex,
            current_device_npub,
            can_manage_devices: logged_in.owner_keys.is_some(),
            authorization_state: public_authorization_state(logged_in.authorization_state),
            devices,
        })
    }

    fn group_snapshot_for_chat_id(&self, chat_id: &str) -> Option<GroupSnapshot> {
        let group_id = parse_group_id_from_chat_id(chat_id)?;
        self.logged_in.as_ref()?.group_manager.group(&group_id)
    }

    fn build_group_details_snapshot(&self, group_id: &str) -> Option<GroupDetailsSnapshot> {
        let logged_in = self.logged_in.as_ref()?;
        let group = logged_in.group_manager.group(group_id)?;
        let local_owner = logged_in.owner_pubkey;
        let mut members = group
            .members
            .iter()
            .map(|owner| {
                let owner_hex = owner.to_string();
                GroupMemberSnapshot {
                    owner_pubkey_hex: owner_hex.clone(),
                    display_name: self.owner_display_label(&owner_hex),
                    npub: owner_npub_from_owner(*owner).unwrap_or_else(|| owner_hex.clone()),
                    is_admin: group.admins.iter().any(|admin| admin == owner),
                    is_creator: group.created_by == *owner,
                    is_local_owner: *owner == local_owner,
                }
            })
            .collect::<Vec<_>>();
        members.sort_by(|left, right| {
            right
                .is_local_owner
                .cmp(&left.is_local_owner)
                .then_with(|| right.is_creator.cmp(&left.is_creator))
                .then_with(|| right.is_admin.cmp(&left.is_admin))
                .then_with(|| left.owner_pubkey_hex.cmp(&right.owner_pubkey_hex))
        });

        Some(GroupDetailsSnapshot {
            group_id: group.group_id,
            name: group.name,
            created_by_display_name: self.owner_display_label(&group.created_by.to_string()),
            created_by_npub: owner_npub_from_owner(group.created_by)
                .unwrap_or_else(|| group.created_by.to_string()),
            can_manage: group.admins.iter().any(|admin| admin == &local_owner),
            revision: group.revision,
            members,
        })
    }

    fn can_use_chats(&self) -> bool {
        matches!(
            self.logged_in
                .as_ref()
                .map(|logged_in| logged_in.authorization_state),
            Some(LocalAuthorizationState::Authorized)
        )
    }

    fn emit_account_bundle_update(&self, owner_keys: Option<&Keys>, device_keys: &Keys) {
        let device_nsec = device_keys
            .secret_key()
            .to_bech32()
            .unwrap_or_else(|_| device_keys.secret_key().to_secret_hex());
        let owner_nsec = owner_keys.map(|keys| {
            keys.secret_key()
                .to_bech32()
                .unwrap_or_else(|_| keys.secret_key().to_secret_hex())
        });
        let owner_pubkey_hex = owner_keys
            .map(|keys| keys.public_key().to_hex())
            .or_else(|| {
                self.logged_in
                    .as_ref()
                    .map(|logged_in| logged_in.owner_pubkey.to_string())
            })
            .unwrap_or_default();
        let _ = self.update_tx.send(AppUpdate::PersistAccountBundle {
            rev: self.state.rev,
            owner_nsec,
            owner_pubkey_hex,
            device_nsec,
        });
    }

    fn emit_state(&mut self) {
        self.state.rev = self.state.rev.saturating_add(1);
        let snapshot = self.state.clone();
        match self.shared_state.write() {
            Ok(mut slot) => *slot = snapshot.clone(),
            Err(poison) => *poison.into_inner() = snapshot.clone(),
        }
        let _ = self.update_tx.send(AppUpdate::FullState(snapshot));
    }

    fn persistence_path(&self) -> PathBuf {
        self.data_dir.join("ndr_demo_core_state.json")
    }

    fn debug_snapshot_path(&self) -> PathBuf {
        self.data_dir.join(DEBUG_SNAPSHOT_FILENAME)
    }

    fn load_persisted(&self) -> anyhow::Result<Option<PersistedState>> {
        let path = self.persistence_path();
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(path)?;
        let value: serde_json::Value = serde_json::from_slice(&bytes)?;
        if value.get("version").and_then(serde_json::Value::as_u64)
            != Some(PERSISTED_STATE_VERSION as u64)
        {
            return Ok(None);
        }
        Ok(Some(serde_json::from_value(value)?))
    }

    fn persist_best_effort(&self) {
        let Some(logged_in) = self.logged_in.as_ref() else {
            return;
        };

        let persisted = PersistedState {
            version: PERSISTED_STATE_VERSION,
            active_chat_id: self.active_chat_id.clone(),
            next_message_id: self.next_message_id,
            session_manager: Some(logged_in.session_manager.snapshot()),
            group_manager: Some(logged_in.group_manager.snapshot()),
            owner_profiles: self.owner_profiles.clone(),
            threads: self
                .threads
                .values()
                .map(|thread| PersistedThread {
                    chat_id: thread.chat_id.clone(),
                    unread_count: thread.unread_count,
                    updated_at_secs: thread.updated_at_secs,
                    messages: thread
                        .messages
                        .iter()
                        .map(|message| PersistedMessage {
                            id: message.id.clone(),
                            chat_id: message.chat_id.clone(),
                            author: message.author.clone(),
                            body: message.body.clone(),
                            is_outgoing: message.is_outgoing,
                            created_at_secs: message.created_at_secs,
                            delivery: (&message.delivery).into(),
                        })
                        .collect(),
                })
                .collect(),
            pending_inbound: self.pending_inbound.clone(),
            pending_outbound: self.pending_outbound.clone(),
            pending_group_controls: self.pending_group_controls.clone(),
            seen_event_ids: self.seen_event_order.iter().cloned().collect(),
            authorization_state: Some(logged_in.authorization_state.into()),
        };

        if let Ok(bytes) = serde_json::to_vec_pretty(&persisted) {
            let _ = fs::create_dir_all(&self.data_dir);
            let _ = fs::write(self.persistence_path(), bytes);
        }
        self.persist_debug_snapshot_best_effort();
    }

    fn clear_persistence_best_effort(&self) {
        let path = self.persistence_path();
        if path.exists() {
            let _ = fs::remove_file(path);
        }
        let debug_path = self.debug_snapshot_path();
        if debug_path.exists() {
            let _ = fs::remove_file(debug_path);
        }
    }

    fn persist_debug_snapshot_best_effort(&self) {
        if self.logged_in.is_none() {
            return;
        }
        if let Ok(bytes) = serde_json::to_vec_pretty(&self.build_runtime_debug_snapshot()) {
            let _ = fs::create_dir_all(&self.data_dir);
            let _ = fs::write(self.debug_snapshot_path(), bytes);
        }
    }

    fn build_runtime_debug_snapshot(&self) -> RuntimeDebugSnapshot {
        let current_protocol_plan = self
            .protocol_subscription_runtime
            .applying_plan
            .clone()
            .or_else(|| self.protocol_subscription_runtime.current_plan.clone())
            .or_else(|| self.compute_protocol_subscription_plan())
            .map(|plan| RuntimeProtocolPlanDebug {
                roster_authors: plan.roster_authors,
                invite_authors: plan.invite_authors,
                invite_response_recipient: plan.invite_response_recipient,
                message_authors: plan.message_authors,
                refresh_in_flight: self.protocol_subscription_runtime.refresh_in_flight,
                refresh_dirty: self.protocol_subscription_runtime.refresh_dirty,
            });

        let tracked_owner_hexes = sorted_hexes(self.tracked_peer_owner_hexes());
        let current_chat_list = self.threads.keys().cloned().collect::<Vec<_>>();
        let (local_owner_pubkey_hex, local_device_pubkey_hex, authorization_state, known_users) =
            if let Some(logged_in) = self.logged_in.as_ref() {
                let snapshot = logged_in.session_manager.snapshot();
                let users = snapshot
                    .users
                    .into_iter()
                    .map(|user| RuntimeKnownUserDebug {
                        owner_pubkey_hex: user.owner_pubkey.to_string(),
                        has_roster: user.roster.is_some(),
                        roster_device_count: user
                            .roster
                            .as_ref()
                            .map(|roster| roster.devices().len())
                            .unwrap_or_default(),
                        device_count: user.devices.len(),
                        authorized_device_count: user
                            .devices
                            .iter()
                            .filter(|device| device.authorized)
                            .count(),
                        active_session_device_count: user
                            .devices
                            .iter()
                            .filter(|device| device.active_session.is_some())
                            .count(),
                        inactive_session_count: user
                            .devices
                            .iter()
                            .map(|device| device.inactive_sessions.len())
                            .sum(),
                    })
                    .collect::<Vec<_>>();
                (
                    Some(logged_in.owner_pubkey.to_string()),
                    Some(local_device_from_keys(&logged_in.device_keys).to_string()),
                    Some(format!("{:?}", logged_in.authorization_state)),
                    users,
                )
            } else {
                (None, None, None, Vec::new())
            };

        RuntimeDebugSnapshot {
            generated_at_secs: unix_now().get(),
            local_owner_pubkey_hex,
            local_device_pubkey_hex,
            authorization_state,
            active_chat_id: self.active_chat_id.clone(),
            current_protocol_plan,
            tracked_owner_hexes,
            known_users,
            pending_outbound: self
                .pending_outbound
                .iter()
                .map(|pending| RuntimePendingOutboundDebug {
                    message_id: pending.message_id.clone(),
                    chat_id: pending.chat_id.clone(),
                    reason: format!("{:?}", pending.reason),
                    publish_mode: format!("{:?}", pending.publish_mode),
                    in_flight: pending.in_flight,
                })
                .collect(),
            pending_group_controls: self
                .pending_group_controls
                .iter()
                .map(|pending| RuntimePendingGroupControlDebug {
                    operation_id: pending.operation_id.clone(),
                    group_id: pending.group_id.clone(),
                    target_owner_hexes: pending.target_owner_hexes.clone(),
                    reason: format!("{:?}", pending.reason),
                    in_flight: pending.in_flight,
                    kind: format!("{:?}", pending.kind),
                })
                .collect(),
            recent_handshake_peers: self
                .recent_handshake_peers
                .values()
                .map(|peer| RuntimeRecentHandshakeDebug {
                    owner_hex: peer.owner_hex.clone(),
                    device_hex: peer.device_hex.clone(),
                    observed_at_secs: peer.observed_at_secs,
                })
                .collect(),
            event_counts: self.debug_event_counters.clone(),
            recent_log: self.debug_log.iter().cloned().collect(),
            toast: self.state.toast.clone(),
            current_chat_list,
        }
    }

    fn export_support_bundle_json(&self) -> String {
        serde_json::to_string_pretty(&self.build_support_bundle())
            .unwrap_or_else(|_| "{}".to_string())
    }

    fn build_support_bundle(&self) -> SupportBundle {
        let runtime = self.build_runtime_debug_snapshot();
        let current_screen = self
            .screen_stack
            .last()
            .cloned()
            .unwrap_or_else(|| self.state.router.default_screen.clone());
        let direct_chat_count = self
            .threads
            .keys()
            .filter(|chat_id| !is_group_chat_id(chat_id))
            .count();
        let group_chat_count = self
            .threads
            .keys()
            .filter(|chat_id| is_group_chat_id(chat_id))
            .count();
        let unread_chat_count = self
            .threads
            .values()
            .filter(|thread| thread.unread_count > 0)
            .count();

        SupportBundle {
            generated_at_secs: unix_now().get(),
            build: SupportBuildMetadata {
                app_version: APP_VERSION.to_string(),
                build_channel: BUILD_CHANNEL.to_string(),
                git_sha: BUILD_GIT_SHA.to_string(),
                build_timestamp_utc: BUILD_TIMESTAMP_UTC.to_string(),
                relay_set_id: RELAY_SET_ID.to_string(),
                trusted_test_build: trusted_test_build(),
            },
            relay_urls: configured_relays(),
            authorization_state: runtime.authorization_state,
            active_chat_id: runtime.active_chat_id,
            current_screen: format!("{current_screen:?}"),
            chat_count: self.threads.len(),
            direct_chat_count,
            group_chat_count,
            unread_chat_count,
            pending_outbound: runtime.pending_outbound,
            pending_group_controls: runtime.pending_group_controls,
            protocol: runtime.current_protocol_plan,
            tracked_owner_hexes: runtime.tracked_owner_hexes,
            known_users: runtime.known_users,
            recent_handshake_peers: runtime.recent_handshake_peers,
            event_counts: runtime.event_counts,
            recent_log: runtime.recent_log,
            current_chat_list: runtime.current_chat_list,
            latest_toast: runtime.toast,
        }
    }

    fn push_debug_log(&mut self, category: &str, detail: impl Into<String>) {
        self.debug_log.push_back(DebugLogEntry {
            timestamp_secs: unix_now().get(),
            category: category.to_string(),
            detail: detail.into(),
        });
        while self.debug_log.len() > MAX_DEBUG_LOG_ENTRIES {
            self.debug_log.pop_front();
        }
    }

    fn start_notifications_loop(&self, client: Client) {
        let mut notifications = client.notifications();
        let tx = self.core_sender.clone();
        self.runtime.spawn(async move {
            loop {
                match notifications.recv().await {
                    Ok(RelayPoolNotification::Event { event, .. }) => {
                        let _ = tx.send(CoreMsg::Internal(Box::new(InternalEvent::RelayEvent(
                            (*event).clone(),
                        ))));
                    }
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    fn schedule_session_connect(&self) {
        let Some(logged_in) = self.logged_in.as_ref() else {
            return;
        };
        let client = logged_in.client.clone();
        let relay_urls = logged_in.relay_urls.clone();
        self.runtime.spawn(async move {
            ensure_session_relays_configured(&client, &relay_urls).await;
            client
                .connect_with_timeout(Duration::from_secs(RELAY_CONNECT_TIMEOUT_SECS))
                .await;
        });
    }

    fn publish_local_identity_artifacts(&self) {
        let Some(logged_in) = self.logged_in.as_ref() else {
            return;
        };
        if logged_in.authorization_state == LocalAuthorizationState::Revoked {
            return;
        }

        let snapshot = logged_in.session_manager.snapshot();
        let local_roster = snapshot
            .users
            .iter()
            .find(|user| user.owner_pubkey == logged_in.owner_pubkey)
            .and_then(|user| user.roster.clone());
        let local_invite = snapshot.local_invite.clone();
        let owner_keys = logged_in.owner_keys.clone();
        let device_keys = logged_in.device_keys.clone();
        let owner_pubkey = logged_in.owner_pubkey;
        let local_profile = self.owner_profiles.get(&owner_pubkey.to_string()).cloned();
        let client = logged_in.client.clone();
        let relay_urls = logged_in.relay_urls.clone();
        let tx = self.core_sender.clone();

        self.runtime.spawn(async move {
            if let (Some(keys), Some(profile)) = (owner_keys.clone(), local_profile) {
                if let Some(label) = profile.preferred_label() {
                    let event =
                        EventBuilder::new(Kind::Metadata, build_profile_metadata_json(&label))
                            .sign_with_keys(&keys);
                    match event {
                        Ok(event) => {
                            if let Err(error) =
                                publish_event_with_retry(&client, &relay_urls, event, "metadata")
                                    .await
                            {
                                let _ = tx.send(CoreMsg::Internal(Box::new(InternalEvent::Toast(
                                    format!("Metadata publish failed: {error}"),
                                ))));
                            }
                        }
                        Err(error) => {
                            let _ = tx.send(CoreMsg::Internal(Box::new(InternalEvent::Toast(
                                error.to_string(),
                            ))));
                        }
                    }
                }
            }

            if let (Some(keys), Some(roster)) = (owner_keys, local_roster) {
                let roster_event = match codec::roster_unsigned_event(owner_pubkey, &roster)
                    .and_then(|unsigned| unsigned.sign_with_keys(&keys).map_err(Into::into))
                {
                    Ok(event) => Some(event),
                    Err(error) => {
                        let _ = tx.send(CoreMsg::Internal(Box::new(InternalEvent::Toast(
                            error.to_string(),
                        ))));
                        None
                    }
                };
                if let Some(roster_event) = roster_event {
                    if let Err(error) =
                        publish_event_with_retry(&client, &relay_urls, roster_event, "roster").await
                    {
                        let _ = tx.send(CoreMsg::Internal(Box::new(InternalEvent::Toast(
                            format!("Roster publish failed: {error}"),
                        ))));
                    }
                }
            }

            if let Some(invite) = local_invite {
                let invite_event = match codec::invite_unsigned_event(&invite)
                    .and_then(|unsigned| unsigned.sign_with_keys(&device_keys).map_err(Into::into))
                {
                    Ok(event) => Some(event),
                    Err(error) => {
                        let _ = tx.send(CoreMsg::Internal(Box::new(InternalEvent::Toast(
                            error.to_string(),
                        ))));
                        None
                    }
                };
                if let Some(invite_event) = invite_event {
                    if let Err(error) =
                        publish_event_with_retry(&client, &relay_urls, invite_event, "invite").await
                    {
                        let _ = tx.send(CoreMsg::Internal(Box::new(InternalEvent::Toast(
                            format!("Invite publish failed: {error}"),
                        ))));
                    }
                }
            }

            let _ = tx.send(CoreMsg::Internal(Box::new(InternalEvent::SyncComplete)));
        });
    }

    fn publish_roster_update(&self, roster: DeviceRoster) {
        let Some(logged_in) = self.logged_in.as_ref() else {
            return;
        };
        let Some(owner_keys) = logged_in.owner_keys.clone() else {
            return;
        };
        let owner_pubkey = logged_in.owner_pubkey;
        let client = logged_in.client.clone();
        let relay_urls = logged_in.relay_urls.clone();
        let tx = self.core_sender.clone();

        self.runtime.spawn(async move {
            match codec::roster_unsigned_event(owner_pubkey, &roster)
                .and_then(|unsigned| unsigned.sign_with_keys(&owner_keys).map_err(Into::into))
            {
                Ok(event) => {
                    if let Err(error) =
                        publish_event_with_retry(&client, &relay_urls, event, "roster").await
                    {
                        let _ = tx.send(CoreMsg::Internal(Box::new(InternalEvent::Toast(
                            format!("Roster publish failed: {error}"),
                        ))));
                    }
                }
                Err(error) => {
                    let _ = tx.send(CoreMsg::Internal(Box::new(InternalEvent::Toast(
                        error.to_string(),
                    ))));
                }
            }

            let _ = tx.send(CoreMsg::Internal(Box::new(InternalEvent::SyncComplete)));
        });
    }

    fn republish_local_identity_artifacts(&self) {
        self.publish_local_identity_artifacts();
    }

    fn request_protocol_subscription_refresh(&mut self) {
        self.request_protocol_subscription_refresh_inner(false);
    }

    fn request_protocol_subscription_refresh_forced(&mut self) {
        self.request_protocol_subscription_refresh_inner(true);
    }

    fn request_protocol_subscription_refresh_inner(&mut self, force: bool) {
        let Some(client) = self
            .logged_in
            .as_ref()
            .map(|logged_in| logged_in.client.clone())
        else {
            self.protocol_subscription_runtime = ProtocolSubscriptionRuntime::default();
            return;
        };

        if self.protocol_subscription_runtime.refresh_in_flight {
            self.push_debug_log("protocol.subscription.defer", "refresh already in flight");
            self.protocol_subscription_runtime.refresh_dirty = true;
            self.protocol_subscription_runtime.force_refresh_dirty |= force;
            return;
        }

        let plan = self.compute_protocol_subscription_plan();
        self.push_debug_log(
            "protocol.subscription.compute",
            summarize_protocol_plan(plan.as_ref()),
        );
        if !force && self.protocol_subscription_runtime.current_plan == plan {
            self.push_debug_log("protocol.subscription.noop", "plan unchanged");
            return;
        }

        let subscription_id = SubscriptionId::new(PROTOCOL_SUBSCRIPTION_ID);
        self.protocol_subscription_runtime.refresh_in_flight = true;
        self.protocol_subscription_runtime.refresh_dirty = false;
        self.protocol_subscription_runtime.force_refresh_dirty = false;
        self.protocol_subscription_runtime.refresh_token = self
            .protocol_subscription_runtime
            .refresh_token
            .wrapping_add(1);
        let token = self.protocol_subscription_runtime.refresh_token;
        self.protocol_subscription_runtime.applying_plan = plan.clone();
        let had_previous = self.protocol_subscription_runtime.current_plan.is_some();
        let filters = plan
            .as_ref()
            .map(build_protocol_filters)
            .unwrap_or_default();
        let tx = self.core_sender.clone();
        self.runtime.spawn(async move {
            let mut applied = true;
            client
                .connect_with_timeout(Duration::from_secs(RELAY_CONNECT_TIMEOUT_SECS))
                .await;
            if had_previous {
                let _ = client.unsubscribe(subscription_id.clone()).await;
            }
            if !filters.is_empty() {
                applied = client
                    .subscribe_with_id(subscription_id, filters, None)
                    .await
                    .is_ok();
            }
            let _ = tx.send(CoreMsg::Internal(Box::new(
                InternalEvent::ProtocolSubscriptionRefreshCompleted {
                    token,
                    applied,
                    plan,
                },
            )));
        });
    }

    fn compute_protocol_subscription_plan(&self) -> Option<ProtocolSubscriptionPlan> {
        let roster_authors = sorted_hexes(self.known_roster_owner_hexes());
        let invite_authors = sorted_hexes(self.known_invite_author_hexes());
        let message_authors = sorted_hexes(self.known_message_author_hexes());
        let invite_response_recipient = self
            .logged_in
            .as_ref()
            .and_then(|logged_in| logged_in.session_manager.snapshot().local_invite)
            .map(|invite| invite.inviter_ephemeral_public_key.to_string());

        if roster_authors.is_empty()
            && invite_authors.is_empty()
            && message_authors.is_empty()
            && invite_response_recipient.is_none()
        {
            return None;
        }

        Some(ProtocolSubscriptionPlan {
            roster_authors,
            invite_authors,
            invite_response_recipient,
            message_authors,
        })
    }

    fn can_poll_pending_device_invites(&self) -> bool {
        self.logged_in
            .as_ref()
            .map(|logged_in| logged_in.owner_keys.is_some())
            .unwrap_or(false)
    }

    fn known_roster_owner_hexes(&self) -> HashSet<String> {
        let mut owners = self.protocol_owner_hexes();
        if let Some(logged_in) = self.logged_in.as_ref() {
            owners.insert(logged_in.owner_pubkey.to_string());
        }
        owners
    }

    fn known_invite_author_hexes(&self) -> HashSet<String> {
        let Some(logged_in) = self.logged_in.as_ref() else {
            return HashSet::new();
        };

        let tracked_owners = self.protocol_owner_hexes();
        let local_device_hex = local_device_from_keys(&logged_in.device_keys).to_string();
        let mut authors = HashSet::new();

        for user in logged_in.session_manager.snapshot().users {
            let owner_hex = user.owner_pubkey.to_string();
            let should_include = owner_hex == logged_in.owner_pubkey.to_string()
                || tracked_owners.contains(&owner_hex);
            if !should_include {
                continue;
            }
            if let Some(roster) = user.roster {
                for device in roster.devices() {
                    let device_hex = device.device_pubkey.to_string();
                    if owner_hex == logged_in.owner_pubkey.to_string()
                        && device_hex == local_device_hex
                    {
                        continue;
                    }
                    authors.insert(device_hex);
                }
            }
        }

        authors
    }

    fn known_message_author_hexes(&self) -> HashSet<String> {
        let mut authors = HashSet::new();
        if let Some(logged_in) = self.logged_in.as_ref() {
            let selected_owners = self.protocol_owner_hexes();
            let local_owner_hex = logged_in.owner_pubkey.to_string();
            for user in logged_in
                .session_manager
                .snapshot()
                .users
                .into_iter()
                .filter(|user| {
                    let owner_hex = user.owner_pubkey.to_string();
                    owner_hex == local_owner_hex || selected_owners.contains(&owner_hex)
                })
            {
                for device in user.devices {
                    if let Some(session) = device.active_session.as_ref() {
                        collect_expected_senders(session, &mut authors);
                    }
                    for session in &device.inactive_sessions {
                        collect_expected_senders(session, &mut authors);
                    }
                }
            }
        }
        authors
    }

    fn sync_active_chat_from_router(&mut self) {
        match self
            .screen_stack
            .iter()
            .rev()
            .find_map(|screen| match screen {
                Screen::Chat { chat_id } => Some(chat_id.clone()),
                _ => None,
            }) {
            Some(chat_id) => {
                self.active_chat_id = Some(chat_id.clone());
                if let Some(thread) = self.threads.get_mut(&chat_id) {
                    thread.unread_count = 0;
                }
            }
            _ => {
                self.active_chat_id = None;
            }
        }
    }

    fn has_seen_event(&self, event_id: &str) -> bool {
        self.seen_event_ids.contains(event_id)
    }

    fn remember_event(&mut self, event_id: String) {
        if !self.seen_event_ids.insert(event_id.clone()) {
            return;
        }

        self.seen_event_order.push_back(event_id);
        while self.seen_event_order.len() > MAX_SEEN_EVENT_IDS {
            if let Some(expired) = self.seen_event_order.pop_front() {
                self.seen_event_ids.remove(&expired);
            }
        }
    }
}

fn configured_relays() -> Vec<String> {
    let compiled_defaults = compiled_default_relays();
    match std::env::var("NDR_DEMO_RELAYS") {
        Ok(value) => {
            let custom: Vec<String> = value
                .split(',')
                .map(str::trim)
                .filter(|entry| !entry.is_empty())
                .map(ToOwned::to_owned)
                .collect();
            if custom.is_empty() {
                compiled_defaults
            } else {
                custom
            }
        }
        Err(_) => compiled_defaults,
    }
}

fn configured_relay_urls() -> Vec<RelayUrl> {
    let parsed: Vec<RelayUrl> = configured_relays()
        .into_iter()
        .filter_map(|relay| RelayUrl::parse(relay).ok())
        .collect();
    if parsed.is_empty() {
        FALLBACK_DEFAULT_RELAYS
            .iter()
            .filter_map(|relay| RelayUrl::parse(*relay).ok())
            .collect()
    } else {
        parsed
    }
}

fn compiled_default_relays() -> Vec<String> {
    let compiled = COMPILED_DEFAULT_RELAYS_CSV
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if compiled.is_empty() {
        FALLBACK_DEFAULT_RELAYS
            .iter()
            .map(|relay| (*relay).to_string())
            .collect()
    } else {
        compiled
    }
}

fn trusted_test_build() -> bool {
    matches!(TRUSTED_TEST_BUILD, "1" | "true" | "TRUE" | "True")
}

pub(crate) fn build_summary() -> String {
    format!("{APP_VERSION} ({BUILD_GIT_SHA})")
}

pub(crate) fn relay_set_id() -> &'static str {
    RELAY_SET_ID
}

pub(crate) fn trusted_test_build_flag() -> bool {
    trusted_test_build()
}

async fn ensure_session_relays_configured(client: &Client, relay_urls: &[RelayUrl]) {
    for relay in relay_urls {
        let _ = client.add_relay(relay.clone()).await;
    }
}

fn sorted_hexes(values: HashSet<String>) -> Vec<String> {
    let mut sorted = values.into_iter().collect::<Vec<_>>();
    sorted.sort();
    sorted.dedup();
    sorted
}

fn build_protocol_filters(plan: &ProtocolSubscriptionPlan) -> Vec<Filter> {
    let mut filters = Vec::new();

    let roster_authors = plan
        .roster_authors
        .iter()
        .filter_map(|hex| PublicKey::parse(hex).ok())
        .collect::<Vec<_>>();
    if !roster_authors.is_empty() {
        filters.push(
            Filter::new()
                .kind(Kind::from(codec::ROSTER_EVENT_KIND as u16))
                .authors(roster_authors.clone()),
        );
        filters.push(Filter::new().kind(Kind::Metadata).authors(roster_authors));
    }

    let invite_authors = plan
        .invite_authors
        .iter()
        .filter_map(|hex| PublicKey::parse(hex).ok())
        .collect::<Vec<_>>();
    if !invite_authors.is_empty() {
        filters.push(
            Filter::new()
                .kind(Kind::from(codec::INVITE_EVENT_KIND as u16))
                .authors(invite_authors),
        );
    }

    if let Some(recipient_hex) = plan.invite_response_recipient.as_ref() {
        if let Ok(recipient) = PublicKey::parse(recipient_hex) {
            filters.push(
                Filter::new()
                    .kind(Kind::from(codec::INVITE_RESPONSE_KIND as u16))
                    .pubkey(recipient),
            );
        }
    }

    let message_authors = plan
        .message_authors
        .iter()
        .filter_map(|hex| PublicKey::parse(hex).ok())
        .collect::<Vec<_>>();
    if !message_authors.is_empty() {
        filters.push(
            Filter::new()
                .kind(Kind::from(codec::MESSAGE_EVENT_KIND as u16))
                .authors(message_authors),
        );
    }

    filters
}

fn build_protocol_state_catch_up_filters(
    plan: &ProtocolSubscriptionPlan,
    now: UnixSeconds,
) -> Vec<Filter> {
    let mut filters = Vec::new();

    let roster_authors = plan
        .roster_authors
        .iter()
        .filter_map(|hex| PublicKey::parse(hex).ok())
        .collect::<Vec<_>>();
    if !roster_authors.is_empty() {
        filters.push(
            Filter::new()
                .kind(Kind::from(codec::ROSTER_EVENT_KIND as u16))
                .authors(roster_authors.clone()),
        );
        filters.push(Filter::new().kind(Kind::Metadata).authors(roster_authors));
    }

    let invite_authors = plan
        .invite_authors
        .iter()
        .filter_map(|hex| PublicKey::parse(hex).ok())
        .collect::<Vec<_>>();
    if !invite_authors.is_empty() {
        filters.push(
            Filter::new()
                .kind(Kind::from(codec::INVITE_EVENT_KIND as u16))
                .authors(invite_authors),
        );
    }

    if let Some(recipient_hex) = plan.invite_response_recipient.as_ref() {
        if let Ok(recipient) = PublicKey::parse(recipient_hex) {
            filters.push(
                Filter::new()
                    .kind(Kind::from(codec::INVITE_RESPONSE_KIND as u16))
                    .pubkey(recipient),
            );
        }
    }

    let message_authors = plan
        .message_authors
        .iter()
        .filter_map(|hex| PublicKey::parse(hex).ok())
        .collect::<Vec<_>>();
    if !message_authors.is_empty() {
        filters.push(
            Filter::new()
                .kind(Kind::from(codec::MESSAGE_EVENT_KIND as u16))
                .authors(message_authors)
                .since(Timestamp::from(
                    now.get().saturating_sub(CATCH_UP_LOOKBACK_SECS),
                )),
        );
    }

    filters
}

fn summarize_protocol_plan(plan: Option<&ProtocolSubscriptionPlan>) -> String {
    let Some(plan) = plan else {
        return "none".to_string();
    };
    format!(
        "rosters={} invites={} invite_response={} messages={}",
        plan.roster_authors.join(","),
        plan.invite_authors.join(","),
        plan.invite_response_recipient
            .clone()
            .unwrap_or_else(|| "-".to_string()),
        plan.message_authors.join(","),
    )
}

fn summarize_relay_gaps(gaps: &[RelayGap]) -> String {
    if gaps.is_empty() {
        return "none".to_string();
    }

    gaps.iter()
        .map(|gap| match gap {
            RelayGap::MissingRoster { owner_pubkey } => {
                format!("MissingRoster({owner_pubkey})")
            }
            RelayGap::MissingDeviceInvite {
                owner_pubkey,
                device_pubkey,
            } => format!("MissingDeviceInvite({owner_pubkey},{device_pubkey})"),
        })
        .collect::<Vec<_>>()
        .join("|")
}

impl OwnerProfileRecord {
    fn preferred_label(&self) -> Option<String> {
        self.display_name.clone().or_else(|| self.name.clone())
    }
}

fn normalize_profile_field(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn build_owner_profile_record(name: &str) -> Option<OwnerProfileRecord> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return None;
    }

    Some(OwnerProfileRecord {
        name: Some(trimmed.to_string()),
        display_name: Some(trimmed.to_string()),
        updated_at_secs: unix_now().get(),
    })
}

fn parse_owner_profile_record(content: &str, updated_at_secs: u64) -> Option<OwnerProfileRecord> {
    let parsed = serde_json::from_str::<NostrProfileMetadata>(content).ok()?;
    let name = normalize_profile_field(parsed.name);
    let display_name = normalize_profile_field(parsed.display_name);
    if name.is_none() && display_name.is_none() {
        return None;
    }

    Some(OwnerProfileRecord {
        name,
        display_name,
        updated_at_secs,
    })
}

fn build_profile_metadata_json(name: &str) -> String {
    serde_json::to_string(&NostrProfileMetadata {
        name: Some(name.to_string()),
        display_name: Some(name.to_string()),
    })
    .unwrap_or_else(|_| format!(r#"{{"name":"{name}","display_name":"{name}"}}"#))
}

fn resolve_message_sender_owner(
    session_manager: &SessionManager,
    envelope: &MessageEnvelope,
    now: UnixSeconds,
) -> Option<OwnerPubkey> {
    let owners: Vec<OwnerPubkey> = session_manager
        .snapshot()
        .users
        .into_iter()
        .map(|user| user.owner_pubkey)
        .collect();

    for owner in owners {
        let mut candidate = session_manager.clone();
        let mut rng = OsRng;
        let mut ctx = ProtocolContext::new(now, &mut rng);
        match candidate.receive(&mut ctx, owner, envelope) {
            Ok(Some(_)) => return Some(owner),
            Ok(None) => {}
            Err(_) => {}
        }
    }

    None
}

fn encode_app_direct_message_payload(chat_id: &str, body: &str) -> anyhow::Result<Vec<u8>> {
    let (normalized_chat_id, _) = parse_peer_input(chat_id)?;
    Ok(serde_json::to_vec(&AppDirectMessagePayload {
        version: APP_DIRECT_MESSAGE_PAYLOAD_VERSION,
        chat_id: normalized_chat_id,
        body: body.to_string(),
    })?)
}

fn encode_pairwise_direct_message_payload(
    author_owner: OwnerPubkey,
    body: &str,
    now: UnixSeconds,
) -> anyhow::Result<Vec<u8>> {
    let author = PublicKey::from_slice(&author_owner.to_bytes())?;
    let millis = now.get().saturating_mul(1000);
    Ok(pairwise_codec::encode_message(
        author,
        body,
        pairwise_codec::EncodeOptions::new(now.get(), millis),
    )?)
}

fn prepare_direct_send_payloads(
    logged_in: &mut LoggedInState,
    owner: OwnerPubkey,
    chat_id: &str,
    body: &str,
    now: UnixSeconds,
) -> anyhow::Result<nostr_double_ratchet::PreparedSend> {
    let remote_payload = encode_pairwise_direct_message_payload(logged_in.owner_pubkey, body, now)?;
    let local_sibling_payload = encode_app_direct_message_payload(chat_id, body)?;
    let mut rng = OsRng;
    let mut ctx = ProtocolContext::new(now, &mut rng);

    let mut prepared =
        logged_in
            .session_manager
            .prepare_remote_send(&mut ctx, owner, remote_payload)?;
    let local_sibling = logged_in
        .session_manager
        .prepare_local_sibling_send(&mut ctx, local_sibling_payload)?;

    prepared.deliveries.extend(local_sibling.deliveries);
    prepared
        .invite_responses
        .extend(local_sibling.invite_responses);
    prepared.relay_gaps.extend(local_sibling.relay_gaps);
    prepared.relay_gaps.sort();
    prepared.relay_gaps.dedup();
    Ok(prepared)
}

fn decode_app_direct_message_payload(payload: &[u8]) -> Option<AppDirectMessagePayload> {
    let decoded = serde_json::from_slice::<AppDirectMessagePayload>(payload).ok()?;
    if decoded.version != APP_DIRECT_MESSAGE_PAYLOAD_VERSION {
        return None;
    }
    Some(decoded)
}

fn encode_app_group_message_payload(body: &str) -> anyhow::Result<Vec<u8>> {
    Ok(serde_json::to_vec(&AppGroupMessagePayload {
        version: APP_GROUP_MESSAGE_PAYLOAD_VERSION,
        body: body.to_string(),
    })?)
}

fn is_retryable_group_payload_error(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("create group sender must match created_by")
        || message.contains("unknown group")
        || message.contains("revision mismatch")
}

fn is_unknown_group_payload_error(error: &anyhow::Error) -> bool {
    error.to_string().contains("unknown group")
}

fn decode_app_group_message_payload(payload: &[u8]) -> Option<AppGroupMessagePayload> {
    let decoded = serde_json::from_slice::<AppGroupMessagePayload>(payload).ok()?;
    if decoded.version != APP_GROUP_MESSAGE_PAYLOAD_VERSION {
        return None;
    }
    Some(decoded)
}

fn is_group_chat_id(chat_id: &str) -> bool {
    chat_id.starts_with(GROUP_CHAT_PREFIX)
}

fn group_chat_id(group_id: &str) -> String {
    format!("{GROUP_CHAT_PREFIX}{group_id}")
}

fn parse_group_id_from_chat_id(chat_id: &str) -> Option<String> {
    chat_id
        .strip_prefix(GROUP_CHAT_PREFIX)
        .map(|group_id| group_id.to_string())
}

fn normalize_group_id(value: &str) -> Option<String> {
    if let Some(group_id) = parse_group_id_from_chat_id(value) {
        if !group_id.trim().is_empty() {
            return Some(group_id);
        }
        return None;
    }
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn chat_kind_for_id(chat_id: &str) -> ChatKind {
    if is_group_chat_id(chat_id) {
        ChatKind::Group
    } else {
        ChatKind::Direct
    }
}

fn collect_expected_senders(session: &SessionState, out: &mut HashSet<String>) {
    if let Some(current) = session.their_current_nostr_public_key {
        out.insert(current.to_string());
    }
    if let Some(next) = session.their_next_nostr_public_key {
        out.insert(next.to_string());
    }
    out.extend(session.skipped_keys.keys().map(ToString::to_string));
}

fn pending_reason_from_prepared(
    prepared: &nostr_double_ratchet::PreparedSend,
) -> Option<PendingSendReason> {
    if prepared
        .relay_gaps
        .iter()
        .any(|gap| matches!(gap, RelayGap::MissingRoster { .. }))
    {
        return Some(PendingSendReason::MissingRoster);
    }
    if prepared
        .relay_gaps
        .iter()
        .any(|gap| matches!(gap, RelayGap::MissingDeviceInvite { .. }))
    {
        return Some(PendingSendReason::MissingDeviceInvite);
    }
    if prepared.deliveries.is_empty() && prepared.invite_responses.is_empty() {
        return Some(PendingSendReason::MissingDeviceInvite);
    }
    None
}

fn pending_reason_from_group_prepared(
    prepared: &nostr_double_ratchet::GroupPreparedSend,
) -> Option<PendingSendReason> {
    if prepared
        .remote
        .relay_gaps
        .iter()
        .any(|gap| matches!(gap, RelayGap::MissingRoster { .. }))
    {
        return Some(PendingSendReason::MissingRoster);
    }
    if prepared
        .remote
        .relay_gaps
        .iter()
        .any(|gap| matches!(gap, RelayGap::MissingDeviceInvite { .. }))
    {
        return Some(PendingSendReason::MissingDeviceInvite);
    }
    if prepared.remote.deliveries.is_empty() && prepared.remote.invite_responses.is_empty() {
        return Some(PendingSendReason::MissingDeviceInvite);
    }
    None
}

fn build_prepared_publish_batch(
    prepared: &nostr_double_ratchet::PreparedSend,
) -> anyhow::Result<Option<PreparedPublishBatch>> {
    let invite_events = prepared
        .invite_responses
        .iter()
        .map(codec::invite_response_event)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let message_events = prepared
        .deliveries
        .iter()
        .map(|delivery| codec::message_event(&delivery.envelope))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    if message_events.is_empty() {
        return Ok(None);
    }

    Ok(Some(PreparedPublishBatch {
        invite_events,
        message_events,
    }))
}

fn build_group_prepared_publish_batch(
    prepared: &nostr_double_ratchet::GroupPreparedSend,
) -> anyhow::Result<Option<PreparedPublishBatch>> {
    build_group_publish_batch(&prepared.remote)
}

fn build_group_local_sibling_publish_batch(
    prepared: &nostr_double_ratchet::GroupPreparedSend,
) -> anyhow::Result<Option<PreparedPublishBatch>> {
    build_group_publish_batch(&prepared.local_sibling)
}

fn build_group_publish_batch(
    prepared: &nostr_double_ratchet::GroupPreparedPublish,
) -> anyhow::Result<Option<PreparedPublishBatch>> {
    let invite_events = prepared
        .invite_responses
        .iter()
        .map(codec::invite_response_event)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let message_events = prepared
        .deliveries
        .iter()
        .map(|delivery| codec::message_event(&delivery.envelope))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    if message_events.is_empty() {
        return Ok(None);
    }

    Ok(Some(PreparedPublishBatch {
        invite_events,
        message_events,
    }))
}

fn publish_mode_for_batch(batch: &PreparedPublishBatch) -> OutboundPublishMode {
    if batch.invite_events.is_empty() {
        OutboundPublishMode::OrdinaryFirstAck
    } else {
        OutboundPublishMode::FirstContactStaged
    }
}

fn migrate_publish_mode(
    current: OutboundPublishMode,
    batch: Option<&PreparedPublishBatch>,
) -> OutboundPublishMode {
    match current {
        OutboundPublishMode::WaitForPeer => batch
            .map(publish_mode_for_batch)
            .unwrap_or(OutboundPublishMode::WaitForPeer),
        other => other,
    }
}

fn pending_reason_for_publish_mode(mode: &OutboundPublishMode) -> PendingSendReason {
    match mode {
        OutboundPublishMode::FirstContactStaged => PendingSendReason::PublishingFirstContact,
        OutboundPublishMode::OrdinaryFirstAck => PendingSendReason::PublishRetry,
        OutboundPublishMode::WaitForPeer => PendingSendReason::MissingDeviceInvite,
    }
}

fn retry_delay_for_publish_mode(mode: &OutboundPublishMode) -> u64 {
    match mode {
        OutboundPublishMode::FirstContactStaged => FIRST_CONTACT_RETRY_DELAY_SECS,
        OutboundPublishMode::OrdinaryFirstAck | OutboundPublishMode::WaitForPeer => {
            PENDING_RETRY_DELAY_SECS
        }
    }
}

fn retry_deadline_for_publish_mode(now_secs: u64, mode: &OutboundPublishMode) -> u64 {
    now_secs.saturating_add(retry_delay_for_publish_mode(mode))
}

pub(crate) fn parse_peer_input(input: &str) -> anyhow::Result<(String, PublicKey)> {
    let mut normalized = input.trim().to_ascii_lowercase();
    if let Some(stripped) = normalized.strip_prefix("nostr:") {
        normalized = stripped.to_string();
    }
    let pubkey = PublicKey::parse(&normalized)?;
    Ok((pubkey.to_hex(), pubkey))
}

pub(crate) fn normalize_peer_input_for_display(input: &str) -> String {
    let mut normalized = input.trim().to_ascii_lowercase();
    if let Some(stripped) = normalized.strip_prefix("nostr:") {
        normalized = stripped.to_string();
    }

    match PublicKey::parse(&normalized) {
        Ok(pubkey) if normalized.starts_with("npub1") => {
            pubkey.to_bech32().unwrap_or_else(|_| normalized.clone())
        }
        Ok(pubkey) => pubkey.to_hex(),
        Err(_) => normalized,
    }
}

fn parse_owner_input(input: &str) -> anyhow::Result<OwnerPubkey> {
    let (_, pubkey) = parse_peer_input(input)?;
    Ok(OwnerPubkey::from_bytes(pubkey.to_bytes()))
}

fn parse_owner_inputs(
    inputs: &[String],
    exclude_owner: OwnerPubkey,
) -> anyhow::Result<Vec<OwnerPubkey>> {
    let mut owners = inputs
        .iter()
        .map(|input| parse_owner_input(input))
        .collect::<anyhow::Result<Vec<_>>>()?;
    owners.retain(|owner| *owner != exclude_owner);
    owners.sort_by_key(|owner| owner.to_string());
    owners.dedup();
    Ok(owners)
}

fn owner_pubkeys_from_hexes(hexes: &[String]) -> anyhow::Result<Vec<OwnerPubkey>> {
    hexes
        .iter()
        .map(|hex| parse_owner_input(hex))
        .collect::<anyhow::Result<Vec<_>>>()
}

fn sorted_owner_hexes(owners: &[OwnerPubkey]) -> Vec<String> {
    let mut hexes = owners.iter().map(ToString::to_string).collect::<Vec<_>>();
    hexes.sort();
    hexes.dedup();
    hexes
}

fn parse_device_input(input: &str) -> anyhow::Result<DevicePubkey> {
    let (_, pubkey) = parse_peer_input(input)?;
    Ok(DevicePubkey::from_bytes(pubkey.to_bytes()))
}

#[cfg(test)]
fn local_owner_from_keys(keys: &Keys) -> OwnerPubkey {
    OwnerPubkey::from_bytes(keys.public_key().to_bytes())
}

fn local_device_from_keys(keys: &Keys) -> DevicePubkey {
    DevicePubkey::from_bytes(keys.public_key().to_bytes())
}

fn owner_npub(peer_hex: &str) -> Option<String> {
    PublicKey::parse(peer_hex).ok()?.to_bech32().ok()
}

fn owner_npub_from_owner(owner_pubkey: OwnerPubkey) -> Option<String> {
    PublicKey::parse(owner_pubkey.to_string())
        .ok()?
        .to_bech32()
        .ok()
}

fn device_npub(device_hex: &str) -> Option<String> {
    PublicKey::parse(device_hex).ok()?.to_bech32().ok()
}

fn local_roster_from_session_manager(session_manager: &SessionManager) -> Option<DeviceRoster> {
    let snapshot = session_manager.snapshot();
    let owner = snapshot.local_owner_pubkey;
    snapshot
        .users
        .into_iter()
        .find(|user| user.owner_pubkey == owner)
        .and_then(|user| user.roster)
}

fn public_authorization_state(state: LocalAuthorizationState) -> DeviceAuthorizationState {
    match state {
        LocalAuthorizationState::Authorized => DeviceAuthorizationState::Authorized,
        LocalAuthorizationState::AwaitingApproval => DeviceAuthorizationState::AwaitingApproval,
        LocalAuthorizationState::Revoked => DeviceAuthorizationState::Revoked,
    }
}

fn derive_local_authorization_state(
    has_owner_signing_authority: bool,
    owner_pubkey: OwnerPubkey,
    local_device_pubkey: DevicePubkey,
    session_manager: &SessionManager,
    previous_state: Option<LocalAuthorizationState>,
) -> LocalAuthorizationState {
    let local_roster = session_manager
        .snapshot()
        .users
        .into_iter()
        .find(|user| user.owner_pubkey == owner_pubkey)
        .and_then(|user| user.roster);
    match local_roster {
        Some(roster) => {
            if roster.get_device(&local_device_pubkey).is_some() {
                LocalAuthorizationState::Authorized
            } else if has_owner_signing_authority {
                LocalAuthorizationState::Authorized
            } else if matches!(
                previous_state,
                Some(LocalAuthorizationState::Authorized) | Some(LocalAuthorizationState::Revoked)
            ) {
                LocalAuthorizationState::Revoked
            } else {
                LocalAuthorizationState::AwaitingApproval
            }
        }
        None if has_owner_signing_authority => LocalAuthorizationState::Authorized,
        None => LocalAuthorizationState::AwaitingApproval,
    }
}

fn chat_unavailable_message(logged_in: Option<&LoggedInState>) -> &'static str {
    match logged_in.map(|logged_in| logged_in.authorization_state) {
        Some(LocalAuthorizationState::AwaitingApproval) => {
            "This device is still waiting for approval."
        }
        Some(LocalAuthorizationState::Revoked) => {
            "This device has been removed from the roster. Log out to continue."
        }
        _ => "Create or restore an account first.",
    }
}

fn unix_now() -> UnixSeconds {
    UnixSeconds(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    )
}

async fn publish_event_with_retry(
    client: &Client,
    relay_urls: &[RelayUrl],
    event: Event,
    label: &str,
) -> anyhow::Result<()> {
    let mut last_error = "no relays configured".to_string();

    for attempt in 0..5 {
        client
            .connect_with_timeout(Duration::from_secs(RELAY_CONNECT_TIMEOUT_SECS))
            .await;
        match publish_event_once(client, relay_urls, &event).await {
            Ok(()) => return Ok(()),
            Err(error) => last_error = error.to_string(),
        }

        if attempt < 4 {
            sleep(Duration::from_millis(750 * (attempt + 1) as u64)).await;
        }
    }

    Err(anyhow::anyhow!("{label}: {last_error}"))
}

async fn publish_events_with_retry(
    client: &Client,
    relay_urls: &[RelayUrl],
    events: Vec<Event>,
    label: &str,
) -> anyhow::Result<()> {
    for event in events {
        publish_event_with_retry(client, relay_urls, event, label).await?;
    }
    Ok(())
}

async fn publish_events_first_ack(
    client: &Client,
    relay_urls: &[RelayUrl],
    events: &[Event],
    label: &str,
) -> anyhow::Result<()> {
    for event in events {
        publish_event_first_ack(client, relay_urls, event, label).await?;
    }
    Ok(())
}

async fn publish_event_first_ack(
    client: &Client,
    relay_urls: &[RelayUrl],
    event: &Event,
    label: &str,
) -> anyhow::Result<()> {
    if relay_urls.is_empty() {
        return Err(anyhow::anyhow!("{label}: no relays configured"));
    }

    client
        .connect_with_timeout(Duration::from_secs(RELAY_CONNECT_TIMEOUT_SECS))
        .await;

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<(), String>>(relay_urls.len().max(1));

    for relay_url in relay_urls.iter().cloned() {
        let client = client.clone();
        let event = event.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            let result = match client.send_event_to([relay_url.clone()], event).await {
                Ok(output) if !output.success.is_empty() => Ok(()),
                Ok(output) => Err(output
                    .failed
                    .values()
                    .flatten()
                    .next()
                    .cloned()
                    .unwrap_or_else(|| "no relay accepted event".to_string())),
                Err(error) => Err(error.to_string()),
            };
            let _ = tx.send(result).await;
        });
    }
    drop(tx);

    let mut first_error = None;
    while let Some(result) = rx.recv().await {
        match result {
            Ok(()) => return Ok(()),
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
    }

    Err(anyhow::anyhow!(
        "{label}: {}",
        first_error.unwrap_or_else(|| "publish failed".to_string())
    ))
}

async fn publish_event_once(
    client: &Client,
    relay_urls: &[RelayUrl],
    event: &Event,
) -> anyhow::Result<()> {
    if relay_urls.is_empty() {
        return Err(anyhow::anyhow!("no relays configured"));
    }

    let output = client
        .send_event_to(relay_urls.to_vec(), event.clone())
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    if output.success.is_empty() {
        let reasons = output
            .failed
            .values()
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        Err(anyhow::anyhow!(if reasons.is_empty() {
            "no relay accepted event".to_string()
        } else {
            reasons.join("; ")
        }))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_relay::{matches_filter, TestRelay};
    use crate::FfiApp;
    use nostr_double_ratchet::AuthorizedDevice;
    use nostr_sdk::prelude::SecretKey;
    use serde_json::{json, Value};
    use std::path::Path;
    use std::sync::{Mutex, OnceLock};
    use std::thread;
    use std::time::{Duration as StdDuration, Instant};
    use tempfile::TempDir;

    fn relay_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    struct RelayEnvGuard {
        previous: Option<String>,
    }

    impl RelayEnvGuard {
        fn local_only() -> Self {
            Self::custom("ws://127.0.0.1:4848")
        }

        fn custom(value: &str) -> Self {
            let previous = std::env::var("NDR_DEMO_RELAYS").ok();
            std::env::set_var("NDR_DEMO_RELAYS", value);
            Self { previous }
        }
    }

    impl Drop for RelayEnvGuard {
        fn drop(&mut self) {
            if let Some(previous) = &self.previous {
                std::env::set_var("NDR_DEMO_RELAYS", previous);
            } else {
                std::env::remove_var("NDR_DEMO_RELAYS");
            }
        }
    }

    fn nsec_for_fill(secret_fill: u8) -> String {
        let keys = Keys::new(SecretKey::from_slice(&[secret_fill; 32]).expect("secret key"));
        keys.secret_key().to_bech32().expect("nsec")
    }

    fn npub_for_fill(secret_fill: u8) -> String {
        let keys = Keys::new(SecretKey::from_slice(&[secret_fill; 32]).expect("secret key"));
        keys.public_key().to_bech32().expect("npub")
    }

    fn pubkey_hex_for_fill(secret_fill: u8) -> String {
        let keys = Keys::new(SecretKey::from_slice(&[secret_fill; 32]).expect("secret key"));
        keys.public_key().to_hex()
    }

    fn device_fill_for_owner(secret_fill: u8) -> u8 {
        secret_fill.wrapping_add(100)
    }

    fn wait_for_state(
        app: &Arc<FfiApp>,
        label: &str,
        predicate: impl Fn(&AppState) -> bool,
    ) -> AppState {
        wait_for_state_timeout(app, label, 15, predicate)
    }

    fn wait_for_state_timeout(
        app: &Arc<FfiApp>,
        label: &str,
        timeout_secs: u64,
        predicate: impl Fn(&AppState) -> bool,
    ) -> AppState {
        let deadline = Instant::now() + StdDuration::from_secs(timeout_secs);
        let mut last = app.state();

        while Instant::now() < deadline {
            last = app.state();
            if predicate(&last) {
                return last;
            }
            thread::sleep(StdDuration::from_millis(100));
        }

        panic!("timed out waiting for {label}: last state = {:?}", last);
    }

    fn app_with_dir(data_dir: &Path, secret_fill: u8) -> Arc<FfiApp> {
        let app = FfiApp::new(
            data_dir.to_string_lossy().into_owned(),
            String::new(),
            "test".to_string(),
        );
        app.dispatch(AppAction::RestoreAccountBundle {
            owner_nsec: Some(nsec_for_fill(secret_fill)),
            owner_pubkey_hex: pubkey_hex_for_fill(secret_fill),
            device_nsec: nsec_for_fill(device_fill_for_owner(secret_fill)),
        });
        wait_for_state(&app, "account restore", |state| {
            state.account.is_some() && !state.busy.restoring_session
        });
        app
    }

    fn app(secret_fill: u8) -> (TempDir, Arc<FfiApp>) {
        let temp_dir = TempDir::new().expect("temp dir");
        let app = app_with_dir(temp_dir.path(), secret_fill);
        (temp_dir, app)
    }

    fn test_core(data_dir: &Path) -> AppCore {
        let (update_tx, _update_rx) = flume::unbounded();
        let (core_tx, _core_rx) = flume::unbounded();
        let shared_state = Arc::new(RwLock::new(AppState::empty()));
        AppCore::new(
            update_tx,
            core_tx,
            data_dir.to_string_lossy().into_owned(),
            shared_state,
        )
    }

    fn persisted_state(data_dir: &Path) -> PersistedState {
        let bytes =
            fs::read(data_dir.join("ndr_demo_core_state.json")).expect("read persisted state");
        serde_json::from_slice(&bytes).expect("parse persisted state")
    }

    fn wait_for_persisted_state(
        data_dir: &Path,
        label: &str,
        predicate: impl Fn(&PersistedState) -> bool,
    ) -> PersistedState {
        let deadline = Instant::now() + StdDuration::from_secs(15);
        while Instant::now() < deadline {
            if data_dir.join("ndr_demo_core_state.json").exists() {
                let state = persisted_state(data_dir);
                if predicate(&state) {
                    return state;
                }
            }
            thread::sleep(StdDuration::from_millis(100));
        }
        panic!("timed out waiting for persisted state: {label}");
    }

    fn pending_publish_event_ids(core: &AppCore, message_id: &str) -> (Vec<String>, Vec<String>) {
        let pending = core
            .pending_outbound
            .iter()
            .find(|pending| pending.message_id == message_id)
            .expect("pending outbound");
        let batch = pending
            .prepared_publish
            .as_ref()
            .expect("prepared publish batch");
        (
            batch
                .invite_events
                .iter()
                .map(|event| event.id.to_string())
                .collect(),
            batch
                .message_events
                .iter()
                .map(|event| event.id.to_string())
                .collect(),
        )
    }

    fn keys_for_fill(secret_fill: u8) -> Keys {
        Keys::new(SecretKey::from_slice(&[secret_fill; 32]).expect("secret key"))
    }

    fn device_keys_for_fill(secret_fill: u8) -> Keys {
        Keys::new(SecretKey::from_slice(&[secret_fill.wrapping_add(100); 32]).expect("secret key"))
    }

    fn start_primary_test_session(
        core: &mut AppCore,
        owner_fill: u8,
        allow_restore: bool,
        allow_protocol_restore: bool,
    ) -> anyhow::Result<()> {
        core.start_primary_session(
            keys_for_fill(owner_fill),
            device_keys_for_fill(owner_fill),
            allow_restore,
            allow_protocol_restore,
        )
    }

    fn established_session_manager_pair(
        alice_fill: u8,
        bob_fill: u8,
        base_secs: u64,
    ) -> (SessionManager, SessionManager, String) {
        let alice_owner_keys = keys_for_fill(alice_fill);
        let bob_owner_keys = keys_for_fill(bob_fill);
        let alice_device_keys = device_keys_for_fill(alice_fill);
        let bob_device_keys = device_keys_for_fill(bob_fill);
        let alice_owner = local_owner_from_keys(&alice_owner_keys);
        let bob_owner = local_owner_from_keys(&bob_owner_keys);
        let alice_device = local_device_from_keys(&alice_device_keys);
        let bob_device = local_device_from_keys(&bob_device_keys);
        let now = UnixSeconds(base_secs);

        let mut alice_manager = SessionManager::new(
            alice_owner,
            alice_device_keys.secret_key().to_secret_bytes(),
        );
        let mut bob_manager =
            SessionManager::new(bob_owner, bob_device_keys.secret_key().to_secret_bytes());

        let alice_roster = DeviceRoster::new(now, vec![AuthorizedDevice::new(alice_device, now)]);
        let bob_roster = DeviceRoster::new(now, vec![AuthorizedDevice::new(bob_device, now)]);
        alice_manager.apply_local_roster(alice_roster.clone());
        bob_manager.apply_local_roster(bob_roster.clone());
        alice_manager.observe_peer_roster(bob_owner, bob_roster.clone());
        bob_manager.observe_peer_roster(alice_owner, alice_roster);

        let bob_invite = {
            let mut rng = OsRng;
            let mut ctx = ProtocolContext::new(UnixSeconds(base_secs + 1), &mut rng);
            bob_manager
                .ensure_local_invite(&mut ctx)
                .expect("ensure bob invite")
                .clone()
        };
        alice_manager
            .observe_device_invite(bob_owner, bob_invite)
            .expect("observe bob invite");

        let prepared = {
            let mut rng = OsRng;
            let mut ctx = ProtocolContext::new(UnixSeconds(base_secs + 2), &mut rng);
            alice_manager
                .prepare_send(&mut ctx, bob_owner, b"bootstrap".to_vec())
                .expect("prepare bootstrap message")
        };
        assert_eq!(prepared.invite_responses.len(), 1);
        assert_eq!(prepared.deliveries.len(), 1);

        {
            let mut rng = OsRng;
            let mut ctx = ProtocolContext::new(UnixSeconds(base_secs + 3), &mut rng);
            bob_manager
                .observe_invite_response(&mut ctx, &prepared.invite_responses[0])
                .expect("observe invite response")
                .expect("processed invite response");
        }
        {
            let mut rng = OsRng;
            let mut ctx = ProtocolContext::new(UnixSeconds(base_secs + 4), &mut rng);
            bob_manager
                .receive(&mut ctx, alice_owner, &prepared.deliveries[0].envelope)
                .expect("receive bootstrap delivery")
                .expect("bootstrap message");
        }

        (alice_manager, bob_manager, bob_owner.to_string())
    }

    fn logged_in_core_with_manager(
        data_dir: &Path,
        secret_fill: u8,
        session_manager: SessionManager,
    ) -> AppCore {
        let owner_keys = keys_for_fill(secret_fill);
        let device_keys = device_keys_for_fill(secret_fill);
        let mut core = test_core(data_dir);
        core.state.account = Some(AccountSnapshot {
            public_key_hex: owner_keys.public_key().to_hex(),
            npub: owner_keys.public_key().to_bech32().expect("npub"),
            display_name: owner_keys.public_key().to_bech32().expect("npub"),
            device_public_key_hex: device_keys.public_key().to_hex(),
            device_npub: device_keys.public_key().to_bech32().expect("device npub"),
            has_owner_signing_authority: true,
            authorization_state: DeviceAuthorizationState::Authorized,
        });
        core.logged_in = Some(LoggedInState {
            owner_pubkey: local_owner_from_keys(&owner_keys),
            owner_keys: Some(owner_keys),
            device_keys: device_keys.clone(),
            client: Client::new(device_keys),
            relay_urls: configured_relay_urls(),
            session_manager,
            group_manager: GroupManager::new(local_owner_from_keys(&keys_for_fill(secret_fill))),
            authorization_state: LocalAuthorizationState::Authorized,
        });
        core.rebuild_state();
        core
    }

    fn publish_local_relay_event(keys: &Keys, event: Event) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("publish runtime");
        runtime.block_on(async {
            let client = Client::new(keys.clone());
            let relay_urls = vec![RelayUrl::parse("ws://127.0.0.1:4848").expect("relay url")];
            ensure_session_relays_configured(&client, &relay_urls).await;
            client.connect().await;
            publish_event_with_retry(&client, &relay_urls, event, "test publish")
                .await
                .expect("publish event");
            let _ = client.shutdown().await;
        });
    }

    fn fetch_local_relay_events(filters: Vec<Filter>) -> Vec<Event> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("fetch runtime");
        runtime.block_on(async move {
            let client = Client::new(keys_for_fill(200));
            client
                .add_relay("ws://127.0.0.1:4848")
                .await
                .expect("add relay");
            client.connect_with_timeout(Duration::from_secs(5)).await;
            let events = client
                .fetch_events(filters, Some(Duration::from_secs(5)))
                .await
                .expect("fetch events");
            let collected = events.into_iter().collect::<Vec<_>>();
            let _ = client.shutdown().await;
            collected
        })
    }

    #[test]
    fn relay_filter_matches_pubkey_tags_and_time_bounds() {
        let event = json!({
            "pubkey": "aaaaaaaa",
            "kind": codec::INVITE_RESPONSE_KIND,
            "created_at": 100,
            "tags": [["p", "deadbeef"], ["d", "double-ratchet/invites/test"]],
        });

        assert!(matches_filter(
            &event,
            &json!({
                "kinds": [codec::INVITE_RESPONSE_KIND],
                "#p": ["deadbeef"],
                "since": 90,
                "until": 110,
            }),
        ));
        assert!(!matches_filter(
            &event,
            &json!({
                "kinds": [codec::INVITE_RESPONSE_KIND],
                "#p": ["cafebabe"],
            }),
        ));
        assert!(!matches_filter(
            &event,
            &json!({
                "kinds": [codec::INVITE_RESPONSE_KIND],
                "#p": ["deadbeef"],
                "since": 101,
            }),
        ));
    }

    #[test]
    fn normalize_and_validate_peer_input_accepts_expected_forms() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let npub = npub_for_fill(11);
        let (hex, _) = parse_peer_input(&npub).expect("parse npub");

        assert_eq!(normalize_peer_input_for_display(&npub), npub);
        assert_eq!(
            normalize_peer_input_for_display(&format!("nostr:{npub}")),
            npub
        );
        assert_eq!(normalize_peer_input_for_display(&hex), hex);
        assert!(parse_peer_input(&format!("nostr:{hex}")).is_ok());
        assert!(parse_peer_input("not-a-key").is_err());
    }

    #[test]
    fn start_session_restores_persisted_threads_only_when_enabled() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let _env = RelayEnvGuard::local_only();
        let data_dir = TempDir::new().expect("temp dir");
        let chat_id = parse_peer_input(&npub_for_fill(22))
            .expect("peer chat id")
            .0;

        let mut seeded = test_core(data_dir.path());
        start_primary_test_session(&mut seeded, 21, false, false).expect("start session");
        seeded.remember_event("event-1".to_string());
        seeded.threads.insert(
            chat_id.clone(),
            ThreadRecord {
                chat_id: chat_id.clone(),
                unread_count: 2,
                updated_at_secs: 55,
                messages: vec![ChatMessageSnapshot {
                    id: "1".to_string(),
                    chat_id: chat_id.clone(),
                    author: "peer".to_string(),
                    body: "restored".to_string(),
                    is_outgoing: false,
                    created_at_secs: 55,
                    delivery: DeliveryState::Received,
                }],
            },
        );
        seeded.active_chat_id = Some(chat_id.clone());
        seeded.screen_stack = vec![Screen::Chat {
            chat_id: chat_id.clone(),
        }];
        seeded.rebuild_state();
        seeded.persist_best_effort();

        let mut restored = test_core(data_dir.path());
        start_primary_test_session(&mut restored, 21, true, true).expect("restore session");
        assert_eq!(restored.active_chat_id.as_deref(), Some(chat_id.as_str()));
        assert_eq!(restored.state.chat_list.len(), 1);
        assert_eq!(
            restored
                .state
                .current_chat
                .as_ref()
                .expect("current chat")
                .messages[0]
                .body,
            "restored"
        );
        assert!(restored.has_seen_event("event-1"));

        let mut fresh = test_core(data_dir.path());
        start_primary_test_session(&mut fresh, 21, false, false).expect("fresh session");
        assert!(fresh.state.chat_list.is_empty());
        assert!(fresh.active_chat_id.is_none());
        assert!(!fresh.has_seen_event("event-1"));
    }

    #[test]
    fn old_or_unversioned_persistence_is_ignored_after_schema_cut() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let _env = RelayEnvGuard::local_only();
        let data_dir = TempDir::new().expect("temp dir");
        let peer_npub = npub_for_fill(32);
        let peer_hex = parse_peer_input(&peer_npub).expect("peer hex").0;

        let old_version = json!({
            "version": PERSISTED_STATE_VERSION - 1,
            "active_chat_id": peer_hex,
            "next_message_id": 2,
            "session_manager": null,
            "threads": [{
                "chat_id": peer_hex,
                "unread_count": 1,
                "updated_at_secs": 7,
                "messages": [{
                    "id": "1",
                    "chat_id": peer_hex,
                    "author": peer_npub,
                    "body": "old version",
                    "is_outgoing": false,
                    "created_at_secs": 7,
                    "delivery": "Received"
                }]
            }]
        });
        fs::write(
            data_dir.path().join("ndr_demo_core_state.json"),
            serde_json::to_vec(&old_version).expect("old version json"),
        )
        .expect("write old version persistence");

        let mut core = test_core(data_dir.path());
        start_primary_test_session(&mut core, 31, true, false).expect("ignore old version state");
        assert!(core.active_chat_id.is_none());
        assert!(core.state.chat_list.is_empty());

        let unversioned = json!({
            "active_chat_id": peer_hex,
            "next_message_id": 2,
            "session_manager": null,
            "threads": []
        });
        fs::write(
            data_dir.path().join("ndr_demo_core_state.json"),
            serde_json::to_vec(&unversioned).expect("unversioned json"),
        )
        .expect("write unversioned persistence");

        let mut core = test_core(data_dir.path());
        start_primary_test_session(&mut core, 31, true, false).expect("ignore unversioned state");
        assert!(core.active_chat_id.is_none());
        assert!(core.state.chat_list.is_empty());
    }

    #[test]
    fn malformed_current_persistence_is_ignored_without_crash() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let _env = RelayEnvGuard::local_only();
        let data_dir = TempDir::new().expect("temp dir");
        let owner_hex = pubkey_hex_for_fill(33);
        let malformed = json!({
            "version": PERSISTED_STATE_VERSION,
            "active_chat_id": "group:bad-group",
            "next_message_id": 2,
            "session_manager": null,
            "group_manager": {
                "local_owner_pubkey": owner_hex,
                "groups": [{
                    "group_id": "bad-group",
                    "name": "BadGroup",
                    "created_by": owner_hex,
                    "members": [owner_hex],
                    "admins": [owner_hex],
                    "revision": 1,
                    "created_at": 1_900_000_000u64,
                    "updated_at": 1_900_000_000u64
                }]
            },
            "threads": []
        });
        fs::write(
            data_dir.path().join("ndr_demo_core_state.json"),
            serde_json::to_vec(&malformed).expect("malformed json"),
        )
        .expect("write malformed persistence");

        let mut core = test_core(data_dir.path());
        start_primary_test_session(&mut core, 33, true, true)
            .expect("ignore malformed current state");
        assert!(core.active_chat_id.is_none());
        assert!(core.state.chat_list.is_empty());
    }

    #[test]
    fn update_screen_stack_opens_chat_and_clears_unread() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let data_dir = TempDir::new().expect("temp dir");
        let mut core = test_core(data_dir.path());
        start_primary_test_session(&mut core, 40, false, false).expect("start session");
        let chat_id = parse_peer_input(&npub_for_fill(41))
            .expect("peer chat id")
            .0;
        core.threads.insert(
            chat_id.clone(),
            ThreadRecord {
                chat_id: chat_id.clone(),
                unread_count: 3,
                updated_at_secs: 100,
                messages: vec![ChatMessageSnapshot {
                    id: "1".to_string(),
                    chat_id: chat_id.clone(),
                    author: "peer".to_string(),
                    body: "hello".to_string(),
                    is_outgoing: false,
                    created_at_secs: 100,
                    delivery: DeliveryState::Received,
                }],
            },
        );

        core.update_screen_stack(vec![Screen::Chat {
            chat_id: chat_id.clone(),
        }]);

        assert_eq!(core.active_chat_id.as_deref(), Some(chat_id.as_str()));
        assert_eq!(core.threads.get(&chat_id).expect("thread").unread_count, 0);
        assert_eq!(
            core.state
                .current_chat
                .as_ref()
                .expect("current chat")
                .chat_id,
            chat_id
        );
    }

    #[test]
    fn push_screen_allows_logged_out_onboarding_routes() {
        let data_dir = TempDir::new().expect("temp dir");
        let mut core = test_core(data_dir.path());

        core.push_screen(Screen::CreateAccount);
        assert!(matches!(core.state.router.default_screen, Screen::Welcome));
        assert!(matches!(
            core.state.router.screen_stack.as_slice(),
            [Screen::CreateAccount]
        ));

        core.push_screen(Screen::RestoreAccount);
        assert!(matches!(
            core.state.router.screen_stack.as_slice(),
            [Screen::RestoreAccount]
        ));

        core.push_screen(Screen::AddDevice);
        assert!(matches!(
            core.state.router.screen_stack.as_slice(),
            [Screen::AddDevice]
        ));
    }

    #[test]
    fn update_screen_stack_logged_out_keeps_only_onboarding_routes() {
        let data_dir = TempDir::new().expect("temp dir");
        let mut core = test_core(data_dir.path());

        core.update_screen_stack(vec![
            Screen::CreateAccount,
            Screen::NewChat,
            Screen::RestoreAccount,
            Screen::ChatList,
            Screen::AddDevice,
        ]);

        assert!(matches!(core.state.router.default_screen, Screen::Welcome));
        assert_eq!(core.state.router.screen_stack.len(), 3);
        assert!(matches!(
            core.state.router.screen_stack[0],
            Screen::CreateAccount
        ));
        assert!(matches!(
            core.state.router.screen_stack[1],
            Screen::RestoreAccount
        ));
        assert!(matches!(
            core.state.router.screen_stack[2],
            Screen::AddDevice
        ));
        assert!(core.state.account.is_none());
        assert!(core.state.current_chat.is_none());
    }

    #[test]
    fn logout_keeps_state_revision_monotonic() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let _env = RelayEnvGuard::local_only();
        let data_dir = TempDir::new().expect("temp dir");
        let mut core = test_core(data_dir.path());
        start_primary_test_session(&mut core, 70, false, false).expect("start session");

        let rev_before_logout = core.state.rev;
        assert!(rev_before_logout > 0);
        assert!(core.state.account.is_some());

        core.logout();

        assert!(core.state.rev > rev_before_logout);
        assert!(matches!(core.state.router.default_screen, Screen::Welcome));
        assert!(core.state.router.screen_stack.is_empty());
        assert!(core.state.account.is_none());
        assert!(core.state.chat_list.is_empty());
        assert!(core.active_chat_id.is_none());
    }

    #[test]
    fn protocol_subscription_uses_stable_id() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let _env = RelayEnvGuard::local_only();
        let data_dir = TempDir::new().expect("temp dir");
        let mut core = test_core(data_dir.path());
        start_primary_test_session(&mut core, 69, false, false).expect("start session");

        let client = core.logged_in.as_ref().expect("logged in").client.clone();
        let protocol_id = SubscriptionId::new(PROTOCOL_SUBSCRIPTION_ID);

        let wait_for_subscription =
            |runtime: &tokio::runtime::Runtime, client: &Client, protocol_id: &SubscriptionId| {
                let deadline = Instant::now() + StdDuration::from_secs(5);
                while Instant::now() < deadline {
                    let subscriptions = runtime.block_on(client.subscriptions());
                    if subscriptions.contains_key(&protocol_id) {
                        return subscriptions;
                    }
                    thread::sleep(StdDuration::from_millis(50));
                }
                runtime.block_on(client.subscriptions())
            };

        let initial = wait_for_subscription(&core.runtime, &client, &protocol_id);
        assert_eq!(initial.len(), 1);
        assert!(initial.contains_key(&protocol_id));

        core.create_chat(&npub_for_fill(70));

        let after_create = wait_for_subscription(&core.runtime, &client, &protocol_id);
        assert_eq!(after_create.len(), 1);
        assert!(after_create.contains_key(&protocol_id));
    }

    #[test]
    fn protocol_subscription_plan_sorts_roster_authors_stably() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let data_dir = TempDir::new().expect("temp dir");
        let mut core = test_core(data_dir.path());
        start_primary_test_session(&mut core, 71, false, false).expect("start session");

        let owner_a = local_owner_from_keys(&keys_for_fill(90)).to_string();
        let owner_b = local_owner_from_keys(&keys_for_fill(89)).to_string();

        core.threads.insert(
            owner_a.clone(),
            ThreadRecord {
                chat_id: owner_a.clone(),
                unread_count: 0,
                updated_at_secs: 10,
                messages: Vec::new(),
            },
        );
        core.threads.insert(
            owner_b.clone(),
            ThreadRecord {
                chat_id: owner_b.clone(),
                unread_count: 0,
                updated_at_secs: 20,
                messages: Vec::new(),
            },
        );
        core.pending_outbound.push(PendingOutbound {
            message_id: "dup-owner-a".to_string(),
            chat_id: owner_a.clone(),
            body: "pending".to_string(),
            prepared_publish: None,
            publish_mode: OutboundPublishMode::WaitForPeer,
            reason: PendingSendReason::MissingRoster,
            next_retry_at_secs: 123,
            in_flight: false,
        });
        core.recent_handshake_peers.insert(
            "device-b".to_string(),
            RecentHandshakePeer {
                owner_hex: owner_b.clone(),
                device_hex: "device-b".to_string(),
                observed_at_secs: 50,
            },
        );

        let plan = core
            .compute_protocol_subscription_plan()
            .expect("subscription plan");
        let mut expected = vec![
            core.logged_in
                .as_ref()
                .expect("logged in")
                .owner_pubkey
                .to_string(),
            owner_a,
            owner_b,
        ];
        expected.sort();
        expected.dedup();

        assert_eq!(plan.roster_authors, expected);
    }

    #[test]
    fn protocol_subscription_refresh_marks_dirty_while_refresh_is_in_flight() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let data_dir = TempDir::new().expect("temp dir");
        let mut core = test_core(data_dir.path());
        start_primary_test_session(&mut core, 72, false, false).expect("start session");

        core.protocol_subscription_runtime.refresh_in_flight = true;
        core.protocol_subscription_runtime.refresh_dirty = false;
        let token_before = core.protocol_subscription_runtime.refresh_token;

        core.request_protocol_subscription_refresh();

        assert!(core.protocol_subscription_runtime.refresh_dirty);
        assert!(core.protocol_subscription_runtime.refresh_in_flight);
        assert_eq!(
            core.protocol_subscription_runtime.refresh_token,
            token_before
        );
    }

    #[test]
    fn established_send_stays_pending_until_publish_ack() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let _env = RelayEnvGuard::custom("ws://127.0.0.1:59999");
        let data_dir = TempDir::new().expect("temp dir");
        let (alice_manager, _bob_manager, chat_id) =
            established_session_manager_pair(73, 74, 1_900_000_000);
        let mut core = logged_in_core_with_manager(data_dir.path(), 73, alice_manager);

        core.send_message(&chat_id, "offline direct send");

        let last_message = core
            .state
            .current_chat
            .as_ref()
            .expect("current chat")
            .messages
            .last()
            .expect("outgoing message");
        assert_eq!(last_message.body, "offline direct send");
        assert!(matches!(last_message.delivery, DeliveryState::Pending));
        assert!(!core.state.busy.sending_message);
        assert_eq!(core.pending_outbound.len(), 1);
        assert_eq!(
            core.pending_outbound[0].publish_mode,
            OutboundPublishMode::OrdinaryFirstAck
        );
        assert_eq!(
            core.pending_outbound[0].reason,
            PendingSendReason::PublishRetry
        );
    }

    #[test]
    fn create_group_routes_into_group_chat() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let data_dir = TempDir::new().expect("temp dir");
        let (alice_manager, _bob_manager, bob_owner_hex) =
            established_session_manager_pair(80, 81, 1_900_000_100);
        let mut core = logged_in_core_with_manager(data_dir.path(), 80, alice_manager);

        core.create_group("Trip crew", &[bob_owner_hex]);

        let current_chat = core.state.current_chat.as_ref().expect("current chat");
        assert!(current_chat.chat_id.starts_with(GROUP_CHAT_PREFIX));
        assert!(matches!(current_chat.kind, ChatKind::Group));
        assert_eq!(current_chat.display_name, "Trip crew");
        assert_eq!(current_chat.member_count, 2);
        assert_eq!(core.state.chat_list.len(), 1);
        assert_eq!(core.pending_group_controls.len(), 1);
    }

    #[test]
    fn incoming_group_metadata_creates_group_thread() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let data_dir = TempDir::new().expect("temp dir");
        let (mut alice_manager, bob_manager, bob_owner_hex) =
            established_session_manager_pair(82, 83, 1_900_000_200);
        let alice_owner = local_owner_from_keys(&keys_for_fill(82));
        let mut alice_groups = GroupManager::new(alice_owner);
        let mut rng = OsRng;
        let mut ctx = ProtocolContext::new(UnixSeconds(1_900_000_201), &mut rng);
        let result = alice_groups
            .create_group(
                &mut alice_manager,
                &mut ctx,
                "Project".to_string(),
                vec![parse_owner_input(&bob_owner_hex).expect("bob owner")],
            )
            .expect("create group");

        let mut bob_core = logged_in_core_with_manager(data_dir.path(), 83, bob_manager);
        let mut receive_rng = OsRng;
        let mut receive_ctx = ProtocolContext::new(UnixSeconds(1_900_000_202), &mut receive_rng);
        let received = bob_core
            .logged_in
            .as_mut()
            .expect("logged in")
            .session_manager
            .receive(
                &mut receive_ctx,
                alice_owner,
                &result.prepared.remote.deliveries[0].envelope,
            )
            .expect("receive group create")
            .expect("group create payload");
        bob_core
            .apply_decrypted_payload(received.owner_pubkey, &received.payload, 1_900_000_202)
            .expect("apply group metadata");
        bob_core.rebuild_state();

        let group_chat_id = group_chat_id(&result.group.group_id);
        let thread = bob_core.threads.get(&group_chat_id).expect("group thread");
        assert!(thread.messages.is_empty());
        assert_eq!(bob_core.state.chat_list[0].display_name, "Project");
        assert!(matches!(bob_core.state.chat_list[0].kind, ChatKind::Group));
    }

    #[test]
    fn incoming_group_message_routes_to_group_thread() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let data_dir = TempDir::new().expect("temp dir");
        let (mut alice_manager, bob_manager, bob_owner_hex) =
            established_session_manager_pair(84, 85, 1_900_000_300);
        let alice_owner = local_owner_from_keys(&keys_for_fill(84));
        let mut alice_groups = GroupManager::new(alice_owner);
        let mut rng = OsRng;
        let mut ctx = ProtocolContext::new(UnixSeconds(1_900_000_301), &mut rng);
        let create = alice_groups
            .create_group(
                &mut alice_manager,
                &mut ctx,
                "Signals".to_string(),
                vec![parse_owner_input(&bob_owner_hex).expect("bob owner")],
            )
            .expect("create group");

        let mut bob_core = logged_in_core_with_manager(data_dir.path(), 85, bob_manager);
        let mut receive_rng = OsRng;
        let mut receive_ctx = ProtocolContext::new(UnixSeconds(1_900_000_302), &mut receive_rng);
        let create_message = bob_core
            .logged_in
            .as_mut()
            .expect("logged in")
            .session_manager
            .receive(
                &mut receive_ctx,
                alice_owner,
                &create.prepared.remote.deliveries[0].envelope,
            )
            .expect("receive create")
            .expect("create payload");
        bob_core
            .apply_decrypted_payload(
                create_message.owner_pubkey,
                &create_message.payload,
                1_900_000_302,
            )
            .expect("apply create");

        let message_send = alice_groups
            .send_message(
                &mut alice_manager,
                &mut ProtocolContext::new(UnixSeconds(1_900_000_303), &mut rng),
                &create.group.group_id,
                encode_app_group_message_payload("hello group").expect("payload"),
            )
            .expect("send group message");
        let group_message = bob_core
            .logged_in
            .as_mut()
            .expect("logged in")
            .session_manager
            .receive(
                &mut ProtocolContext::new(UnixSeconds(1_900_000_304), &mut receive_rng),
                alice_owner,
                &message_send.remote.deliveries[0].envelope,
            )
            .expect("receive group message")
            .expect("group payload");
        bob_core
            .apply_decrypted_payload(
                group_message.owner_pubkey,
                &group_message.payload,
                1_900_000_304,
            )
            .expect("apply group message");

        let group_chat_id = group_chat_id(&create.group.group_id);
        let group_thread = bob_core.threads.get(&group_chat_id).expect("group thread");
        assert_eq!(group_thread.messages.len(), 1);
        assert_eq!(group_thread.messages[0].body, "hello group");
        assert!(!bob_core.threads.contains_key(&alice_owner.to_string()));
    }

    #[test]
    fn restored_pending_group_create_rebuilds_publish_without_new_group_id() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let _env = RelayEnvGuard::custom("ws://127.0.0.1:59999");
        let data_dir = TempDir::new().expect("temp dir");

        let mut core = test_core(data_dir.path());
        start_primary_test_session(&mut core, 86, false, false).expect("start session");

        let bob_owner_keys = keys_for_fill(87);
        let bob_device_keys = device_keys_for_fill(87);
        let bob_owner = local_owner_from_keys(&bob_owner_keys);
        let bob_device = local_device_from_keys(&bob_device_keys);
        let mut bob_manager =
            SessionManager::new(bob_owner, bob_device_keys.secret_key().to_secret_bytes());

        core.create_group("Restore group", &[bob_owner.to_string()]);

        assert_eq!(core.pending_group_controls.len(), 1);
        assert!(core.pending_group_controls[0].prepared_publish.is_none());
        core.pending_group_controls[0].next_retry_at_secs = 0;
        core.pending_group_controls[0].in_flight = true;
        let original_group_id = core.pending_group_controls[0].group_id.clone();

        {
            let logged_in = core.logged_in.as_mut().expect("logged in");
            let now = UnixSeconds(1_900_001_400);
            let bob_roster = DeviceRoster::new(now, vec![AuthorizedDevice::new(bob_device, now)]);
            logged_in
                .session_manager
                .observe_peer_roster(bob_owner, bob_roster);
            let mut rng = OsRng;
            let mut ctx = ProtocolContext::new(UnixSeconds(1_900_001_401), &mut rng);
            let bob_invite = bob_manager
                .ensure_local_invite(&mut ctx)
                .expect("ensure bob invite")
                .clone();
            logged_in
                .session_manager
                .observe_device_invite(bob_owner, bob_invite)
                .expect("observe bob invite");
        }
        core.persist_best_effort();
        drop(core);

        let mut restored = test_core(data_dir.path());
        start_primary_test_session(&mut restored, 86, true, true).expect("restore session");

        let groups = restored
            .logged_in
            .as_ref()
            .expect("logged in")
            .group_manager
            .groups();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].group_id, original_group_id);

        let pending = restored
            .pending_group_controls
            .iter()
            .find(|pending| pending.group_id == original_group_id)
            .expect("pending group control restored");
        assert!(pending.prepared_publish.is_some());
        assert_eq!(pending.group_id, original_group_id);
    }

    #[test]
    fn retry_pending_outbound_reuses_same_prepared_events_without_advancing_session() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let _env = RelayEnvGuard::custom("ws://127.0.0.1:59999");
        let data_dir = TempDir::new().expect("temp dir");
        let (alice_manager, _bob_manager, chat_id) =
            established_session_manager_pair(77, 78, 1_900_000_100);
        let mut core = logged_in_core_with_manager(data_dir.path(), 77, alice_manager);

        core.send_message(&chat_id, "retry me");

        let message_id = core.pending_outbound[0].message_id.clone();
        let event_ids_before = pending_publish_event_ids(&core, &message_id);

        core.handle_internal(InternalEvent::PublishFinished {
            message_id: message_id.clone(),
            chat_id: chat_id.clone(),
            success: false,
        });

        let pending_after_failure = core
            .pending_outbound
            .iter()
            .find(|pending| pending.message_id == message_id)
            .expect("pending outbound after failure");
        assert_eq!(
            pending_after_failure.publish_mode,
            OutboundPublishMode::OrdinaryFirstAck
        );
        assert_eq!(
            pending_after_failure.reason,
            PendingSendReason::PublishRetry
        );
        assert!(!pending_after_failure.in_flight);
        assert!(pending_after_failure.next_retry_at_secs > 0);

        let snapshot_before_retry = core
            .logged_in
            .as_ref()
            .expect("logged in")
            .session_manager
            .snapshot();
        if let Some(pending) = core
            .pending_outbound
            .iter_mut()
            .find(|pending| pending.message_id == message_id)
        {
            pending.next_retry_at_secs = 0;
        }

        core.retry_pending_outbound(UnixSeconds(1_900_000_200));

        let snapshot_after_retry = core
            .logged_in
            .as_ref()
            .expect("logged in")
            .session_manager
            .snapshot();
        let event_ids_after = pending_publish_event_ids(&core, &message_id);

        assert_eq!(event_ids_after, event_ids_before);
        assert_eq!(snapshot_after_retry, snapshot_before_retry);
        assert!(
            core.pending_outbound
                .iter()
                .find(|pending| pending.message_id == message_id)
                .expect("pending outbound after retry")
                .in_flight
        );
    }

    #[test]
    fn publish_finished_success_marks_message_sent_and_clears_pending() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let _env = RelayEnvGuard::custom("ws://127.0.0.1:59999");
        let data_dir = TempDir::new().expect("temp dir");
        let (alice_manager, _bob_manager, chat_id) =
            established_session_manager_pair(79, 80, 1_900_000_300);
        let mut core = logged_in_core_with_manager(data_dir.path(), 79, alice_manager);

        core.send_message(&chat_id, "mark me sent");
        let message_id = core.pending_outbound[0].message_id.clone();

        core.handle_internal(InternalEvent::PublishFinished {
            message_id: message_id.clone(),
            chat_id: chat_id.clone(),
            success: true,
        });

        assert!(core.pending_outbound.is_empty());
        let last_message = core
            .state
            .current_chat
            .as_ref()
            .expect("current chat")
            .messages
            .last()
            .expect("message");
        assert_eq!(last_message.id, message_id);
        assert!(matches!(last_message.delivery, DeliveryState::Sent));
    }

    #[test]
    fn publish_events_first_ack_succeeds_when_any_relay_accepts() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let _env = RelayEnvGuard::custom("ws://127.0.0.1:4848,ws://127.0.0.1:59999");
        let _relay = TestRelay::start();
        let owner_keys = keys_for_fill(81);
        let device_keys = device_keys_for_fill(81);
        let owner = local_owner_from_keys(&owner_keys);
        let roster = DeviceRoster::new(
            UnixSeconds(1_900_000_400),
            vec![AuthorizedDevice::new(
                local_device_from_keys(&device_keys),
                UnixSeconds(1_900_000_400),
            )],
        );
        let event = codec::roster_unsigned_event(owner, &roster)
            .expect("roster event")
            .sign_with_keys(&owner_keys)
            .expect("sign roster");
        let client = Client::new(device_keys);
        let relay_urls = configured_relay_urls();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        runtime.block_on(async {
            ensure_session_relays_configured(&client, &relay_urls).await;
            client.connect().await;
            publish_events_first_ack(&client, &relay_urls, &[event.clone()], "test first ack")
                .await
                .expect("first ack publish");
            let _ = client.shutdown().await;
        });

        let events = fetch_local_relay_events(vec![
            Filter::new().kind(Kind::from(codec::ROSTER_EVENT_KIND as u16))
        ]);
        assert!(events.iter().any(|candidate| candidate.id == event.id));
    }

    #[test]
    fn publish_events_first_ack_fails_when_all_relays_fail() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let _env = RelayEnvGuard::custom("ws://127.0.0.1:59999");
        let owner_keys = keys_for_fill(82);
        let device_keys = device_keys_for_fill(82);
        let owner = local_owner_from_keys(&owner_keys);
        let roster = DeviceRoster::new(
            UnixSeconds(1_900_000_500),
            vec![AuthorizedDevice::new(
                local_device_from_keys(&device_keys),
                UnixSeconds(1_900_000_500),
            )],
        );
        let event = codec::roster_unsigned_event(owner, &roster)
            .expect("roster event")
            .sign_with_keys(&owner_keys)
            .expect("sign roster");
        let client = Client::new(device_keys);
        let relay_urls = configured_relay_urls();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let result = runtime.block_on(async {
            ensure_session_relays_configured(&client, &relay_urls).await;
            client.connect().await;
            let result =
                publish_events_first_ack(&client, &relay_urls, &[event], "test first ack").await;
            let _ = client.shutdown().await;
            result
        });

        assert!(result.is_err());
    }

    #[test]
    fn prepared_pending_outbound_persists_across_restore() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let _env = RelayEnvGuard::custom("ws://127.0.0.1:59999");
        let data_dir = TempDir::new().expect("temp dir");
        let (alice_manager, _bob_manager, chat_id) =
            established_session_manager_pair(79, 80, 1_900_000_300);
        let mut core = logged_in_core_with_manager(data_dir.path(), 79, alice_manager);

        core.send_message(&chat_id, "persist me");
        let message_id = core.pending_outbound[0].message_id.clone();
        let event_ids_before = pending_publish_event_ids(&core, &message_id);
        core.persist_best_effort();

        let mut restored = test_core(data_dir.path());
        start_primary_test_session(&mut restored, 79, true, true)
            .expect("restore session with prepared pending outbound");

        let restored_message_id = restored.pending_outbound[0].message_id.clone();
        let event_ids_after = pending_publish_event_ids(&restored, &restored_message_id);

        assert_eq!(restored.pending_outbound.len(), 1);
        assert_eq!(event_ids_after, event_ids_before);
    }

    #[test]
    fn pending_inbound_persists_across_restore() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let data_dir = TempDir::new().expect("temp dir");
        let owner_keys = keys_for_fill(75);
        let device_keys = device_keys_for_fill(75);
        let owner = local_owner_from_keys(&owner_keys);
        let now = UnixSeconds(500);
        let local_device = local_device_from_keys(&device_keys);

        let mut session_manager =
            SessionManager::new(owner, device_keys.secret_key().to_secret_bytes());
        session_manager.apply_local_roster(DeviceRoster::new(
            now,
            vec![AuthorizedDevice::new(local_device, now)],
        ));

        let mut core = logged_in_core_with_manager(data_dir.path(), 75, session_manager);
        core.pending_inbound
            .push(PendingInbound::envelope(MessageEnvelope {
                sender: local_device_from_keys(&device_keys_for_fill(76)),
                signer_secret_key: [9; 32],
                created_at: UnixSeconds(501),
                encrypted_header: "header".to_string(),
                ciphertext: "ciphertext".to_string(),
            }));
        core.persist_best_effort();

        let persisted = persisted_state(data_dir.path());
        assert_eq!(persisted.pending_inbound.len(), 1);
        assert_eq!(
            match &persisted.pending_inbound[0] {
                PendingInbound::Envelope { envelope } => envelope.ciphertext.as_str(),
                PendingInbound::Decrypted { .. } => panic!("expected persisted envelope"),
            },
            "ciphertext"
        );

        let mut restored = test_core(data_dir.path());
        start_primary_test_session(&mut restored, 75, true, true)
            .expect("restore session with pending inbound");

        assert_eq!(restored.pending_inbound.len(), 1);
        assert_eq!(
            match &restored.pending_inbound[0] {
                PendingInbound::Envelope { envelope } => envelope.encrypted_header.as_str(),
                PendingInbound::Decrypted { .. } => panic!("expected restored envelope"),
            },
            "header"
        );
    }

    #[test]
    fn decrypted_pending_inbound_sender_is_tracked_for_recovery() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let data_dir = TempDir::new().expect("temp dir");
        let owner_keys = keys_for_fill(75);
        let device_keys = device_keys_for_fill(75);
        let owner = local_owner_from_keys(&owner_keys);
        let now = UnixSeconds(500);
        let local_device = local_device_from_keys(&device_keys);

        let mut session_manager =
            SessionManager::new(owner, device_keys.secret_key().to_secret_bytes());
        session_manager.apply_local_roster(DeviceRoster::new(
            now,
            vec![AuthorizedDevice::new(local_device, now)],
        ));

        let mut core = logged_in_core_with_manager(data_dir.path(), 75, session_manager);
        let sender_owner = local_owner_from_keys(&keys_for_fill(76));
        core.pending_inbound.push(PendingInbound::decrypted(
            sender_owner,
            b"pending".to_vec(),
            now.get(),
        ));

        assert!(core
            .tracked_peer_owner_hexes()
            .contains(&sender_owner.to_string()));
    }

    #[test]
    fn recent_handshake_peer_tracks_claimed_owner_after_roster_verification() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let data_dir = TempDir::new().expect("temp dir");

        let alice_device_keys = device_keys_for_fill(120);
        let alice_claimed_owner_keys = keys_for_fill(121);
        let bob_owner_keys = keys_for_fill(122);
        let bob_device_keys = device_keys_for_fill(122);

        let alice_device_owner = local_owner_from_keys(&alice_device_keys);
        let alice_claimed_owner = local_owner_from_keys(&alice_claimed_owner_keys);
        let bob_owner = local_owner_from_keys(&bob_owner_keys);
        let now = UnixSeconds(1_900_000_400);

        let mut alice_manager = SessionManager::new(
            alice_claimed_owner,
            alice_device_keys.secret_key().to_secret_bytes(),
        );
        let mut bob_manager =
            SessionManager::new(bob_owner, bob_device_keys.secret_key().to_secret_bytes());

        let bob_roster = DeviceRoster::new(
            now,
            vec![AuthorizedDevice::new(
                local_device_from_keys(&bob_device_keys),
                now,
            )],
        );
        bob_manager.apply_local_roster(bob_roster.clone());
        alice_manager.observe_peer_roster(bob_owner, bob_roster);

        let bob_invite = {
            let mut rng = OsRng;
            let mut ctx = ProtocolContext::new(UnixSeconds(now.get() + 1), &mut rng);
            bob_manager
                .ensure_local_invite(&mut ctx)
                .expect("ensure bob invite")
                .clone()
        };
        alice_manager
            .observe_device_invite(bob_owner, bob_invite)
            .expect("observe bob invite");

        let prepared = {
            let mut rng = OsRng;
            let mut ctx = ProtocolContext::new(UnixSeconds(now.get() + 2), &mut rng);
            alice_manager
                .prepare_send(&mut ctx, bob_owner, b"claim migration".to_vec())
                .expect("prepare first-contact send")
        };
        assert_eq!(prepared.invite_responses.len(), 1);

        let mut core = logged_in_core_with_manager(data_dir.path(), 122, bob_manager);
        let processed = {
            let logged_in = core.logged_in.as_mut().expect("logged in");
            let mut rng = OsRng;
            let mut ctx = ProtocolContext::new(UnixSeconds(now.get() + 3), &mut rng);
            logged_in
                .session_manager
                .observe_invite_response(&mut ctx, &prepared.invite_responses[0])
                .expect("observe invite response")
                .expect("processed invite response")
        };
        assert_eq!(processed.owner_pubkey, alice_device_owner);

        core.remember_recent_handshake_peer(
            processed.owner_pubkey.to_string(),
            processed.device_pubkey.to_string(),
            now.get(),
        );
        assert!(core
            .protocol_owner_hexes()
            .contains(&alice_device_owner.to_string()));
        assert!(core
            .protocol_owner_hexes()
            .contains(&alice_claimed_owner.to_string()));

        {
            let logged_in = core.logged_in.as_mut().expect("logged in");
            let alice_roster = DeviceRoster::new(
                UnixSeconds(now.get() + 4),
                vec![AuthorizedDevice::new(
                    local_device_from_keys(&alice_device_keys),
                    UnixSeconds(now.get() + 4),
                )],
            );
            logged_in
                .session_manager
                .observe_peer_roster(alice_claimed_owner, alice_roster);
        }

        let migrated_owner_hexes = core.reconcile_recent_handshake_peers();
        assert!(migrated_owner_hexes
            .iter()
            .any(|(_, owner_hex)| owner_hex == &alice_claimed_owner.to_string()));
        assert!(core
            .protocol_owner_hexes()
            .contains(&alice_claimed_owner.to_string()));
        assert!(!core
            .protocol_owner_hexes()
            .contains(&alice_device_owner.to_string()));
    }

    #[test]
    fn protocol_state_catch_up_fetches_recent_message_events() {
        let plan = ProtocolSubscriptionPlan {
            roster_authors: vec![
                "0193d5c691ea39b12343c37ac26a0455cf4c64bdacfa218e29ee582c552859db".to_string(),
            ],
            invite_authors: vec![],
            invite_response_recipient: Some(
                "a3f5e52d1a528f41f4dcffc8ef7c46cc32299f92472fba3aa8006839becbfad6".to_string(),
            ),
            message_authors: vec![
                "e7d079b75972e1774a5cb0fe36566faea814999bab5330081cfb6d4763314915".to_string(),
            ],
        };

        let now = UnixSeconds(1_900_000_000);
        let filters = build_protocol_state_catch_up_filters(&plan, now);
        let serialized = serde_json::to_value(&filters).expect("serialize filters");
        let filters = serialized.as_array().expect("filter array");

        let message_filter = filters
            .iter()
            .find(|filter| {
                filter["kinds"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .any(|kind| kind.as_u64() == Some(codec::MESSAGE_EVENT_KIND as u64))
            })
            .expect("message catch-up filter");

        assert_eq!(
            message_filter["authors"]
                .as_array()
                .expect("authors array")
                .len(),
            1
        );
        assert_eq!(
            message_filter["since"].as_u64(),
            Some(now.get().saturating_sub(CATCH_UP_LOOKBACK_SECS))
        );
    }

    #[test]
    fn invite_response_delivery_requires_matching_pubkey_tag() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let _env = RelayEnvGuard::local_only();
        let _relay = TestRelay::start();
        let (_alice_dir, alice) = app(71);
        let (_bob_dir, bob) = app(72);
        let (charlie_dir, _charlie) = app(73);

        alice.dispatch(AppAction::CreateChat {
            peer_input: npub_for_fill(72),
        });
        let alice_chat_id = wait_for_state(&alice, "alice chat create", |state| {
            state.current_chat.is_some()
        })
        .current_chat
        .expect("alice chat")
        .chat_id;

        alice.dispatch(AppAction::SendMessage {
            chat_id: alice_chat_id,
            text: "hello bob".to_string(),
        });

        wait_for_state(&bob, "bob receives hello", |state| {
            state
                .chat_list
                .iter()
                .any(|chat| chat.last_message_preview.as_deref() == Some("hello bob"))
        });

        let charlie_persisted = wait_for_persisted_state(
            charlie_dir.path(),
            "charlie stays scoped to local invite responses",
            |persisted| {
                persisted
                    .session_manager
                    .as_ref()
                    .map(|snapshot| snapshot.users.len() == 1)
                    .unwrap_or(false)
            },
        );
        assert_eq!(
            charlie_persisted
                .session_manager
                .expect("charlie session manager")
                .users
                .len(),
            1
        );
    }

    #[test]
    fn pending_send_waits_for_missing_roster_then_delivers() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let _env = RelayEnvGuard::local_only();
        let _relay = TestRelay::start();
        let (alice_dir, alice) = app(81);

        alice.dispatch(AppAction::CreateChat {
            peer_input: npub_for_fill(82),
        });
        let chat_id = wait_for_state(&alice, "alice chat create", |state| {
            state.current_chat.is_some()
        })
        .current_chat
        .expect("alice chat")
        .chat_id;

        alice.dispatch(AppAction::SendMessage {
            chat_id,
            text: "waiting on roster".to_string(),
        });

        let alice_pending = wait_for_persisted_state(
            alice_dir.path(),
            "alice waits for missing roster",
            |persisted| {
                persisted.pending_outbound.len() == 1
                    && persisted.pending_outbound[0].reason == PendingSendReason::MissingRoster
            },
        );
        assert_eq!(alice_pending.pending_outbound.len(), 1);

        let (_bob_dir, bob) = app(82);
        wait_for_state(&bob, "bob receives queued first message", |state| {
            state
                .chat_list
                .iter()
                .any(|chat| chat.last_message_preview.as_deref() == Some("waiting on roster"))
        });
        let alice_cleared = wait_for_persisted_state(
            alice_dir.path(),
            "alice clears pending outbound after roster arrives",
            |persisted| persisted.pending_outbound.is_empty(),
        );
        assert!(alice_cleared.pending_outbound.is_empty());
    }

    #[test]
    fn pending_send_waits_for_missing_device_invite_then_publishes() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let _env = RelayEnvGuard::local_only();
        let _relay = TestRelay::start();

        let bob_owner_keys = keys_for_fill(92);
        let bob_device_keys = device_keys_for_fill(92);
        let bob_owner = local_owner_from_keys(&bob_owner_keys);
        let now = unix_now();
        let bob_roster = DeviceRoster::new(
            now,
            vec![AuthorizedDevice::new(
                local_device_from_keys(&bob_device_keys),
                now,
            )],
        );
        let roster_event = codec::roster_unsigned_event(bob_owner, &bob_roster)
            .expect("bob roster event")
            .sign_with_keys(&bob_owner_keys)
            .expect("sign bob roster");
        publish_local_relay_event(&bob_owner_keys, roster_event);

        let (_alice_dir, alice) = app(91);
        alice.dispatch(AppAction::CreateChat {
            peer_input: npub_for_fill(92),
        });
        let chat_id = wait_for_state(&alice, "alice chat create", |state| {
            state.current_chat.is_some()
        })
        .current_chat
        .expect("alice chat")
        .chat_id;

        alice.dispatch(AppAction::SendMessage {
            chat_id: chat_id.clone(),
            text: "waiting on invite".to_string(),
        });

        wait_for_state(&alice, "alice send pending on missing invite", |state| {
            state
                .current_chat
                .as_ref()
                .and_then(|chat| chat.messages.last())
                .map(|message| matches!(message.delivery, DeliveryState::Pending))
                .unwrap_or(false)
        });

        let mut session_manager =
            SessionManager::new(bob_owner, bob_device_keys.secret_key().to_secret_bytes());
        session_manager.apply_local_roster(bob_roster);
        let mut rng = OsRng;
        let mut ctx = ProtocolContext::new(unix_now(), &mut rng);
        let invite = session_manager
            .ensure_local_invite(&mut ctx)
            .expect("ensure bob invite")
            .clone();
        let invite_event = codec::invite_unsigned_event(&invite)
            .expect("bob invite event")
            .sign_with_keys(&bob_device_keys)
            .expect("sign bob invite");
        publish_local_relay_event(&bob_device_keys, invite_event);

        let alice_after_invite =
            wait_for_state(&alice, "alice publishes after invite appears", |state| {
                state
                    .current_chat
                    .as_ref()
                    .and_then(|chat| chat.messages.last())
                    .map(|message| matches!(message.delivery, DeliveryState::Sent))
                    .unwrap_or(false)
            });
        assert_eq!(
            alice_after_invite
                .current_chat
                .expect("alice current chat")
                .messages
                .last()
                .expect("alice last message")
                .body,
            "waiting on invite"
        );
    }

    #[test]
    fn local_relay_round_trip_and_reverse_initiation_work() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let _env = RelayEnvGuard::local_only();
        let _relay = TestRelay::start();
        let (_alice_dir, alice) = app(51);
        let (_bob_dir, bob) = app(52);

        alice.dispatch(AppAction::CreateChat {
            peer_input: npub_for_fill(52),
        });
        let alice_state = wait_for_state(&alice, "alice chat create", |state| {
            state.current_chat.is_some() && state.chat_list.len() == 1
        });
        let alice_chat_id = alice_state
            .current_chat
            .as_ref()
            .expect("alice chat")
            .chat_id
            .clone();

        alice.dispatch(AppAction::SendMessage {
            chat_id: alice_chat_id.clone(),
            text: "hello bob".to_string(),
        });
        let _alice_after_send = wait_for_state(&alice, "alice outbound queued", |state| {
            state
                .current_chat
                .as_ref()
                .map(|chat| !chat.messages.is_empty())
                .unwrap_or(false)
        });

        let bob_state = wait_for_state(&bob, "bob receives first message", |state| {
            state.chat_list.iter().any(|chat| {
                chat.last_message_preview.as_deref() == Some("hello bob") && chat.unread_count == 1
            })
        });
        let bob_chat_id = bob_state.chat_list[0].chat_id.clone();
        bob.dispatch(AppAction::OpenChat {
            chat_id: bob_chat_id.clone(),
        });
        let bob_open = wait_for_state(&bob, "bob opens chat", |state| {
            state
                .current_chat
                .as_ref()
                .map(|chat| chat.messages.len() == 1)
                .unwrap_or(false)
                && state
                    .chat_list
                    .first()
                    .map(|chat| chat.unread_count == 0)
                    .unwrap_or(false)
        });
        assert_eq!(
            bob_open
                .current_chat
                .as_ref()
                .expect("bob current chat")
                .messages[0]
                .body,
            "hello bob"
        );

        bob.dispatch(AppAction::SendMessage {
            chat_id: bob_chat_id.clone(),
            text: "hi alice".to_string(),
        });
        let _bob_after_reply = wait_for_state(&bob, "bob sends reply", |state| {
            state
                .current_chat
                .as_ref()
                .and_then(|chat| chat.messages.last())
                .map(|message| {
                    message.is_outgoing
                        && message.body == "hi alice"
                        && matches!(message.delivery, DeliveryState::Sent)
                })
                .unwrap_or(false)
        });
        let alice_with_reply = wait_for_state(&alice, "alice receives reply", |state| {
            state
                .current_chat
                .as_ref()
                .map(|chat| chat.messages.len() >= 2)
                .unwrap_or(false)
        });
        assert_eq!(
            alice_with_reply
                .current_chat
                .as_ref()
                .expect("alice current chat")
                .messages
                .last()
                .expect("alice last message")
                .body,
            "hi alice"
        );
        assert!(alice_with_reply.toast.is_none());

        let (_charlie_dir, charlie) = app(53);
        let (_dana_dir, dana) = app(54);
        dana.dispatch(AppAction::CreateChat {
            peer_input: npub_for_fill(53),
        });
        let dana_chat_id = wait_for_state(&dana, "dana chat create", |state| {
            state.current_chat.is_some()
        })
        .current_chat
        .expect("dana chat")
        .chat_id;
        dana.dispatch(AppAction::SendMessage {
            chat_id: dana_chat_id,
            text: "reverse first".to_string(),
        });
        let charlie_state = wait_for_state(&charlie, "charlie receives reverse message", |state| {
            state
                .chat_list
                .iter()
                .any(|chat| chat.last_message_preview.as_deref() == Some("reverse first"))
        });
        assert_eq!(
            charlie_state.chat_list[0].last_message_preview.as_deref(),
            Some("reverse first")
        );
    }

    #[test]
    fn primary_bootstrap_uses_separate_device_identity_and_one_device_roster() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let _env = RelayEnvGuard::local_only();
        let data_dir = TempDir::new().expect("temp dir");
        let mut core = test_core(data_dir.path());

        start_primary_test_session(&mut core, 140, false, false).expect("start primary session");

        let account = core.state.account.as_ref().expect("account");
        assert_ne!(account.public_key_hex, account.device_public_key_hex);
        assert!(account.has_owner_signing_authority);
        assert!(matches!(
            account.authorization_state,
            DeviceAuthorizationState::Authorized
        ));

        let snapshot = core
            .logged_in
            .as_ref()
            .expect("logged in")
            .session_manager
            .snapshot();
        assert_eq!(
            snapshot.local_owner_pubkey.to_string(),
            account.public_key_hex
        );
        assert_eq!(
            snapshot.local_device_pubkey.to_string(),
            account.device_public_key_hex
        );

        let local_roster = snapshot
            .users
            .into_iter()
            .find(|user| user.owner_pubkey == snapshot.local_owner_pubkey)
            .and_then(|user| user.roster)
            .expect("local roster");
        assert_eq!(local_roster.devices().len(), 1);
        assert_eq!(
            local_roster.devices()[0].device_pubkey.to_string(),
            account.device_public_key_hex
        );
    }

    #[test]
    fn linked_device_starts_awaiting_approval_with_device_invite() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let data_dir = TempDir::new().expect("temp dir");
        let mut core = test_core(data_dir.path());

        core.start_linked_device(&npub_for_fill(141));

        let account = core.state.account.as_ref().expect("account");
        assert!(!account.has_owner_signing_authority);
        assert!(matches!(
            account.authorization_state,
            DeviceAuthorizationState::AwaitingApproval
        ));
        assert!(matches!(
            core.state.router.default_screen,
            Screen::AwaitingDeviceApproval
        ));
        assert!(core
            .logged_in
            .as_ref()
            .expect("logged in")
            .session_manager
            .snapshot()
            .local_invite
            .is_some());
        let roster = core.state.device_roster.as_ref().expect("device roster");
        assert_eq!(roster.devices.len(), 1);
        assert!(!roster.devices[0].is_authorized);
        assert!(roster.devices[0].is_current_device);
    }

    #[test]
    fn linked_device_becomes_authorized_when_owner_roster_arrives() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let data_dir = TempDir::new().expect("temp dir");
        let owner_keys = keys_for_fill(142);
        let owner = local_owner_from_keys(&owner_keys);
        let mut core = test_core(data_dir.path());

        core.start_linked_device(&owner_keys.public_key().to_bech32().expect("owner npub"));
        let device_pubkey = parse_device_input(
            &core
                .state
                .account
                .as_ref()
                .expect("account")
                .device_public_key_hex,
        )
        .expect("device pubkey");
        let now = unix_now();
        let roster = DeviceRoster::new(now, vec![AuthorizedDevice::new(device_pubkey, now)]);
        let roster_event = codec::roster_unsigned_event(owner, &roster)
            .expect("roster event")
            .sign_with_keys(&owner_keys)
            .expect("sign roster");

        core.handle_relay_event(roster_event);

        assert!(matches!(
            core.state
                .account
                .as_ref()
                .expect("account")
                .authorization_state,
            DeviceAuthorizationState::Authorized
        ));
        assert!(matches!(core.state.router.default_screen, Screen::ChatList));
        assert!(core
            .state
            .device_roster
            .as_ref()
            .expect("roster")
            .devices
            .iter()
            .any(
                |device| device.device_pubkey_hex == device_pubkey.to_string()
                    && device.is_authorized
            ));
    }

    #[test]
    fn linked_device_transitions_to_revoked_when_owner_roster_excludes_it() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let data_dir = TempDir::new().expect("temp dir");
        let owner_keys = keys_for_fill(143);
        let owner = local_owner_from_keys(&owner_keys);
        let mut core = test_core(data_dir.path());

        core.start_linked_device(&owner_keys.public_key().to_bech32().expect("owner npub"));
        let device_pubkey = parse_device_input(
            &core
                .state
                .account
                .as_ref()
                .expect("account")
                .device_public_key_hex,
        )
        .expect("device pubkey");

        let authorized_now = unix_now();
        let authorized_roster = DeviceRoster::new(
            authorized_now,
            vec![AuthorizedDevice::new(device_pubkey, authorized_now)],
        );
        let authorized_event = codec::roster_unsigned_event(owner, &authorized_roster)
            .expect("authorized roster event")
            .sign_with_keys(&owner_keys)
            .expect("sign authorized roster");
        core.handle_relay_event(authorized_event);

        let revoked_roster = DeviceRoster::new(UnixSeconds(authorized_now.get() + 1), Vec::new());
        let revoked_event = codec::roster_unsigned_event(owner, &revoked_roster)
            .expect("revoked roster event")
            .sign_with_keys(&owner_keys)
            .expect("sign revoked roster");
        core.handle_relay_event(revoked_event);

        assert!(matches!(
            core.state
                .account
                .as_ref()
                .expect("account")
                .authorization_state,
            DeviceAuthorizationState::Revoked
        ));
        assert!(matches!(
            core.state.router.default_screen,
            Screen::DeviceRevoked
        ));
    }

    #[test]
    fn add_and_remove_authorized_device_updates_local_roster_snapshot() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let data_dir = TempDir::new().expect("temp dir");
        let mut core = test_core(data_dir.path());
        start_primary_test_session(&mut core, 144, false, false).expect("start primary session");

        let added_device_npub = device_keys_for_fill(145)
            .public_key()
            .to_bech32()
            .expect("device npub");
        core.add_authorized_device(&added_device_npub);
        assert!(core
            .state
            .device_roster
            .as_ref()
            .expect("device roster")
            .devices
            .iter()
            .any(|device| device.device_npub == added_device_npub && device.is_authorized));

        let added_device_hex = normalize_peer_input_for_display(&added_device_npub);
        core.remove_authorized_device(&added_device_hex);
        assert!(!core
            .state
            .device_roster
            .as_ref()
            .expect("device roster")
            .devices
            .iter()
            .any(|device| device.device_pubkey_hex == added_device_hex));
    }

    #[test]
    fn primary_identity_publishes_owner_signed_roster_and_device_signed_invite() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let _env = RelayEnvGuard::local_only();
        let _relay = TestRelay::start();
        let data_dir = TempDir::new().expect("temp dir");
        let mut core = test_core(data_dir.path());
        start_primary_test_session(&mut core, 146, false, false).expect("start primary session");

        let account = core.state.account.as_ref().expect("account");
        let owner_key = PublicKey::parse(&account.public_key_hex).expect("owner key");
        let device_key = PublicKey::parse(&account.device_public_key_hex).expect("device key");

        let deadline = Instant::now() + StdDuration::from_secs(5);
        let mut events = Vec::new();
        while Instant::now() < deadline {
            events = fetch_local_relay_events(vec![
                Filter::new().kind(Kind::from(codec::ROSTER_EVENT_KIND as u16))
            ]);
            let has_roster = events
                .iter()
                .any(|event| codec::parse_roster_event(event).is_ok() && event.pubkey == owner_key);
            let has_invite = events.iter().any(|event| {
                codec::parse_invite_event(event).is_ok() && event.pubkey == device_key
            });
            if has_roster && has_invite {
                break;
            }
            thread::sleep(StdDuration::from_millis(100));
        }
        assert!(events.iter().any(|event| {
            codec::parse_roster_event(event).is_ok() && event.pubkey == owner_key
        }));
        assert!(events.iter().any(|event| {
            codec::parse_invite_event(event).is_ok() && event.pubkey == device_key
        }));
    }

    #[test]
    fn local_relay_duplicate_replay_is_ignored_and_state_restores() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let _env = RelayEnvGuard::local_only();
        let relay = TestRelay::start();
        let alice_dir = TempDir::new().expect("alice dir");
        let bob_dir = TempDir::new().expect("bob dir");
        let alice = app_with_dir(alice_dir.path(), 61);
        let bob = app_with_dir(bob_dir.path(), 62);

        alice.dispatch(AppAction::CreateChat {
            peer_input: npub_for_fill(62),
        });
        let chat_id = wait_for_state(&alice, "alice chat create", |state| {
            state.current_chat.is_some()
        })
        .current_chat
        .expect("alice chat")
        .chat_id;
        alice.dispatch(AppAction::SendMessage {
            chat_id: chat_id.clone(),
            text: "dedupe".to_string(),
        });

        let bob_chat_id = wait_for_state(&bob, "bob first delivery", |state| {
            state
                .chat_list
                .iter()
                .any(|chat| chat.last_message_preview.as_deref() == Some("dedupe"))
        })
        .chat_list[0]
            .chat_id
            .clone();
        bob.dispatch(AppAction::OpenChat {
            chat_id: bob_chat_id.clone(),
        });
        wait_for_state(&bob, "bob chat open", |state| {
            state
                .current_chat
                .as_ref()
                .map(|chat| chat.messages.len() == 1)
                .unwrap_or(false)
        });

        relay.replay_stored();
        thread::sleep(StdDuration::from_secs(1));
        let bob_after_replay = bob.state();
        assert_eq!(
            bob_after_replay
                .current_chat
                .as_ref()
                .expect("bob current chat")
                .messages
                .len(),
            1
        );
        assert!(bob_after_replay.toast.is_none());

        drop(bob);
        let restored_bob = app_with_dir(bob_dir.path(), 62);
        let restored_state = wait_for_state(&restored_bob, "restored bob thread", |state| {
            !state.chat_list.is_empty()
        });
        assert_eq!(
            restored_state.chat_list[0].last_message_preview.as_deref(),
            Some("dedupe")
        );
        assert!(restored_state.toast.is_none());
    }

    #[test]
    fn linked_device_fanout_and_revocation_work_on_local_relay() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let _env = RelayEnvGuard::local_only();
        let _relay = TestRelay::start();
        let primary_dir = TempDir::new().expect("primary dir");
        let linked_dir = TempDir::new().expect("linked dir");
        let peer_dir = TempDir::new().expect("peer dir");

        let primary = app_with_dir(primary_dir.path(), 71);
        let primary_account =
            wait_for_state(&primary, "primary account", |state| state.account.is_some())
                .account
                .expect("primary account");

        let linked = FfiApp::new(
            linked_dir.path().to_string_lossy().into_owned(),
            String::new(),
            "test".to_string(),
        );
        linked.dispatch(AppAction::StartLinkedDevice {
            owner_input: primary_account.npub.clone(),
        });

        let linked_account = wait_for_state(&linked, "linked awaiting approval", |state| {
            state
                .account
                .as_ref()
                .map(|account| {
                    matches!(
                        account.authorization_state,
                        DeviceAuthorizationState::AwaitingApproval
                    )
                })
                .unwrap_or(false)
        })
        .account
        .expect("linked account");

        primary.dispatch(AppAction::AddAuthorizedDevice {
            device_input: linked_account.device_npub.clone(),
        });

        wait_for_state(&linked, "linked authorized", |state| {
            state
                .account
                .as_ref()
                .map(|account| {
                    matches!(
                        account.authorization_state,
                        DeviceAuthorizationState::Authorized
                    )
                })
                .unwrap_or(false)
        });

        let peer = app_with_dir(peer_dir.path(), 72);
        let peer_account = wait_for_state(&peer, "peer account", |state| state.account.is_some())
            .account
            .expect("peer account");

        let peer_chat_id = peer_account.public_key_hex.clone();
        let primary_chat_id = primary_account.public_key_hex.clone();

        primary.dispatch(AppAction::CreateChat {
            peer_input: peer_account.npub.clone(),
        });
        wait_for_state(&primary, "primary chat with peer", |state| {
            state
                .current_chat
                .as_ref()
                .map(|chat| chat.chat_id == peer_chat_id)
                .unwrap_or(false)
        });

        primary.dispatch(AppAction::SendMessage {
            chat_id: peer_chat_id.clone(),
            text: "m1".to_string(),
        });

        wait_for_state(&peer, "peer received m1", |state| {
            state.chat_list.iter().any(|thread| {
                thread.chat_id == primary_chat_id
                    && thread.last_message_preview.as_deref() == Some("m1")
            })
        });
        let linked_after_m1 = wait_for_state(&linked, "linked received synced m1", |state| {
            state.chat_list.iter().any(|thread| {
                thread.chat_id == peer_chat_id
                    && thread.last_message_preview.as_deref() == Some("m1")
            })
        });
        assert!(linked_after_m1.chat_list.iter().any(|thread| {
            thread.chat_id == peer_chat_id && thread.last_message_preview.as_deref() == Some("m1")
        }));

        peer.dispatch(AppAction::OpenChat {
            chat_id: primary_chat_id.clone(),
        });
        wait_for_state(&peer, "peer chat open", |state| {
            state
                .current_chat
                .as_ref()
                .map(|chat| chat.chat_id == primary_chat_id)
                .unwrap_or(false)
        });
        peer.dispatch(AppAction::SendMessage {
            chat_id: primary_chat_id.clone(),
            text: "m2".to_string(),
        });

        wait_for_state(&primary, "primary received m2", |state| {
            state.chat_list.iter().any(|thread| {
                thread.chat_id == peer_chat_id
                    && thread.last_message_preview.as_deref() == Some("m2")
            })
        });
        let linked_after_m2 = wait_for_state(&linked, "linked received m2", |state| {
            state.chat_list.iter().any(|thread| {
                thread.chat_id == peer_chat_id
                    && thread.last_message_preview.as_deref() == Some("m2")
            })
        });
        linked.dispatch(AppAction::OpenChat {
            chat_id: peer_chat_id.clone(),
        });
        let linked_chat = wait_for_state(&linked, "linked chat open", |state| {
            state
                .current_chat
                .as_ref()
                .map(|chat| {
                    chat.chat_id == peer_chat_id
                        && chat
                            .messages
                            .iter()
                            .any(|message| message.body == "m1" && message.is_outgoing)
                        && chat
                            .messages
                            .iter()
                            .any(|message| message.body == "m2" && !message.is_outgoing)
                })
                .unwrap_or(false)
        })
        .current_chat
        .expect("linked chat");
        assert_eq!(linked_chat.chat_id, peer_chat_id);
        assert!(linked_after_m2.chat_list.iter().any(|thread| {
            thread.chat_id == peer_chat_id && thread.last_message_preview.as_deref() == Some("m2")
        }));

        linked.dispatch(AppAction::SendMessage {
            chat_id: peer_chat_id.clone(),
            text: "m3".to_string(),
        });

        wait_for_state(&peer, "peer received m3", |state| {
            state
                .current_chat
                .as_ref()
                .map(|chat| {
                    chat.chat_id == primary_chat_id
                        && chat
                            .messages
                            .iter()
                            .any(|message| message.body == "m3" && !message.is_outgoing)
                })
                .unwrap_or(false)
        });
        primary.dispatch(AppAction::OpenChat {
            chat_id: peer_chat_id.clone(),
        });
        wait_for_state(&primary, "primary received synced m3", |state| {
            state
                .current_chat
                .as_ref()
                .map(|chat| {
                    chat.chat_id == peer_chat_id
                        && chat
                            .messages
                            .iter()
                            .any(|message| message.body == "m3" && message.is_outgoing)
                })
                .unwrap_or(false)
        });

        primary.dispatch(AppAction::RemoveAuthorizedDevice {
            device_pubkey_hex: linked_account.device_public_key_hex.clone(),
        });
        wait_for_state(&linked, "linked revoked", |state| {
            state
                .account
                .as_ref()
                .map(|account| {
                    matches!(
                        account.authorization_state,
                        DeviceAuthorizationState::Revoked
                    )
                })
                .unwrap_or(false)
        });

        let before_count = linked
            .state()
            .current_chat
            .as_ref()
            .map(|chat| chat.messages.len())
            .unwrap_or(0);
        linked.dispatch(AppAction::SendMessage {
            chat_id: peer_chat_id.clone(),
            text: "blocked".to_string(),
        });
        let blocked_state = wait_for_state(&linked, "linked send blocked", |state| {
            state.toast.as_deref()
                == Some("This device has been removed from the roster. Log out to continue.")
        });
        assert_eq!(
            blocked_state
                .current_chat
                .as_ref()
                .map(|chat| chat.messages.len())
                .unwrap_or(0),
            before_count
        );
    }

    #[test]
    fn linked_device_can_be_approved_after_single_owner_qr_scan() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let _env = RelayEnvGuard::local_only();
        let _relay = TestRelay::start();
        let primary_dir = TempDir::new().expect("primary dir");
        let linked_dir = TempDir::new().expect("linked dir");

        let primary = app_with_dir(primary_dir.path(), 214);
        let primary_account =
            wait_for_state(&primary, "primary account", |state| state.account.is_some())
                .account
                .expect("primary account");

        let linked = FfiApp::new(
            linked_dir.path().to_string_lossy().into_owned(),
            String::new(),
            "test".to_string(),
        );
        linked.dispatch(AppAction::StartLinkedDevice {
            owner_input: primary_account.npub.clone(),
        });

        let linked_account = wait_for_state(&linked, "linked awaiting approval", |state| {
            state
                .account
                .as_ref()
                .map(|account| {
                    matches!(
                        account.authorization_state,
                        DeviceAuthorizationState::AwaitingApproval
                    )
                })
                .unwrap_or(false)
        })
        .account
        .expect("linked account");
        let linked_device_pubkey =
            PublicKey::parse(&linked_account.device_public_key_hex).expect("linked device pubkey");

        let invite_deadline = Instant::now() + StdDuration::from_secs(5);
        loop {
            let invites = fetch_local_relay_events(vec![
                Filter::new().kind(Kind::from(codec::INVITE_EVENT_KIND as u16))
            ]);
            let published = invites.iter().any(|event| {
                codec::parse_invite_event(event).is_ok() && event.pubkey == linked_device_pubkey
            });
            if published {
                break;
            }
            assert!(
                Instant::now() < invite_deadline,
                "timed out waiting for linked device invite publish"
            );
            thread::sleep(StdDuration::from_millis(50));
        }

        primary.dispatch(AppAction::PushScreen {
            screen: Screen::DeviceRoster,
        });

        wait_for_state(
            &primary,
            "primary discovered pending linked device",
            |state| {
                state
                    .device_roster
                    .as_ref()
                    .map(|roster| {
                        roster.devices.iter().any(|device| {
                            device.device_pubkey_hex == linked_account.device_public_key_hex
                                && !device.is_authorized
                        })
                    })
                    .unwrap_or(false)
            },
        );

        primary.dispatch(AppAction::AddAuthorizedDevice {
            device_input: linked_account.device_public_key_hex.clone(),
        });

        wait_for_state(&primary, "primary authorized linked device", |state| {
            state
                .device_roster
                .as_ref()
                .map(|roster| {
                    roster.devices.iter().any(|device| {
                        device.device_pubkey_hex == linked_account.device_public_key_hex
                            && device.is_authorized
                    })
                })
                .unwrap_or(false)
        });
        wait_for_state(&linked, "linked authorized", |state| {
            state
                .account
                .as_ref()
                .map(|account| {
                    matches!(
                        account.authorization_state,
                        DeviceAuthorizationState::Authorized
                    )
                })
                .unwrap_or(false)
        });
    }

    #[test]
    fn linked_device_appears_live_when_primary_device_roster_is_already_open() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let _env = RelayEnvGuard::local_only();
        let _relay = TestRelay::start();
        let primary_dir = TempDir::new().expect("primary dir");
        let linked_dir = TempDir::new().expect("linked dir");

        let primary = app_with_dir(primary_dir.path(), 216);
        let primary_account =
            wait_for_state(&primary, "primary account", |state| state.account.is_some())
                .account
                .expect("primary account");

        primary.dispatch(AppAction::PushScreen {
            screen: Screen::DeviceRoster,
        });
        wait_for_state(&primary, "primary device roster open", |state| {
            state.device_roster.is_some()
        });

        let linked = FfiApp::new(
            linked_dir.path().to_string_lossy().into_owned(),
            String::new(),
            "test".to_string(),
        );
        linked.dispatch(AppAction::StartLinkedDevice {
            owner_input: primary_account.npub.clone(),
        });

        let linked_account = wait_for_state(&linked, "linked awaiting approval", |state| {
            state
                .account
                .as_ref()
                .map(|account| {
                    matches!(
                        account.authorization_state,
                        DeviceAuthorizationState::AwaitingApproval
                    )
                })
                .unwrap_or(false)
        })
        .account
        .expect("linked account");

        wait_for_state(
            &primary,
            "primary observed pending linked device via live relay invite",
            |state| {
                state
                    .device_roster
                    .as_ref()
                    .map(|roster| {
                        roster.devices.iter().any(|device| {
                            device.device_pubkey_hex == linked_account.device_public_key_hex
                                && !device.is_authorized
                        })
                    })
                    .unwrap_or(false)
            },
        );
    }

    #[test]
    fn primary_discovers_pending_linked_device_without_opening_device_roster() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let _env = RelayEnvGuard::local_only();
        let _relay = TestRelay::start();
        let primary_dir = TempDir::new().expect("primary dir");
        let linked_dir = TempDir::new().expect("linked dir");

        let primary = app_with_dir(primary_dir.path(), 217);
        let primary_account =
            wait_for_state(&primary, "primary account", |state| state.account.is_some())
                .account
                .expect("primary account");

        let linked = FfiApp::new(
            linked_dir.path().to_string_lossy().into_owned(),
            String::new(),
            "test".to_string(),
        );
        linked.dispatch(AppAction::StartLinkedDevice {
            owner_input: primary_account.npub.clone(),
        });

        let linked_account = wait_for_state(&linked, "linked awaiting approval", |state| {
            state
                .account
                .as_ref()
                .map(|account| {
                    matches!(
                        account.authorization_state,
                        DeviceAuthorizationState::AwaitingApproval
                    )
                })
                .unwrap_or(false)
        })
        .account
        .expect("linked account");

        wait_for_state_timeout(
            &primary,
            "primary discovered pending linked device without opening device roster",
            15,
            |state| {
                state
                    .device_roster
                    .as_ref()
                    .map(|roster| {
                        roster.devices.iter().any(|device| {
                            device.device_pubkey_hex == linked_account.device_public_key_hex
                                && !device.is_authorized
                        })
                    })
                    .unwrap_or(false)
            },
        );
    }

    struct ScenarioClient {
        owner_fill: Option<u8>,
        data_dir: TempDir,
        app: Arc<FfiApp>,
    }

    #[allow(dead_code)]
    enum ScenarioStep {
        SendDirect {
            sender: String,
            recipient: String,
            text: String,
        },
        SendGroup {
            sender: String,
            chat_id: String,
            text: String,
        },
    }

    struct ScenarioRunner<'a> {
        _guard: std::sync::MutexGuard<'a, ()>,
        _env: RelayEnvGuard,
        relay: TestRelay,
        clients: BTreeMap<String, ScenarioClient>,
    }

    impl<'a> ScenarioRunner<'a> {
        fn local() -> Self {
            Self {
                _guard: relay_test_lock()
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner()),
                _env: RelayEnvGuard::local_only(),
                relay: TestRelay::start(),
                clients: BTreeMap::new(),
            }
        }

        fn add_owner(&mut self, label: &str, fill: u8) {
            let data_dir = TempDir::new().expect("temp dir");
            let app = app_with_dir(data_dir.path(), fill);
            self.clients.insert(
                label.to_string(),
                ScenarioClient {
                    owner_fill: Some(fill),
                    data_dir,
                    app,
                },
            );
        }

        fn add_linked(&mut self, label: &str, owner_label: &str) {
            let data_dir = TempDir::new().expect("temp dir");
            let app = FfiApp::new(
                data_dir.path().to_string_lossy().into_owned(),
                String::new(),
                "test".to_string(),
            );
            app.dispatch(AppAction::StartLinkedDevice {
                owner_input: self.owner_npub(owner_label),
            });
            let linked_account = wait_for_state(&app, "linked awaiting approval", |state| {
                state
                    .account
                    .as_ref()
                    .map(|account| {
                        matches!(
                            account.authorization_state,
                            DeviceAuthorizationState::AwaitingApproval
                        )
                    })
                    .unwrap_or(false)
            })
            .account
            .expect("linked account");

            self.app(owner_label)
                .dispatch(AppAction::AddAuthorizedDevice {
                    device_input: linked_account.device_npub.clone(),
                });
            wait_for_state(&app, "linked authorized", |state| {
                state
                    .account
                    .as_ref()
                    .map(|account| {
                        matches!(
                            account.authorization_state,
                            DeviceAuthorizationState::Authorized
                        )
                    })
                    .unwrap_or(false)
            });

            self.clients.insert(
                label.to_string(),
                ScenarioClient {
                    owner_fill: None,
                    data_dir,
                    app,
                },
            );
        }

        fn app(&self, label: &str) -> &Arc<FfiApp> {
            &self
                .clients
                .get(label)
                .unwrap_or_else(|| panic!("missing scenario client `{label}`"))
                .app
        }

        fn data_dir(&self, label: &str) -> &Path {
            self.clients
                .get(label)
                .unwrap_or_else(|| panic!("missing scenario client `{label}`"))
                .data_dir
                .path()
        }

        fn account(&self, label: &str) -> AccountSnapshot {
            wait_for_state(self.app(label), "scenario account", |state| {
                state.account.is_some()
            })
            .account
            .expect("account")
        }

        fn owner_npub(&self, label: &str) -> String {
            self.account(label).npub
        }

        fn owner_hex(&self, label: &str) -> String {
            self.account(label).public_key_hex
        }

        fn open_chat(&self, label: &str, chat_id: &str) {
            self.app(label).dispatch(AppAction::OpenChat {
                chat_id: chat_id.to_string(),
            });
            wait_for_state(self.app(label), "open chat", |state| {
                state
                    .current_chat
                    .as_ref()
                    .map(|chat| chat.chat_id == chat_id)
                    .unwrap_or(false)
            });
        }

        fn create_group(&self, sender: &str, name: &str, members: &[&str]) -> String {
            self.create_group_by_inputs(
                sender,
                name,
                members
                    .iter()
                    .map(|member| self.owner_npub(member))
                    .collect(),
            )
        }

        fn create_group_by_inputs(
            &self,
            sender: &str,
            name: &str,
            member_inputs: Vec<String>,
        ) -> String {
            self.app(sender).dispatch(AppAction::CreateGroup {
                name: name.to_string(),
                member_inputs,
            });
            wait_for_state_timeout(self.app(sender), "group create", 30, |state| {
                state
                    .current_chat
                    .as_ref()
                    .map(|chat| matches!(chat.kind, ChatKind::Group))
                    .unwrap_or(false)
            })
            .current_chat
            .expect("group chat")
            .chat_id
        }

        #[allow(dead_code)]
        fn add_group_members(&self, sender: &str, group_id: &str, members: &[&str]) {
            self.app(sender).dispatch(AppAction::AddGroupMembers {
                group_id: group_id.to_string(),
                member_inputs: members
                    .iter()
                    .map(|member| self.owner_npub(member))
                    .collect(),
            });
        }

        fn add_group_members_by_inputs(
            &self,
            sender: &str,
            group_id: &str,
            member_inputs: Vec<String>,
        ) {
            self.app(sender).dispatch(AppAction::AddGroupMembers {
                group_id: group_id.to_string(),
                member_inputs,
            });
        }

        fn remove_group_member(&self, sender: &str, group_id: &str, member: &str) {
            self.app(sender).dispatch(AppAction::RemoveGroupMember {
                group_id: group_id.to_string(),
                owner_pubkey_hex: self.owner_hex(member),
            });
        }

        fn run_step(&self, step: ScenarioStep) {
            match step {
                ScenarioStep::SendDirect {
                    sender,
                    recipient,
                    text,
                } => {
                    self.app(&sender).dispatch(AppAction::SendMessage {
                        chat_id: self.owner_hex(&recipient),
                        text,
                    });
                }
                ScenarioStep::SendGroup {
                    sender,
                    chat_id,
                    text,
                } => {
                    self.app(&sender)
                        .dispatch(AppAction::SendMessage { chat_id, text });
                }
            }
        }

        fn wait_for_group(&self, label: &str, chat_id: &str, member_count: u64) {
            wait_for_state_timeout(self.app(label), "group thread", 45, |state| {
                state.chat_list.iter().any(|thread| {
                    thread.chat_id == chat_id
                        && matches!(thread.kind, ChatKind::Group)
                        && thread.member_count == member_count
                })
            });
            self.open_chat(label, chat_id);
        }

        fn wait_for_message(&self, label: &str, chat_id: &str, text: &str, is_outgoing: bool) {
            self.open_chat(label, chat_id);
            wait_for_state_timeout(self.app(label), "group message", 45, |state| {
                state
                    .current_chat
                    .as_ref()
                    .filter(|chat| chat.chat_id == chat_id)
                    .map(|chat| {
                        chat.messages.iter().any(|message| {
                            message.body == text && message.is_outgoing == is_outgoing
                        })
                    })
                    .unwrap_or(false)
            });
        }

        fn wait_for_message_count(&self, label: &str, chat_id: &str, expected_count: usize) {
            self.open_chat(label, chat_id);
            wait_for_state_timeout(self.app(label), "message count", 60, |state| {
                state
                    .current_chat
                    .as_ref()
                    .filter(|chat| chat.chat_id == chat_id)
                    .map(|chat| chat.messages.len() == expected_count)
                    .unwrap_or(false)
            });
        }

        fn restart_owner(&mut self, label: &str) {
            let mut client = self
                .clients
                .remove(label)
                .unwrap_or_else(|| panic!("missing scenario client `{label}`"));
            let owner_fill = client
                .owner_fill
                .expect("scenario restart only supports deterministic owner clients");
            client.app.shutdown_blocking();
            drop(client.app);
            client.app = app_with_dir(client.data_dir.path(), owner_fill);
            self.clients.insert(label.to_string(), client);
        }

        fn replay_stored(&self) {
            self.relay.replay_stored();
            thread::sleep(StdDuration::from_secs(1));
        }
    }

    #[test]
    fn support_bundle_export_is_redacted_and_contains_build_metadata() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let (_data_dir, app) = app(211);

        let bundle = app.export_support_bundle_json();
        let parsed: Value = serde_json::from_str(&bundle).expect("support bundle json");

        assert_eq!(parsed["build"]["app_version"].as_str(), Some(APP_VERSION));
        assert_eq!(parsed["build"]["relay_set_id"].as_str(), Some(RELAY_SET_ID));
        assert!(parsed["relay_urls"].is_array());
        assert!(parsed["known_users"].is_array());
        assert!(parsed["pending_outbound"].is_array());
        assert!(!bundle.contains("secret_key"));
        assert!(!bundle.contains("\"body\""));
    }

    #[test]
    fn create_account_with_name_publishes_metadata_event() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let _env = RelayEnvGuard::local_only();
        let _relay = TestRelay::start();
        let data_dir = TempDir::new().expect("temp dir");
        let mut core = test_core(data_dir.path());

        core.create_account("Alice");

        let owner_hex = core
            .logged_in
            .as_ref()
            .expect("logged in")
            .owner_pubkey
            .to_string();
        let owner_pubkey = PublicKey::parse(&owner_hex).expect("owner pubkey");

        let deadline = Instant::now() + StdDuration::from_secs(5);
        let metadata_event = loop {
            let events = fetch_local_relay_events(vec![Filter::new()
                .kind(Kind::Metadata)
                .authors(vec![owner_pubkey])]);
            if let Some(event) = events.into_iter().next() {
                break event;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for metadata event"
            );
            thread::sleep(StdDuration::from_millis(50));
        };

        let metadata: NostrProfileMetadata =
            serde_json::from_str(&metadata_event.content).expect("metadata json");
        assert_eq!(metadata.name.as_deref(), Some("Alice"));
        assert_eq!(metadata.display_name.as_deref(), Some("Alice"));
        assert_eq!(
            core.state.account.as_ref().expect("account").display_name,
            "Alice"
        );
    }

    #[test]
    fn metadata_event_updates_direct_chat_display_name() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let data_dir = TempDir::new().expect("temp dir");
        let mut core = test_core(data_dir.path());
        start_primary_test_session(&mut core, 212, false, false).expect("start session");

        let peer_keys = keys_for_fill(213);
        let peer_npub = peer_keys.public_key().to_bech32().expect("peer npub");
        let peer_hex = peer_keys.public_key().to_hex();

        core.create_chat(&peer_npub);

        let metadata_event = EventBuilder::new(
            Kind::Metadata,
            serde_json::to_string(&NostrProfileMetadata {
                name: Some("Bob".to_string()),
                display_name: Some("Bob".to_string()),
            })
            .expect("metadata"),
        )
        .sign_with_keys(&peer_keys)
        .expect("metadata event");

        core.handle_relay_event(metadata_event);

        let chat = core
            .state
            .chat_list
            .iter()
            .find(|chat| chat.chat_id == peer_hex)
            .expect("chat row");
        assert_eq!(chat.display_name, "Bob");
        assert_eq!(chat.subtitle.as_deref(), Some(peer_npub.as_str()));
        assert_eq!(
            core.state
                .current_chat
                .as_ref()
                .expect("current chat")
                .display_name,
            "Bob"
        );
    }

    #[test]
    fn metadata_event_updates_group_member_display_name() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let data_dir = TempDir::new().expect("temp dir");
        let (alice_manager, _bob_manager, bob_owner_hex) =
            established_session_manager_pair(214, 215, 1_900_000_600);
        let mut core = logged_in_core_with_manager(data_dir.path(), 214, alice_manager);

        core.create_group("Project crew", std::slice::from_ref(&bob_owner_hex));
        let group_id = core
            .state
            .current_chat
            .as_ref()
            .expect("current chat")
            .group_id
            .clone()
            .expect("group id");

        let bob_keys = keys_for_fill(215);
        let metadata_event = EventBuilder::new(
            Kind::Metadata,
            serde_json::to_string(&NostrProfileMetadata {
                name: Some("Bob".to_string()),
                display_name: Some("Bobby".to_string()),
            })
            .expect("metadata"),
        )
        .sign_with_keys(&bob_keys)
        .expect("metadata event");

        core.handle_relay_event(metadata_event);

        let details = core
            .build_group_details_snapshot(&group_id)
            .expect("group details");
        let bob_member = details
            .members
            .iter()
            .find(|member| member.owner_pubkey_hex == bob_owner_hex)
            .expect("bob member");
        assert_eq!(bob_member.display_name, "Bobby");
        assert_eq!(
            details
                .members
                .iter()
                .find(|member| member.is_local_owner)
                .expect("local member")
                .display_name,
            core.state.account.as_ref().expect("account").display_name
        );
    }

    #[test]
    fn owner_profile_restores_display_name() {
        let _guard = relay_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let data_dir = TempDir::new().expect("temp dir");
        let mut seeded = test_core(data_dir.path());
        start_primary_test_session(&mut seeded, 216, false, false).expect("seed session");
        seeded.set_local_profile_name("Alice");
        seeded.rebuild_state();
        seeded.persist_best_effort();

        let mut restored = test_core(data_dir.path());
        start_primary_test_session(&mut restored, 216, true, true).expect("restore session");

        assert_eq!(
            restored
                .state
                .account
                .as_ref()
                .expect("account")
                .display_name,
            "Alice"
        );
    }

    #[test]
    fn twenty_owner_group_converges() {
        let mut scenario = ScenarioRunner::local();
        scenario.add_owner("admin", 220);
        for offset in 1..20 {
            scenario.add_owner(&format!("owner{offset:02}"), 220 + offset as u8);
        }

        let mut member_labels = Vec::new();
        for offset in 1..20 {
            member_labels.push(format!("owner{offset:02}"));
        }
        let member_refs = member_labels.iter().map(String::as_str).collect::<Vec<_>>();
        let chat_id = scenario.create_group("admin", "Twenty owners", &member_refs);

        for label in member_refs.iter().copied().chain(std::iter::once("admin")) {
            scenario.wait_for_group(label, &chat_id, 20);
        }

        let senders = std::iter::once("admin".to_string()).chain(member_labels.clone());
        for (index, sender) in senders.enumerate() {
            let text = format!("twenty-{index:02}");
            scenario.run_step(ScenarioStep::SendGroup {
                sender: sender.clone(),
                chat_id: chat_id.clone(),
                text: text.clone(),
            });
            for label in member_refs.iter().copied().chain(std::iter::once("admin")) {
                let is_outgoing = label == sender;
                scenario.wait_for_message(label, &chat_id, &text, is_outgoing);
            }
        }

        for label in member_refs.iter().copied().chain(std::iter::once("admin")) {
            scenario.wait_for_message_count(label, &chat_id, 20);
        }
    }

    #[test]
    fn group_with_linked_devices_converges() {
        let mut scenario = ScenarioRunner::local();
        scenario.add_owner("primary", 240);
        scenario.add_linked("linked", "primary");
        scenario.add_owner("admin", 242);
        scenario.add_owner("member", 243);

        let chat_id = scenario.create_group("admin", "Linked converge", &["primary", "member"]);
        for label in ["admin", "primary", "linked", "member"] {
            scenario.wait_for_group(label, &chat_id, 3);
        }

        for (sender, text) in [
            ("admin", "from-admin"),
            ("linked", "from-linked"),
            ("member", "from-member"),
        ] {
            scenario.run_step(ScenarioStep::SendGroup {
                sender: sender.to_string(),
                chat_id: chat_id.clone(),
                text: text.to_string(),
            });
        }

        scenario.wait_for_message("admin", &chat_id, "from-admin", true);
        scenario.wait_for_message("primary", &chat_id, "from-admin", false);
        scenario.wait_for_message("linked", &chat_id, "from-admin", false);
        scenario.wait_for_message("member", &chat_id, "from-admin", false);

        scenario.wait_for_message("admin", &chat_id, "from-linked", false);
        scenario.wait_for_message("primary", &chat_id, "from-linked", true);
        scenario.wait_for_message("linked", &chat_id, "from-linked", true);
        scenario.wait_for_message("member", &chat_id, "from-linked", false);

        scenario.wait_for_message("admin", &chat_id, "from-member", false);
        scenario.wait_for_message("primary", &chat_id, "from-member", false);
        scenario.wait_for_message("linked", &chat_id, "from-member", false);
        scenario.wait_for_message("member", &chat_id, "from-member", true);
    }

    #[test]
    fn linked_device_receives_existing_group_after_next_primary_send() {
        let mut scenario = ScenarioRunner::local();
        scenario.add_owner("primary", 244);
        scenario.add_owner("admin", 245);
        scenario.add_owner("member", 246);

        let chat_id = scenario.create_group("admin", "Late linked", &["primary", "member"]);
        scenario.wait_for_group("admin", &chat_id, 3);
        scenario.wait_for_group("primary", &chat_id, 3);
        scenario.wait_for_group("member", &chat_id, 3);

        scenario.add_linked("linked", "primary");
        let primary_owner_hex = scenario.owner_hex("primary");
        wait_for_persisted_state(
            scenario.data_dir("primary"),
            "primary sibling transport ready",
            |persisted| {
                persisted
                    .session_manager
                    .as_ref()
                    .and_then(|snapshot| {
                        snapshot
                            .users
                            .iter()
                            .find(|user| user.owner_pubkey.to_string() == primary_owner_hex)
                    })
                    .and_then(|user| {
                        user.roster.as_ref().map(|roster| {
                            roster.devices.iter().all(|roster_device| {
                                user.devices.iter().any(|device| {
                                    device.device_pubkey == roster_device.device_pubkey
                                        && device.public_invite.is_some()
                                })
                            })
                        })
                    })
                    .unwrap_or(false)
            },
        );
        wait_for_persisted_state(
            scenario.data_dir("linked"),
            "linked sibling transport ready",
            |persisted| {
                persisted
                    .session_manager
                    .as_ref()
                    .and_then(|snapshot| {
                        snapshot
                            .users
                            .iter()
                            .find(|user| user.owner_pubkey.to_string() == primary_owner_hex)
                    })
                    .and_then(|user| {
                        user.roster.as_ref().map(|roster| {
                            roster.devices.iter().all(|roster_device| {
                                user.devices.iter().any(|device| {
                                    device.device_pubkey == roster_device.device_pubkey
                                        && device.public_invite.is_some()
                                })
                            })
                        })
                    })
                    .unwrap_or(false)
            },
        );
        thread::sleep(StdDuration::from_secs(1));
        assert!(!scenario
            .app("linked")
            .state()
            .chat_list
            .iter()
            .any(|thread| thread.chat_id == chat_id));

        scenario.run_step(ScenarioStep::SendGroup {
            sender: "primary".to_string(),
            chat_id: chat_id.clone(),
            text: "after-link-bootstrap".to_string(),
        });

        scenario.wait_for_group("linked", &chat_id, 3);
        scenario.wait_for_message("linked", &chat_id, "after-link-bootstrap", true);
        scenario.wait_for_message("admin", &chat_id, "after-link-bootstrap", false);
        scenario.wait_for_message("member", &chat_id, "after-link-bootstrap", false);
    }

    #[test]
    fn restart_mid_group_create_recovers() {
        let mut scenario = ScenarioRunner::local();
        scenario.add_owner("admin", 250);

        let pending_member_npub = npub_for_fill(251);
        let chat_id = scenario.create_group_by_inputs(
            "admin",
            "Restart create",
            vec![pending_member_npub.clone()],
        );
        wait_for_persisted_state(
            scenario.data_dir("admin"),
            "pending group create",
            |persisted| !persisted.pending_group_controls.is_empty(),
        );

        scenario.restart_owner("admin");
        scenario.add_owner("member", 251);

        scenario.wait_for_group("member", &chat_id, 2);
        scenario.run_step(ScenarioStep::SendGroup {
            sender: "admin".to_string(),
            chat_id: chat_id.clone(),
            text: "after-create-restart".to_string(),
        });
        scenario.wait_for_message("member", &chat_id, "after-create-restart", false);
    }

    #[test]
    fn restart_mid_member_add_recovers() {
        let mut scenario = ScenarioRunner::local();
        scenario.add_owner("admin", 252);
        scenario.add_owner("member1", 253);

        let chat_id = scenario.create_group("admin", "Restart add", &["member1"]);
        scenario.wait_for_group("member1", &chat_id, 2);

        scenario.add_group_members_by_inputs("admin", &chat_id, vec![npub_for_fill(254)]);
        scenario.wait_for_group("admin", &chat_id, 3);
        let persisted_group_id = parse_group_id_from_chat_id(&chat_id).expect("group id");
        wait_for_persisted_state(
            scenario.data_dir("admin"),
            "pending member add",
            |persisted| {
                !persisted.pending_group_controls.is_empty()
                    && persisted
                        .threads
                        .iter()
                        .any(|thread| thread.chat_id == chat_id)
                    && persisted
                        .group_manager
                        .as_ref()
                        .and_then(|snapshot| {
                            snapshot
                                .groups
                                .iter()
                                .find(|group| group.group_id == persisted_group_id)
                        })
                        .map(|group| group.members.len() == 3)
                        .unwrap_or(false)
            },
        );

        scenario.restart_owner("admin");
        scenario.add_owner("member2", 254);

        for label in ["admin", "member1", "member2"] {
            scenario.wait_for_group(label, &chat_id, 3);
        }
        scenario.run_step(ScenarioStep::SendGroup {
            sender: "member2".to_string(),
            chat_id: chat_id.clone(),
            text: "member-two-arrived".to_string(),
        });
        scenario.wait_for_message("admin", &chat_id, "member-two-arrived", false);
        scenario.wait_for_message("member1", &chat_id, "member-two-arrived", false);
    }

    #[test]
    fn duplicate_and_replayed_events_do_not_duplicate_threads_or_messages() {
        let mut scenario = ScenarioRunner::local();
        scenario.add_owner("admin", 244);
        scenario.add_owner("member", 246);

        let chat_id = scenario.create_group("admin", "Replay safe", &["member"]);
        scenario.wait_for_group("member", &chat_id, 2);

        scenario.run_step(ScenarioStep::SendGroup {
            sender: "admin".to_string(),
            chat_id: chat_id.clone(),
            text: "dedupe-group".to_string(),
        });
        scenario.wait_for_message("admin", &chat_id, "dedupe-group", true);
        scenario.wait_for_message("member", &chat_id, "dedupe-group", false);

        scenario.replay_stored();

        let admin_state = scenario.app("admin").state();
        let member_state = scenario.app("member").state();
        assert_eq!(
            admin_state
                .chat_list
                .iter()
                .filter(|thread| thread.chat_id == chat_id)
                .count(),
            1
        );
        assert_eq!(
            member_state
                .chat_list
                .iter()
                .filter(|thread| thread.chat_id == chat_id)
                .count(),
            1
        );
        scenario.wait_for_message_count("admin", &chat_id, 1);
        scenario.wait_for_message_count("member", &chat_id, 1);
    }

    #[test]
    fn member_removal_blocks_future_sends() {
        let mut scenario = ScenarioRunner::local();
        scenario.add_owner("admin", 247);
        scenario.add_owner("member", 248);

        let chat_id = scenario.create_group("admin", "Removal block", &["member"]);
        scenario.wait_for_group("member", &chat_id, 2);
        scenario.remove_group_member("admin", &chat_id, "member");
        scenario.wait_for_group("admin", &chat_id, 1);
        scenario.wait_for_group("member", &chat_id, 1);

        scenario.open_chat("member", &chat_id);
        let before_count = scenario
            .app("member")
            .state()
            .current_chat
            .as_ref()
            .map(|chat| chat.messages.len())
            .unwrap_or(0);
        scenario.run_step(ScenarioStep::SendGroup {
            sender: "member".to_string(),
            chat_id: chat_id.clone(),
            text: "blocked-after-removal".to_string(),
        });
        thread::sleep(StdDuration::from_secs(1));
        let after_state = scenario.app("member").state();
        assert_eq!(
            after_state
                .current_chat
                .as_ref()
                .map(|chat| chat.messages.len())
                .unwrap_or(0),
            before_count
        );
    }

    #[test]
    fn post_removal_messages_reach_only_remaining_members() {
        let mut scenario = ScenarioRunner::local();
        scenario.add_owner("admin", 249);
        scenario.add_owner("member1", 250);
        scenario.add_owner("member2", 251);

        let chat_id = scenario.create_group("admin", "Post removal", &["member1", "member2"]);
        for label in ["admin", "member1", "member2"] {
            scenario.wait_for_group(label, &chat_id, 3);
        }

        scenario.remove_group_member("admin", &chat_id, "member2");
        scenario.wait_for_group("admin", &chat_id, 2);
        scenario.wait_for_group("member1", &chat_id, 2);
        scenario.wait_for_group("member2", &chat_id, 2);

        scenario.run_step(ScenarioStep::SendGroup {
            sender: "admin".to_string(),
            chat_id: chat_id.clone(),
            text: "after-removal".to_string(),
        });
        scenario.wait_for_message("admin", &chat_id, "after-removal", true);
        scenario.wait_for_message("member1", &chat_id, "after-removal", false);
        thread::sleep(StdDuration::from_secs(2));
        let member2_state = scenario.app("member2").state();
        assert!(member2_state
            .current_chat
            .as_ref()
            .map(|chat| chat
                .messages
                .iter()
                .all(|message| message.body != "after-removal"))
            .unwrap_or(true));
    }
}
