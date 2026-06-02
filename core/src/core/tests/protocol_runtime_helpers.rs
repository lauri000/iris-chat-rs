#[derive(Clone)]
struct SwitchableFailStorage {
    inner: nostr_double_ratchet_runtime::InMemoryStorage,
    fail_puts: Arc<std::sync::atomic::AtomicBool>,
}

impl SwitchableFailStorage {
    fn new() -> Self {
        Self {
            inner: nostr_double_ratchet_runtime::InMemoryStorage::new(),
            fail_puts: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    fn set_fail_puts(&self, fail: bool) {
        self.fail_puts
            .store(fail, std::sync::atomic::Ordering::SeqCst);
    }
}

impl StorageAdapter for SwitchableFailStorage {
    fn get(&self, key: &str) -> nostr_double_ratchet_runtime::Result<Option<String>> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, value: String) -> nostr_double_ratchet_runtime::Result<()> {
        if self.fail_puts.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(nostr_double_ratchet_runtime::Error::Storage(
                "injected storage failure".to_string(),
            ));
        }
        self.inner.put(key, value)
    }

    fn del(&self, key: &str) -> nostr_double_ratchet_runtime::Result<()> {
        self.inner.del(key)
    }

    fn list(&self, prefix: &str) -> nostr_double_ratchet_runtime::Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

#[derive(Clone)]
struct CountingStorage {
    inner: nostr_double_ratchet_runtime::InMemoryStorage,
    put_count: Arc<std::sync::atomic::AtomicUsize>,
}

impl CountingStorage {
    fn new() -> Self {
        Self {
            inner: nostr_double_ratchet_runtime::InMemoryStorage::new(),
            put_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    fn put_count(&self) -> usize {
        self.put_count.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl StorageAdapter for CountingStorage {
    fn get(&self, key: &str) -> nostr_double_ratchet_runtime::Result<Option<String>> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, value: String) -> nostr_double_ratchet_runtime::Result<()> {
        self.put_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.put(key, value)
    }

    fn del(&self, key: &str) -> nostr_double_ratchet_runtime::Result<()> {
        self.inner.del(key)
    }

    fn list(&self, prefix: &str) -> nostr_double_ratchet_runtime::Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

fn protocol_plan_for_test(
    message_authors: Vec<PublicKey>,
    group_sender_key_authors: Vec<PublicKey>,
) -> ProtocolSubscriptionPlan {
    ProtocolSubscriptionPlan {
        runtime_subscriptions: vec!["ndr-protocol".to_string()],
        roster_authors: Vec::new(),
        invite_authors: Vec::new(),
        message_authors: message_authors
            .into_iter()
            .map(|pubkey| pubkey.to_hex())
            .collect(),
        group_sender_key_authors: group_sender_key_authors
            .into_iter()
            .map(|pubkey| pubkey.to_hex())
            .collect(),
        invite_response_recipient: None,
    }
}

fn runtime_rumor_json(
    author: PublicKey,
    kind: u32,
    content: &str,
    created_at_secs: u64,
    tags: Vec<Vec<String>>,
) -> (String, String) {
    let tags = tags
        .into_iter()
        .map(|tag| nostr::Tag::parse(tag).expect("runtime rumor tag"))
        .collect::<Vec<_>>();
    let mut rumor = UnsignedEvent::new(
        author,
        Timestamp::from_secs(created_at_secs),
        Kind::Custom(kind as u16),
        tags,
        content.to_string(),
    );
    rumor.ensure_id();
    let id = rumor.id.as_ref().expect("runtime rumor id").to_string();
    (
        serde_json::to_string(&rumor).expect("runtime rumor json"),
        id,
    )
}

fn appcore_direct_message_event_for_test(
    receiver_engine: &mut ProtocolEngine,
    sender_keys: &Keys,
    body: &str,
    created_at_secs: u64,
) -> Event {
    appcore_direct_message_event_with_author_keys_for_test(
        receiver_engine,
        sender_keys,
        body,
        created_at_secs,
    )
    .0
}

fn appcore_direct_message_event_with_author_keys_for_test(
    receiver_engine: &mut ProtocolEngine,
    sender_keys: &Keys,
    body: &str,
    created_at_secs: u64,
) -> (Event, Keys) {
    let invite = receiver_engine
        .local_invite()
        .expect("receiver local invite");
    let (mut sender_session, response) = invite
        .accept_with_owner(
            sender_keys.public_key(),
            sender_keys.secret_key().to_secret_bytes(),
            Some(sender_keys.public_key().to_hex()),
            Some(sender_keys.public_key()),
        )
        .expect("sender accepts receiver invite");
    let response_event = invite_response_event(&response).expect("invite response event");
    receiver_engine
        .observe_invite_response_event(&response_event)
        .expect("receiver observes invite response");

    let (content, _) = runtime_rumor_json(
        sender_keys.public_key(),
        CHAT_MESSAGE_KIND,
        body,
        created_at_secs,
        Vec::new(),
    );
    let plan = sender_session
        .plan_send(content.as_bytes(), NdrUnixSeconds(created_at_secs))
        .expect("sender plans message");
    let sent = sender_session.apply_send(plan);
    let author_keys = Keys::new(
        nostr::SecretKey::from_slice(&sent.envelope.signer_secret_key)
            .expect("message event author secret key"),
    );
    (
        message_event(&sent.envelope).expect("message event"),
        author_keys,
    )
}

fn encrypted_direct_unsigned_event_for_push_test(
    receiver_engine: &mut ProtocolEngine,
    sender_keys: &Keys,
    rumor: &mut UnsignedEvent,
    created_at_secs: u64,
) -> Event {
    let invite = receiver_engine
        .local_invite()
        .expect("receiver local invite");
    let (mut sender_session, response) = invite
        .accept_with_owner(
            sender_keys.public_key(),
            sender_keys.secret_key().to_secret_bytes(),
            Some(sender_keys.public_key().to_hex()),
            Some(sender_keys.public_key()),
        )
        .expect("sender accepts receiver invite");
    receiver_engine
        .observe_invite_response_event(
            &invite_response_event(&response).expect("invite response event"),
        )
        .expect("receiver observes invite response");

    rumor.ensure_id();
    let payload = serde_json::to_vec(rumor).expect("rumor json");
    let plan = sender_session
        .plan_send(&payload, NdrUnixSeconds(created_at_secs))
        .expect("sender plans message");
    let sent = sender_session.apply_send(plan);
    message_event(&sent.envelope).expect("message event")
}

fn logged_in_test_core_with_updates(
    label: &str,
    owner: &Keys,
    device: &Keys,
) -> (AppCore, flume::Receiver<AppUpdate>, tempfile::TempDir) {
    let temp_dir = tempfile::Builder::new()
        .prefix(&format!("iris-chat-rs-test-{label}-"))
        .tempdir()
        .expect("temp dir");
    let (update_tx, update_rx) = flume::unbounded();
    let mut core = AppCore::new(
        update_tx,
        flume::unbounded().0,
        temp_dir.path().to_string_lossy().to_string(),
        Arc::new(RwLock::new(AppState::empty())),
    );
    core.logged_in = Some(LoggedInState {
        owner_pubkey: owner.public_key(),
        owner_keys: Some(owner.clone()),
        device_keys: device.clone(),
        client: Client::new(device.clone()),
        relay_urls: Vec::new(),
        authorization_state: LocalAuthorizationState::Authorized,
    });
    let storage = Arc::new(crate::core::storage::SqliteStorageAdapter::new(
        core.app_store.shared(),
        owner.public_key().to_hex(),
        device.public_key().to_hex(),
    )) as Arc<dyn StorageAdapter>;
    install_test_protocol_engine(&mut core, owner, device, storage, None, None);
    (core, update_rx, temp_dir)
}

fn signed_pairwise_message_event_for_test(
    sender_keys: &Keys,
    header: &str,
    content: &str,
) -> Event {
    EventBuilder::new(Kind::from(MESSAGE_EVENT_KIND as u16), content)
        .tag(nostr::Tag::parse(["header", header]).expect("header tag"))
        .sign_with_keys(sender_keys)
        .expect("signed pairwise message event")
}

fn drain_app_updates(update_rx: &flume::Receiver<AppUpdate>) {
    while update_rx.try_recv().is_ok() {}
}
