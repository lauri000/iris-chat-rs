use super::*;

fn unique_pubkeys(pubkeys: impl IntoIterator<Item = PublicKey>) -> Vec<PublicKey> {
    let mut seen = HashSet::new();
    pubkeys
        .into_iter()
        .filter(|pubkey| seen.insert(*pubkey))
        .collect()
}

pub(super) fn direct_message_history_filter(
    author_pubkeys: impl IntoIterator<Item = PublicKey>,
) -> Filter {
    Filter::new()
        .kind(Kind::from(MESSAGE_EVENT_KIND as u16))
        .authors(unique_pubkeys(author_pubkeys))
}

pub(super) fn group_sender_key_history_filter(
    author_pubkeys: impl IntoIterator<Item = PublicKey>,
) -> Filter {
    Filter::new()
        .kind(Kind::from(GROUP_SENDER_KEY_MESSAGE_KIND as u16))
        .authors(unique_pubkeys(author_pubkeys))
}

pub(super) fn sorted_hexes(values: HashSet<String>) -> Vec<String> {
    let mut sorted = values.into_iter().collect::<Vec<_>>();
    sorted.sort();
    sorted.dedup();
    sorted
}

pub(super) fn summarize_protocol_plan(plan: Option<&ProtocolSubscriptionPlan>) -> String {
    let Some(plan) = plan else {
        return "none".to_string();
    };
    format!(
        "runtime={} roster_authors={} invite_authors={} message_authors={} message_recipients={} group_sender_key_authors={} invite_response_recipient={}",
        plan.runtime_subscriptions.join(","),
        plan.roster_authors.len(),
        plan.invite_authors.len(),
        plan.message_authors.len(),
        plan.message_recipients.len(),
        plan.group_sender_key_authors.len(),
        plan.invite_response_recipient.as_deref().unwrap_or("")
    )
}

pub(super) fn build_protocol_subscription_filters(plan: &ProtocolSubscriptionPlan) -> Vec<Filter> {
    let roster_authors = pubkeys_from_hexes(&plan.roster_authors);
    let invite_authors = pubkeys_from_hexes(&plan.invite_authors);
    let message_authors = pubkeys_from_hexes(&plan.message_authors);
    let group_sender_key_authors = pubkeys_from_hexes(&plan.group_sender_key_authors);
    let invite_response_recipients = plan
        .invite_response_recipient
        .as_deref()
        .map(pubkeys_from_comma_separated_hexes)
        .unwrap_or_default();

    let mut filters = Vec::new();
    if !roster_authors.is_empty() {
        filters.push(
            Filter::new()
                .kind(Kind::from(APP_KEYS_EVENT_KIND as u16))
                .authors(roster_authors)
                .identifier(NDR_APP_KEYS_D_TAG),
        );
    }
    if !invite_authors.is_empty() {
        filters.push(
            Filter::new()
                .kind(Kind::from(INVITE_EVENT_KIND as u16))
                .authors(invite_authors.clone())
                .custom_tag(SingleLetterTag::lowercase(Alphabet::L), NDR_INVITES_L_TAG),
        );
        filters.push(
            Filter::new()
                .kind(Kind::from(INVITE_RESPONSE_KIND as u16))
                .authors(invite_authors),
        );
    }
    if !message_authors.is_empty() {
        filters.push(direct_message_history_filter(message_authors));
    }
    if !group_sender_key_authors.is_empty() {
        filters.push(group_sender_key_history_filter(group_sender_key_authors));
    }
    if !invite_response_recipients.is_empty() {
        filters.push(
            Filter::new()
                .kind(Kind::from(INVITE_RESPONSE_KIND as u16))
                .pubkeys(invite_response_recipients),
        );
    }
    filters
}

pub(super) fn pubkeys_from_hexes(hexes: &[String]) -> Vec<PublicKey> {
    hexes
        .iter()
        .filter_map(|hex| PublicKey::parse(hex).ok())
        .collect()
}

pub(super) fn pubkeys_from_comma_separated_hexes(hexes: &str) -> Vec<PublicKey> {
    hexes
        .split(',')
        .filter(|hex| !hex.is_empty())
        .filter_map(|hex| PublicKey::parse(hex).ok())
        .collect()
}
