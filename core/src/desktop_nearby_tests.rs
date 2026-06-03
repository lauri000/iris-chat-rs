use super::*;

struct NoopDesktopNearbyObserver;

impl DesktopNearbyObserver for NoopDesktopNearbyObserver {
    fn desktop_nearby_changed(&self, _snapshot: DesktopNearbySnapshot) {}
}

#[test]
#[ignore = "LAN multicast discovery is host-network dependent and flakes in sandboxed CI"]
fn desktop_lan_services_discover_each_other_on_same_host() {
    let Some(local_addr) = private_local_ipv4() else {
        eprintln!("skipping LAN nearby smoke: no private local IPv4 route");
        return;
    };
    if !local_multicast_loopback_available(local_addr) {
        eprintln!("skipping LAN nearby smoke: local multicast loopback unavailable");
        return;
    }
    if !local_tcp_hairpin_available(local_addr) {
        eprintln!("skipping LAN nearby smoke: local TCP hairpin unavailable");
        return;
    }

    let alice_dir = tempfile::TempDir::new().expect("alice temp dir");
    let bob_dir = tempfile::TempDir::new().expect("bob temp dir");
    let alice_app = FfiApp::new(
        alice_dir.path().to_string_lossy().to_string(),
        String::new(),
        "test".to_string(),
    );
    let bob_app = FfiApp::new(
        bob_dir.path().to_string_lossy().to_string(),
        String::new(),
        "test".to_string(),
    );
    let alice = DesktopNearbyService::new(alice_app.clone(), Arc::new(NoopDesktopNearbyObserver));
    let bob = DesktopNearbyService::new(bob_app.clone(), Arc::new(NoopDesktopNearbyObserver));

    alice.start("Alice".to_string());
    bob.start("Bob".to_string());

    let started = Instant::now();
    let mut alice_snapshot = alice.snapshot();
    let mut bob_snapshot = bob.snapshot();
    while started.elapsed() < Duration::from_secs(20) {
        alice_snapshot = alice.snapshot();
        bob_snapshot = bob.snapshot();
        if alice_snapshot.status == "Local network unavailable"
            || bob_snapshot.status == "Local network unavailable"
        {
            break;
        }
        if !alice_snapshot.peers.is_empty() && !bob_snapshot.peers.is_empty() {
            break;
        }
        thread::sleep(Duration::from_millis(250));
    }

    alice.stop();
    bob.stop();
    alice_app.shutdown();
    bob_app.shutdown();

    if alice_snapshot.status == "Local network unavailable"
        || bob_snapshot.status == "Local network unavailable"
    {
        eprintln!(
            "skipping LAN nearby smoke: local network unavailable (alice={}, bob={})",
            alice_snapshot.status, bob_snapshot.status
        );
        return;
    }

    assert!(
        !alice_snapshot.peers.is_empty() && !bob_snapshot.peers.is_empty(),
        "LAN nearby peers should discover each other; alice={alice_snapshot:?} bob={bob_snapshot:?}"
    );
}

#[test]
fn verified_nearby_identity_beats_advertised_device_name() {
    let owner = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let expected = fallback_profile_name_for_identity(owner);

    assert_eq!(
        nearby_peer_name(Some("iPhone"), Some(owner), None, Some("iPhone")),
        expected
    );
}

#[test]
fn advertised_profile_name_beats_identity_fallback() {
    let owner = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    assert_eq!(
        nearby_peer_name(Some("iPhone"), Some(owner), Some("Alice"), Some("iPhone")),
        "Alice"
    );
}

fn local_multicast_loopback_available(local_addr: Ipv4Addr) -> bool {
    let Some(first) = multicast_probe_socket(local_addr, 0) else {
        return false;
    };
    let port = match first.local_addr() {
        Ok(addr) => addr.port(),
        Err(_) => return false,
    };
    let Some(second) = multicast_probe_socket(local_addr, port) else {
        return false;
    };
    let first_probe = b"iris-lan-nearby-smoke-first";
    let second_probe = b"iris-lan-nearby-smoke-second";
    if first
        .send_to(first_probe, SocketAddrV4::new(MDNS_GROUP, port))
        .is_err()
        || !multicast_probe_received(&second, first_probe)
    {
        return false;
    }
    second
        .send_to(second_probe, SocketAddrV4::new(MDNS_GROUP, port))
        .is_ok()
        && multicast_probe_received(&first, second_probe)
}

fn local_tcp_hairpin_available(local_addr: Ipv4Addr) -> bool {
    let listener = match TcpListener::bind(SocketAddrV4::new(local_addr, 0)) {
        Ok(listener) => listener,
        Err(_) => return false,
    };
    let port = match listener.local_addr() {
        Ok(addr) => addr.port(),
        Err(_) => return false,
    };
    TcpStream::connect_timeout(
        &SocketAddr::V4(SocketAddrV4::new(local_addr, port)),
        Duration::from_secs(1),
    )
    .is_ok()
}

fn multicast_probe_socket(local_addr: Ipv4Addr, port: u16) -> Option<UdpSocket> {
    let socket = match Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)) {
        Ok(socket) => socket,
        Err(_) => return None,
    };
    if socket.set_reuse_address(true).is_err() {
        return None;
    }
    #[cfg(unix)]
    {
        let _ = socket.set_reuse_port(true);
    }
    if socket
        .bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port).into())
        .is_err()
    {
        return None;
    }
    if socket.join_multicast_v4(&MDNS_GROUP, &local_addr).is_err()
        || socket.set_multicast_if_v4(&local_addr).is_err()
        || socket.set_multicast_loop_v4(true).is_err()
        || socket.set_multicast_ttl_v4(1).is_err()
    {
        return None;
    }
    let receiver: UdpSocket = socket.into();
    if receiver
        .set_read_timeout(Some(Duration::from_millis(500)))
        .is_err()
    {
        return None;
    }
    Some(receiver)
}

fn multicast_probe_received(receiver: &UdpSocket, probe: &[u8]) -> bool {
    let started = Instant::now();
    let mut buffer = [0u8; 64];
    while started.elapsed() < Duration::from_secs(1) {
        match receiver.recv_from(&mut buffer) {
            Ok((count, _)) if buffer.get(..count) == Some(probe) => return true,
            Ok(_) => {}
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.kind() == std::io::ErrorKind::TimedOut =>
            {
                return false;
            }
            Err(_) => return false,
        }
    }
    false
}
