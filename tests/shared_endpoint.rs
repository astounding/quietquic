// SPDX-License-Identifier: 0BSD
//! Real-UDP validation for the socket-owning shared endpoint API.

mod common;

use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;

use quietquic::config::{ClientConfigFile, ClientEntry, ServerSecrets};
use quietquic::endpoint::{
    Capability, Endpoint, EndpointConfig, EndpointError, EndpointTermination,
};
use socket2::{Domain, Protocol, SockAddr, SockRef, Socket, Type};
use tokio::time::{sleep, timeout};

const STEP: Duration = Duration::from_secs(10);
const SHORT: Duration = Duration::from_millis(300);
const PSK: &str = "00000000000000000000000000000000000000000000000000000000000000ac";

fn configured_addr(offset: u16) -> SocketAddr {
    let port = std::env::var("QUIETQUIC_TEST_PORT_BASE")
        .ok()
        .map(|base| {
            base.parse::<u16>()
                .expect("QUIETQUIC_TEST_PORT_BASE must be a u16")
                .checked_add(offset)
                .expect("test port offset overflow")
        })
        .unwrap_or(0);
    SocketAddr::new(common::test_ip(), port)
}

fn credential(client_id: &str) -> ClientEntry {
    let secrets: ServerSecrets = toml::from_str(&format!(
        "listen='127.0.0.1:1'\n[[clients]]\nclient_id='{client_id}'\npsk='{PSK}'\n"
    ))
    .expect("valid test credential");
    secrets.clients.into_iter().next().unwrap()
}

fn client(client_id: &str, server: SocketAddr) -> ClientConfigFile {
    toml::from_str(&format!(
        "client_id='{client_id}'\npsk='{PSK}'\nserver='{server}'\n"
    ))
    .expect("valid client configuration")
}

async fn endpoint(offset: u16, config: EndpointConfig) -> Endpoint {
    Endpoint::bind(configured_addr(offset), config)
        .await
        .expect("bind endpoint")
}

async fn connected_pair(
    dialer: &Endpoint,
    acceptor: &Endpoint,
) -> (quietquic::conn::Connection, quietquic::conn::Connection) {
    let dial = dialer.connect(client("shared", acceptor.local_addr()));
    let accept = acceptor.accept();
    let (outgoing, incoming) = tokio::join!(timeout(STEP, dial), timeout(STEP, accept));
    let outgoing = outgoing
        .expect("connect timed out")
        .expect("connect failed");
    let incoming = incoming
        .expect("accept timed out")
        .expect("accept failed")
        .expect("accept ended unexpectedly");
    (outgoing, incoming)
}

async fn transfer(
    sender: &quietquic::conn::Connection,
    receiver: &quietquic::conn::Connection,
    payload: &'static [u8],
) {
    let (mut send, sender_recv) = timeout(STEP, sender.open_bi())
        .await
        .expect("open_bi timed out")
        .expect("open_bi failed");

    let send_all = async {
        send.write_all(payload).await.expect("write payload");
        send.finish_and_wait()
            .await
            .expect("finish acknowledgement");
        (send, sender_recv)
    };
    let receive_all = async {
        let (receiver_send, mut recv) = receiver.accept_bi().await.expect("accept_bi failed");
        let bytes = recv
            .read_to_end(payload.len() + 1)
            .await
            .expect("read payload");
        (receiver_send, recv, bytes)
    };
    let (send_result, recv_result) =
        tokio::join!(timeout(STEP, send_all), timeout(STEP, receive_all),);
    let _send = send_result.expect("send/FIN timed out");
    let (_receiver_send, _recv, bytes) = recv_result.expect("receive timed out");
    assert_eq!(bytes, payload);
}

#[tokio::test]
async fn two_outgoing_connections_share_one_fixed_socket_and_isolate_close() {
    let server = endpoint(4000, EndpointConfig::accept(vec![credential("shared")])).await;
    let client_endpoint = endpoint(4001, EndpointConfig::dial()).await;
    let client_addr = client_endpoint.local_addr();

    let first = client_endpoint.connect(client("shared", server.local_addr()));
    let second = client_endpoint.connect(client("shared", server.local_addr()));
    let ((first, second), (server_first, server_second)) = tokio::join!(
        async {
            tokio::join!(
                async {
                    timeout(STEP, first)
                        .await
                        .expect("first connect timeout")
                        .expect("first connect")
                },
                async {
                    timeout(STEP, second)
                        .await
                        .expect("second connect timeout")
                        .expect("second connect")
                },
            )
        },
        async {
            let first = timeout(STEP, server.accept())
                .await
                .expect("first accept timeout")
                .expect("first accept error")
                .expect("first accept end");
            let second = timeout(STEP, server.accept())
                .await
                .expect("second accept timeout")
                .expect("second accept error")
                .expect("second accept end");
            (first, second)
        },
    );

    assert_eq!(client_endpoint.local_addr(), client_addr);
    assert_eq!(server_first.remote_address(), client_addr);
    assert_eq!(server_second.remote_address(), client_addr);
    transfer(&first, &server_first, b"first connection").await;
    transfer(&second, &server_second, b"second connection").await;

    timeout(STEP, first.close(7, b"close one"))
        .await
        .expect("first close timed out")
        .expect("first close failed");
    transfer(&server_second, &second, b"sibling survived").await;
}

#[tokio::test]
async fn simultaneous_dial_and_accept_works_on_both_endpoints() {
    let a = endpoint(4010, EndpointConfig::both(vec![credential("shared")])).await;
    let b = endpoint(4011, EndpointConfig::both(vec![credential("shared")])).await;

    let (a_out, b_out, a_in, b_in) = tokio::join!(
        timeout(STEP, a.connect(client("shared", b.local_addr()))),
        timeout(STEP, b.connect(client("shared", a.local_addr()))),
        timeout(STEP, a.accept()),
        timeout(STEP, b.accept()),
    );
    let a_out = a_out.expect("A connect timeout").expect("A connect");
    let b_out = b_out.expect("B connect timeout").expect("B connect");
    let a_in = a_in
        .expect("A accept timeout")
        .expect("A accept")
        .expect("A accept end");
    let b_in = b_in
        .expect("B accept timeout")
        .expect("B accept")
        .expect("B accept end");

    transfer(&a_out, &b_in, b"A initiated").await;
    transfer(&b_out, &a_in, b"B initiated").await;
}

#[tokio::test]
async fn dropping_outgoing_attempt_does_not_harm_later_connection() {
    let server = endpoint(4020, EndpointConfig::accept(vec![credential("shared")])).await;
    let client_endpoint = endpoint(4021, EndpointConfig::dial()).await;
    let unused = UdpSocket::bind(configured_addr(4022)).expect("reserve unreachable address");
    let unreachable = unused.local_addr().unwrap();

    let mut attempt = Box::pin(client_endpoint.connect(client("shared", unreachable)));
    tokio::select! {
        result = attempt.as_mut() => panic!("unreachable attempt unexpectedly completed: {result:?}"),
        _ = sleep(Duration::from_millis(40)) => {}
    }
    drop(attempt);
    drop(unused);

    let (outgoing, incoming) = connected_pair(&client_endpoint, &server).await;
    transfer(&outgoing, &incoming, b"healthy after cancellation").await;
}

#[tokio::test]
async fn streams_retain_connection_after_endpoint_and_connection_handles_drop() {
    let server = endpoint(4030, EndpointConfig::accept(vec![credential("shared")])).await;
    let client_endpoint = endpoint(4031, EndpointConfig::dial()).await;
    let (outgoing, incoming) = connected_pair(&client_endpoint, &server).await;

    let (mut send, client_recv) = timeout(STEP, outgoing.open_bi())
        .await
        .expect("open timeout")
        .expect("open stream");
    drop(outgoing);
    drop(client_endpoint);

    let send_task = async {
        send.write_all(b"stream-owned endpoint")
            .await
            .expect("write");
        send.finish_and_wait()
            .await
            .expect("finish acknowledgement");
        (send, client_recv)
    };
    let recv_task = async {
        let (server_send, mut recv) = incoming.accept_bi().await.expect("accept stream");
        let bytes = recv
            .read_to_end(1024)
            .await
            .expect("read after endpoint drop");
        (server_send, recv, bytes)
    };
    let (send_result, got) = tokio::join!(timeout(STEP, send_task), timeout(STEP, recv_task));
    let _send = send_result.expect("retained stream send timed out");
    let (_server_send, _recv, bytes) = got.expect("retained stream read timed out");
    assert_eq!(bytes, b"stream-owned endpoint");
}

#[tokio::test]
async fn canceling_accept_waiter_preserves_the_next_connection() {
    let server = endpoint(4040, EndpointConfig::accept(vec![credential("shared")])).await;
    let client_endpoint = endpoint(4041, EndpointConfig::dial()).await;

    assert!(timeout(Duration::from_millis(40), server.accept())
        .await
        .is_err());
    let (outgoing, incoming) = connected_pair(&client_endpoint, &server).await;
    transfer(&outgoing, &incoming, b"accepted after waiter cancellation").await;
}

#[tokio::test]
async fn pause_preserves_queued_connection_and_rejects_new_admission_until_resume() {
    let mut server_config = EndpointConfig::accept(vec![credential("shared")]);
    server_config.incoming_handshake_timeout = SHORT;
    let server = endpoint(4050, server_config).await;
    let client_endpoint = endpoint(4051, EndpointConfig::dial()).await;

    let queued = timeout(
        STEP,
        client_endpoint.connect(client("shared", server.local_addr())),
    )
    .await
    .expect("queued connect timeout")
    .expect("queued connect failed");
    server.pause_admission().expect("pause admission");
    let queued_peer = timeout(STEP, server.accept())
        .await
        .expect("queued accept timeout")
        .expect("queued accept failed")
        .expect("queued accept ended");
    transfer(&queued, &queued_peer, b"queued before pause").await;

    let paused_attempt = client_endpoint.connect(client("shared", server.local_addr()));
    assert!(
        timeout(SHORT, paused_attempt).await.is_err(),
        "paused admission connected"
    );
    server.resume_admission().expect("resume admission");
    let (resumed, resumed_peer) = connected_pair(&client_endpoint, &server).await;
    transfer(&resumed, &resumed_peer, b"admitted after resume").await;
}

#[tokio::test]
async fn explicit_shutdown_is_terminal_and_wait_closed_releases_socket() {
    let socket = UdpSocket::bind(configured_addr(4060)).expect("bind preowned socket");
    let address = socket.local_addr().unwrap();
    let mut config = EndpointConfig::dial();
    config.cleanup_timeout = SHORT;
    let endpoint = Endpoint::from_socket(socket, config).expect("take socket ownership");

    endpoint
        .close(0, b"test shutdown")
        .expect("initiate shutdown");
    endpoint.close(1, b"idempotent").expect("repeat shutdown");
    assert_eq!(
        timeout(STEP, endpoint.terminated())
            .await
            .expect("termination timeout"),
        EndpointTermination::Closed
    );
    assert!(matches!(
        endpoint.connect(client("shared", address)).await,
        Err(EndpointError::Closed)
    ));
    timeout(STEP, endpoint.wait_closed())
        .await
        .expect("cleanup timeout");
    drop(endpoint);

    let rebound = UdpSocket::bind(address).expect("socket was not released after wait_closed");
    assert_eq!(rebound.local_addr().unwrap(), address);
}

#[tokio::test]
async fn construction_capabilities_are_static() {
    let dial = endpoint(4070, EndpointConfig::dial()).await;
    assert_eq!(
        dial.pause_admission(),
        Err(EndpointError::CapabilityDisabled)
    );
    assert_eq!(
        dial.resume_admission(),
        Err(EndpointError::CapabilityDisabled)
    );

    let accept = endpoint(4071, EndpointConfig::accept(vec![credential("shared")])).await;
    assert!(matches!(
        accept.connect(client("shared", dial.local_addr())).await,
        Err(EndpointError::CapabilityDisabled)
    ));
    assert_eq!(EndpointConfig::dial().capability, Capability::Dial);
    assert_eq!(
        EndpointConfig::accept(vec![credential("shared")]).capability,
        Capability::Accept
    );
    assert_eq!(
        EndpointConfig::both(vec![credential("shared")]).capability,
        Capability::Both
    );

    let invalid = Endpoint::bind(configured_addr(4072), EndpointConfig::accept(Vec::new())).await;
    assert!(matches!(
        invalid,
        Err(EndpointError::InvalidConfiguration(_))
    ));
}

#[tokio::test]
async fn pending_outgoing_attempt_retains_endpoint_after_last_handle_drop() {
    let server = endpoint(4080, EndpointConfig::accept(vec![credential("shared")])).await;
    let dialer = endpoint(4081, EndpointConfig::dial()).await;
    let attempt = dialer.connect(client("shared", server.local_addr()));
    drop(dialer);

    let (outgoing, incoming) = tokio::join!(timeout(STEP, attempt), timeout(STEP, server.accept()));
    let outgoing = outgoing
        .expect("retained attempt timed out")
        .expect("retained attempt failed");
    let incoming = incoming
        .expect("accept timed out")
        .expect("accept failed")
        .expect("accept ended unexpectedly");
    transfer(&outgoing, &incoming, b"attempt-owned endpoint").await;
}

#[tokio::test]
async fn cloned_accept_waiters_are_fifo_at_driver_receipt() {
    let server = endpoint(4090, EndpointConfig::accept(vec![credential("shared")])).await;
    let first_waiter = server.clone();
    let second_waiter = server.clone();
    let first_task = tokio::spawn(async move {
        first_waiter
            .accept()
            .await
            .expect("first accept failed")
            .expect("first accept ended")
    });
    tokio::task::yield_now().await;
    let second_task = tokio::spawn(async move {
        second_waiter
            .accept()
            .await
            .expect("second accept failed")
            .expect("second accept ended")
    });
    tokio::task::yield_now().await;

    let first_client = endpoint(4091, EndpointConfig::dial()).await;
    let first_addr = first_client.local_addr();
    let first_out = timeout(
        STEP,
        first_client.connect(client("shared", server.local_addr())),
    )
    .await
    .expect("first connect timeout")
    .expect("first connect failed");
    let first_in = timeout(STEP, first_task)
        .await
        .expect("first waiter timeout")
        .expect("first waiter panicked");
    assert_eq!(first_in.remote_address(), first_addr);

    let second_client = endpoint(4092, EndpointConfig::dial()).await;
    let second_addr = second_client.local_addr();
    let second_out = timeout(
        STEP,
        second_client.connect(client("shared", server.local_addr())),
    )
    .await
    .expect("second connect timeout")
    .expect("second connect failed");
    let second_in = timeout(STEP, second_task)
        .await
        .expect("second waiter timeout")
        .expect("second waiter panicked");
    assert_eq!(second_in.remote_address(), second_addr);

    transfer(&first_out, &first_in, b"first FIFO waiter").await;
    transfer(&second_out, &second_in, b"second FIFO waiter").await;
}

#[tokio::test]
async fn supplied_ipv6_sockets_preserve_address_and_transfer() {
    let server_socket = match UdpSocket::bind("[::1]:0") {
        Ok(socket) => socket,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::AddrNotAvailable | std::io::ErrorKind::Unsupported
            ) =>
        {
            eprintln!("skipping IPv6 supplied-socket test: {error}");
            return;
        }
        Err(error) => panic!("bind IPv6 server socket: {error}"),
    };
    let server_addr = server_socket.local_addr().expect("IPv6 server address");
    let server = Endpoint::from_socket(
        server_socket,
        EndpointConfig::accept(vec![credential("shared")]),
    )
    .expect("take IPv6 server socket");
    assert_eq!(server.local_addr(), server_addr);

    let client_socket = UdpSocket::bind("[::1]:0").expect("bind IPv6 client socket");
    let client_addr = client_socket.local_addr().expect("IPv6 client address");
    let dialer = Endpoint::from_socket(client_socket, EndpointConfig::dial())
        .expect("take IPv6 client socket");
    assert_eq!(dialer.local_addr(), client_addr);

    let (outgoing, incoming) = connected_pair(&dialer, &server).await;
    assert_eq!(incoming.remote_address(), client_addr);
    assert!(outgoing.remote_address().is_ipv6());
    transfer(&outgoing, &incoming, b"IPv6 supplied sockets").await;
}

#[tokio::test]
async fn invalid_address_family_dial_does_not_harm_healthy_sibling() {
    let server = endpoint(4100, EndpointConfig::accept(vec![credential("shared")])).await;
    let dialer = endpoint(4101, EndpointConfig::dial()).await;
    let (healthy, healthy_peer) = connected_pair(&dialer, &server).await;

    let invalid = dialer
        .connect(client("shared", "[::1]:9".parse().unwrap()))
        .await;
    assert!(matches!(
        invalid,
        Err(EndpointError::InvalidConfiguration(_))
    ));

    transfer(
        &healthy,
        &healthy_peer,
        b"healthy after address-family rejection",
    )
    .await;
}

#[tokio::test]
async fn supplied_dual_stack_socket_dials_ipv4_with_fixed_source_port() {
    let server = endpoint(4110, EndpointConfig::accept(vec![credential("shared")])).await;
    assert!(server.local_addr().is_ipv4(), "test server must be IPv4");

    let socket = match Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP)) {
        Ok(socket) => socket,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::AddrNotAvailable | std::io::ErrorKind::Unsupported
            ) =>
        {
            eprintln!("skipping dual-stack supplied-socket test: {error}");
            return;
        }
        Err(error) => panic!("create IPv6 UDP socket: {error}"),
    };
    if let Err(error) = socket.set_only_v6(false) {
        if matches!(
            error.kind(),
            std::io::ErrorKind::AddrNotAvailable | std::io::ErrorKind::Unsupported
        ) {
            eprintln!("skipping dual-stack supplied-socket test: {error}");
            return;
        }
        panic!("enable IPv4-mapped destinations: {error}");
    }
    let requested_port = std::env::var("QUIETQUIC_TEST_PORT_BASE")
        .ok()
        .map(|base| {
            base.parse::<u16>()
                .expect("QUIETQUIC_TEST_PORT_BASE must be a u16")
                .checked_add(4111)
                .expect("test port offset overflow")
        })
        .unwrap_or(0);
    let bind_address: SocketAddr = format!("[::]:{requested_port}").parse().unwrap();
    if let Err(error) = socket.bind(&SockAddr::from(bind_address)) {
        if matches!(
            error.kind(),
            std::io::ErrorKind::AddrNotAvailable | std::io::ErrorKind::Unsupported
        ) {
            eprintln!("skipping dual-stack supplied-socket test: {error}");
            return;
        }
        panic!("bind dual-stack UDP socket: {error}");
    }
    assert!(!SockRef::from(&socket).only_v6().expect("query IPV6_V6ONLY"));
    let socket: UdpSocket = socket.into();
    let fixed_port = socket
        .local_addr()
        .expect("dual-stack local address")
        .port();
    let dialer = Endpoint::from_socket(socket, EndpointConfig::dial())
        .expect("take dual-stack socket ownership");
    assert_eq!(dialer.local_addr().port(), fixed_port);

    let (outgoing, incoming) = connected_pair(&dialer, &server).await;
    assert_eq!(
        incoming.remote_address().port(),
        fixed_port,
        "IPv4 peer must observe the supplied socket's source port"
    );
    assert!(incoming.remote_address().is_ipv4());
    transfer(&outgoing, &incoming, b"dual-stack to IPv4").await;
}

#[tokio::test]
async fn last_endpoint_drop_closes_queued_inbound_but_owned_connection_survives() {
    let server = endpoint(4120, EndpointConfig::accept(vec![credential("shared")])).await;
    let first_client = endpoint(4121, EndpointConfig::dial()).await;
    let (first_out, first_in) = connected_pair(&first_client, &server).await;
    transfer(&first_out, &first_in, b"accepted before endpoint drop").await;

    let queued_client = endpoint(4122, EndpointConfig::dial()).await;
    let queued_out = timeout(
        STEP,
        queued_client.connect(client("shared", server.local_addr())),
    )
    .await
    .expect("queued connection handshake timed out")
    .expect("queued connection handshake failed");

    drop(server);

    timeout(STEP, queued_out.closed())
        .await
        .expect("queued inbound connection was not closed after final Endpoint drop");
    transfer(
        &first_in,
        &first_out,
        b"application-owned connection survived endpoint drop",
    )
    .await;
}

#[tokio::test]
async fn endpoint_shutdown_uses_one_cleanup_cap_with_retained_connection_owners() {
    const CONNECTIONS: usize = 5;
    let socket = UdpSocket::bind(configured_addr(4130)).expect("bind shutdown socket");
    let address = socket.local_addr().unwrap();
    let mut config = EndpointConfig::accept(vec![credential("shared")]);
    config.cleanup_timeout = Duration::from_millis(200);
    let server = Endpoint::from_socket(socket, config).expect("take shutdown socket");

    let mut clients = Vec::new();
    let mut server_connections = Vec::new();
    let mut retained_streams = Vec::new();
    for offset in 0..CONNECTIONS {
        let dialer = endpoint(4140 + offset as u16, EndpointConfig::dial()).await;
        let (outgoing, incoming) = connected_pair(&dialer, &server).await;
        let streams = timeout(STEP, incoming.open_bi())
            .await
            .expect("open retained stream timed out")
            .expect("open retained stream failed");
        clients.push((dialer, outgoing));
        server_connections.push(incoming);
        retained_streams.push(streams);
    }

    let started = std::time::Instant::now();
    server
        .close(0, b"bounded shutdown")
        .expect("close endpoint");
    timeout(Duration::from_secs(2), server.wait_closed())
        .await
        .expect("endpoint cleanup exceeded its overall cap");
    assert!(
        started.elapsed() < Duration::from_millis(700),
        "cleanup appears to have multiplied the 200ms cap by {CONNECTIONS} connections: {:?}",
        started.elapsed()
    );
    for connection in &server_connections {
        timeout(STEP, connection.closed())
            .await
            .expect("retained connection did not observe endpoint shutdown");
    }
    drop(retained_streams);
    drop(server_connections);
    drop(clients);
    drop(server);
    UdpSocket::bind(address).expect("wait_closed did not release the endpoint socket");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_endpoint_lifecycle_works_on_multithread_runtime() {
    let server = endpoint(4150, EndpointConfig::accept(vec![credential("shared")])).await;
    let dialer = endpoint(4151, EndpointConfig::dial()).await;
    let (outgoing, incoming) = connected_pair(&dialer, &server).await;

    transfer(&outgoing, &incoming, b"multi-thread runtime").await;
    server
        .close(0, b"multi-thread shutdown")
        .expect("close server");
    timeout(STEP, server.wait_closed())
        .await
        .expect("multi-thread cleanup timed out");
    timeout(STEP, incoming.closed())
        .await
        .expect("connection did not observe multi-thread shutdown");
}

#[tokio::test]
async fn flow_control_blocked_large_write_does_not_starve_sibling_transfer() {
    let server = endpoint(4160, EndpointConfig::accept(vec![credential("shared")])).await;
    let dialer = endpoint(4161, EndpointConfig::dial()).await;

    let first = dialer.connect(client("shared", server.local_addr()));
    let second = dialer.connect(client("shared", server.local_addr()));
    let ((large_out, small_out), (large_in, small_in)) = tokio::join!(
        async {
            tokio::join!(
                async { timeout(STEP, first).await.unwrap().unwrap() },
                async { timeout(STEP, second).await.unwrap().unwrap() },
            )
        },
        async {
            let large = timeout(STEP, server.accept())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let small = timeout(STEP, server.accept())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            (large, small)
        },
    );

    let (mut large_send, large_sender_recv) = timeout(STEP, large_out.open_bi())
        .await
        .expect("open large stream timed out")
        .expect("open large stream failed");
    let payload = vec![0x5a; 4 * 1024 * 1024];
    let expected_len = payload.len();
    let large_writer = tokio::spawn(async move {
        large_send
            .write_all(&payload)
            .await
            .expect("large write failed");
        large_send
            .finish_and_wait()
            .await
            .expect("large FIN acknowledgement failed");
        (large_send, large_sender_recv)
    });

    tokio::task::yield_now().await;
    assert!(
        !large_writer.is_finished(),
        "large write unexpectedly completed without the peer reading"
    );
    timeout(
        Duration::from_secs(3),
        transfer(&small_out, &small_in, b"small sibling progresses"),
    )
    .await
    .expect("flow-control-blocked large write starved its sibling");

    let large_reader = async {
        let (server_send, mut recv) = large_in.accept_bi().await.expect("accept large stream");
        let bytes = recv
            .read_to_end(expected_len + 1)
            .await
            .expect("read large stream");
        (server_send, recv, bytes)
    };
    let (writer, reader) = tokio::join!(timeout(STEP, large_writer), timeout(STEP, large_reader),);
    let _retained = writer
        .expect("large writer timed out")
        .expect("large writer task panicked");
    let (_server_send, _recv, bytes) = reader.expect("large reader timed out");
    assert_eq!(bytes.len(), expected_len);
    assert!(bytes.iter().all(|byte| *byte == 0x5a));
}
