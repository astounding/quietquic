//! Supported shared-endpoint behavior over real UDP.
//! One socket per endpoint throughout, including overlapping dials.

use std::collections::{HashMap, HashSet};
use std::io::ErrorKind;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::time::{Duration, Instant};

use quietquic_proto::config::{ClientConfigFile, EndpointConfig, ServerSecrets};
use quietquic_proto::conn::SendFin;
use quietquic_proto::endpoint::Endpoint;
use quietquic_proto::freshness::now_minutes;
use quietquic_proto::outcome::{
    ConnectionError, ConnectionHandle, DatagramOutcome, Event, ReadOutcome, WriteOutcome,
};
use quinn_proto::VarInt;

const PSK: &str = "00000000000000000000000000000000000000000000000000000000000000ab";

struct Peer {
    ep: Endpoint,
    socket: UdpSocket,
    bound: SocketAddr,
    connected: HashSet<ConnectionHandle>,
    lost: HashMap<ConnectionHandle, ConnectionError>,
    sent: Vec<Vec<u8>>,
}

fn socket(offset: u16) -> UdpSocket {
    let ip: IpAddr = std::env::var("QUIETQUIC_TEST_ADDR")
        .unwrap_or_else(|_| "127.0.0.1".into())
        .parse()
        .unwrap();
    let port = std::env::var("QUIETQUIC_TEST_PORT_BASE")
        .ok()
        .map(|v| v.parse::<u16>().unwrap().checked_add(offset).unwrap())
        .unwrap_or(0);
    // Reserve a port if unspecified, then explicitly bind the selected port.
    let selected = UdpSocket::bind(SocketAddr::new(ip, port)).unwrap();
    let addr = selected.local_addr().unwrap();
    drop(selected);
    let socket = UdpSocket::bind(addr).unwrap();
    socket.set_nonblocking(true).unwrap();
    socket
}

fn config(addr: SocketAddr) -> ClientConfigFile {
    toml::from_str(&format!(
        "client_id='experiment'\npsk='{PSK}'\nserver='{addr}'\n"
    ))
    .unwrap()
}

impl Peer {
    fn new(ep: Endpoint, socket: UdpSocket) -> Self {
        Self {
            ep,
            bound: socket.local_addr().unwrap(),
            socket,
            connected: HashSet::new(),
            lost: HashMap::new(),
            sent: Vec::new(),
        }
    }

    fn server(offset: u16) -> Self {
        let socket = socket(offset);
        let secrets: ServerSecrets = toml::from_str(&format!(
            "listen='{}'\n[[clients]]\nclient_id='experiment'\npsk='{PSK}'\n",
            socket.local_addr().unwrap()
        ))
        .unwrap();
        Self::new(
            Endpoint::new(EndpointConfig::both(secrets.clients)).unwrap(),
            socket,
        )
    }

    fn client(offset: u16, remote: SocketAddr) -> (Self, ConnectionHandle) {
        let socket = socket(offset);
        let (ep, ch) = Endpoint::new_client(Instant::now(), now_minutes(), config(remote)).unwrap();
        (Self::new(ep, socket), ch)
    }

    fn dial(&mut self, remote: SocketAddr) -> ConnectionHandle {
        self.ep
            .connect(Instant::now(), now_minutes(), config(remote), None, None)
            .unwrap()
    }

    fn step(&mut self) {
        assert_eq!(self.socket.local_addr().unwrap(), self.bound);
        let mut buf = [0; 65_535];
        loop {
            match self.socket.recv_from(&mut buf) {
                Ok((n, from)) => {
                    self.ep.handle_datagram(Instant::now(), from, &buf[..n]);
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) => panic!("UDP receive failed: {e}"),
            }
        }
        let now = Instant::now();
        if self.ep.next_timeout().is_some_and(|t| t <= now) {
            self.ep.handle_timeout(now);
        }
        while let Some(tx) = self.ep.poll_transmit(now) {
            assert_eq!(
                self.socket.send_to(&tx.contents, tx.destination).unwrap(),
                tx.contents.len()
            );
            self.sent.push(tx.contents);
        }
        while let Some(event) = self.ep.poll_event() {
            match event {
                Event::Connected(ch) => {
                    assert!(self.connected.insert(ch));
                }
                Event::ConnectionLost { conn, reason } => {
                    self.lost.insert(conn, reason);
                }
                _ => {}
            }
        }
    }

    fn close(&mut self, ch: ConnectionHandle) {
        self.ep.conn_mut(ch).unwrap().conn_mut().close(
            Instant::now(),
            VarInt::from_u32(7),
            b"experiment close".to_vec().into(),
        );
    }
}

fn drive_until(a: &mut Peer, b: &mut Peer, done: impl Fn(&Peer, &Peer) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        a.step();
        b.step();
        if done(a, b) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "drive timeout; a lost={:?}, b lost={:?}",
            a.lost,
            b.lost
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

// Exercise real flow control and FIN acknowledgement, with byte-exact checking.
fn transfer(
    a: &mut Peer,
    b: &mut Peer,
    ac: ConnectionHandle,
    bc: ConnectionHandle,
    payload: &[u8],
) {
    let id = a.ep.conn_mut(ac).unwrap().open_bi().unwrap();
    let mut written = 0;
    let mut received = Vec::new();
    let mut accepted = false;
    let mut finished = false;
    let mut eof = false;
    let mut reverse_finished = false;
    let mut reverse_eof = false;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if written < payload.len() {
            match a
                .ep
                .conn_mut(ac)
                .unwrap()
                .stream_write(id, &payload[written..])
                .unwrap()
            {
                WriteOutcome::Wrote(n) => written += n,
                WriteOutcome::Blocked => {}
            }
        }
        if written == payload.len() && !finished {
            a.ep.conn_mut(ac).unwrap().stream_finish(id).unwrap();
            finished = true;
        }
        a.step();
        b.step();
        if !accepted {
            if let Some(got) = b.ep.conn_mut(bc).unwrap().accept_bi().unwrap() {
                assert_eq!(got, id);
                accepted = true;
            }
        }
        if accepted && !eof {
            let mut buf = [0; 4096];
            loop {
                match b
                    .ep
                    .conn_mut(bc)
                    .unwrap()
                    .stream_read(id, &mut buf)
                    .unwrap()
                {
                    ReadOutcome::Read(n) => received.extend_from_slice(&buf[..n]),
                    ReadOutcome::Blocked => break,
                    ReadOutcome::Finished => {
                        eof = true;
                        break;
                    }
                }
            }
        }
        if eof && !reverse_finished {
            b.ep.conn_mut(bc).unwrap().stream_finish(id).unwrap();
            reverse_finished = true;
        }
        if reverse_finished && !reverse_eof {
            let mut buf = [0; 1];
            match a
                .ep
                .conn_mut(ac)
                .unwrap()
                .stream_read(id, &mut buf)
                .unwrap()
            {
                ReadOutcome::Finished => reverse_eof = true,
                ReadOutcome::Blocked => {}
                ReadOutcome::Read(_) => panic!("unexpected reverse payload"),
            }
        }
        if eof
            && reverse_eof
            && a.ep.conn_mut(ac).unwrap().send_fin(id) == Some(SendFin::Acked)
            && b.ep.conn_mut(bc).unwrap().send_fin(id) == Some(SendFin::Acked)
        {
            assert_eq!(received, payload);
            a.ep.conn_mut(ac).unwrap().forget_stream(id);
            b.ep.conn_mut(bc).unwrap().forget_stream(id);
            return;
        }
        assert!(Instant::now() < deadline, "transfer timed out");
        std::thread::sleep(Duration::from_millis(1));
    }
}

// After settling legitimate traffic, exercise the exact pre-filter contract:
// a rejected injection queues no datagrams, including with live connections.
fn assert_silent(a: &mut Peer, b: &mut Peer, replay: &[u8]) {
    for _ in 0..150 {
        a.step();
        b.step();
        std::thread::sleep(Duration::from_millis(1));
    }
    let mut unknown_short = vec![0x40; 100];
    unknown_short[1..9].fill(0xff);
    for data in [b"junk".as_slice(), unknown_short.as_slice(), replay] {
        let now = Instant::now();
        assert!(
            b.ep.poll_transmit(now).is_none(),
            "baseline must be quiescent"
        );
        assert_eq!(
            b.ep.handle_datagram(now, a.bound, data),
            DatagramOutcome::Dropped
        );
        assert!(
            b.ep.poll_transmit(now).is_none(),
            "rejected traffic queued a response"
        );
    }
}

#[test]
fn overlapping_outgoing_connections_share_fixed_socket_and_survive_close() {
    let mut b = Peer::server(3100);
    let (mut a, first) = Peer::client(3101, b.bound);
    drive_until(&mut a, &mut b, |a, b| {
        a.connected.len() == 1 && b.connected.len() == 1
    });
    let remote_first = *b.connected.iter().next().unwrap();
    let replay = a.sent[0].clone();
    let second = a.dial(b.bound);
    drive_until(&mut a, &mut b, |a, b| {
        a.connected.len() == 2 && b.connected.len() == 2
    });
    let remote_second = *b.connected.iter().find(|&&ch| ch != remote_first).unwrap();
    transfer(
        &mut a,
        &mut b,
        first,
        remote_first,
        b"old connection still works",
    );
    transfer(
        &mut a,
        &mut b,
        second,
        remote_second,
        &vec![0x71; 512 * 1024],
    );
    assert_silent(&mut a, &mut b, &replay);
    a.close(first);
    drive_until(&mut a, &mut b, |a, b| {
        a.lost.contains_key(&first) && b.lost.contains_key(&remote_first)
    });
    assert!(a.ep.conn_mut(first).is_none());
    transfer(
        &mut b,
        &mut a,
        remote_second,
        second,
        b"survivor opens reverse stream",
    );
    // A new outgoing handshake with an invalid PSK must time out independently.
    let mut bad = config(b.bound);
    bad.psk = config_with_wrong_psk(b.bound).psk;
    let failed =
        a.ep.connect(Instant::now(), now_minutes(), bad, None, None)
            .unwrap();
    let deadline = Instant::now() + Duration::from_secs(40);
    while !a.lost.contains_key(&failed) {
        transfer(
            &mut a,
            &mut b,
            second,
            remote_second,
            b"healthy during failed handshake",
        );
        assert!(Instant::now() < deadline, "bad handshake did not time out");
        std::thread::sleep(Duration::from_millis(25));
    }
    assert_eq!(a.lost[&failed], ConnectionError::TimedOut);
    assert_eq!(
        b.connected.len(),
        2,
        "wrong PSK must not create a connection"
    );
    transfer(
        &mut a,
        &mut b,
        second,
        remote_second,
        b"healthy after handshake timeout",
    );
}

fn config_with_wrong_psk(addr: SocketAddr) -> ClientConfigFile {
    toml::from_str(&format!(
        "client_id='bad'\npsk='{}'\nserver='{addr}'\n",
        "11".repeat(32)
    ))
    .unwrap()
}

#[test]
fn simultaneous_incoming_and_outgoing_share_fixed_socket_without_relaxing_admission() {
    let mut a = Peer::server(3110);
    let mut b = Peer::server(3111);
    let outgoing_a = a.dial(b.bound);
    let outgoing_b = b.dial(a.bound);
    // Neither side has received anything when both attempts are created.
    drive_until(&mut a, &mut b, |a, b| {
        a.connected.len() == 2 && b.connected.len() == 2
    });
    let incoming_a = *a.connected.iter().find(|&&ch| ch != outgoing_a).unwrap();
    let incoming_b = *b.connected.iter().find(|&&ch| ch != outgoing_b).unwrap();
    assert_eq!(a.ep.client_id(incoming_a), Some("experiment"));
    assert_eq!(b.ep.client_id(incoming_b), Some("experiment"));
    assert_eq!(a.ep.client_id(outgoing_a), None);
    assert_eq!(b.ep.client_id(outgoing_b), None);
    transfer(&mut a, &mut b, outgoing_a, incoming_b, b"A initiated");
    transfer(&mut b, &mut a, outgoing_b, incoming_a, b"B initiated");
    let initial_a = a.sent[0].clone();
    let initial_b = b.sent[0].clone();
    assert_silent(&mut a, &mut b, &initial_a);
    assert_silent(&mut b, &mut a, &initial_b);
    a.close(outgoing_a);
    drive_until(&mut a, &mut b, |a, b| {
        a.lost.contains_key(&outgoing_a) && b.lost.contains_key(&incoming_b)
    });
    transfer(
        &mut a,
        &mut b,
        incoming_a,
        outgoing_b,
        b"A sends on B's surviving connection",
    );
    transfer(
        &mut b,
        &mut a,
        outgoing_b,
        incoming_a,
        &vec![0x92; 512 * 1024],
    );
    assert_silent(&mut a, &mut b, &initial_a);
}
