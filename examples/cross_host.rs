// SPDX-License-Identifier: 0BSD
//! Bounded manual cross-host validation; not a production server.
//!
//! `cross_host probe-server BIND`
//! `cross_host probe-client BIND REMOTE`
//! `cross_host server BIND PSK_FILE`
//! `cross_host client BIND REMOTE PSK_FILE`
//! PSK_FILE contains one temporary 64-character hex key; never log that file.

use quietquic::config::{ClientConfigFile, ServerSecrets};
use quietquic::conn::{ConnError, Connection, ConnectionError, RecvStream, SendStream};
use quietquic::{Endpoint, EndpointConfig};
use std::error::Error;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::timeout;

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const STEP: Duration = Duration::from_secs(45);
const PROBE: &[u8] = b"quietquic-cross-host-reachability-v1";

fn payload(size: usize, seed: u8) -> Vec<u8> {
    (0..size)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

async fn duplex(streams: (SendStream, RecvStream), size: usize, server: bool) -> Result<()> {
    let (mut send, mut recv) = streams;
    let outbound = payload(size, if server { 71 } else { 19 });
    let expected = payload(size, if server { 19 } else { 71 });
    let ((), bytes) = timeout(STEP, async {
        tokio::try_join!(
            async {
                send.write_all(&outbound).await?;
                send.finish_and_wait().await?;
                Ok::<_, Box<dyn Error>>(())
            },
            async { Ok::<_, Box<dyn Error>>(recv.read_to_end(size).await?) },
        )
    })
    .await??;
    if bytes != expected {
        return Err("payload mismatch".into());
    }
    println!("PASS duplex bytes_each_direction={size} FIN_acknowledged=true server={server}");
    Ok(())
}

async fn initial(conn: &Connection, server: bool) -> Result<()> {
    let streams = if server {
        timeout(STEP, conn.accept_bi()).await??
    } else {
        timeout(STEP, conn.open_bi()).await??
    };
    duplex(streams, 1024 * 1024, server).await?;
    // Independently initiated by the public host, over the CGNAT-originated connection.
    let streams = if server {
        timeout(STEP, conn.open_bi()).await??
    } else {
        timeout(STEP, conn.accept_bi()).await??
    };
    duplex(streams, 512 * 1024, server).await?;
    ready(conn, server).await?;
    println!("PASS streams_initiated_by_both_hosts server={server}");
    Ok(())
}

// Application barrier: the sender has completed its payload FIN waits. The
// marker's own FIN need not be acknowledged before deliberate shutdown.
async fn ready(conn: &Connection, sending: bool) -> Result<()> {
    timeout(STEP, async {
        if sending {
            let (mut send, _recv) = conn.open_bi().await?;
            send.write_all(b"ready").await?;
            send.finish().await?;
        } else {
            let (_send, mut recv) = conn.accept_bi().await?;
            if recv.read_to_end(5).await? != b"ready" {
                return Err("invalid completion marker".into());
            }
        }
        Ok::<_, Box<dyn Error>>(())
    })
    .await?
}

fn observe_close(reason: ConnectionError, closed: &watch::Sender<bool>) -> Result<bool> {
    if !matches!(reason, ConnectionError::ApplicationClosed { code: 51, .. }) {
        return Err(format!("unexpected first close: {reason:?}").into());
    }
    closed.send_replace(true);
    println!("PASS first_connection_closed_by_peer code=51");
    Ok(false)
}

async fn serve_conn(conn: Connection, closed: watch::Sender<bool>) -> Result<bool> {
    initial(&conn, true).await?;
    tokio::select! {
        reason = conn.closed() => observe_close(reason, &closed),
        stream = conn.accept_bi() => {
            let stream = match stream {
                Ok(stream) => stream,
                Err(ConnError::ConnectionLost { reason }) => return observe_close(reason, &closed),
                Err(error) => return Err(error.into()),
            };
            let mut observed = closed.subscribe();
            timeout(STEP, async {
                while !*observed.borrow_and_update() { observed.changed().await?; }
                Ok::<_, watch::error::RecvError>(())
            }).await??;
            duplex(stream, 2 * 1024 * 1024, true).await?;
            ready(&conn, false).await?;
            println!("PASS sibling_transfer_after_observed_peer_close");
            Ok(true)
        }
    }
}

async fn transport(server: bool, bind: SocketAddr, remote: SocketAddr, key: &str) -> Result<()> {
    if bind.port() == 0 {
        return Err("select an explicit fixed bind port".into());
    }
    let key = key.trim();
    if key.len() != 64 || !key.bytes().all(|c| c.is_ascii_hexdigit()) {
        return Err("PSK file must contain 64 hex characters".into());
    }
    let mut config = if server {
        let secrets: ServerSecrets = toml::from_str(&format!(
            "listen='{bind}'\n[[clients]]\nclient_id='cross-host'\npsk='{key}'\n"
        ))?;
        EndpointConfig::accept(secrets.clients)
    } else {
        EndpointConfig::dial()
    };
    config.cleanup_timeout = Duration::from_secs(2);
    let endpoint = Endpoint::bind(bind, config).await?;
    let local = endpoint.local_addr();
    println!("READY transport server={server} bound={local}");
    if server {
        let first = timeout(STEP, endpoint.accept())
            .await??
            .ok_or("accept ended")?;
        let second = timeout(STEP, endpoint.accept())
            .await??
            .ok_or("accept ended")?;
        println!(
            "PEERS first={} second={}",
            first.remote_address(),
            second.remote_address()
        );
        if first.remote_address() != second.remote_address() {
            return Err("connections used different observed source endpoints".into());
        }
        let (closed, _) = watch::channel(false);
        let (a, b) = tokio::try_join!(
            serve_conn(first, closed.clone()),
            serve_conn(second, closed)
        )?;
        if a == b {
            return Err("expected exactly one surviving connection".into());
        }
    } else {
        let cfg: ClientConfigFile = toml::from_str(&format!(
            "client_id='cross-host'\npsk='{key}'\nserver='{remote}'\n"
        ))?;
        let (first, second) = timeout(STEP, async {
            tokio::try_join!(endpoint.connect(cfg.clone()), endpoint.connect(cfg))
        })
        .await??;
        println!("PASS overlapping_connections bound={local} remote={remote}");
        tokio::try_join!(initial(&first, false), initial(&second, false))?;
        first.close(51, b"cross-host sibling isolation").await?;
        timeout(STEP, first.closed()).await?;
        let stream = timeout(STEP, second.open_bi()).await??;
        duplex(stream, 2 * 1024 * 1024, false).await?;
        ready(&second, true).await?;
        // Keep the surviving connection alive until the server has observed its
        // final FIN acknowledgement and deliberately terminates the run.
        let reason = timeout(STEP, second.closed()).await?;
        if !matches!(reason, ConnectionError::ApplicationClosed { code: 0, .. }) {
            return Err(format!("unexpected final close: {reason:?}").into());
        }
    }
    if endpoint.local_addr() != local {
        return Err("local socket changed".into());
    }
    endpoint.close(0, b"cross-host validation complete")?;
    timeout(STEP, endpoint.wait_closed()).await?;
    let rebound = std::net::UdpSocket::bind(local)?;
    println!(
        "PASS endpoint_shutdown_and_rebind bound={}",
        rebound.local_addr()?
    );
    println!("PASS cross_host server={server}");
    Ok(())
}

async fn run() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).ok_or("missing mode")?.as_str();
    let bind: SocketAddr = args.get(2).ok_or("missing BIND")?.parse()?;
    match mode {
        "probe-server" => {
            let socket = tokio::net::UdpSocket::bind(bind).await?;
            println!("READY UDP_probe bound={}", socket.local_addr()?);
            let mut buf = [0; 256];
            loop {
                let (n, from) = socket.recv_from(&mut buf).await?;
                if &buf[..n] == PROBE {
                    socket.send_to(PROBE, from).await?;
                    println!("PASS UDP_probe observed_client={from}");
                    break;
                }
            }
        }
        "probe-client" => {
            let remote: SocketAddr = args.get(3).ok_or("missing REMOTE")?.parse()?;
            let socket = tokio::net::UdpSocket::bind(bind).await?;
            socket.send_to(PROBE, remote).await?;
            let mut buf = [0; 256];
            let (n, from) = timeout(Duration::from_secs(5), socket.recv_from(&mut buf)).await??;
            if from != remote || &buf[..n] != PROBE {
                return Err("UDP probe mismatch".into());
            }
            println!(
                "PASS UDP_probe local={} remote={from}",
                socket.local_addr()?
            );
        }
        "server" => {
            transport(
                true,
                bind,
                bind,
                &std::fs::read_to_string(args.get(3).ok_or("missing PSK_FILE")?)?,
            )
            .await?
        }
        "client" => {
            transport(
                false,
                bind,
                args.get(3).ok_or("missing REMOTE")?.parse()?,
                &std::fs::read_to_string(args.get(4).ok_or("missing PSK_FILE")?)?,
            )
            .await?
        }
        _ => return Err("expected probe-server, probe-client, server, or client".into()),
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    timeout(Duration::from_secs(180), run()).await?
}
