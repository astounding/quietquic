// SPDX-License-Identifier: 0BSD
//! A minimal cloaked echo server, for manual and cross-host testing.
//!
//! Binds, accepts ONE connection, accepts one bidirectional stream, reads it to
//! end, echoes the bytes back, and exits. Unauthorized peers get nothing — the
//! process simply never sees them.
//!
//! ```text
//! cargo run --example echo_server -- <listen_addr> <psk_hex> [client_id]
//! cargo run --example echo_server -- 0.0.0.0:29411 $(printf '00%.0s' {1..32})
//! ```
//!
//! `psk_hex` is 64 hex characters (32 bytes) and must match the client's.

use std::time::Duration;

use quietquic::config::ServerSecrets;
use quietquic::server::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let listen = args
        .next()
        .ok_or("usage: echo_server <listen_addr> <psk_hex> [client_id]")?;
    let psk = args.next().ok_or("missing <psk_hex> (64 hex chars)")?;
    let client_id = args.next().unwrap_or_else(|| "peer".to_string());

    let secrets: ServerSecrets = toml::from_str(&format!(
        "listen = \"{listen}\"\n[[clients]]\nclient_id = \"{client_id}\"\npsk = \"{psk}\"\n"
    ))?;

    let server = Server::bind(secrets).await?;
    println!("listening on {} (udp)", server.local_addr());
    println!("waiting for an authorized peer; anything else is ignored silently");

    // Bound the wait so a manual run cannot hang forever.
    let conn = tokio::time::timeout(Duration::from_secs(120), server.accept())
        .await
        .map_err(|_| "timed out waiting for a connection")?
        .ok_or("server driver stopped")?;
    println!("accepted connection from {}", conn.remote_address());

    let (mut send, mut recv) = conn.accept_bi().await?;
    let got = recv.read_to_end(1024 * 1024).await?;
    println!(
        "received {} bytes: {:?}",
        got.len(),
        String::from_utf8_lossy(&got)
    );

    // Echo back on the same stream. Safe because this is strictly sequential —
    // read to end, THEN write. Concurrent read+write on one stream is not
    // supported; use two streams for full duplex.
    send.write_all(&got).await?;
    send.finish().await?;
    println!("echoed {} bytes back", got.len());

    conn.close(0, b"").await?;
    Ok(())
}
