// SPDX-License-Identifier: 0BSD
//! Dial two peers from one fixed UDP socket.
//!
//! Usage: `shared_endpoint LOCAL_ADDR CLIENT_A.toml CLIENT_B.toml`

use quietquic::config::ClientConfigFile;
use quietquic::endpoint::{Endpoint, EndpointConfig};
use std::error::Error;
use std::net::SocketAddr;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let local: SocketAddr = args.next().ok_or("missing LOCAL_ADDR")?.parse()?;
    let first: ClientConfigFile = toml::from_str(&std::fs::read_to_string(
        args.next().ok_or("missing CLIENT_A.toml")?,
    )?)?;
    let second: ClientConfigFile = toml::from_str(&std::fs::read_to_string(
        args.next().ok_or("missing CLIENT_B.toml")?,
    )?)?;
    if args.next().is_some() {
        return Err("unexpected extra argument".into());
    }

    let endpoint = Endpoint::bind(local, EndpointConfig::dial()).await?;
    println!("both connections use {}", endpoint.local_addr());

    let (first, second) = tokio::try_join!(endpoint.connect(first), endpoint.connect(second))?;
    send(&first, b"first peer").await?;
    send(&second, b"second peer").await?;

    endpoint.close(0, b"example complete")?;
    endpoint.wait_closed().await;
    Ok(())
}

async fn send(
    connection: &quietquic::conn::Connection,
    message: &[u8],
) -> Result<(), quietquic::conn::ConnError> {
    let (mut send, recv) = connection.open_bi().await?;
    send.write_all(message).await?;
    send.finish_and_wait().await?;
    drop((send, recv));
    Ok(())
}
