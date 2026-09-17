//! Convenience one-connection client built on the shared endpoint driver.
use crate::config::ClientConfigFile;
use crate::conn::{Connection, ConnectionError};
use crate::endpoint::{ConnectOptions, Endpoint, EndpointConfig, EndpointError};
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

/// Failure to establish a convenience client connection.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClientError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("connect timed out")]
    TimedOut,
    #[error("endpoint: {0}")]
    Endpoint(EndpointError),
}

/// A convenience client; retain an Endpoint instead when sharing a socket.
pub struct Client;
impl Client {
    pub async fn connect(cfg: ClientConfigFile) -> Result<Connection, ClientError> {
        connect(cfg).await
    }
}

pub async fn connect(cfg: ClientConfigFile) -> Result<Connection, ClientError> {
    connect_with_timeout(cfg, Duration::from_secs(10)).await
}

async fn connect_with_timeout(
    cfg: ClientConfigFile,
    timeout: Duration,
) -> Result<Connection, ClientError> {
    let local: SocketAddr = cfg.bind.unwrap_or_else(|| {
        if cfg.server.is_ipv4() {
            (Ipv4Addr::UNSPECIFIED, 0).into()
        } else {
            (Ipv6Addr::UNSPECIFIED, 0).into()
        }
    });
    if local.is_ipv4() != cfg.server.is_ipv4() {
        return Err(ClientError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "local and remote addresses use different IP versions",
        )));
    }
    let endpoint = Endpoint::bind(local, EndpointConfig::dial())
        .await
        .map_err(ClientError::Endpoint)?;
    let attempt = endpoint.connect_with(
        cfg,
        ConnectOptions {
            handshake_timeout: Some(timeout),
            ..Default::default()
        },
    );
    // The attempt, then returned Connection, retain the socket independently.
    drop(endpoint);
    attempt.await.map_err(|error| match error {
        EndpointError::Connection(ConnectionError::TimedOut) => ClientError::TimedOut,
        other => ClientError::Endpoint(other),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClientConfigFile;
    use tokio::net::UdpSocket;

    fn test_bind_addr() -> SocketAddr {
        let ip = std::env::var("QUIETQUIC_TEST_ADDR").unwrap_or_else(|_| "127.0.0.1".into());
        let port = std::env::var("QUIETQUIC_TEST_PORT_BASE")
            .ok()
            .and_then(|base| base.parse::<u16>().ok())
            .and_then(|base| base.checked_add(2000))
            .unwrap_or(0);
        SocketAddr::new(
            ip.parse()
                .expect("QUIETQUIC_TEST_ADDR must be an IP address"),
            port,
        )
    }

    /// A server that never sends a single byte back must not hang `connect`
    /// forever: with a short internal timeout, `connect_with_timeout` must
    /// return `ClientError::TimedOut` well within a bounded wall-clock budget.
    ///
    /// Binds a real UDP socket and drops it immediately: the port is silent
    /// (RST/ICMP-unreachable is *not* guaranteed to surface to the client's
    /// unconnected UDP socket on all platforms), which is exactly the "server
    /// never responds" case the timeout guards against — no Initial ack, no
    /// handshake progress, ever.
    #[tokio::test]
    async fn connect_times_out_when_nothing_answers() {
        let silent_addr = {
            let socket = UdpSocket::bind(test_bind_addr()).await.unwrap();
            let addr = socket.local_addr().unwrap();
            drop(socket);
            addr
        };

        let psk_hex = "0000000000000000000000000000000000000000000000000000000000000009";
        let cfg: ClientConfigFile = toml::from_str(&format!(
            "client_id=\"a\"\npsk=\"{psk_hex}\"\nserver=\"{silent_addr}\"\n"
        ))
        .unwrap();

        let start = std::time::Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(12),
            connect_with_timeout(cfg, Duration::from_millis(500)),
        )
        .await
        .expect("connect_with_timeout itself must not hang past the outer test guard");

        match result {
            Err(ClientError::TimedOut) => {}
            Err(other) => panic!("expected ClientError::TimedOut, got a different error: {other}"),
            Ok(_) => panic!("expected ClientError::TimedOut, but connect unexpectedly succeeded"),
        }
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "connect should have returned promptly after its short internal timeout, took {:?}",
            start.elapsed()
        );
    }
}
