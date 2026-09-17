//! Accept-only convenience wrapper around a shared endpoint.
use crate::config::ServerSecrets;
pub use crate::conn::{
    ConnError, Connection, ConnectionError, QuinnHandle, RecvStream, SendStream,
};
use crate::endpoint::{Endpoint, EndpointConfig, EndpointError};
use std::io;
use std::net::SocketAddr;

/// An accept-only endpoint. Clone or use `endpoint()` for explicit control.
#[derive(Clone)]
pub struct Server {
    endpoint: Endpoint,
}

impl Server {
    pub async fn bind(secrets: ServerSecrets) -> io::Result<Self> {
        let endpoint = Endpoint::bind(secrets.listen, EndpointConfig::accept(secrets.clients))
            .await
            .map_err(|error| io::Error::other(error.to_string()))?;
        Ok(Self { endpoint })
    }
    pub fn local_addr(&self) -> SocketAddr {
        self.endpoint.local_addr()
    }
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }
    /// Compatibility convenience. Use `accept_result` to distinguish shutdown
    /// from endpoint failure.
    pub async fn accept(&self) -> Option<Connection> {
        self.accept_result().await.ok().flatten()
    }
    pub async fn accept_result(&self) -> Result<Option<Connection>, EndpointError> {
        self.endpoint.accept().await
    }
}
