// SPDX-License-Identifier: 0BSD
//! Configuration types and their parsing.
//!
//! This module contains **no filesystem access** — parsing happens from strings
//! only, so the sans-IO core performs no I/O. To load secrets from a file, see
//! `quietquic::config::FileSource`, which reads the file (and warns about
//! group/world-readable permissions) before handing the text here.

use quinn_proto::{TransportConfig, VarInt};
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// Largest library-managed deadline accepted by endpoint configuration.
pub const MAX_ENDPOINT_DURATION: Duration = Duration::from_secs(24 * 60 * 60);

/// Cloneable immutable QUIC transport snapshot.
///
/// Construction always disables unidirectional stream credit because this
/// release exposes bidirectional streams only.
#[derive(Clone)]
pub struct TransportSettings(Arc<TransportConfig>);

impl TransportSettings {
    pub fn new(mut config: TransportConfig) -> Self {
        config.max_concurrent_uni_streams(VarInt::from_u32(0));
        Self(Arc::new(config))
    }

    pub fn as_quinn(&self) -> &TransportConfig {
        &self.0
    }

    pub(crate) fn arc(&self) -> Arc<TransportConfig> {
        self.0.clone()
    }
}

impl From<TransportConfig> for TransportSettings {
    fn from(value: TransportConfig) -> Self {
        Self::new(value)
    }
}

impl std::fmt::Debug for TransportSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TransportSettings(..)")
    }
}

impl Default for TransportSettings {
    fn default() -> Self {
        let mut transport = TransportConfig::default();
        transport.max_idle_timeout(Some(
            Duration::from_secs(60)
                .try_into()
                .expect("60s fits QUIC varint"),
        ));
        transport.keep_alive_interval(Some(Duration::from_secs(20)));
        transport.max_concurrent_bidi_streams(VarInt::from_u32(32));
        transport.stream_receive_window(VarInt::from_u32(1024 * 1024));
        transport.receive_window(VarInt::from_u32(8 * 1024 * 1024));
        transport.send_window(8 * 1024 * 1024);
        Self::new(transport)
    }
}

/// A 32-byte pre-shared key. Zeroized on drop; `Debug` never prints the bytes.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct Psk([u8; 32]);

impl Psk {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Debug for Psk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Psk(***)")
    }
}

impl<'de> Deserialize<'de> for Psk {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        let bytes = hex::decode(&s).map_err(serde::de::Error::custom)?;
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| serde::de::Error::custom("psk must be 32 bytes (64 hex chars)"))?;
        Ok(Psk(arr))
    }
}

/// A single client's identity and PSK, as listed in the server's secrets file.
#[derive(Clone, Deserialize)]
pub struct ClientEntry {
    pub client_id: String,
    pub psk: Psk,
}

/// Server-side secrets: the listen address plus all authorized clients.
#[derive(Clone, Deserialize)]
pub struct ServerSecrets {
    pub listen: SocketAddr,
    pub clients: Vec<ClientEntry>,
}

/// Client-side config: this client's identity, PSK, and the server to dial.
#[derive(Clone, Deserialize)]
pub struct ClientConfigFile {
    pub client_id: String,
    pub psk: Psk,
    pub server: SocketAddr,
    /// Local address to bind the outbound socket to. Optional.
    ///
    /// When omitted (the default) the client binds `0.0.0.0:0` / `[::]:0` — an
    /// ephemeral port on any interface, matching what ordinary QUIC clients do.
    /// **Leave it unset unless you have a reason:** a fixed source port is a
    /// mild fingerprint, and the whole point of this transport is to look
    /// unremarkable.
    ///
    /// Set it when the deployment demands it:
    /// * an egress firewall that only permits UDP from an allowlisted source
    ///   port, or needs a stable port for a stateful pinhole;
    /// * a multi-homed host where traffic must leave a *specific* interface
    ///   (e.g. back up over the VPN, never the metered WAN) — bind that
    ///   interface's address with port `0` to pin the interface but keep an
    ///   ephemeral port;
    /// * NAT traversal that wants a predictable source port.
    ///
    /// Address family must match `server`; mismatches are rejected up front
    /// rather than surfacing as an opaque OS error.
    #[serde(default)]
    pub bind: Option<SocketAddr>,
}

/// Operations an endpoint may initiate. Capabilities are fixed at construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capability {
    Dial,
    Accept,
    Both,
}

impl Capability {
    pub const fn can_dial(self) -> bool {
        matches!(self, Self::Dial | Self::Both)
    }

    pub const fn can_accept(self) -> bool {
        matches!(self, Self::Accept | Self::Both)
    }
}

/// Immutable defaults copied by each newly-created connection.
#[derive(Clone)]
pub struct EndpointConfig {
    pub capability: Capability,
    pub credentials: Vec<ClientEntry>,
    pub transport: TransportSettings,
    pub max_pending_incoming: usize,
    pub outgoing_handshake_timeout: Duration,
    pub incoming_handshake_timeout: Duration,
    pub cleanup_timeout: Duration,
    /// Maximum endpoint events and datagrams processed for one connection in a pass.
    pub max_connection_work: usize,
    /// Maximum datagrams buffered by the sans-I/O endpoint for its caller.
    pub max_outbound_datagrams: usize,
}

impl std::fmt::Debug for EndpointConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EndpointConfig")
            .field("capability", &self.capability)
            .field("credentials", &self.credentials.len())
            .field("transport", &self.transport)
            .field("max_pending_incoming", &self.max_pending_incoming)
            .field(
                "outgoing_handshake_timeout",
                &self.outgoing_handshake_timeout,
            )
            .field(
                "incoming_handshake_timeout",
                &self.incoming_handshake_timeout,
            )
            .field("cleanup_timeout", &self.cleanup_timeout)
            .field("max_connection_work", &self.max_connection_work)
            .field("max_outbound_datagrams", &self.max_outbound_datagrams)
            .finish()
    }
}

impl EndpointConfig {
    pub fn dial() -> Self {
        Self {
            capability: Capability::Dial,
            ..Self::default()
        }
    }

    pub fn accept(credentials: Vec<ClientEntry>) -> Self {
        Self {
            capability: Capability::Accept,
            credentials,
            ..Self::default()
        }
    }

    pub fn both(credentials: Vec<ClientEntry>) -> Self {
        Self {
            capability: Capability::Both,
            credentials,
            ..Self::default()
        }
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.capability.can_accept() && self.credentials.is_empty() {
            return Err(ConfigError::Invalid(
                "accept capability requires credentials".into(),
            ));
        }
        if self.max_pending_incoming == 0 {
            return Err(ConfigError::Invalid(
                "max_pending_incoming must be positive".into(),
            ));
        }
        if self.max_connection_work == 0 {
            return Err(ConfigError::Invalid(
                "max_connection_work must be positive".into(),
            ));
        }
        if self.max_outbound_datagrams == 0 {
            return Err(ConfigError::Invalid(
                "max_outbound_datagrams must be positive".into(),
            ));
        }
        for (name, value) in [
            (
                "outgoing_handshake_timeout",
                self.outgoing_handshake_timeout,
            ),
            (
                "incoming_handshake_timeout",
                self.incoming_handshake_timeout,
            ),
            ("cleanup_timeout", self.cleanup_timeout),
        ] {
            if value.is_zero() {
                return Err(ConfigError::Invalid(format!(
                    "{name} must be finite and positive"
                )));
            }
            if value > MAX_ENDPOINT_DURATION {
                return Err(ConfigError::Invalid(format!(
                    "{name} exceeds the 24 hour maximum"
                )));
            }
        }
        validate_credentials(&self.credentials)
    }
}

impl Default for EndpointConfig {
    fn default() -> Self {
        Self {
            capability: Capability::Dial,
            credentials: Vec::new(),
            transport: TransportSettings::default(),
            max_pending_incoming: 128,
            outgoing_handshake_timeout: Duration::from_secs(10),
            incoming_handshake_timeout: Duration::from_secs(10),
            cleanup_timeout: Duration::from_secs(10),
            max_connection_work: 64,
            max_outbound_datagrams: 256,
        }
    }
}

pub(crate) fn validate_credentials(entries: &[ClientEntry]) -> Result<(), ConfigError> {
    use std::collections::HashSet;
    let mut ids = HashSet::new();
    let mut psks = HashSet::new();
    for entry in entries {
        if entry.client_id.trim().is_empty() {
            return Err(ConfigError::Invalid("client_id must not be empty".into()));
        }
        if !ids.insert(entry.client_id.clone()) {
            return Err(ConfigError::Invalid(format!(
                "duplicate client_id {:?}",
                entry.client_id
            )));
        }
        if !psks.insert(*entry.psk.as_bytes()) {
            return Err(ConfigError::Invalid(format!(
                "duplicate PSK makes client identity ambiguous (client_id {:?})",
                entry.client_id
            )));
        }
    }
    Ok(())
}

/// Errors that can occur while loading or parsing config/secrets.
///
/// The `Io` variant exists for consumers that load config from a file (see
/// `quietquic::config::FileSource`); this crate itself never performs I/O.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("parse: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("invalid configuration: {0}")]
    Invalid(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn psk_parses_from_hex() {
        let toml = r#"
listen = "127.0.0.1:4443"
[[clients]]
client_id = "a"
psk = "0000000000000000000000000000000000000000000000000000000000000001"
"#;
        let s: ServerSecrets = toml::from_str(toml).unwrap();
        assert_eq!(s.clients.len(), 1);
        assert_eq!(s.clients[0].client_id, "a");
        assert_eq!(s.clients[0].psk.as_bytes()[31], 1);
    }

    #[test]
    fn psk_debug_is_redacted() {
        let p = Psk([1u8; 32]);
        assert_eq!(format!("{:?}", p), "Psk(***)");
    }

    #[test]
    fn bad_hex_rejected() {
        let toml = r#"
listen = "127.0.0.1:4443"
[[clients]]
client_id = "a"
psk = "xyz"
"#;
        assert!(toml::from_str::<ServerSecrets>(toml).is_err());
    }

    #[test]
    fn config_error_can_report_semantic_validation() {
        assert_eq!(
            ConfigError::Invalid("duplicate client_id".into()).to_string(),
            "invalid configuration: duplicate client_id"
        );
    }

    #[test]
    fn endpoint_deadlines_and_caps_must_be_positive() {
        for set_deadline in [
            |cfg: &mut EndpointConfig, value| cfg.outgoing_handshake_timeout = value,
            |cfg: &mut EndpointConfig, value| cfg.incoming_handshake_timeout = value,
            |cfg: &mut EndpointConfig, value| cfg.cleanup_timeout = value,
        ] {
            let mut cfg = EndpointConfig::dial();
            set_deadline(&mut cfg, Duration::ZERO);
            assert!(matches!(cfg.validate(), Err(ConfigError::Invalid(_))));

            let mut cfg = EndpointConfig::dial();
            set_deadline(&mut cfg, MAX_ENDPOINT_DURATION + Duration::from_nanos(1));
            assert!(matches!(cfg.validate(), Err(ConfigError::Invalid(_))));

            let mut cfg = EndpointConfig::dial();
            set_deadline(&mut cfg, MAX_ENDPOINT_DURATION);
            assert!(
                cfg.validate().is_ok(),
                "the finite upper bound is inclusive"
            );
        }

        let mut cfg = EndpointConfig::dial();
        cfg.max_pending_incoming = 0;
        assert!(matches!(cfg.validate(), Err(ConfigError::Invalid(_))));

        let mut cfg = EndpointConfig::dial();
        cfg.max_outbound_datagrams = 0;
        assert!(matches!(cfg.validate(), Err(ConfigError::Invalid(_))));
    }

    #[test]
    fn accepting_requires_credentials() {
        assert!(EndpointConfig::dial().validate().is_ok());
        assert!(matches!(
            EndpointConfig::accept(Vec::new()).validate(),
            Err(ConfigError::Invalid(_))
        ));
    }

    #[test]
    fn capabilities_are_fixed_and_explicit() {
        assert!(Capability::Dial.can_dial());
        assert!(!Capability::Dial.can_accept());
        assert!(Capability::Accept.can_accept());
        assert!(!Capability::Accept.can_dial());
        assert!(Capability::Both.can_dial());
        assert!(Capability::Both.can_accept());
    }

    #[test]
    fn custom_transport_always_disables_unidirectional_credit() {
        let mut transport = TransportConfig::default();
        transport.max_concurrent_uni_streams(VarInt::from_u32(17));
        let settings = TransportSettings::new(transport);
        let debug = format!("{:?}", settings.as_quinn());
        assert!(debug.contains("max_concurrent_uni_streams: 0"), "{debug}");
    }
}
