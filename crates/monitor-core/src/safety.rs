use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TargetSafetyError {
    #[error("unsupported URL scheme: {0}")]
    UnsupportedScheme(String),
    #[error("target did not resolve")]
    NoAddresses,
    #[error("target resolves to non-public address: {0}")]
    NonPublicAddress(IpAddr),
}

/// Validates that a URL scheme and every DNS answer are safe for outbound probing.
///
/// # Errors
///
/// Returns an error for unsupported schemes, empty DNS answers, or any non-public
/// address. Rejecting the complete answer set prevents DNS rebinding fallback.
pub fn validate_public_target(
    scheme: &str,
    _host: &str,
    addresses: &[IpAddr],
) -> Result<(), TargetSafetyError> {
    if !matches!(scheme, "http" | "https") {
        return Err(TargetSafetyError::UnsupportedScheme(scheme.to_owned()));
    }
    if addresses.is_empty() {
        return Err(TargetSafetyError::NoAddresses);
    }
    for address in addresses {
        if !is_public(*address) {
            return Err(TargetSafetyError::NonPublicAddress(*address));
        }
    }
    Ok(())
}

fn is_public(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(ip) => is_public_v4(ip),
        IpAddr::V6(ip) => is_public_v6(ip),
    }
}

fn is_public_v4(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();
    !(ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_unspecified()
        || ip.is_multicast()
        || ip.is_broadcast()
        || ip.is_documentation()
        || octets[0] == 0
        || octets[0] >= 240
        || (octets[0] == 100 && (64..=127).contains(&octets[1]))
        || (octets[0] == 192 && octets[1] == 0 && octets[2] == 0)
        || (octets[0] == 192 && octets[1] == 88 && octets[2] == 99)
        || (octets[0] == 198 && (octets[1] == 18 || octets[1] == 19)))
}

fn is_public_v6(ip: Ipv6Addr) -> bool {
    let segments = ip.segments();
    !(ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        // IPv4-mapped IPv6 is rejected even when the embedded IPv4 is public.
        // Transports can reinterpret it as IPv4, making family enforcement ambiguous.
        || has_v6_prefix(ip, Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0, 0), 96)
        // Only currently allocated global-unicast space is accepted. This also
        // rejects translation, discard-only, deprecated site-local, and reserved space.
        || (segments[0] & 0xe000) != 0x2000
        || has_v6_prefix(ip, Ipv6Addr::new(0x0100, 0, 0, 0, 0, 0, 0, 0), 64)
        || has_v6_prefix(ip, Ipv6Addr::new(0x2001, 0, 0, 0, 0, 0, 0, 0), 23)
        || has_v6_prefix(ip, Ipv6Addr::new(0x2002, 0, 0, 0, 0, 0, 0, 0), 16)
        || has_v6_prefix(ip, Ipv6Addr::new(0x3fff, 0, 0, 0, 0, 0, 0, 0), 20)
        || (segments[0] & 0xfe00) == 0xfc00
        || (segments[0] & 0xffc0) == 0xfe80
        || (segments[0] & 0xffc0) == 0xfec0
        || (segments[0] == 0x2001 && segments[1] == 0x0db8))
}

fn has_v6_prefix(ip: Ipv6Addr, prefix: Ipv6Addr, prefix_length: u32) -> bool {
    let mask = u128::MAX << (128 - prefix_length);
    u128::from(ip) & mask == u128::from(prefix) & mask
}
