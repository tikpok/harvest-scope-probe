use std::{sync::Arc, time::Duration};

use rustls::{
    client::{danger::ServerCertVerifier, WebPkiServerVerifier},
    pki_types::{pem::PemObject, CertificateDer, ServerName, UnixTime},
    RootCertStore,
};

// Freeze validation at capture time so certificate expiry cannot change the result.
fn verify_chain(host: &str, pem: &[u8]) -> Result<(), rustls::Error> {
    let chain = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .expect("valid public certificate fixture");
    let roots = RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    let verifier = WebPkiServerVerifier::builder_with_provider(
        Arc::new(roots),
        Arc::new(rustls::crypto::ring::default_provider()),
    )
    .build()
    .expect("configured trust store");
    verifier
        .verify_server_cert(
            &chain[0],
            &chain[1..],
            &ServerName::try_from(host).expect("valid DNS name"),
            &[],
            UnixTime::since_unix_epoch(Duration::from_secs(1_792_627_200)),
        )
        .map(|_| ())
}

#[test]
fn rejects_emudhra_direct_and_cross_signed_chains() {
    let mut accepted = Vec::new();
    let mut checked = 0;
    for entry in std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/tls"))
        .expect("TLS fixtures")
    {
        let path = entry.expect("fixture entry").path();
        let host = path.file_stem().unwrap().to_str().unwrap();
        if !host.ends_with("-emudhra.we-have-a-problem.com") {
            continue;
        }
        checked += 1;
        match verify_chain(host, &std::fs::read(&path).expect("public chain")) {
            Ok(()) => accepted.push(host.to_owned()),
            Err(error) => {
                let detail = format!("{error:?}");
                assert!(
                    detail.contains("CertDistrusted") || detail.contains("UnknownIssuer"),
                    "{host} must fail because of distrust, got {detail}"
                );
            }
        }
    }
    assert_eq!(checked, 12, "all disclosed chain variants must be present");
    assert!(accepted.is_empty(), "distrusted chains accepted: {accepted:?}");
}

#[test]
fn accepts_legitimate_public_https_chain() {
    verify_chain(
        "www.python.org",
        include_bytes!("fixtures/tls/www.python.org.chain"),
    )
    .expect("unrelated HTTPS must continue to validate");
}
