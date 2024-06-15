use std::{net::{Ipv4Addr, SocketAddrV4}, sync::Arc};

use tokio::net::UdpSocket;

use super::*;

#[tokio::test]
async fn event_tick() {
    let mut event_manager = event::TimeboundEventManager::new(Duration::from_millis(100));
    event_manager.put_event(
        event::TimeboundAction::RemoveBreadcrumb(message::NumId(0)),
        Duration::from_millis(100),
    );
    event_manager.put_event(
        event::TimeboundAction::SendHeartbeats,
        Duration::from_millis(220),
    );
    event_manager.put_event(
        event::TimeboundAction::SendHeartbeats,
        Duration::from_millis(490),
    );
    event_manager.put_event(
        event::TimeboundAction::SendHeartbeats,
        Duration::from_millis(480),
    );
    for i in 0..4 {
        let events = event_manager.tick().await;
        match i {
            0 => {
                let events = events.expect("Should be 1 event after 1st tick");
                assert_eq!(1, events.len());
                assert!(
                    matches!(events[0], event::TimeboundAction::RemoveBreadcrumb(_)),
                    "Actual was {:?}",
                    events[0]
                );
            }
            1 => {
                let events = events.expect("Should be 1 event after 2nd tick");
                assert_eq!(1, events.len());
                assert!(
                    matches!(events[0], event::TimeboundAction::SendHeartbeats),
                    "Actual was {:?}",
                    events[0]
                );
            }
            2 => {
                assert!(events.is_none());
            }
            3 => {
                let events = events.expect("Should be 2 events after 4th tick");
                assert_eq!(2, events.len());
                assert!(
                    matches!(events[0], event::TimeboundAction::SendHeartbeats),
                    "Actual was {:?}",
                    events[0]
                );
                assert!(
                    matches!(events[1], event::TimeboundAction::SendHeartbeats),
                    "Actual was {:?}",
                    events[1]
                );
            }
            _ => {}
        }
    }
}

fn key_store() -> (crypto::KeyStore, event::TimeboundEventManager) {
    let key_store = crypto::KeyStore::new();
    let event_manager = event::TimeboundEventManager::new(Duration::from_secs(1));
    (key_store, event_manager)
}

fn generate_public_key(
    peer_id: message::NumId,
    key_store: &mut crypto::KeyStore,
    event_manager: &mut event::TimeboundEventManager,
) -> Vec<u8> {
    key_store
        .public_key(peer_id, event_manager)
        .expect("Some when no symmetric key exists")
}

#[tokio::test]
async fn crypto_public_key() {
    let peer_id = message::NumId(0);
    let (mut key_store, mut event_manager) = key_store();
    let public_key = generate_public_key(peer_id, &mut key_store, &mut event_manager);
    let public_key2 = generate_public_key(peer_id, &mut key_store, &mut event_manager);
    assert_eq!(public_key, public_key2);
    let public_key3 = generate_public_key(message::NumId(1), &mut key_store, &mut event_manager);
    assert_ne!(public_key3, public_key);
    assert_ne!(public_key3, public_key2);
    key_store
        .agree(peer_id, public_key, &mut event_manager)
        .unwrap();
    assert!(key_store.public_key(peer_id, &mut event_manager).is_none());
}

#[tokio::test]
async fn crypto_agree_transform() {
    let peer_id = message::NumId(0);
    let (mut key_store, mut event_manager) = key_store();
    generate_public_key(peer_id, &mut key_store, &mut event_manager);
    key_store
        .agree(peer_id, vec![8u8; 16], &mut event_manager)
        .expect_err("Wrong public key should return Err");
    let public_key = generate_public_key(peer_id, &mut key_store, &mut event_manager);
    key_store
        .agree(peer_id, public_key.clone(), &mut event_manager)
        .expect("Agreement should succeed");
    assert!(key_store.agreement_exists(&peer_id));
    let peer_id2 = message::NumId(1);
    key_store
        .agree(peer_id2, public_key, &mut event_manager)
        .expect("No private key should fail silently");
    assert!(!key_store.agreement_exists(&peer_id2));

    let plaintext = b"Test message".to_vec();
    let mut payload = plaintext.clone();
    assert!(matches!(
        key_store.transform(peer_id2, &mut payload, crypto::Direction::Encode),
        Err(crypto::Error::NoKey)
    ));
    let nonce = key_store
        .transform(peer_id, &mut payload, crypto::Direction::Encode)
        .expect("Encoding should succeed");
    let mut payload_clone = payload.clone();
    let mut payload_tampered = payload.clone();
    payload_tampered[0] += 1;
    let mut nonce_tampered = nonce.clone();
    nonce_tampered[0] += 1;
    let err_payload = key_store.transform(
        peer_id,
        &mut payload_tampered,
        crypto::Direction::Decode(&nonce),
    );
    assert!(
        matches!(err_payload, Err(crypto::Error::Unspecified)),
        "Actual was {:?}",
        err_payload
    );
    let err_nonce = key_store.transform(
        peer_id,
        &mut payload_clone,
        crypto::Direction::Decode(&nonce_tampered),
    );
    assert!(
        matches!(err_nonce, Err(crypto::Error::Unspecified)),
        "Actual was {:?}",
        err_nonce
    );
    payload = key_store
        .transform(peer_id, &mut payload, crypto::Direction::Decode(&nonce))
        .expect("Decoding should succeed");
    assert_eq!(plaintext, payload);
}

#[tokio::test]
async fn stage_message() {
    let encrypted_message = [
        155, 216, 10, 194, 68, 110, 26, 164, 234, 6, 95, 224, 9, 145, 130, 173, 220, 176, 86, 76,
        34, 118, 129, 65, 23, 96, 206, 249, 154, 143, 94, 159, 34, 177, 223, 61, 38, 8, 183, 19,
        112, 194, 133, 87, 51, 51, 248, 236, 233, 114, 255, 47, 40, 196, 1, 196, 2, 200, 125, 146,
        8, 87, 183, 88, 241, 152, 226, 174, 142, 252, 105, 99, 63, 23, 227, 51, 219, 43, 90, 135,
        163, 193, 205, 155, 211, 163, 130, 138, 5, 172, 103, 125, 118, 212, 190, 89, 187, 96, 252,
        203, 195, 32, 137, 246, 178, 160, 196, 96, 99, 79, 111, 178, 224, 40, 142, 252, 204, 223,
        188, 182, 79, 168, 11, 10, 57, 245, 143, 19, 123, 56, 58, 194, 48, 4, 181, 226, 115, 62,
        212, 40, 136, 162, 48, 48, 37, 213, 223, 76, 111, 34, 137, 35, 111, 68, 118, 209, 196, 198,
        131, 200, 117, 219, 108, 60, 42, 56, 232, 96, 82, 195, 84, 112, 140, 209, 35, 161, 173, 88,
        136, 197, 206, 7, 234, 252, 65, 121, 75, 23, 75, 219, 168, 242, 135, 24, 168, 236, 8, 247,
        134, 47, 192, 49, 174, 80, 126, 171, 106, 89, 214, 22,
    ];
    let endpoint = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 8080);
    let endpoint_pair = node::EndpointPair::new(endpoint.clone(), endpoint);
    let (message_staging, ..) = test_utils::setup_staging(false, message::NumId(0), Vec::with_capacity(0), endpoint_pair, Arc::new(UdpSocket::bind(endpoint).await.unwrap()));
    message_staging.stage_message();
}
