use std::sync::Arc;

use message_processing::stage;
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
    payload_tampered[0] = payload_tampered[0].overflowing_add(1).0;
    let mut nonce_tampered = nonce.clone();
    nonce_tampered[0] = nonce_tampered[0].overflowing_add(1).0;
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
async fn stage_handle_key_agreement() {
    let id = message::NumId(0);
    let peer_id = message::NumId(1);
    let endpoint_pair = node::EndpointPair::new(
        node::EndpointPair::default_socket(),
        node::EndpointPair::default_socket(),
    );
    let (mut message_staging, ..) = test_utils::setup_staging(
        false,
        id,
        Vec::with_capacity(0),
        endpoint_pair,
        Arc::new(
            UdpSocket::bind(endpoint_pair.private_endpoint)
                .await
                .unwrap(),
        ),
    );
    let (_, mut event_manager) = key_store();
    let public_key = message_staging
        .key_store()
        .public_key(peer_id, &mut event_manager)
        .unwrap();
    let mut key_agreement_message = message::KeyAgreementMessage::new(
        public_key.clone(),
        peer_id,
        message::MessageDirectionAgreement::Request,
    );
    let request_result = message_staging
        .handle_key_agreement_pub(key_agreement_message.clone(), endpoint_pair.public_endpoint);
    let stage::HandleKeyAgreementResult::SendResponse(dest, _) = request_result else {
        panic!("Receiving a request should trigger a response. Actual was {:?}", request_result);
    };
    assert_eq!(peer_id, dest.id);
    message_staging.key_store().remove_symmetric_key(&peer_id);
    key_agreement_message.direction = message::MessageDirectionAgreement::Response;
    let response_result = message_staging
        .handle_key_agreement_pub(key_agreement_message.clone(), endpoint_pair.public_endpoint);
    let stage::HandleKeyAgreementResult::SendCachedOutboundMessages(send_checked_inputs) =
        response_result
    else {
        panic!("Receiving a response should trigger sending any cached outbound messages. Actual was {:?}", response_result);
    };
    assert_eq!(0, send_checked_inputs.len());
    let duplicate_result = message_staging
        .handle_key_agreement_pub(key_agreement_message.clone(), endpoint_pair.public_endpoint);
    assert!(matches!(duplicate_result, stage::HandleKeyAgreementResult::SymmetricKeyExists), "Actual was {:?}", duplicate_result);
    message_staging.key_store().remove_symmetric_key(&peer_id);
    let public_key = vec![8u8, 16];
    key_agreement_message.public_key = public_key;
    let error_result = message_staging
        .handle_key_agreement_pub(key_agreement_message, endpoint_pair.public_endpoint);
    assert!(
        matches!(
            error_result,
            stage::HandleKeyAgreementResult::AgreementError
        ),
        "Actual was {:?}",
        error_result
    );
}
