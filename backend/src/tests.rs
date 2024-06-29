use std::sync::Arc;

use message_processing::stage;
use tokio::{net::UdpSocket, sync::mpsc};

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

async fn setup_staging(
    myself: message::Peer,
) -> (
    stage::MessageStaging,
    message_processing::CipherSender,
    message::Peer,
    mpsc::UnboundedSender<stage::ClientApiRequest>,
    mpsc::UnboundedSender<(
        message::Message,
        mpsc::UnboundedSender<http::SerdeHttpResponse>,
    )>,
) {
    test_utils::setup_staging(
        false,
        myself.id,
        Vec::with_capacity(0),
        myself.endpoint_pair,
        Arc::new(
            UdpSocket::bind(myself.endpoint_pair.private_endpoint)
                .await
                .unwrap(),
        ),
    )
}

fn get_future_event(
    duration: Duration,
    message_staging: &stage::MessageStaging,
) -> &event::TimeboundAction {
    let future = duration.as_millis()
        / message_staging
            .event_manager()
            .interval()
            .period()
            .as_millis();
    &message_staging
        .event_manager()
        .events()
        .get(&future)
        .expect("Should be an event here")[0]
}

#[tokio::test]
async fn stage_handle_key_agreement() {
    let myself = message::Peer::default();
    let peer_id = message::NumId(1);
    let (mut message_staging, ..) = setup_staging(myself).await;
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
    let request_result = message_staging.handle_key_agreement_pub(
        key_agreement_message.clone(),
        myself.endpoint_pair.public_endpoint,
    );
    let stage::HandleKeyAgreementResult::SendResponse(dest, _) = request_result else {
        panic!(
            "Receiving a request should trigger a response. Actual was {:?}",
            request_result
        );
    };
    assert_eq!(peer_id, dest.id);
    message_staging.key_store().remove_symmetric_key(&peer_id);
    key_agreement_message.direction = message::MessageDirectionAgreement::Response;
    let response_result = message_staging.handle_key_agreement_pub(
        key_agreement_message.clone(),
        myself.endpoint_pair.public_endpoint,
    );
    let stage::HandleKeyAgreementResult::SendCachedOutboundMessages(send_checked_inputs) =
        response_result
    else {
        panic!("Receiving a response should trigger sending any cached outbound messages. Actual was {:?}", response_result);
    };
    assert_eq!(0, send_checked_inputs.len());
    let duplicate_result = message_staging.handle_key_agreement_pub(
        key_agreement_message.clone(),
        myself.endpoint_pair.public_endpoint,
    );
    assert!(
        matches!(
            duplicate_result,
            stage::HandleKeyAgreementResult::SymmetricKeyExists
        ),
        "Actual was {:?}",
        duplicate_result
    );
    message_staging.key_store().remove_symmetric_key(&peer_id);
    let public_key = vec![8u8, 16];
    key_agreement_message.public_key = public_key;
    let error_result = message_staging
        .handle_key_agreement_pub(key_agreement_message, myself.endpoint_pair.public_endpoint);
    assert!(
        matches!(
            error_result,
            stage::HandleKeyAgreementResult::AgreementError
        ),
        "Actual was {:?}",
        error_result
    );
}

#[tokio::test]
async fn stage_send_heartbeats() {
    let myself = message::Peer::default();
    let (mut message_staging, ..) = setup_staging(myself).await;
    let peer = message::Peer::new(node::EndpointPair::default(), message::NumId(1));
    message_staging
        .session_manager()
        .new_source_retrieval(String::from("example"));
    message_staging
        .session_manager()
        .add_destination_source_retrieval("example", peer);
    message_staging
        .session_manager()
        .new_sink_retrieval(String::from("example2"));
    message_staging.session_manager().add_destination_sink(
        "example2",
        message::Peer::new(node::EndpointPair::default(), message::NumId(2)),
    );
    message_staging
        .session_manager()
        .new_sink_retrieval(String::from("example3"));
    message_staging
        .session_manager()
        .add_destination_sink("example3", peer);
    let (dests, message, to_be_chunked, cache_id) = message_staging.send_heartbeats_pub();
    let mut dests = dests.into_iter().collect::<Vec<message::Peer>>();
    assert_eq!(2, dests.len());
    dests.sort_by_key(|p| p.id.0);
    assert_eq!(1, dests[0].id.0);
    assert_eq!(2, dests[1].id.0);
    assert!(
        matches!(message.metadata(), message::MetadataKind::Heartbeat),
        "Actual was {:?}",
        message.metadata()
    );
    assert_eq!(false, message.check_expiry());
    assert_eq!(true, to_be_chunked);
    assert!(cache_id.is_none());
    let event = get_future_event(
        message_processing::HEARTBEAT_INTERVAL_SECONDS,
        &message_staging,
    );
    assert!(
        matches!(event, event::TimeboundAction::SendHeartbeats),
        "Actual was {:?}",
        event
    );
}

#[tokio::test]
async fn stage_initial_retrieval_request() {
    let myself = message::Peer::default();
    let (mut message_staging, ..) = setup_staging(myself).await;
    let (tx, _rx) = mpsc::unbounded_channel();
    let http_request = http::SerdeHttpRequest::new_test_request();
    let request = message::Message::new(
        message::Peer::default(),
        message::NumId(0),
        None,
        message::MetadataKind::Stream(message::StreamMetadata::new(
            message::StreamPayloadKind::Request(http_request),
            String::from("example"),
        )),
        message::MessageDirection::OneHop,
    );
    let mut from_http_handler = (request, tx);
    assert!(message_staging
        .initial_retrieval_request_pub(from_http_handler.clone())
        .await
        .is_none());
    let host_name = String::from("example");
    assert_eq!(
        true,
        message_staging
            .session_manager()
            .source_active_retrieval(&host_name)
    );
    let event = get_future_event(message_processing::SRP_TTL_SECONDS, &message_staging);
    assert!(
        matches!(event, event::TimeboundAction::RemoveHttpHandlerTx(_)),
        "Actual was {:?}",
        event
    );
    let peer = message::Peer::new(node::EndpointPair::default(), message::NumId(1));
    message_staging
        .session_manager()
        .add_destination_source_retrieval(&host_name, peer);
    from_http_handler.0.set_id(message::NumId(1));
    let (dests, message, to_be_chunked, cache_id) = message_staging
        .initial_retrieval_request_pub(from_http_handler)
        .await
        .expect("Should return Some when a retrieval source exists");
    assert_eq!(1, dests.len());
    assert_eq!(peer, dests.into_iter().last().unwrap());
    match message.metadata() {
        message::MetadataKind::Stream(message::StreamMetadata {
            host_name: host_name_actual,
            ..
        }) if &host_name == host_name_actual => {}
        _ => panic!("Actual was {:?}", message.metadata()),
    }
    assert_eq!(1, message.id().0);
    assert_eq!(false, message.check_expiry());
    assert!(
        matches!(message.direction(), message::MessageDirection::OneHop),
        "Actual was {:?}",
        message.direction()
    );
    assert_eq!(true, to_be_chunked);
    assert!(cache_id.is_some());
}

#[tokio::test]
async fn stage_get_direction() {
    let myself = message::Peer::default();
    let (mut message_staging, ..) = setup_staging(myself).await;
    let host_name = String::from("example");
    let mut search_message = message::Message::new_search_request(
        message::NumId(0),
        message::SearchMetadata::new(
            myself,
            host_name.clone(),
            message::SearchMetadataKind::Retrieval,
        ),
    );
    let result_forward = message_staging.get_direction_pub(&mut search_message);
    assert!(
        matches!(result_forward, stage::PropagationDirection::Forward),
        "Actual was {:?}",
        result_forward
    );
    let result_stop = message_staging.get_direction_pub(&mut search_message);
    assert!(
        matches!(result_stop, stage::PropagationDirection::Stop),
        "Actual was {:?}",
        result_stop
    );
    search_message.set_direction(message::MessageDirection::Response);
    let result_reverse = message_staging.get_direction_pub(&mut search_message);
    assert!(
        matches!(result_reverse, stage::PropagationDirection::Reverse),
        "Actual was {:?}",
        result_reverse
    );
    search_message.set_direction(message::MessageDirection::Request);
    message_staging
        .session_manager()
        .add_local_host(host_name, node::EndpointPair::default_socket());
    search_message.set_id(message::NumId(1));
    let result_reverse = message_staging.get_direction_pub(&mut search_message);
    assert!(
        matches!(result_reverse, stage::PropagationDirection::Reverse),
        "Actual was {:?}",
        result_reverse
    );
    let mut discover_message = message::Message::new_discover_peer_request(
        myself,
        message::Peer::new(node::EndpointPair::default(), message::NumId(1)),
        2,
    );
    let result_forward = message_staging.get_direction_pub(&mut discover_message);
    assert!(
        matches!(result_forward, stage::PropagationDirection::Forward),
        "Actual was {:?}",
        result_forward
    );
    let event = get_future_event(message_processing::DPP_TTL_MILLIS / 2, &message_staging);
    assert!(
        matches!(event, event::TimeboundAction::SendEarlyReturnMessage(_)),
        "Actual was {:?}",
        event
    );
    let event = get_future_event(message_processing::SRP_TTL_SECONDS, &message_staging);
    assert!(
        matches!(event, event::TimeboundAction::RemoveBreadcrumb(_)),
        "Actual was {:?}",
        event
    );
    search_message.set_id(message::NumId(2));
    let result_reverse = message_staging.get_direction_pub(&mut search_message);
    assert!(
        matches!(result_reverse, stage::PropagationDirection::Reverse),
        "Actual was {:?}",
        result_reverse
    );
    search_message.set_id(message::NumId(3));
    let message::MetadataKind::Search(metadata) = search_message.metadata_mut() else {
        panic!()
    };
    metadata.set_kind(message::SearchMetadataKind::Distribution);
    let result_forward = message_staging.get_direction_pub(&mut search_message);
    assert!(
        matches!(result_forward, stage::PropagationDirection::Forward),
        "Actual was {:?}",
        result_forward
    );
    search_message.set_id(message::NumId(4));
    let message::MetadataKind::Search(metadata) = search_message.metadata_mut() else {
        panic!()
    };
    metadata.origin_mut().id = message::NumId(1);
    let result_forward = message_staging.get_direction_pub(&mut search_message);
    assert!(
        matches!(result_forward, stage::PropagationDirection::Forward),
        "Actual was {:?}",
        result_forward
    );
    search_message.set_id(message::NumId(5));
    message_staging
        .session_manager()
        .remove_local_host("example");
    let result_reverse = message_staging.get_direction_pub(&mut search_message);
    assert!(
        matches!(result_reverse, stage::PropagationDirection::Reverse),
        "Actual was {:?}",
        result_reverse
    );
}

#[test]
fn peer_add_peer() {
    let mut peer_ops = peer::PeerOps::new();
    for i in 0..peer::MAX_PEERS {
        let peer_id = message::NumId(i.into());
        peer_ops.add_peer(
            message::Peer::new(node::EndpointPair::default(), peer_id),
            i.into(),
        );
        assert!(peer_ops.has_peer(peer_id));
        assert_eq!(usize::from(i + 1), peer_ops.peers().len());
    }
    let peer_duplicate = message::Peer::new(node::EndpointPair::default(), message::NumId(0));
    peer_ops.add_peer(peer_duplicate, 99);
    assert_eq!(usize::from(peer::MAX_PEERS), peer_ops.peers().len());
    assert!(peer_ops.has_peer(message::NumId(0)));
    assert_eq!(99, *peer_ops.get_peer_score(peer_duplicate).unwrap());
    let next_index = peer::MAX_PEERS;
    let next_id = message::NumId(next_index.into());
    peer_ops.add_peer(
        message::Peer::new(node::EndpointPair::default(), next_id),
        -1,
    );
    assert_eq!(usize::from(peer::MAX_PEERS), peer_ops.peers().len());
    assert!(peer_ops.has_peer(message::NumId(0)));
    assert!(!peer_ops.has_peer(next_id));
    peer_ops.add_peer(
        message::Peer::new(node::EndpointPair::default(), next_id),
        next_index.into(),
    );
    let curr_peers = peer_ops.peers_and_scores();
    assert_eq!(usize::from(peer::MAX_PEERS), curr_peers.len());
    assert!(!peer_ops.has_peer(message::NumId(1)));
    assert!(peer_ops.has_peer(next_id));
    assert_eq!(99, curr_peers.iter().max_by_key(|p| p.1).unwrap().1);
    assert_eq!(2, curr_peers.iter().min_by_key(|p| p.1).unwrap().1);
}
