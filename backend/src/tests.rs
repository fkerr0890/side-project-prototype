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
async fn crypto_agree() {
    let peer_id = message::NumId(0);
    let (mut key_store, mut event_manager) = key_store();
    let public_key = generate_public_key(peer_id, &mut key_store, &mut event_manager);
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
}
