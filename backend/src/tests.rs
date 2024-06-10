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

#[tokio::test]
async fn crypto_public_key() {
    let mut key_store = crypto::KeyStore::new();
    let peer_id = message::NumId(0);
    let mut event_manager = event::TimeboundEventManager::new(Duration::from_secs(1));
    let public_key = key_store
        .public_key(peer_id, &mut event_manager)
        .expect("Some when no symmetric key exists");
    let public_key2 = key_store
        .public_key(peer_id, &mut event_manager)
        .expect("Some when no symmetric key exists");
    assert_eq!(public_key, public_key2);
    let public_key3 = key_store
        .public_key(message::NumId(1), &mut event_manager)
        .expect("Some when no symmetric key exists");
    assert_ne!(public_key3, public_key);
    assert_ne!(public_key3, public_key2);
    key_store
        .agree(peer_id, public_key, &mut event_manager)
        .unwrap();
    assert!(key_store.public_key(peer_id, &mut event_manager).is_none());
}
