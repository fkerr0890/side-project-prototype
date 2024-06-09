use std::time::Duration;
use rustc_hash::FxHashMap;

use tokio::time;

use crate::message::{Message, NumId};

pub struct TimeboundEventManager {
    events: FxHashMap<u128, Vec<TimeboundAction>>,
    interval: time::Interval,
    now: u128
}

impl TimeboundEventManager {
    pub fn new(tick_duration: Duration) -> Self {
        assert!(!tick_duration.is_zero());
        Self {
            interval: time::interval(tick_duration),
            events: FxHashMap::default(),
            now: 0
        }
    }

    pub async fn tick(&mut self) -> Option<Vec<TimeboundAction>> {
        self.interval.tick().await;
        self.now += 1;
        self.events.remove(&self.now)
    }

    pub fn put_event(&mut self, action: TimeboundAction, wait_time: Duration) {
        let num_ticks = wait_time.as_millis() / self.interval.period().as_millis();
        let actions = self.events.entry(self.now + num_ticks).or_default();
        actions.push(action);
    }
}

#[derive(Debug)]
pub enum TimeboundAction {
    LockDestsDistribution(String, NumId),
    SendHeartbeats,
    RemoveCachedStreamMessage(NumId),
    RemoveStagedMessage(NumId),
    RemoveHttpHandlerTx(NumId),
    RemoveCachedOutboundMessages(NumId),
    RemoveBreadcrumb(NumId),
    RemoveSymmetricKey(NumId),
    RemovePrivateKey(NumId),
    RemoveUnconfirmedPeer(NumId),
    SendEarlyReturnMessage(Message),
    FinalizeDiscover(NumId)
}
