use std::{collections::HashMap, time::Duration};

use tokio::time::sleep;

use crate::message::{Message, NumId};

pub struct TimelineEventManager {
    events: HashMap<u128, Vec<TimeboundAction>>,
    precision: StepPrecision,
    now: u128
}

impl TimelineEventManager {
    pub fn new(precision: StepPrecision) -> Self {
        Self {
            precision,
            events: HashMap::new(),
            now: 0
        }
    }

    pub async fn step(&mut self) -> Option<Vec<TimeboundAction>> {
        self.sleep_one_step().await;
        self.events.remove(&self.now)
    }

    pub fn put_event(&mut self, action: TimeboundAction, wait_time: Duration) {
        let wait_time = match self.precision {
            StepPrecision::Milli => wait_time.as_millis(),
            StepPrecision::Sec => wait_time.as_secs() as u128
        };
        let actions = self.events.entry(self.now + wait_time).or_default();
        actions.push(action);
    }

    async fn sleep_one_step(&mut self){
        match self.precision {
            StepPrecision::Milli => sleep(Duration::from_millis(1)).await,
            StepPrecision::Sec => sleep(Duration::from_secs(1)).await
        }
        self.now += 1;
    }
}

pub enum StepPrecision {
    Milli,
    Sec
}

#[derive(Debug)]
pub enum TimeboundAction {
    LockDestsDistribution(String, NumId),
    SendHeartbeats,
    RemoveCachedStreamMessage(NumId),
    RemoveStagedMessage(NumId),
    RemoveHttpHandlerTx(NumId),
    RemoveCachedOutboundMessages(NumId),
    RemoveBreadcrumb(NumId, Option<Message>),
    RemoveSymmetricKey(NumId),
    RemovePrivateKey(NumId)
}