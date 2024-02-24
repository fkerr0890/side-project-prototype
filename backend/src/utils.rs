use std::{collections::{HashMap, HashSet}, fmt::{Debug, Display}, hash::Hash, sync::{Arc, Mutex}, time::Duration};

use tokio::time;

#[derive(Debug, Clone)]
pub struct TransientMap<K: Send + Hash + Eq + Display, V: Send + Debug> {
    ttl: TtlType,
    map: Arc<Mutex<HashMap<K, V>>>
}

impl<K: Send + Hash + Eq + Clone + Display + 'static, V: Send + Debug + 'static> TransientMap<K, V> {
    pub fn new(ttl: TtlType) -> Self {
        Self {
            ttl,
            map: Arc::new(Mutex::new(HashMap::new()))
        }
    }

    pub fn set_timer(&self, key: K) -> bool {
        self.start_timer(key, None::<fn()>)
    }

    pub fn set_timer_with_send_action(&self, key: K, send_action: impl FnMut() + Send + 'static) -> bool {
        self.start_timer(key, Some(send_action))
    }

    fn start_timer(&self, key: K, send_action: Option<impl FnMut() + Send + 'static>) -> bool {
        if self.map.lock().unwrap().contains_key(&key) {
            return false;
        }
        // println!("TransientMap {}: Adding {}", name, key);
        let (map, ttl) = (self.map.clone(), self.ttl);
        tokio::spawn(async move {
            ttl.sleep().await;
            if let Some(mut send_action) = send_action {
                send_action();
            }
            else {
                map.lock().unwrap().remove(&key);
            }
        });
        true
    }

    pub fn map(&self) -> &Arc<Mutex<HashMap<K, V>>> { &self.map }
}

pub struct TransientSet<K: Send + Hash + Eq + Display> {
    ttl: TtlType,
    set: Arc<Mutex<HashSet<K>>>
}

impl<K: Send + Hash + Eq + Clone + Display + 'static> TransientSet<K> {
    pub fn new(ttl: TtlType) -> Self {
        Self {
            ttl,
            set: Arc::new(Mutex::new(HashSet::new()))
        }
    }

    pub fn insert(&self, key: &K) -> bool {
        if self.set.lock().unwrap().contains(key) {
            return false;
        }
        let (set, ttl, key_owned) = (self.set.clone(), self.ttl, key.to_owned());
        tokio::spawn(async move {
            ttl.sleep().await;
            // println!("TransientSet: Removing {}", key_owned);
            set.lock().unwrap().remove(&key_owned);
        });
        self.set.lock().unwrap().insert(key.to_owned());
        true
    }

    pub fn set(&self) -> &Arc<Mutex<HashSet<K>>> { &self.set }
}

#[derive(Clone, Copy, Debug)]
pub enum TtlType {
    Secs(u64),
    Millis(u64)
}
impl TtlType {
    pub fn sleep(&self) -> time::Sleep {
        match self {
            Self::Secs(secs) => time::sleep(Duration::from_secs(*secs)),
            Self::Millis(millis) => time::sleep(Duration::from_millis(*millis)),
        }
    }
}