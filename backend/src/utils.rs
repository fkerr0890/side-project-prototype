use std::{collections::{HashMap, HashSet}, fmt::Display, hash::Hash, sync::{Arc, Mutex}, time::Duration};

use tokio::time::sleep;

pub struct TransientMap<K: Send + Hash + Eq + Display, V: Send> {
    ttl: u64,
    map: Arc<Mutex<HashMap<K, V>>>
}

impl<K: Send + Hash + Eq + Clone + Display + 'static, V: Send + 'static> TransientMap<K, V> {
    pub fn new(ttl_secs: u64) -> Self {
        Self {
            ttl: ttl_secs,
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
            sleep(Duration::from_secs(ttl)).await;
            // println!("TransientMap: Removing {}", key);
            let removed_value = map.lock().unwrap().remove(&key);
            if let Some(send_action) = send_action {
                send_action();
            }
        });
        true
    }

    pub fn map(&self) -> &Arc<Mutex<HashMap<K, V>>> { &self.map }
}

pub struct TransientSet<K: Send + Hash + Eq + Display> {
    ttl: u64,
    set: Arc<Mutex<HashSet<K>>>
}

impl<K: Send + Hash + Eq + Clone + Display + 'static> TransientSet<K> {
    pub fn new(ttl_secs: u64) -> Self {
        Self {
            ttl: ttl_secs,
            set: Arc::new(Mutex::new(HashSet::new()))
        }
    }

    pub fn insert(&self, key: &K) -> bool {
        if self.set.lock().unwrap().contains(key) {
            return false;
        }
        let (set, ttl, key_owned) = (self.set.clone(), self.ttl, key.to_owned());
        tokio::spawn(async move {
            sleep(Duration::from_secs(ttl)).await;
            // println!("TransientSet: Removing {}", key_owned);
            set.lock().unwrap().remove(&key_owned);
        });
        self.set.lock().unwrap().insert(key.to_owned());
        true
    }

    pub fn set(&self) -> &Arc<Mutex<HashSet<K>>> { &self.set }
}