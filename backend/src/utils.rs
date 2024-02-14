use std::{collections::HashMap, fmt::Display, hash::Hash, sync::{Arc, Mutex}, time::Duration};

use tokio::time::sleep;
pub struct TransientMap<K: Send + Hash + PartialEq + Eq + Display, V: Send + Default> {
    ttl: u64,
    map: Arc<Mutex<HashMap<K, V>>>
}

impl<K: Send + Hash + PartialEq + Eq + Clone + Display + 'static, V: Send + Default + 'static> TransientMap<K, V> {
    pub fn new(ttl: u64) -> Self {
        Self {
            ttl,
            map: Arc::new(Mutex::new(HashMap::new()))
        }
    }

    pub fn set_timer(&self, key: K) {
        if self.map.lock().unwrap().contains_key(&key) {
            return;
        }
        let (map, ttl) = (self.map.clone(), self.ttl);
        tokio::spawn(async move {
            sleep(Duration::from_secs(ttl)).await;
            println!("TransientMap: Removing {}", key);
            map.lock().unwrap().remove(&key);
        });
    }

    pub fn map(&self) -> &Arc<Mutex<HashMap<K, V>>> { &self.map }
}