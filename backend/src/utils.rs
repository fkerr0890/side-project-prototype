use std::{collections::{HashMap, HashSet}, fmt::{Debug, Display}, hash::Hash, sync::{Arc, Mutex}, time::Duration};

use tokio::{task::AbortHandle, time};

#[derive(Debug)]
pub struct TransientMap<K: Send + Hash + Eq + Display, V: Send + Debug> {
    ttl: TtlType,
    map: Arc<Mutex<HashMap<K, V>>>,
    abort_handles: Option<HashMap<K, AbortHandle>>
}

impl<K: Send + Hash + Eq + Clone + Display + 'static, V: Send + Debug + 'static> TransientMap<K, V> {
    pub fn new(ttl: TtlType, extend_timer: bool) -> Self {
        Self {
            ttl,
            map: Arc::new(Mutex::new(HashMap::new())),
            abort_handles: if extend_timer { Some(HashMap::new()) } else { None }
        }
    }

    pub fn set_timer(&mut self, key: K) -> bool {
        self.start_timer(key, None::<fn()>)
    }

    pub fn set_timer_with_send_action(&mut self, key: K, send_action: impl FnMut() + Send + 'static) -> bool {
        self.start_timer(key, Some(send_action))
    }

    fn start_timer(&mut self, key: K, send_action: Option<impl FnMut() + Send + 'static>) -> bool {
        let contains_key = if self.map.lock().unwrap().contains_key(&key) {
            if let Some(ref mut abort_handles) = self.abort_handles {
                abort_handles.remove(&key).unwrap().abort();
                true
            }
            else {
                return false;
            }
        } else { false };
        // println!("TransientMap {}: Adding {}", name, key);
        let (map, ttl, key_clone) = (self.map.clone(), self.ttl, key.clone());
        let abort_handle = tokio::spawn(async move {
            ttl.sleep().await;
            if let Some(mut send_action) = send_action {
                send_action();
            }
            else {
                map.lock().unwrap().remove(&key_clone);
            }
        }).abort_handle();
        if let Some(ref mut abort_handles) = self.abort_handles {
            abort_handles.insert(key, abort_handle);
        }
        !contains_key
    }

    pub fn map(&self) -> &Arc<Mutex<HashMap<K, V>>> { &self.map }
}

#[derive(Debug)]
pub struct TransientSet<K: Send + Hash + Eq + Display> {
    ttl: TtlType,
    set: Arc<Mutex<HashSet<K>>>,
    abort_handles: Option<HashMap<K, AbortHandle>>
}

impl<K: Send + Hash + Eq + Clone + Display + 'static> TransientSet<K> {
    pub fn new(ttl: TtlType, extend_timer: bool) -> Self {
        Self {
            ttl,
            set: Arc::new(Mutex::new(HashSet::new())),
            abort_handles: if extend_timer { Some(HashMap::new()) } else { None }
        }
    }

    pub fn insert(&mut self, key: K) -> bool {
        let contains_key = if self.set.lock().unwrap().contains(&key) {
            if let Some(ref mut abort_handles) = self.abort_handles {
                abort_handles.remove(&key).unwrap().abort();
                true
            }
            else {
                return false;
            }
        } else { false };
        let (set, ttl, key_clone) = (self.set.clone(), self.ttl, key.clone());
        let abort_handle = tokio::spawn(async move {
            ttl.sleep().await;
            // println!("TransientSet: Removing {}", key_owned);
            set.lock().unwrap().remove(&key_clone);
        }).abort_handle();
        if let Some(ref mut abort_handles) = self.abort_handles {
            abort_handles.insert(key.clone(), abort_handle);
        }
        self.set.lock().unwrap().insert(key);
        !contains_key
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

#[macro_export]
macro_rules! option_early_return {
    ($expr:expr, $ret_expr:expr) => {
        {
            let Some(val) = $expr else { $ret_expr; return };
            val
        }
    };
    ($expr:expr) => {
        {
            let Some(val) = $expr else { return };
            val
        }
    };
}

#[macro_export]
macro_rules! result_early_return {
    ($expr:expr, $ret_expr:expr) => {
        match $expr {
            Ok(val) => val,
            Err(error) => { error!(%error); return $ret_expr; }
        }
    };
    ($expr:expr) => {
        match $expr {
            Ok(val) => val,
            Err(error) => { error!(%error); return; }
        }
    };
}