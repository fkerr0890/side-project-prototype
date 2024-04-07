use std::{collections::{HashMap, HashSet, VecDeque}, fmt::Debug, hash::Hash, sync::{Arc, Mutex}, time::Duration};

use tokio::{task::AbortHandle, time};

pub trait ArcCollection {
    type K: Send + Hash + Eq + Clone;
    type V;

    fn contains_key(&self, key: &Self::K) -> bool;
    fn push(&mut self, key: Self::K);
    fn pop(&mut self, key: &Self::K) -> Option<Self::V>;
}

pub struct ArcMap<K, V>(Arc<Mutex<HashMap<K, V>>>);
impl<K, V> ArcMap<K, V> {
    pub fn new() -> Self { Self(Arc::new(Mutex::new(HashMap::new()))) }
    pub fn map(&self) -> &Arc<Mutex<HashMap<K, V>>> { &self.0 }
}
impl<K: Send + Hash + Eq + Clone, V> ArcCollection for ArcMap<K, V> {
    type K = K;
    type V = V;

    fn contains_key(&self, key: &K) -> bool { self.0.lock().unwrap().contains_key(key) }
    fn push(&mut self, _key: K) { unimplemented!() }
    fn pop(&mut self, key: &K) -> Option<V> { self.0.lock().unwrap().remove(key) }
}
impl<K, V> Clone for ArcMap<K, V> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[derive(Clone)]
pub struct ArcSet<K: Clone>(Arc<Mutex<HashSet<K>>>);
impl<K: Clone> ArcSet<K> {
    pub fn new() -> Self { Self(Arc::new(Mutex::new(HashSet::new()))) }
    pub fn set(&self) -> &Arc<Mutex<HashSet<K>>> { &self.0 }
}
impl<K: Send + Hash + Eq + Clone> ArcCollection for ArcSet<K> {
    type K = K;
    type V = K;

    fn contains_key(&self, key: &K) -> bool { self.0.lock().unwrap().contains(key) }
    fn push(&mut self, key: K) { self.0.lock().unwrap().insert(key); }
    fn pop(&mut self, key: &K) -> Option<K> { self.0.lock().unwrap().remove(key); None }
}

#[derive(Clone)]
pub struct ArcDeque<K: Clone>(Arc<Mutex<VecDeque<K>>>);
impl<K: Clone> ArcDeque<K> {
    pub fn new() -> Self { Self(Arc::new(Mutex::new(VecDeque::new()))) }
}
impl<K: Copy> ArcDeque<K> {
    pub fn front(&self) -> Option<K> { self.0.lock().unwrap().front().copied() }
}
impl<K: Send + Hash + Eq + Clone> ArcCollection for ArcDeque<K> {
    type K = K;
    type V = K;

    fn contains_key(&self, key: &K) -> bool { self.0.lock().unwrap().contains(key) }
    fn push(&mut self, key: K) { self.0.lock().unwrap().push_back(key); }
    fn pop(&mut self, _key: &K) -> Option<K> { self.0.lock().unwrap().pop_front() }
}

pub struct TransientCollection<C: ArcCollection> {
    ttl: TtlType,
    collection: C,
    abort_handles: Option<Arc<Mutex<HashMap<C::K, AbortHandle>>>>
}
impl<C: ArcCollection + Clone + Send> ArcCollection for TransientCollection<C> {
    type K = C::K;
    type V = C::V;

    fn contains_key(&self, key: &Self::K) -> bool { self.collection.contains_key(key) }
    fn push(&mut self, key: Self::K) { self.collection.push(key); }
    fn pop(&mut self, key: &Self::K) -> Option<Self::V> { self.collection.pop(key) }
}

impl<C: ArcCollection + Clone + Send + 'static> TransientCollection<C> {
    pub fn new(ttl: TtlType, extend_timer: bool, collection: C) -> Self {
        Self {
            ttl,
            collection,
            abort_handles: if extend_timer { Some(Arc::new(Mutex::new(HashMap::new()))) } else { None }
        }
    }

    pub fn from_existing(existing: &Self, ttl: TtlType) -> Self {
        Self {
            ttl,
            collection: existing.collection.clone(),
            abort_handles: existing.abort_handles.as_ref().and_then(|arc| Some(arc.clone()))
        }
    }

    pub fn insert(&mut self, key: <TransientCollection<C> as ArcCollection>::K) -> bool { self.start_timer(key.clone(), Some(key), None::<fn()>)}

    fn remove_existing_handle(&mut self, key: &<TransientCollection<C> as ArcCollection>::K, value: Option<<TransientCollection<C> as ArcCollection>::K>) -> (bool, bool) {
        if self.collection.contains_key(&key) {
            if let Some(ref mut abort_handles) = self.abort_handles {
                abort_handles.lock().unwrap().remove(&key).unwrap().abort();
                (true, false)
            }
            else {
                (true, true)
            }
        } else {
            if let Some(value) = value { 
                self.collection.push(value);
            }
            (false, false)
        }
    }

    fn start_timer(&mut self, key: <TransientCollection<C> as ArcCollection>::K, value: Option<<TransientCollection<C> as ArcCollection>::K>, send_action: Option<impl FnMut() + Send + 'static>) -> bool {
        let (contains_key, early_return) = self.remove_existing_handle(&key, value);
        if early_return {
            return !contains_key;
        }
        let (mut collection, ttl, key_clone) = (self.collection.clone(), self.ttl, key.clone());
        let abort_handle = tokio::spawn(async move {
            ttl.sleep().await;
            if let Some(mut send_action) = send_action {
                send_action();
                tokio::spawn(async move {
                    time::sleep(Duration::from_secs(2)).await;
                    collection.pop(&key_clone);
                });
            }
            else {
                collection.pop(&key_clone);
            }
        }).abort_handle();
        if let Some(ref mut abort_handles) = self.abort_handles {
            abort_handles.lock().unwrap().insert(key, abort_handle);
        }
        !contains_key
    }

    pub fn collection(&self) -> &C { &self.collection }
    pub fn collection_mut(&mut self) -> &mut C { &mut self.collection }
}

impl<K: Send + Hash + Eq + Clone + 'static, V: Send + 'static> TransientCollection<ArcMap<K, V>> {
    pub fn set_timer(&mut self, key: K) -> bool { self.start_timer(key, None, None::<fn()>) }
    pub fn set_timer_with_send_action(&mut self, key: K, send_action: impl FnMut() + Send + 'static) -> bool { self.start_timer(key, None, Some(send_action)) }
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
    ($expr:expr, $ret_value:literal) => {
        {
            let Some(val) = $expr else { return $ret_value };
            val
        }
    };
    ($expr:expr, $ret_action:expr) => {
        {
            let Some(val) = $expr else { $ret_action; return };
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