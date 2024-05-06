use crate::lock;
use std::{collections::{HashMap, HashSet, VecDeque}, fmt::Debug, hash::Hash, sync::{Arc, Mutex}, time::Duration};

use tokio::{sync::mpsc, task::AbortHandle, time};

pub trait ArcCollection {
    type K: Send + Hash + Eq + Clone + Debug;
    type V;

    fn contains_key(&self, key: &Self::K) -> bool;
    fn push(&mut self, key: Self::K);
    fn pop(&mut self, key: &Self::K) -> Option<Self::V>;
}

pub struct ArcMap<K, V>(Arc<Mutex<HashMap<K, V>>>);
impl<K, V> Default for ArcMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> ArcMap<K, V> {
    pub fn new() -> Self { Self(Arc::new(Mutex::new(HashMap::new()))) }
    pub fn map(&self) -> &Arc<Mutex<HashMap<K, V>>> { &self.0 }
}
impl<K: Send + Hash + Eq + Clone + Debug, V> ArcCollection for ArcMap<K, V> {
    type K = K;
    type V = V;

    fn contains_key(&self, key: &K) -> bool { lock!(self.0).contains_key(key) }
    fn push(&mut self, _key: K) { unimplemented!() }
    fn pop(&mut self, key: &K) -> Option<V> { lock!(self.0).remove(key) }
}
impl<K, V> Clone for ArcMap<K, V> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[derive(Clone)]
pub struct ArcSet<K: Clone>(Arc<Mutex<HashSet<K>>>);
impl<K: Clone> Default for ArcSet<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Clone> ArcSet<K> {
    pub fn new() -> Self { Self(Arc::new(Mutex::new(HashSet::new()))) }
    pub fn set(&self) -> &Arc<Mutex<HashSet<K>>> { &self.0 }
}
impl<K: Send + Hash + Eq + Clone + Debug> ArcCollection for ArcSet<K> {
    type K = K;
    type V = K;

    fn contains_key(&self, key: &K) -> bool { lock!(self.0).contains(key) }
    fn push(&mut self, key: K) { lock!(self.0).insert(key); }
    fn pop(&mut self, key: &K) -> Option<K> { lock!(self.0).remove(key); None }
}

#[derive(Clone)]
pub struct ArcDeque<K: Clone>(Arc<Mutex<VecDeque<K>>>);
impl<K: Clone> Default for ArcDeque<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Clone> ArcDeque<K> {
    pub fn new() -> Self { Self(Arc::new(Mutex::new(VecDeque::new()))) }
}
impl<K: Copy> ArcDeque<K> {
    pub fn front(&self) -> Option<K> { lock!(self.0).front().copied() }
}
impl<K: Send + Hash + Eq + Clone + Debug> ArcCollection for ArcDeque<K> {
    type K = K;
    type V = K;

    fn contains_key(&self, key: &K) -> bool { lock!(self.0).contains(key) }
    fn push(&mut self, key: K) { lock!(self.0).push_back(key); }
    fn pop(&mut self, _key: &K) -> Option<K> { lock!(self.0).pop_front() }
}

pub struct TimerOptions {
    ttl: Option<Duration>,
    override_early_return: bool
}

impl TimerOptions {
    pub fn new() -> Self { Self { ttl: None, override_early_return: false } }
    pub fn with_ttl(mut self, ttl: Option<Duration>) -> Self { self.ttl = ttl; self }
    pub fn override_early_return(mut self, override_early_return: bool) -> Self { self.override_early_return = override_early_return; self }
}

impl Default for TimerOptions {
    fn default() -> Self {
        Self::new()
    }
}

type AbortHandles<T> = Option<Arc<Mutex<T>>>;
pub struct TransientCollection<C: ArcCollection> {
    ttl: Duration,
    collection: C,
    abort_handles: AbortHandles<HashMap<C::K, AbortHandle>>
}
impl<C: ArcCollection + Clone + Send> ArcCollection for TransientCollection<C> {
    type K = C::K;
    type V = C::V;

    fn contains_key(&self, key: &Self::K) -> bool { self.collection.contains_key(key) }
    fn push(&mut self, key: Self::K) { self.collection.push(key); }
    fn pop(&mut self, key: &Self::K) -> Option<Self::V> { self.collection.pop(key) }
}

impl<C: ArcCollection + Clone + Send + 'static> TransientCollection<C> {
    pub fn new(ttl: Duration, extend_timer: bool, collection: C) -> Self {
        Self {
            ttl,
            collection,
            abort_handles: if extend_timer { Some(Arc::new(Mutex::new(HashMap::new()))) } else { None }
        }
    }

    pub fn from_existing(existing: &Self, ttl: Duration) -> Self {
        Self {
            ttl,
            collection: existing.collection.clone(),
            abort_handles: existing.abort_handles.clone()
        }
    }

    pub fn ttl(&self) -> Duration { self.ttl }
    pub fn insert_key(&mut self, key: C::K, key_label: &str) -> bool { self.start_timer(key.clone(), Some(key), None::<fn()>, key_label, TimerOptions::default())}
    pub fn set_timer(&mut self, key: C::K, options: TimerOptions, key_label: &str) -> bool { self.start_timer(key, None, None::<fn()>, key_label, options) }
    pub fn set_timer_with_send_action(&mut self, key: C::K, options: TimerOptions, send_action: impl FnOnce() + Send + 'static, key_label: &str) -> bool { self.start_timer(key, None, Some(send_action), key_label, options) }

    fn remove_existing_handle(&mut self, key: &C::K, value: Option<C::K>) -> (bool, bool) {
        if self.collection.contains_key(key) {
            if let Some(ref mut abort_handles) = self.abort_handles {
                lock!(abort_handles).remove(key).unwrap().abort();
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

    fn start_timer(&mut self, key: C::K, value: Option<C::K>, send_action: Option<impl FnOnce() + Send + 'static>, _key_label: &str, options: TimerOptions) -> bool {
        // println!("Creating {:?}, label = {key_label}", key);
        let (contains_key, early_return) = self.remove_existing_handle(&key, value);
        if early_return && !options.override_early_return {
            // println!("No recreate {:?}, label = {key_label}", key);
            return !contains_key;
        }
        let (mut collection, ttl, key_clone) = (self.collection.clone(), if let Some(ttl) = options.ttl { ttl } else { self.ttl }, key.clone());
        let abort_handle = tokio::spawn(async move {
            time::sleep(ttl).await;
            // println!("Removing {:?}, label = {key_label}", key_clone);
            if let Some(send_action) = send_action {
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
            lock!(abort_handles).insert(key, abort_handle);
        }
        !contains_key
    }

    pub fn collection(&self) -> &C { &self.collection }
    pub fn collection_mut(&mut self) -> &mut C { &mut self.collection }
}

impl<K: Send + Hash + Eq + Clone + Debug + 'static, V: Send + 'static> TransientCollection<ArcMap<K, V>> {
    pub fn insert(&mut self, key: K, value: V, key_label: &str) -> bool {
        let is_new_key = self.set_timer(key.clone(), TimerOptions::default(), key_label);
        if !is_new_key {
            return false;
        }
        lock!(self.collection.map()).insert(key, value);
        true
    }
}

pub struct BidirectionalMpsc<T, U> {
    tx: mpsc::UnboundedSender<T>,
    rx: mpsc::UnboundedReceiver<U>
}

impl<T, U> BidirectionalMpsc<T, U> {
    pub fn channel() -> (Self, BidirectionalMpsc<U, T>) {
        let (tx1, rx1) = mpsc::unbounded_channel();
        let (tx2, rx2) = mpsc::unbounded_channel();
        (Self { tx: tx1, rx: rx2}, BidirectionalMpsc { tx: tx2, rx: rx1 })
    }

    pub fn send(&self, message: T) -> Result<(), mpsc::error::SendError<T>> {
        self.tx.send(message)
    }

    pub async fn recv(&mut self) -> Option<U> {
        self.rx.recv().await
    }
}

#[macro_export]
macro_rules! option_early_return {
    ($expr:expr, $ret_value:expr) => {
        {
            let Some(val) = $expr else { return $ret_value };
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
            Err(error) => { tracing::error!(%error); return $ret_expr; }
        }
    };
    ($expr:expr) => {
        match $expr {
            Ok(val) => val,
            Err(error) => { tracing::error!(%error); return; }
        }
    };
}

#[macro_export]
macro_rules! time {
    ($block:block) => {
        {
            let now = std::time::Instant::now();
            let output = $block;
            let duration = now.elapsed();
            let mut data = $crate::MAX_TIME.lock().unwrap();
            if data.0 < duration {
                data.0 = duration;
                data.3 = format!(" at {} {}", file!(), line!()); 
            }
            data.1 += duration;
            data.2 += 1;
            output
        }
    }
}

#[macro_export]
macro_rules! lock {
    ($expr:expr) => {
        $crate::time!({ $expr.lock().unwrap() })
    };
}