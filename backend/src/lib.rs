use std::{sync::Mutex, time::Duration};

pub static MAX_TIME: Mutex<(Duration, Duration, u32, String)> = Mutex::new((Duration::ZERO, Duration::ZERO, 0, String::new()));

pub mod message_processing;
pub mod nat_traversal;
pub mod message;
pub mod peer;
pub mod http;
pub mod node;
pub mod crypto;
pub mod utils;
pub mod event;
pub mod test_utils;