use std::{sync::Mutex, time::Duration};

pub static MAX_TIME: Mutex<(Duration, Duration, u32, String)> =
    Mutex::new((Duration::ZERO, Duration::ZERO, 0, String::new()));

pub mod crypto;
pub mod event;
pub mod http;
pub mod message;
pub mod message_processing;
pub mod nat_traversal;
pub mod node;
pub mod peer;
pub mod test_utils;
#[cfg(test)]
pub mod tests;
pub mod utils;
