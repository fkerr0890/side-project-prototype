use std::net::SocketAddrV4;
use rustc_hash::FxHashMap;
use tracing::{instrument, warn};

use crate::message::{DiscoverMetadata, NumId, Peer};

pub struct DiscoverPeerProcessor {
    message_staging: FxHashMap<NumId, DiscoverMetadata>
}

impl DiscoverPeerProcessor {
    pub fn new() -> Self {
        Self {
            message_staging: FxHashMap::default(),
        }
    }

    #[instrument(level = "trace", skip(self))]
    pub fn continue_propagating(&self, metadata: &mut DiscoverMetadata, myself: Peer) -> bool {
        metadata.peer_list.push(myself);
        metadata.hop_count.0 -= 1;
        metadata.hop_count.0 > 0
    }

    pub fn get_score(first: SocketAddrV4, second: SocketAddrV4) -> i32 {
        (second.port() as i32).abs_diff(first.port() as i32) as i32
    }

    pub fn peer_len_curr_max(&self, id: &NumId) -> usize {
        if let Some(staged_metadata) = self.message_staging.get(id) {
            staged_metadata.peer_list.len()
        }
        else {
            0
        }
    }

    #[instrument(level = "trace", skip_all, fields(hop_count = ?metadata.hop_count))]
    pub fn stage_message(&mut self, id: NumId, metadata: DiscoverMetadata, peer_len_curr_max: usize) {
        if metadata.peer_list.len() > peer_len_curr_max {
            self.message_staging.insert(id, metadata);
        }
    }

    pub fn new_peers(&mut self, id: &NumId) -> DiscoverMetadata {
        self.message_staging.remove(id).unwrap()
    }
}