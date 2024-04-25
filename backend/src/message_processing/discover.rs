use std::net::SocketAddrV4;

use tokio::sync::mpsc;
use tracing::{instrument, warn};

use crate::{lock, message::{DiscoverMetadata, DpMessageKind, Message, MessageDirection, MetadataKind, NumId}, result_early_return, utils::{ArcCollection, ArcMap, TimerOptions, TransientCollection}};

use super::DPP_TTL_MILLIS;

pub struct DiscoverPeerProcessor {
    message_staging: TransientCollection<ArcMap<NumId, DiscoverMetadata>>
}

impl DiscoverPeerProcessor {
    pub fn new() -> Self {
        Self {
            message_staging: TransientCollection::new(DPP_TTL_MILLIS * 2, false, ArcMap::new()),
        }
    }

    #[instrument(level = "trace", skip(self))]
    pub fn continue_propagating(&self, metadata: &mut DiscoverMetadata) -> bool {
        metadata.hop_count.0 -= 1;
        metadata.hop_count.0 > 0
    }

    pub fn get_score(first: SocketAddrV4, second: SocketAddrV4) -> i32 {
        (second.port() as i32).abs_diff(first.port() as i32) as i32
    }

    pub fn peer_len_curr_max(&self, id: &NumId) -> usize {
        if let Some(staged_metadata) = lock!(self.message_staging.collection().map()).get(id) {
            staged_metadata.peer_list.len()
        }
        else {
            0
        }
    }

    pub fn set_staging_early_return(&mut self, outbound: mpsc::UnboundedSender<Message>, id: NumId) {
        let mut message_staging_clone = self.message_staging.collection().clone();
        self.message_staging.set_timer_with_send_action(id, TimerOptions::new(), move || { Self::send_final_response_static(&mut message_staging_clone, id, &outbound); }, "Discover:MessageStaging");
    }

    #[instrument(level = "trace", skip_all, fields(hop_count = ?metadata.hop_count))]
    pub fn stage_message(&mut self, id: NumId, metadata: DiscoverMetadata, peer_len_curr_max: usize) -> Option<Message> {
        let target_num_peers = metadata.hop_count.1;
        let peers_len = metadata.peer_list.len();
        if peers_len == target_num_peers as usize {
            return Some(Message::new(metadata.origin, id, None, MetadataKind::Discover(metadata), MessageDirection::Response));
        }
        if peers_len > peer_len_curr_max {
            lock!(self.message_staging.collection().map()).insert(id, metadata);
        }
        None
    }

    #[instrument(level = "trace", skip(message_staging))]
    fn send_final_response_static(message_staging: &mut ArcMap<NumId, DiscoverMetadata>, id: NumId, outbound: &mpsc::UnboundedSender<Message>) {
        if let Some(mut staged_metadata) = message_staging.pop(&id) {
            staged_metadata.kind = DpMessageKind::IveGotSome;
            result_early_return!(outbound.send(Message::new(staged_metadata.origin, id, None, MetadataKind::Discover(staged_metadata), MessageDirection::Response)));
        }
    }
}