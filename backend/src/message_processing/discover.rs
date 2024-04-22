use std::{net::SocketAddrV4, sync::{Arc, Mutex}};

use tokio::{net::UdpSocket, sync::mpsc};
use tracing::{instrument, warn};

use crate::{crypto::KeyStore, lock, message::{DiscoverMetadata, DiscoverPeerMessage, DpMessageKind, Message, MessageDirection, Messagea, MetadataKind, NumId, Peer, Sender}, option_early_return, result_early_return, utils::{ArcCollection, ArcMap, TransientCollection}};

use super::{BreadcrumbService, EmptyOption, OutboundGateway, DPP_TTL_MILLIS};

pub struct DiscoverPeerProcessor {
    outbound_gateway: OutboundGateway,
    breadcrumb_service: BreadcrumbService,
    from_staging: mpsc::UnboundedReceiver<DiscoverPeerMessage>,
    message_staging: TransientCollection<ArcMap<NumId, DiscoverMetadata>>
}

impl DiscoverPeerProcessor {
    pub fn new(outbound_gateway: OutboundGateway, breadcrumb_service: BreadcrumbService, from_staging: mpsc::UnboundedReceiver<DiscoverPeerMessage>) -> Self {
        Self {
            outbound_gateway,
            breadcrumb_service,
            from_staging,
            message_staging: TransientCollection::new(DPP_TTL_MILLIS * 2, false, ArcMap::new()),
        }
    }

    pub async fn receive(&mut self) -> EmptyOption {
        let message = self.from_staging.recv().await?;
        match message {
            DiscoverPeerMessage { kind: DpMessageKind::INeedSome, .. } => self.request_new_peers(message),
            DiscoverPeerMessage { kind: DpMessageKind::Request, ..} => self.propagate_request(message),
            DiscoverPeerMessage { kind: DpMessageKind::Response, .. } => self.return_response(message, None),
            _ => warn!(?message, "Received unsupported message")
        }
        Some(())
    }

    #[instrument(level = "trace", skip(self))]
    pub fn continue_propagating(&self, metadata: &mut DiscoverMetadata) -> bool {
        metadata.hop_count.0 -= 1;
        metadata.hop_count.0 > 0
    }

    #[instrument(level = "trace", skip(self))]
    fn propagate_request(&mut self, mut request: DiscoverPeerMessage) {
        let origin = request.origin().unwrap();
        let sender = request.sender();
        let sender = if sender.id == origin.id { None } else { Some(sender) };
        let mut hairpin_response = DiscoverPeerMessage::new(DpMessageKind::Response,
            Some(origin),
            request.id(),
        request.hop_count());
        hairpin_response.try_decrement_hop_count();
        if !request.try_decrement_hop_count() {
            // gateway::log_debug("At hairpin");
            self.return_response(hairpin_response, sender);
            return;
        }
        // let early_return_context = (
        //     hairpin_response,
        //     Sender::new(self.outbound_gateway.myself.endpoint_pair.private_endpoint,
        //         self.outbound_gateway.myself.id),
        //         self.outbound_gateway.myself,
        //         self.outbound_gateway.socket.clone(),
        //         self.outbound_gateway.key_store.clone());
        // if self.breadcrumb_service.try_add_breadcrumb(Some(early_return_context), request.id(), sender) {
        //     self.outbound_gateway.send_request(&mut request, sender);
        // }
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

    pub fn set_staging_early_return(&mut self, outbound: mpsc::UnboundedSender<Messagea>, id: NumId) {
        let mut message_staging_clone = self.message_staging.collection().clone();
        self.message_staging.set_timer_with_send_action(id, move || { Self::send_final_response_static(&mut message_staging_clone, id, &outbound); }, "Discover:MessageStaging");
    }

    #[instrument(level = "trace", skip_all, fields(hop_count = ?metadata.hop_count))]
    pub fn stage_message1(&mut self, id: NumId, metadata: DiscoverMetadata, peer_len_curr_max: usize) -> Option<Messagea> {
        let target_num_peers = metadata.hop_count.1;
        let peers_len = metadata.peer_list.len();
        if peers_len == target_num_peers as usize {
            return Some(Messagea::new(metadata.origin, id, None, MetadataKind::Discover(metadata), MessageDirection::Response));
        }
        if peers_len > peer_len_curr_max {
            lock!(self.message_staging.collection().map()).insert(id, metadata);
        }
        None
    }

    #[instrument(level = "trace", skip_all, fields(hop_count = ?message.hop_count()))]
    fn stage_message(&mut self, message: DiscoverPeerMessage) -> bool {
        // let staged_peers_len = 'b1: {
        //     if let Some(staged_message) = lock!(self.message_staging.collection().map()).get(&message.id()) {
        //         break 'b1 staged_message.peer_list().len();
        //     }
        //     let mut message_staging_clone = self.message_staging.collection().clone();
        //     let (socket, dest, myself, id, key_store) = (self.outbound_gateway.socket.clone(), message.origin().unwrap(), self.outbound_gateway.myself, message.id(), self.outbound_gateway.key_store.clone());
        //     self.message_staging.set_timer_with_send_action(message.id(), move || { Self::send_final_response_static(&socket, dest, myself, &mut message_staging_clone, id, key_store.clone()); }, "Discover:MessageStaging");
        //     0
        // };

        // let target_num_peers = message.hop_count().1;
        // let peers_len = message.peer_list().len();
        // if peers_len > staged_peers_len {
        //     lock!(self.message_staging.collection().map()).insert(message.id(), message);
        // }
        // peers_len == target_num_peers as usize
        false
    }

    #[instrument(level = "trace", skip(self))]
    fn return_response(&mut self, mut response: DiscoverPeerMessage, dest: Option<Sender>) {
        response.add_peer(self.outbound_gateway.myself);
        let dest = if dest.is_some() { Some(dest) } else { self.breadcrumb_service.get_dest(&response.id()) };
        let dest = option_early_return!(dest);
        if let Some(dest) = dest {
            self.outbound_gateway.send_individual(dest, &mut response, false);
        }
        else {
            let (id, origin) = (response.id(), response.origin().unwrap());
            if self.stage_message(response) {
                self.send_final_response(id, origin);
            }
        }
    }

    fn send_final_response(&mut self, id: NumId, dest: Peer) {
        // Self::send_final_response_static(&self.outbound_gateway.socket, dest, self.outbound_gateway.myself, self.message_staging.collection_mut(), id, self.outbound_gateway.key_store.clone());
    }

    #[instrument(level = "trace", skip(message_staging))]
    fn send_final_response_static(message_staging: &mut ArcMap<NumId, DiscoverMetadata>, id: NumId, outbound: &mpsc::UnboundedSender<Messagea>) {
        if let Some(mut staged_metadata) = message_staging.pop(&id) {
            staged_metadata.kind = DpMessageKind::IveGotSome;
            result_early_return!(outbound.send(Messagea::new(staged_metadata.origin, id, None, MetadataKind::Discover(staged_metadata), MessageDirection::Response)));
        }
    }

    #[instrument(level = "trace", skip(self))]
    fn request_new_peers(&mut self, mut message: DiscoverPeerMessage) {
        // self.breadcrumb_service.try_add_breadcrumb(None, message.id(), Some(Sender::new(self.outbound_gateway.myself.endpoint_pair.private_endpoint, self.outbound_gateway.myself.id)));
        let introducer = message.get_last_peer();
        message.set_kind(DpMessageKind::Request);
        // self.outbound_gateway.send(introducer, &mut message.set_origin_if_unset(self.outbound_gateway.myself), false);
    }
}