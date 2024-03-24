use std::{collections::HashMap, net::SocketAddrV4, sync::{Arc, Mutex}};

use tokio::{net::UdpSocket, sync::mpsc};
use tracing::{instrument, warn};

use crate::{crypto::KeyStore, message::{DiscoverPeerMessage, DpMessageKind, NumId, Message, Peer}, node::EndpointPair, option_early_return, utils::{TransientMap, TtlType}};

use super::{EmptyOption, OutboundGateway, DPP_TTL_MILLIS};

pub struct DiscoverPeerProcessor {
    outbound_gateway: OutboundGateway,
    from_staging: mpsc::UnboundedReceiver<DiscoverPeerMessage>,
    message_staging: TransientMap<NumId, DiscoverPeerMessage>
}

impl DiscoverPeerProcessor {
    pub fn new(outbound_gateway: OutboundGateway, from_staging: mpsc::UnboundedReceiver<DiscoverPeerMessage>) -> Self {
        Self {
            outbound_gateway,
            from_staging,
            message_staging: TransientMap::new(TtlType::Millis(DPP_TTL_MILLIS * 2), false),
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
    fn propagate_request(&mut self, mut request: DiscoverPeerMessage) {
        let origin = request.origin().unwrap();
        let sender = if request.sender() == origin.endpoint_pair.public_endpoint || request.sender() == origin.endpoint_pair.private_endpoint { EndpointPair::default_socket() } else { request.sender() };
        let mut hairpin_response = DiscoverPeerMessage::new(DpMessageKind::Response,
            Some(origin.clone()),
            request.id(),
        request.hop_count());
        hairpin_response.try_decrement_hop_count();
        if !request.try_decrement_hop_count() {
            // gateway::log_debug("At hairpin");
            self.return_response(hairpin_response, Some(request.sender()));
        }
        else if self.outbound_gateway.try_add_breadcrumb(Some(hairpin_response), request.id(), sender) {
            self.outbound_gateway.send_request(&mut request, None);
        }
    }

    pub fn get_score(first: SocketAddrV4, second: SocketAddrV4) -> i32 {
        (second.port() as i32).abs_diff(first.port() as i32) as i32
    }

    #[instrument(level = "trace", skip_all, fields(hop_count = ?message.hop_count()))]
    fn stage_message(&mut self, message: DiscoverPeerMessage) -> bool {
        let staged_peers_len = 'b1: {
            if let Some(staged_message) = self.message_staging.map().lock().unwrap().get(&message.id()) {
                break 'b1 staged_message.peer_list().len();
            }
            let message_staging_clone = self.message_staging.map().clone();
            let (socket, key_store, dest, myself, id) = (self.outbound_gateway.socket.clone(), self.outbound_gateway.key_store.clone(), message.origin().unwrap().endpoint_pair, self.outbound_gateway.myself, message.id());
            self.message_staging.set_timer_with_send_action(message.id(), move || { Self::send_final_response_static(&socket, &key_store, dest, myself, &message_staging_clone, id); });
            0
        };

        let target_num_peers = message.hop_count().1;
        let peers_len = message.peer_list().len();
        if peers_len > staged_peers_len {
            self.message_staging.map().lock().unwrap().insert(message.id(), message);
        }
        peers_len == target_num_peers as usize
    }

    #[instrument(level = "trace", skip(self))]
    fn return_response(&mut self, mut response: DiscoverPeerMessage, dest: Option<SocketAddrV4>) {
        response.add_peer(self.outbound_gateway.myself);
        let dest = if dest.is_some() { dest } else { self.outbound_gateway.get_dest(&response.id()) };
        let dest = option_early_return!(dest);
        if dest == EndpointPair::default_socket() {
            let (id, origin) = (response.id(), response.origin().unwrap().endpoint_pair);
            if self.stage_message(response) {
                self.send_final_response(id, origin);
            }
        }
        else {
            self.outbound_gateway.send_individual(dest, &mut response, false, false);
        }
    }

    fn send_final_response(&self, id: NumId, dest: EndpointPair) {
        Self::send_final_response_static(&self.outbound_gateway.socket, &self.outbound_gateway.key_store, dest, self.outbound_gateway.myself, self.message_staging.map(), id);
    }

    #[instrument(level = "trace", skip(socket, key_store, myself, message_staging))]
    fn send_final_response_static(socket: &Arc<UdpSocket>, key_store: &Arc<Mutex<KeyStore>>, dest: EndpointPair, myself: Peer, message_staging: &Arc<Mutex<HashMap<NumId, DiscoverPeerMessage>>>, id: NumId) {
        let staged_message = {
            let mut message_staging = message_staging.lock().unwrap();
            message_staging.remove(&id)
        };
        if let Some(mut staged_message) = staged_message {
            staged_message.set_kind(DpMessageKind::IveGotSome);
            OutboundGateway::send_static(socket, key_store, dest.public_endpoint, myself, &mut staged_message, false, false);
            OutboundGateway::send_static(socket, key_store, dest.private_endpoint, myself, &mut staged_message, false, false);
        }
    }

    #[instrument(level = "trace", skip(self))]
    fn request_new_peers(&mut self, mut message: DiscoverPeerMessage) {
        self.outbound_gateway.try_add_breadcrumb(None, message.id(), self.outbound_gateway.myself.endpoint_pair.private_endpoint);
        let introducer = message.get_last_peer();
        message.set_kind(DpMessageKind::Request);
        self.outbound_gateway.send(introducer.endpoint_pair,
            &mut message.set_origin_if_unset(self.outbound_gateway.myself),
            false,
            false);
    }
}