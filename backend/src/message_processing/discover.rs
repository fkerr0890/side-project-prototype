use std::{collections::HashMap, net::SocketAddrV4, sync::{Arc, Mutex}};

use tokio::{net::UdpSocket, sync::mpsc};
use tracing::{instrument, warn};

use crate::{message::{DiscoverPeerMessage, DpMessageKind, Message, NumId, Peer, Sender}, node::EndpointPair, option_early_return, utils::{TransientMap, TtlType}};

use super::{BreadcrumbService, EmptyOption, OutboundGateway, ToBeEncrypted, DPP_TTL_MILLIS};

pub struct DiscoverPeerProcessor {
    outbound_gateway: OutboundGateway,
    breadcrumb_service: BreadcrumbService,
    from_staging: mpsc::UnboundedReceiver<DiscoverPeerMessage>,
    message_staging: TransientMap<NumId, DiscoverPeerMessage>
}

impl DiscoverPeerProcessor {
    pub fn new(outbound_gateway: OutboundGateway, breadcrumb_service: BreadcrumbService, from_staging: mpsc::UnboundedReceiver<DiscoverPeerMessage>) -> Self {
        Self {
            outbound_gateway,
            breadcrumb_service,
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
        let sender = request.sender();
        let sender = if sender.id == origin.id { None } else { Some(sender) };
        let mut hairpin_response = DiscoverPeerMessage::new(DpMessageKind::Response,
            Some(origin.clone()),
            request.id(),
        request.hop_count());
        hairpin_response.try_decrement_hop_count();
        if !request.try_decrement_hop_count() {
            // gateway::log_debug("At hairpin");
            self.return_response(hairpin_response, sender);
            return;
        }
        let early_return_context = (hairpin_response, self.outbound_gateway.myself.endpoint_pair.private_endpoint, self.outbound_gateway.myself, self.outbound_gateway.socket.clone());
        if self.breadcrumb_service.try_add_breadcrumb(Some(early_return_context), request.id(), sender) {
            self.outbound_gateway.send_request(&mut request, sender);
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
            let (socket, dest, myself, id) = (self.outbound_gateway.socket.clone(), message.origin().unwrap().endpoint_pair, self.outbound_gateway.myself, message.id());
            self.message_staging.set_timer_with_send_action(message.id(), move || { Self::send_final_response_static(&socket, dest, myself, &message_staging_clone, id); });
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
    fn return_response(&mut self, mut response: DiscoverPeerMessage, dest: Option<Sender>) {
        response.add_peer(self.outbound_gateway.myself);
        let dest = if dest.is_some() { Some(dest) } else { self.breadcrumb_service.get_dest(&response.id()) };
        let dest = option_early_return!(dest);
        if let Some(dest) = dest {
            self.outbound_gateway.send_individual(dest.socket, &mut response, false, false);
        }
        else {
            let (id, origin) = (response.id(), response.origin().unwrap().endpoint_pair);
            if self.stage_message(response) {
                self.send_final_response(id, origin);
            }
        }
    }

    fn send_final_response(&self, id: NumId, dest: EndpointPair) {
        Self::send_final_response_static(&self.outbound_gateway.socket, dest, self.outbound_gateway.myself, self.message_staging.map(), id);
    }

    #[instrument(level = "trace", skip(socket, myself, message_staging))]
    fn send_final_response_static(socket: &Arc<UdpSocket>, dest: EndpointPair, myself: Peer, message_staging: &Arc<Mutex<HashMap<NumId, DiscoverPeerMessage>>>, id: NumId) {
        let staged_message = {
            let mut message_staging = message_staging.lock().unwrap();
            message_staging.remove(&id)
        };
        if let Some(mut staged_message) = staged_message {
            staged_message.set_kind(DpMessageKind::IveGotSome);
            OutboundGateway::send_private_public_static(socket, dest, myself, &mut staged_message, ToBeEncrypted::False, false);
        }
    }

    #[instrument(level = "trace", skip(self))]
    fn request_new_peers(&mut self, mut message: DiscoverPeerMessage) {
        self.breadcrumb_service.try_add_breadcrumb(None, message.id(), Some(Sender::new(self.outbound_gateway.myself.endpoint_pair.private_endpoint, self.outbound_gateway.myself.id)));
        let introducer = message.get_last_peer();
        message.set_kind(DpMessageKind::Request);
        self.outbound_gateway.send(introducer.endpoint_pair,
            &mut message.set_origin_if_unset(self.outbound_gateway.myself),
            false,
            false);
    }
}