use std::{net::SocketAddrV4, sync::{Arc, Mutex}};

use tokio::{net::UdpSocket, sync::mpsc};

use crate::{crypto::KeyStore, message::{DiscoverPeerMessage, DpMessageKind, Id, Message, Peer, Sender}, node::EndpointPair, utils::{TransientMap, TtlType}};

use super::{EmptyResult, OutboundGateway, DPP_TTL_MILLIS};

pub struct DiscoverPeerProcessor {
    outbound_gateway: OutboundGateway,
    from_staging: mpsc::UnboundedReceiver<DiscoverPeerMessage>,
    message_staging: TransientMap<Id, DiscoverPeerMessage>
}

impl DiscoverPeerProcessor {
    pub fn new(outbound_gateway: OutboundGateway, from_staging: mpsc::UnboundedReceiver<DiscoverPeerMessage>) -> Self {
        Self {
            outbound_gateway,
            from_staging,
            message_staging: TransientMap::new(TtlType::Millis(DPP_TTL_MILLIS * 2)),
        }
    }

    pub async fn receive(&mut self) -> EmptyResult {
        let message = self.from_staging.recv().await.ok_or("DiscoverPeerProcessor: failed to receive message from gateway")?;
        match message {
            DiscoverPeerMessage { kind: DpMessageKind::INeedSome, .. } => self.request_new_peers(message),
            DiscoverPeerMessage { kind: DpMessageKind::Request, ..} => self.propagate_request(message),
            DiscoverPeerMessage { kind: DpMessageKind::Response, .. } => self.return_response(message, None),
            _ => { println!("DiscoverPeerProcessor received unsupported message {:?}", message); Ok(()) }
        }
    }

    fn propagate_request(&mut self, mut request: DiscoverPeerMessage) -> EmptyResult {
        let origin = request.origin().unwrap();
        let sender = if request.sender() == origin.endpoint_pair().public_endpoint || request.sender() == origin.endpoint_pair().private_endpoint { EndpointPair::default_socket() } else { request.sender() };
        let mut hairpin_response = DiscoverPeerMessage::new(DpMessageKind::Response,
            Some(origin.clone()),
            request.id().to_owned(),
        request.hop_count());
        hairpin_response.try_decrement_hop_count();
        if !request.try_decrement_hop_count() {
            // gateway::log_debug("At hairpin");
            self.return_response(hairpin_response, Some(request.sender()))?;
        }
        else if self.outbound_gateway.try_add_breadcrumb(Some(hairpin_response), request.id(), sender) {
            self.outbound_gateway.send_request(&mut request, None, false)?;
        }
        Ok(())
    }

    pub fn get_score(first: SocketAddrV4, second: SocketAddrV4) -> i32 {
        (second.port() as i32).abs_diff(first.port() as i32) as i32
    }

    fn stage_message(&mut self, message: DiscoverPeerMessage) -> bool {
        let staged_peers_len = 'b1: {
            if let Some(staged_message) = self.message_staging.map().lock().unwrap().get(message.id()) {
                break 'b1 staged_message.peer_list().len();
            }
            let message_staging_clone = self.message_staging.clone();
            let (socket, key_store, uuid, dest, sender) = (self.outbound_gateway.socket.clone(), self.outbound_gateway.key_store.clone(), message.id().to_owned(), message.origin().unwrap().endpoint_pair(), self.outbound_gateway.myself.clone());
            self.message_staging.set_timer_with_send_action(uuid.clone(), move || { Self::send_final_response_static(&socket, &key_store, dest, sender.clone(), &message_staging_clone, &uuid); });
            0
        };

        let target_num_peers = message.hop_count().1;
        let peers_len = message.peer_list().len();
        println!("hop count: {:?}, peer list len: {}", message.hop_count(), peers_len);
        if peers_len > staged_peers_len {
            // self.message_staging.set_timer(message.id().to_owned(), String::from("DiscoverPeerStaging"));
            self.message_staging.map().lock().unwrap().insert(message.id().to_owned(), message);
        }
        peers_len == target_num_peers as usize
    }

    fn return_response(&mut self, mut response: DiscoverPeerMessage, dest: Option<SocketAddrV4>) -> EmptyResult {
        response.add_peer(self.outbound_gateway.myself.clone());
        let Some(dest) = (if dest.is_some() { dest } else { self.outbound_gateway.get_dest(response.id()) }) else { return Ok(()) };
        if dest == EndpointPair::default_socket() {
            let (uuid, origin) = (response.id().to_owned(), response.origin().unwrap().endpoint_pair());
            if self.stage_message(response) {
                self.send_final_response(&uuid, origin);
            }
            Ok(())
        }
        else {
            self.outbound_gateway.send_individual(dest, &mut response, false, false)
        }
    }

    fn send_final_response(&self, uuid: &Id, dest: EndpointPair) {
        Self::send_final_response_static(&self.outbound_gateway.socket, &self.outbound_gateway.key_store, dest, self.outbound_gateway.myself.clone(), &self.message_staging, uuid);
    }

    fn send_final_response_static(socket: &Arc<UdpSocket>, key_store: &Arc<Mutex<KeyStore>>, dest: EndpointPair, sender: Peer, message_staging: &TransientMap<Id, DiscoverPeerMessage>, uuid: &Id) {
        println!("Sending final response");
        let staged_message = {
            let mut message_staging = message_staging.map().lock().unwrap();
            message_staging.remove(uuid)
        };
        if let Some(mut staged_message) = staged_message {
            staged_message.set_kind(DpMessageKind::IveGotSome);
            let (endpoint_pair, uuid) = sender.into_parts();
            OutboundGateway::send_static(socket, key_store, dest.public_endpoint, Sender::new(endpoint_pair.public_endpoint, uuid.clone()), &mut staged_message, false, false).ok();
            OutboundGateway::send_static(socket, key_store, dest.private_endpoint, Sender::new(endpoint_pair.private_endpoint, uuid), &mut staged_message, false, false).ok();
        }
    }

    fn request_new_peers(&mut self, mut message: DiscoverPeerMessage) -> EmptyResult {
        //TODO: Keep connection with introducer alive
        self.outbound_gateway.try_add_breadcrumb(None, message.id(), self.outbound_gateway.myself.endpoint_pair().private_endpoint);
        let introducer = message.get_last_peer();
        message.set_kind(DpMessageKind::Request);
        self.outbound_gateway.send(introducer.endpoint_pair(),
            &mut message.set_origin_if_unset(self.outbound_gateway.myself.clone()),
            false,
            false)
    }
}