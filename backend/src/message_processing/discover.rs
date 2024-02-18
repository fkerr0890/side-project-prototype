use std::{collections::HashMap, net::SocketAddrV4, sync::{Arc, Mutex}, time::Duration};

use tokio::{net::UdpSocket, sync::mpsc, time::sleep};

use crate::{crypto::KeyStore, gateway::EmptyResult, message::{DiscoverPeerMessage, DpMessageKind, Id, Message}, node::EndpointPair, utils::TransientMap};

use super::{MessageProcessor, DPP_TTL};

pub struct DiscoverPeerProcessor {
    message_processor: MessageProcessor,
    from_staging: mpsc::UnboundedReceiver<DiscoverPeerMessage>,
    message_staging: Arc<Mutex<HashMap<Id, DiscoverPeerMessage>>>
}

impl DiscoverPeerProcessor {
    pub fn new(message_processor: MessageProcessor, from_staging: mpsc::UnboundedReceiver<DiscoverPeerMessage>) -> Self {
        Self {
            message_processor,
            from_staging,
            message_staging: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn receive(&mut self) -> EmptyResult {
        let message = self.from_staging.recv().await.ok_or("DiscoverPeerProcessor: failed to receive message from gateway")?;
        match message {
            DiscoverPeerMessage { kind: DpMessageKind::INeedSome, .. } => self.request_new_peers(message),
            DiscoverPeerMessage { kind: DpMessageKind::Request, ..} => self.propogate_request(message),
            DiscoverPeerMessage { kind: DpMessageKind::Response, .. } => self.return_response(message),
            DiscoverPeerMessage { kind: DpMessageKind::IveGotSome, .. } => self.add_new_peers(message),
        }
    }

    fn propogate_request(&mut self, mut request: DiscoverPeerMessage) -> EmptyResult {
        request = request.set_origin_if_unset(self.message_processor.endpoint_pair.public_endpoint);
        let (sender, origin) = (request.sender(), request.origin());
        if self.message_processor.try_add_breadcrumb(request.id(), request.sender()) {
            let mut hairpin_response = DiscoverPeerMessage::new(DpMessageKind::Response,
                request.origin(),
                request.id().to_owned(),
            request.hop_count());
            hairpin_response.try_decrement_hop_count();
            if !request.try_decrement_hop_count() {
                // gateway::log_debug("At hairpin");
                self.return_response(hairpin_response)?;
                return Ok(())
            }
            self.message_processor.set_breadcrumb_ttl(Some(hairpin_response), request.id(), DPP_TTL);
            // gateway::log_debug("Propogating request");
            self.message_processor.send_request(&mut request, None, false)?;
        }
        let endpoint_pair = if sender != EndpointPair::default_socket() {
            EndpointPair::new(sender, sender)
        } else {
            EndpointPair::new(origin, origin)
        };        
        self.message_processor.add_new_peers(vec![endpoint_pair]);
        Ok(())
    }

    pub fn get_score(first: SocketAddrV4, second: SocketAddrV4) -> i32 {
        (second.port() as i32).abs_diff(first.port() as i32) as i32
    }

    fn stage_message(&mut self, message: DiscoverPeerMessage) -> bool {
        let staged_peers_len = {
            let message_staging = self.message_staging.lock().unwrap();
            let entry = message_staging.get(message.id());
            if let Some(staged_message) = entry {
                staged_message.peer_list().len()
            }
            else {
                let message_staging_clone = self.message_staging.clone();
                let (socket, key_store, uuid, dest, sender) = (self.message_processor.socket.clone(), self.message_processor.key_store.clone(), message.id().to_owned(), self.message_processor.get_dest(message.id()).unwrap(), self.message_processor.endpoint_pair.public_endpoint);
                tokio::spawn(async move {
                    sleep(Duration::from_millis(DPP_TTL*2)).await;
                    Self::send_final_response_static(&socket, &key_store, dest, sender, &message_staging_clone, &uuid);
                    message_staging_clone.lock().unwrap().remove(&uuid);
                });
                0
            }
        };

        let target_num_peers = message.hop_count().1;
        let peers_len = message.peer_list().len();
        println!("hop count: {:?}, peer list len: {}", message.hop_count(), peers_len);
        if peers_len > staged_peers_len {
            // self.message_staging.set_timer(message.id().to_owned(), String::from("DiscoverPeerStaging"));
            self.message_staging.lock().unwrap().insert(message.id().to_owned(), message);
        }
        peers_len == target_num_peers as usize
    }

    fn return_response(&mut self, mut response: DiscoverPeerMessage) -> EmptyResult {
        response.add_peer(self.message_processor.endpoint_pair);
        let Some(dest) = self.message_processor.get_dest(response.id()) else { return Ok(()) };
        if response.origin() == self.message_processor.endpoint_pair.public_endpoint {
            let uuid = response.id().to_owned();
            if self.stage_message(response) {
                self.send_final_response(&uuid);
            }
            Ok(())
        }
        else {
            self.message_processor.send(dest, &mut response, false, false)
        }
    }

    fn send_final_response(&self, uuid: &Id) {
        let dest = self.message_processor.get_dest(uuid).unwrap();
        Self::send_final_response_static(&self.message_processor.socket, &self.message_processor.key_store, dest, self.message_processor.endpoint_pair.public_endpoint, &self.message_staging, uuid);
    }

    fn send_final_response_static(socket: &Arc<UdpSocket>, key_store: &Arc<Mutex<KeyStore>>, dest: SocketAddrV4, sender: SocketAddrV4, message_staging: &Arc<Mutex<HashMap<Id, DiscoverPeerMessage>>>, uuid: &Id) {
        let staged_message = {
            let mut message_staging = message_staging.lock().unwrap();
            message_staging.remove(uuid)
        };
        if let Some(staged_message) = staged_message {
            MessageProcessor::send_static(socket, key_store, dest, sender, &mut staged_message.set_kind(DpMessageKind::IveGotSome), false, false).ok();
        }
    }

    fn request_new_peers(&self, mut message: DiscoverPeerMessage) -> EmptyResult {
        // gateway::log_debug(&format!("Got INeedSome, introducer = {}", message.peer_list()[0].public_endpoint));
        let introducer = message.get_last_peer();
        self.message_processor.send(introducer.public_endpoint, 
            &mut message
                .set_kind(DpMessageKind::Request),
            false,
            false)
    }

    fn add_new_peers(&self, message: DiscoverPeerMessage) -> EmptyResult {
        self.message_processor.add_new_peers(message.into_peer_list());
        Ok(())
    }
}