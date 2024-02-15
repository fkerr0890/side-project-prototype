use std::{collections::HashMap, net::SocketAddrV4};

use tokio::sync::{mpsc, oneshot};

use crate::{gateway::EmptyResult, message::{Message, SearchMessage, SearchMessageKind, StreamMessage, StreamMessageKind}, node::EndpointPair, utils::TransientSet};

use super::{send_error_response, MessageProcessor, ACTIVE_SESSION_TTL, SRP_TTL_MILLIS};

pub struct SearchRequestProcessor {
    message_processor: MessageProcessor,
    from_staging: mpsc::UnboundedReceiver<SearchMessage>,
    to_smp: mpsc::UnboundedSender<StreamMessage>,
    local_hosts: HashMap<String, SocketAddrV4>,
    active_sessions: TransientSet<String>
}

impl SearchRequestProcessor {
    pub fn new(message_processor: MessageProcessor, from_staging: mpsc::UnboundedReceiver<SearchMessage>, to_smp: mpsc::UnboundedSender<StreamMessage>, local_hosts: HashMap<String, SocketAddrV4>) -> Self {
        Self {
            message_processor,
            from_staging,
            to_smp,
            local_hosts,
            active_sessions: TransientSet::new(ACTIVE_SESSION_TTL)
        }
    }

    pub fn handle_search_request(&mut self, mut search_request: SearchMessage) -> EmptyResult {
        if !self.message_processor.try_add_breadcrumb(search_request.id(), search_request.sender()) {
            return Ok(())
        }
        else {
            self.message_processor.set_breadcrumb_ttl(None, search_request.id(), SRP_TTL_MILLIS);
        }
        if self.local_hosts.contains_key(search_request.host_name()) {
            println!("Found host {} at {}, uuid: {}", search_request.host_name(), self.message_processor.endpoint_pair.public_endpoint.port(), search_request.id());
            let (dest, origin) = (search_request.sender(), search_request.origin());
            let (uuid, host_name) = search_request.into_uuid_host_name();
            return self.return_search_responses(self.construct_search_response(uuid, dest, origin, host_name))
        }
        self.message_processor.send_request(&mut search_request, None, true)
    }

    fn return_search_responses(&mut self, mut search_response: SearchMessage) -> EmptyResult {
        let Some(dest) = self.message_processor.get_dest(search_response.id()) else { return Ok(()) };
        if dest == EndpointPair::default_socket() {
            let origin = search_response.origin();
            let (uuid, host_name, peer_public_key) = search_response.into_uuid_host_name_public_key();
            let mut key_store = self.message_processor.key_store.lock().unwrap();
            let my_public_key = key_store.requester_public_key(origin);
            key_store.agree(origin, peer_public_key).unwrap();
            let mut key_agreement_message = StreamMessage::new(host_name, uuid, StreamMessageKind::KeyAgreement, my_public_key.as_ref().to_vec());
            key_agreement_message.replace_dest_and_timestamp(origin);
            self.to_smp
                .send(key_agreement_message)
                .map_err(|e| send_error_response(e, file!(), line!()))
        }
        else {
            self.message_processor.send(dest, &mut search_response, false, false)
        }
    }
    
    fn construct_search_response(&self, uuid: String, dest: SocketAddrV4, origin: SocketAddrV4, host_name: String) -> SearchMessage {
        let (tx, rx) = oneshot::channel();
        self.message_processor.send_nat_heartbeats(rx, origin);
        let public_key = self.message_processor.key_store.lock().unwrap().host_public_key(origin, tx);
        SearchMessage::key_response(dest, self.message_processor.endpoint_pair.public_endpoint, self.message_processor.endpoint_pair.public_endpoint, uuid, host_name, public_key.as_ref().to_vec())
    }

    pub async fn receive(&mut self) -> EmptyResult  {
        let mut message = self.from_staging.recv().await.ok_or("SearchRequestProcessor: failed to receive message from gateway")?;
        if message.origin() == EndpointPair::default_socket() {
            if !self.active_sessions.insert(message.host_name()) {
                println!("SearchMessageProcessor: Blocked search request for {}, reason: active session exists, {:?}", message.host_name(), message);
                return Ok(());
            }
            message.set_origin(self.message_processor.endpoint_pair.public_endpoint)
        }
        match message {
            SearchMessage { kind: SearchMessageKind::Request, ..} => self.handle_search_request(message),
            SearchMessage { kind: SearchMessageKind::Response(_), .. } => self.return_search_responses(message)
        }
    }
}