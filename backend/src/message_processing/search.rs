use std::net::SocketAddrV4;

use tokio::sync::mpsc;

use crate::{message::{Id, Message, Peer, SearchMessage, SearchMessageInnerKind, SearchMessageKind, StreamMessage, StreamMessageInnerKind, StreamMessageKind}, node::EndpointPair, utils::{TransientSet, TtlType}};

use super::{EmptyResult, send_error_response, OutboundGateway, ACTIVE_SESSION_TTL_SECONDS};

pub struct SearchRequestProcessor<F: Fn(&SearchMessage) -> bool> {
    outbound_gateway: OutboundGateway,
    from_staging: mpsc::UnboundedReceiver<SearchMessage>,
    to_smp: mpsc::UnboundedSender<StreamMessage>,
    active_sessions: TransientSet<String>,
    stop_condition: F
}

impl<F: Fn(&SearchMessage) -> bool> SearchRequestProcessor<F> {
    pub fn new(outbound_gateway: OutboundGateway, from_staging: mpsc::UnboundedReceiver<SearchMessage>, to_smp: mpsc::UnboundedSender<StreamMessage>, stop_condition: F) -> Self {
        Self {
            outbound_gateway,
            from_staging,
            to_smp,
            active_sessions: TransientSet::new(TtlType::Secs(ACTIVE_SESSION_TTL_SECONDS)),
            stop_condition
        }
    }

    pub fn handle_search_request(&mut self, mut search_request: SearchMessage, is_resource_kind: bool) -> EmptyResult {
        if !self.outbound_gateway.try_add_breadcrumb(None, search_request.id(), search_request.sender()) {
            return Ok(())
        }
        if (self.stop_condition)(&search_request) {
            println!("Found host {} at {:?}, uuid: {}", search_request.host_name(), self.outbound_gateway.myself, search_request.id());
            let (uuid, host_name, origin) = search_request.into_uuid_host_name_origin();
            let Some(search_response) = self.construct_search_response(uuid, origin, host_name, is_resource_kind) else { return Ok(()) };
            return self.return_search_responses(search_response, is_resource_kind)
        }
        self.outbound_gateway.send_request(&mut search_request, None)
    }

    fn return_search_responses(&mut self, mut search_response: SearchMessage, is_resource_kind: bool) -> EmptyResult {
        let Some(dest) = self.outbound_gateway.get_dest(search_response.id()) else { return Ok(()) };
        if dest == EndpointPair::default_socket() {
            let sender = search_response.sender();
            let (uuid, host_name, peer_public_key, origin) = search_response.into_uuid_host_name_public_key_origin();
            let mut key_store = self.outbound_gateway.key_store.lock().unwrap();
            let origin = if sender == origin.endpoint_pair().private_endpoint { sender } else { origin.endpoint_pair().public_endpoint };
            let my_public_key = key_store.requester_public_key(origin);
            key_store.agree(origin, peer_public_key).unwrap();
            let kind = if is_resource_kind { StreamMessageKind::Resource(StreamMessageInnerKind::KeyAgreement) } else { StreamMessageKind::Resource(StreamMessageInnerKind::KeyAgreement) };
            let mut key_agreement_message = StreamMessage::new(host_name, uuid, kind, my_public_key.as_ref().to_vec());
            key_agreement_message.replace_dest(origin);
            self.to_smp
                .send(key_agreement_message)
                .map_err(|e| send_error_response(e, file!(), line!()))
        }
        else {
            self.outbound_gateway.send_individual(dest, &mut search_response, false, false)
        }
    }
    
    fn construct_search_response(&self, uuid: Id, origin: Option<Peer>, host_name: String, is_resource_kind: bool) -> Option<SearchMessage> {
        let endpoint_pair = origin.unwrap().endpoint_pair();
        if let Some(mut search_response) = self.build_response(uuid.clone(), endpoint_pair.private_endpoint, host_name.clone(), is_resource_kind) {
            self.outbound_gateway.send_individual(endpoint_pair.private_endpoint, &mut search_response, false, false).ok();
        }
        self.build_response(uuid, endpoint_pair.public_endpoint, host_name, is_resource_kind)
    }

    fn build_response(&self, uuid: Id, peer_addr: SocketAddrV4, host_name: String, is_resource_kind: bool) -> Option<SearchMessage> {
        let public_key = self.outbound_gateway.key_store.lock().unwrap().host_public_key(peer_addr);
        Some(SearchMessage::key_response(self.outbound_gateway.myself.clone(), uuid, host_name, public_key.as_ref().to_vec(), is_resource_kind))
    }

    pub async fn receive(&mut self) -> EmptyResult  {
        let mut message = self.from_staging.recv().await.ok_or("SearchRequestProcessor: failed to receive message from gateway")?;
        if let None = message.origin() {
            if !self.active_sessions.insert(message.host_name().clone()) {
                println!("SearchMessageProcessor: Blocked search request for {}, reason: active session exists, {:?}", message.host_name(), message);
                return Ok(());
            }
            message.set_origin(self.outbound_gateway.myself.clone());
            message.replace_dest(self.outbound_gateway.myself.endpoint_pair().public_endpoint);
        }
        match message {
            SearchMessage { kind: SearchMessageKind::Resource(SearchMessageInnerKind::Request), ..} => self.handle_search_request(message, true),
            SearchMessage { kind: SearchMessageKind::Resource(SearchMessageInnerKind::Response(_)), .. } => self.return_search_responses(message, true),
            SearchMessage { kind: SearchMessageKind::Distribution(SearchMessageInnerKind::Request), ..} => self.handle_search_request(message, false),
            SearchMessage { kind: SearchMessageKind::Distribution(SearchMessageInnerKind::Response(_)), .. } => self.return_search_responses(message, false)
        }
    }
}