use std::net::SocketAddrV4;

use tokio::sync::mpsc;
use tracing::{debug, instrument, error};

use crate::{message::{Message, NumId, Peer, SearchMessage, SearchMessageInnerKind, SearchMessageKind, Sender, StreamMessage, StreamMessageInnerKind, StreamMessageKind}, option_early_return, result_early_return, utils::{TransientSet, TtlType}};

use super::{BreadcrumbService, EmptyOption, OutboundGateway, ACTIVE_SESSION_TTL_SECONDS};

pub struct SearchRequestProcessor<F: Fn(&SearchMessage) -> bool> {
    outbound_gateway: OutboundGateway,
    breadcrumb_service: BreadcrumbService,
    from_staging: mpsc::UnboundedReceiver<SearchMessage>,
    to_smp: mpsc::UnboundedSender<StreamMessage>,
    active_sessions: TransientSet<String>,
    stop_condition: F
}

impl<F: Fn(&SearchMessage) -> bool> SearchRequestProcessor<F> {
    pub fn new(outbound_gateway: OutboundGateway, breadcrumb_service: BreadcrumbService, from_staging: mpsc::UnboundedReceiver<SearchMessage>, to_smp: mpsc::UnboundedSender<StreamMessage>, stop_condition: F) -> Self {
        Self {
            outbound_gateway,
            breadcrumb_service,
            from_staging,
            to_smp,
            active_sessions: TransientSet::new(TtlType::Secs(ACTIVE_SESSION_TTL_SECONDS), true),
            stop_condition
        }
    }

    #[instrument(level = "trace", skip_all, fields(search_request.sender = ?search_request.sender(), search_request.id = %search_request.id()))]
    pub fn handle_search_request(&mut self, mut search_request: SearchMessage, is_resource_kind: bool) {
        let sender = search_request.sender_option();
        if !self.breadcrumb_service.try_add_breadcrumb(None, search_request.id(), sender) {
            return;
        }
        if (self.stop_condition)(&search_request) {
            debug!(host_name = search_request.host_name(), curr_node = ?self.outbound_gateway.myself, message_id = %search_request.id(), "Stopped propagating search request");
            let (id, host_name, origin) = search_request.into_id_host_name_origin();
            let search_response = option_early_return!(self.construct_search_response(id, origin, host_name, is_resource_kind));
            return self.return_search_responses(search_response, is_resource_kind);
        }
        self.outbound_gateway.send_request(&mut search_request, sender);
    }

    #[instrument(level = "trace", skip_all, fields(search_response.sender = ?search_response.sender(), search_response.id = %search_response.id()))]
    fn return_search_responses(&mut self, mut search_response: SearchMessage, is_resource_kind: bool) {
        let dest = option_early_return!(self.breadcrumb_service.get_dest(&search_response.id()));
        if let Some(dest) = dest {
            self.outbound_gateway.send_individual(dest.socket, &mut search_response, false, false);
        }
        else {
            let sender = search_response.sender();
            let (id, host_name, peer_public_key, origin) = search_response.into_id_host_name_public_key_origin();
            let mut key_store = self.outbound_gateway.key_store.lock().unwrap();
            let origin = if sender.socket == origin.endpoint_pair.private_endpoint { sender } else { Sender::new(origin.endpoint_pair.public_endpoint, origin.id) };
            let my_public_key = key_store.requester_public_key(origin.socket);
            result_early_return!(key_store.agree(origin.socket, peer_public_key));
            let kind = if is_resource_kind { StreamMessageKind::Resource(StreamMessageInnerKind::KeyAgreement) } else { StreamMessageKind::Resource(StreamMessageInnerKind::KeyAgreement) };
            let mut key_agreement_message = StreamMessage::new(host_name, id, kind, my_public_key.as_ref().to_vec());
            key_agreement_message.set_sender(origin);
            result_early_return!(self.to_smp.send(key_agreement_message));
        }
    }
    
    fn construct_search_response(&self, id: NumId, origin: Option<Peer>, host_name: String, is_resource_kind: bool) -> Option<SearchMessage> {
        let endpoint_pair = origin.unwrap().endpoint_pair;
        if let Some(mut search_response) = self.build_response(id, endpoint_pair.private_endpoint, host_name.clone(), is_resource_kind) {
            self.outbound_gateway.send_individual(endpoint_pair.private_endpoint, &mut search_response, false, false);
        }
        self.build_response(id, endpoint_pair.public_endpoint, host_name, is_resource_kind)
    }

    fn build_response(&self, id: NumId, peer_addr: SocketAddrV4, host_name: String, is_resource_kind: bool) -> Option<SearchMessage> {
        let public_key = self.outbound_gateway.key_store.lock().unwrap().host_public_key(peer_addr);
        Some(SearchMessage::key_response(self.outbound_gateway.myself.clone(), id, host_name, public_key.as_ref().to_vec(), is_resource_kind))
    }

    pub async fn receive(&mut self) -> EmptyOption {
        let mut message = self.from_staging.recv().await?;
        if let None = message.origin() {
            if !self.active_sessions.insert(message.host_name().clone()) {
                debug!(host_name = message.host_name(), search_message = ?message, "SearchMessageProcessor: Blocked search request, reason: active session exists"); return Some(());
            }
            message.set_origin(self.outbound_gateway.myself.clone());
            message.replace_dest(self.outbound_gateway.myself.endpoint_pair.public_endpoint);
        }
        match message {
            SearchMessage { kind: SearchMessageKind::Resource(SearchMessageInnerKind::Request), ..} => self.handle_search_request(message, true),
            SearchMessage { kind: SearchMessageKind::Resource(SearchMessageInnerKind::Response(_)), .. } => self.return_search_responses(message, true),
            SearchMessage { kind: SearchMessageKind::Distribution(SearchMessageInnerKind::Request), ..} => self.handle_search_request(message, false),
            SearchMessage { kind: SearchMessageKind::Distribution(SearchMessageInnerKind::Response(_)), .. } => self.return_search_responses(message, false)
        }
        Some(())
    }
}