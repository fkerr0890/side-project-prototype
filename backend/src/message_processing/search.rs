use tokio::sync::mpsc;
use tracing::{debug, instrument};

use crate::{lock, message::{Message, NumId, Peer, SearchMessage, SearchMessageInnerKind, SearchMessageKind, Sender, StreamMessage, StreamMessageKind}, option_early_return, result_early_return, utils::{ArcSet, TransientCollection, TtlType}};

use super::{BreadcrumbService, EmptyOption, OutboundGateway, ACTIVE_SESSION_TTL_SECONDS};

pub struct SearchRequestProcessor<F: Fn(&SearchMessage) -> bool> {
    outbound_gateway: OutboundGateway,
    breadcrumb_service: BreadcrumbService,
    from_staging: mpsc::UnboundedReceiver<SearchMessage>,
    to_smp: mpsc::UnboundedSender<StreamMessage>,
    active_sessions: TransientCollection<ArcSet<String>>,
    stop_condition: F
}

impl<F: Fn(&SearchMessage) -> bool> SearchRequestProcessor<F> {
    pub fn new(outbound_gateway: OutboundGateway, breadcrumb_service: BreadcrumbService, from_staging: mpsc::UnboundedReceiver<SearchMessage>, to_smp: mpsc::UnboundedSender<StreamMessage>, stop_condition: F) -> Self {
        Self {
            outbound_gateway,
            breadcrumb_service,
            from_staging,
            to_smp,
            active_sessions: TransientCollection::new(TtlType::Secs(ACTIVE_SESSION_TTL_SECONDS), true, ArcSet::new()),
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
            let search_response = option_early_return!(self.construct_search_response(id, origin.unwrap(), host_name, is_resource_kind));
            return self.return_search_responses(search_response);
        }
        self.outbound_gateway.send_request(&mut search_request, sender);
    }

    #[instrument(level = "trace", skip_all, fields(search_response.sender = ?search_response.sender(), search_response.id = %search_response.id()))]
    fn return_search_responses(&mut self, mut search_response: SearchMessage) {
        let dest = option_early_return!(self.breadcrumb_service.get_dest(&search_response.id()));
        if let Some(dest) = dest {
            self.outbound_gateway.send_individual(dest, &mut search_response, false);
        }
        else {
            let sender = if let Some(sender) = search_response.sender() { sender } else { Sender::new(self.outbound_gateway.myself.endpoint_pair.private_endpoint, self.outbound_gateway.myself.id) };
            let (id, host_name, peer_public_key, origin) = search_response.into_id_host_name_public_key_origin();
            let mut key_store = lock!(self.outbound_gateway.key_store);
            let origin = if sender.socket == origin.endpoint_pair.private_endpoint { sender } else { Sender::new(origin.endpoint_pair.public_endpoint, origin.id) };
            let my_public_key = key_store.public_key(origin.id);
            result_early_return!(key_store.agree(origin.id, peer_public_key));
            let mut key_agreement_message = StreamMessage::new(host_name, id, StreamMessageKind::KeyAgreement, my_public_key);
            key_agreement_message.set_sender(origin);
            result_early_return!(self.to_smp.send(key_agreement_message));
        }
    }
    
    fn construct_search_response(&self, id: NumId, origin: Peer, host_name: String, is_resource_kind: bool) -> Option<SearchMessage> {
        if let Some(mut search_response) = self.build_response(id, origin.id, host_name.clone(), is_resource_kind) {
            self.outbound_gateway.send_individual(Sender::new(origin.endpoint_pair.private_endpoint, origin.id), &mut search_response, false);
        }
        self.build_response(id, origin.id, host_name, is_resource_kind)
    }

    fn build_response(&self, id: NumId, peer_id: NumId, host_name: String, is_resource_kind: bool) -> Option<SearchMessage> {
        let public_key = lock!(self.outbound_gateway.key_store).public_key(peer_id);
        Some(SearchMessage::key_response(self.outbound_gateway.myself, id, host_name, public_key, is_resource_kind))
    }

    pub async fn receive(&mut self) -> EmptyOption {
        let mut message = self.from_staging.recv().await?;
        if message.origin().is_none() {
            if !self.active_sessions.insert(message.host_name().clone(), "Search:ActiveSession") {
                debug!(host_name = message.host_name(), search_message = ?message, "SearchMessageProcessor: Blocked search request, reason: active session exists"); return Some(());
            }
            message.set_origin(self.outbound_gateway.myself);
            message.replace_dest(self.outbound_gateway.myself.endpoint_pair.public_endpoint);
        }
        match message {
            SearchMessage { kind: SearchMessageKind::Resource(SearchMessageInnerKind::Request), ..} => self.handle_search_request(message, true),
            SearchMessage { kind: SearchMessageKind::Resource(SearchMessageInnerKind::Response(_)), .. } => self.return_search_responses(message),
            SearchMessage { kind: SearchMessageKind::Distribution(SearchMessageInnerKind::Request), ..} => self.handle_search_request(message, false),
            SearchMessage { kind: SearchMessageKind::Distribution(SearchMessageInnerKind::Response(_)), .. } => self.return_search_responses(message)
        }
        Some(())
    }
}