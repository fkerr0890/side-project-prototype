use std::{collections::HashMap, net::SocketAddrV4};

use tokio::sync::mpsc;
use tracing::{debug, instrument};

use crate::{lock, message::{Message, MessageDirection, Messagea, MetadataKind, NumId, Peer, SearchMessage, SearchMessageInnerKind, SearchMessageKind, SearchMetadata, SearchMetadataKind, Sender, StreamMessage, StreamMessageKind, StreamMetadata, StreamPayloadKind}, node::EndpointPair, option_early_return, result_early_return, utils::{ArcSet, TransientCollection}};

use super::{BreadcrumbService, EmptyOption, OutboundGateway, ACTIVE_SESSION_TTL_SECONDS};

pub struct SearchRequestProcessor {
    outbound_gateway: OutboundGateway,
    breadcrumb_service: BreadcrumbService,
    from_staging: mpsc::UnboundedReceiver<SearchMessage>,
    to_smp: mpsc::UnboundedSender<StreamMessage>,
    active_sessions: TransientCollection<ArcSet<String>>,
    local_hosts: HashMap<String, SocketAddrV4>
}

impl SearchRequestProcessor {
    pub fn new(outbound_gateway: OutboundGateway, breadcrumb_service: BreadcrumbService, from_staging: mpsc::UnboundedReceiver<SearchMessage>, to_smp: mpsc::UnboundedSender<StreamMessage>, local_hosts: HashMap<String, SocketAddrV4>) -> Self {
        Self {
            outbound_gateway,
            breadcrumb_service,
            from_staging,
            to_smp,
            active_sessions: TransientCollection::new(ACTIVE_SESSION_TTL_SECONDS, true, ArcSet::new()),
            local_hosts
        }
    }

    #[instrument(level = "trace", skip_all, fields(dest, host_name, kind = ?metadata.kind))]
    pub fn continue_propagating(&self, dest: Peer, host_name: &str, metadata: &SearchMetadata) -> bool {
        let should_stop = match metadata.kind {
            SearchMetadataKind::Retrieval => self.check_stop_retrieval(host_name),
            SearchMetadataKind::Distribution => self.check_stop_distribution(dest, metadata)
        };
        if !should_stop {
            return true;
        }
        debug!("Stopped propagating search request");
        false
    }

    fn check_stop_retrieval(&self, host_name: &str) -> bool {
        self.local_hosts.contains_key(host_name)
    }

    fn check_stop_distribution(&self, dest: Peer, metadata: &SearchMetadata) -> bool {
        let origin = metadata.origin.unwrap();
        origin.id != dest.id
    }

    #[instrument(level = "trace", skip_all, fields(search_request.sender = ?search_request.sender(), search_request.id = %search_request.id()))]
    pub fn handle_search_request(&mut self, mut search_request: SearchMessage, is_resource_kind: bool) {
        let sender = search_request.sender_option();
        // if !self.breadcrumb_service.try_add_breadcrumb(None, search_request.id(), sender) {
        //     return;
        // }
        // if (self.stop_condition)(&search_request) {
        //     debug!(host_name = search_request.host_name(), curr_node = ?self.outbound_gateway.myself, message_id = %search_request.id(), "Stopped propagating search request");
        //     let (id, host_name, origin) = search_request.into_id_host_name_origin();
        //     let search_response = self.construct_search_response(id, origin.unwrap(), host_name, is_resource_kind);
        //     return self.return_search_responses(search_response);
        // }
        self.outbound_gateway.send_request(&mut search_request, sender);
    }

    #[instrument(level = "trace", skip_all, fields(search_response.sender = ?search_response.sender(), search_response.id = %search_response.id()))]
    fn return_search_responses(&mut self, mut search_response: SearchMessage) {
        let dest = option_early_return!(self.breadcrumb_service.get_dest(&search_response.id()));
        if let Some(dest) = dest {
            self.outbound_gateway.send_individual(dest, &mut search_response, false);
        }
        else {
            // let sender = if let Some(sender) = search_response.sender() { sender } else { Sender::new(self.outbound_gateway.myself.endpoint_pair.private_endpoint, self.outbound_gateway.myself.id) };
            // let (id, host_name, peer_public_key, origin) = search_response.into_id_host_name_public_key_origin();
            // let mut key_store = lock!(self.outbound_gateway.key_store);
            // let origin = if sender.socket == origin.endpoint_pair.private_endpoint { sender } else { Sender::new(origin.endpoint_pair.public_endpoint, origin.id) };
            // let my_public_key = key_store.public_key(origin.id);
            // result_early_return!(key_store.agree(origin.id, peer_public_key));
            // let mut key_agreement_message = StreamMessage::new(host_name, id, StreamMessageKind::KeyAgreement, my_public_key);
            // key_agreement_message.set_sender(origin);
            // result_early_return!(self.to_smp.send(key_agreement_message));
        }
    }

    #[instrument(level = "trace", skip_all, fields(id))]
    pub fn execute_final_action(&self, id: NumId, sender: Peer, origin: Peer, host_name: String) -> Messagea {
        let sender = if sender.endpoint_pair.private_endpoint == EndpointPair::default_socket() { origin } else { sender };
        Messagea::new(sender, id, None, MetadataKind::Stream(StreamMetadata::new(id, StreamPayloadKind::Empty, host_name)), MessageDirection::Request)
    }
    
    fn construct_search_response(&self, id: NumId, origin: Peer, host_name: String, is_resource_kind: bool) -> SearchMessage {
        self.build_response(id, origin.id, host_name, is_resource_kind)
    }

    fn build_response(&self, id: NumId, peer_id: NumId, host_name: String, is_resource_kind: bool) -> SearchMessage {
        // let public_key = lock!(self.outbound_gateway.key_store).public_key(peer_id);
        SearchMessage::key_response(self.outbound_gateway.myself, id, host_name, Vec::new(), is_resource_kind)
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