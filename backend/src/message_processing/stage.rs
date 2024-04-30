use std::{collections::{HashMap, HashSet}, fmt::Debug, net::SocketAddrV4, sync::{Arc, Mutex}};
use ring::aead;
use tokio::{select, sync::mpsc, time::sleep};
use tracing::{debug, field, info, instrument, trace, warn};
use crate::{crypto::{Direction, KeyStore}, http::SerdeHttpResponse, lock, message::{DistributeMetadata, DpMessageKind, InboundMessage, KeyAgreementMessage, Message, MessageDirection, MetadataKind, NumId, Peer, SearchMetadata, SearchMetadataKind, Sender, StreamMetadata, StreamPayloadKind}, message_processing::HEARTBEAT_INTERVAL_SECONDS, node::EndpointPair, option_early_return, peer::PeerOps, result_early_return, utils::{ArcCollection, ArcMap, TimerOptions, TransientCollection}};

use super::{search, stream::{DistributionResponse, StreamSessionManager}, BreadcrumbService, DiscoverPeerProcessor, EarlyReturnContext, EmptyOption, OutboundGateway, DPP_TTL_MILLIS, SRP_TTL_SECONDS};

pub struct MessageStaging {
    from_inbound_gateway: mpsc::UnboundedReceiver<(SocketAddrV4, (usize, [u8; 1024]))>,
    message_staging: TransientCollection<ArcMap<NumId, HashMap<usize, InboundMessage>>>,
    cached_outbound_messages: TransientCollection<ArcMap<NumId, Vec<(Message, Vec<Peer>)>>>,
    unconfirmed_peers: TransientCollection<ArcMap<NumId, EndpointPair>>,
    outbound_gateway: OutboundGateway,
    key_store: KeyStore,
    peer_ops: Arc<Mutex<PeerOps>>,
    breadcrumb_service: BreadcrumbService,
    discover_peer_processor: DiscoverPeerProcessor,
    stream_session_manager: StreamSessionManager,
    outbound_channel_tx: mpsc::UnboundedSender<Message>,
    outbound_channel_rx: mpsc::UnboundedReceiver<Message>,
    to_http_handlers: TransientCollection<ArcMap<NumId, mpsc::UnboundedSender<SerdeHttpResponse>>>,
    from_http_handlers: mpsc::UnboundedReceiver<(Message, mpsc::UnboundedSender<SerdeHttpResponse>)>,
    initial_http_requests: TransientCollection<ArcMap<NumId, Message>>
}

impl MessageStaging {
    pub fn new(
        from_inbound_gateway: mpsc::UnboundedReceiver<(SocketAddrV4, (usize, [u8; 1024]))>,
        outbound_gateway: OutboundGateway,
        discover_peer_processor: DiscoverPeerProcessor,
        from_http_handlers: mpsc::UnboundedReceiver<(Message, mpsc::UnboundedSender<SerdeHttpResponse>)>,
        local_hosts: HashMap<String, SocketAddrV4>,
        intial_peers: Vec<Peer>) -> Self
    {
        let (outbound_channel_tx, outbound_channel_rx) = mpsc::unbounded_channel();
        let mut peer_ops = PeerOps::new();
        for peer in intial_peers {
            peer_ops.add_initial_peer(peer)
        }
        Self {
            from_inbound_gateway,
            message_staging: TransientCollection::new(SRP_TTL_SECONDS, false, ArcMap::new()),
            cached_outbound_messages: TransientCollection::new(SRP_TTL_SECONDS, false, ArcMap::new()),
            unconfirmed_peers: TransientCollection::new(HEARTBEAT_INTERVAL_SECONDS*2, false, ArcMap::new()),
            outbound_gateway,
            key_store: KeyStore::new(),
            peer_ops: Arc::new(Mutex::new(peer_ops)),
            //TODO: Fix ttl
            breadcrumb_service: BreadcrumbService::new(SRP_TTL_SECONDS),
            outbound_channel_tx,
            outbound_channel_rx,
            discover_peer_processor,
            stream_session_manager: StreamSessionManager::new(local_hosts),
            to_http_handlers: TransientCollection::new(SRP_TTL_SECONDS, false, ArcMap::new()),
            from_http_handlers,
            initial_http_requests: TransientCollection::new(SRP_TTL_SECONDS, false, ArcMap::new())
        }
    }

    pub async fn receive(&mut self) -> EmptyOption {
        let (sender_addr, message_bytes) = select! {
            message = self.from_inbound_gateway.recv() => message?,
            message = self.outbound_channel_rx.recv() => return Some(self.process_outbound_message(message?).await),
            message = self.from_http_handlers.recv() => return Some(self.handle_http_request(message?).await)
        };
        if let Ok(message) = bincode::deserialize::<KeyAgreementMessage>(&message_bytes.1) {
            return Some(self.handle_key_agreement(message, sender_addr).await);
        }
        if let Some(message) = self.stage_message(message_bytes, sender_addr) {
            self.send_outbound_message(message).await;
        };
        Some(())
    }

    #[instrument(level = "trace", skip_all, fields(%sender_addr))]
    fn stage_message(&mut self, message_bytes: (usize, [u8; 1024]), sender_addr: SocketAddrV4) -> Option<Message> {
        // let (is_encrypted, sender) = (message_bytes.take_is_encrypted(), message_bytes.separate_parts().sender().socket);
        let (length, message_bytes) = message_bytes;
        let (message_bytes, _tail) = message_bytes.split_at(length);
        let (message_bytes, suffix) = message_bytes.split_at(message_bytes.len() - aead::NONCE_LEN - 16);
        let (peer_id_bytes, nonce) = suffix.split_at(suffix.len() - aead::NONCE_LEN);
        let peer_id = NumId(u128::from_be_bytes(peer_id_bytes.try_into().unwrap()));
        let mut message_bytes = message_bytes.to_vec();
        message_bytes = self.key_store.transform(peer_id, &mut message_bytes, Direction::Decode(nonce)).unwrap();
        let inbound_message: InboundMessage = result_early_return!(bincode::deserialize(&message_bytes), None);
        if inbound_message.separate_parts().sender().socket != sender_addr {
            warn!(sender = %inbound_message.separate_parts().sender().socket, actual_sender = %sender_addr, "Sender doesn't match actual sender");
            return None;
        }
        if let Some(endpoint_pair) = self.unconfirmed_peers.pop(&inbound_message.separate_parts().sender().id) {
            // println!("Confirmed peer {}", inbound_message.separate_parts().sender().id());
            self.add_new_peer(Peer::new(endpoint_pair, inbound_message.separate_parts().sender().id));
        }
        let (index, num_chunks) = inbound_message.separate_parts().position();
        if num_chunks == 1 {
            return Some(self.reassemble_message(vec![inbound_message]))
        }
        let id = inbound_message.separate_parts().id();
        let staged_messages_len = {
            self.message_staging.set_timer(id, TimerOptions::new(), "Stage:MessageStaging");
            let mut message_staging = lock!(self.message_staging.collection().map());
            let staged_messages= message_staging.entry(id).or_insert(HashMap::with_capacity(num_chunks));
            staged_messages.insert(index, inbound_message);
            staged_messages.len()
        };
        if staged_messages_len == num_chunks {
            // gateway::log_debug("Collected all messages");
            let messages = option_early_return!(self.message_staging.pop(&id), None);
            let messages: Vec<InboundMessage> = messages.into_values().collect();
            return Some(self.reassemble_message(messages))
        }
        None
    }

    fn reassemble_message(&mut self, message_parts: Vec<InboundMessage>) -> Message {
        let (message_bytes, senders, timestamp) = InboundMessage::reassemble_message(message_parts);
        let mut message = bincode::deserialize::<Message>(&message_bytes).unwrap();
        for sender in senders {
            message.set_sender(sender);
        }
        message.set_timestamp(timestamp);
        message
    }

    #[instrument(level = "trace", skip_all, fields(dest = %message.dest().id, id = %message.id(), metadata = ?message.metadata(), myself = %self.outbound_gateway.myself.id))]
    async fn process_outbound_message(&mut self, mut message: Message) {
        let id = message.id();
        if let MetadataKind::Stream(StreamMetadata { payload: StreamPayloadKind::Request(_), host_name}) = message.metadata() {
            return { self.send_checked(self.stream_session_manager.get_destinations_source_retrieval(host_name), message, true).await; }
        }
        if let MetadataKind::Stream(StreamMetadata { payload: StreamPayloadKind::DistributionRequest(_), host_name }) = message.metadata() {
            return { self.send_checked(self.stream_session_manager.get_destinations_source_distribution(host_name), message, true).await; }
        }
        if let MetadataKind::Heartbeat = message.metadata() {
            return self.process_outbound_heartbeat(message).await;
        }
        if let MetadataKind::Discover(metadata) = message.metadata_mut() {
            if let DpMessageKind::INeedSome = metadata.kind {
                let introducer = metadata.peer_list.pop().unwrap();
                self.breadcrumb_service.try_add_breadcrumb(id, None, None, Some(DPP_TTL_MILLIS));
                metadata.kind = DpMessageKind::Request;
                return { self.send_checked(vec![introducer], message, false).await; }
            }
            if let DpMessageKind::IveGotSome = metadata.kind {
                return { self.send_checked(vec![message.dest()], message, false).await; }
            }
        }
        self.send_response(message).await;
    }

    #[instrument(level = "trace", skip_all, fields(peers = field::Empty))]
    async fn process_outbound_heartbeat(&mut self, message: Message) {
        let mut peers: HashSet<Peer> = HashSet::from_iter(lock!(self.peer_ops).peers().into_iter());
        peers.extend(self.stream_session_manager.get_all_destinations_source_retrieval().into_iter());
        peers.extend(self.stream_session_manager.get_all_destinations_source_distribution().into_iter());
        peers.extend(self.stream_session_manager.get_all_destinations_sink().into_iter());
        peers.remove(&self.outbound_gateway.myself);
        tracing::Span::current().record("peers", format!("{:?}", peers));
        self.send_checked(peers, message, true).await;
    }

    #[instrument(level = "trace", skip_all, fields(id = %message.id(), metadata = ?message.metadata(), direction = ?message.direction(), myself = %self.outbound_gateway.myself.id, new_direction = field::Empty))]
    async fn send_outbound_message(&mut self, mut message: Message) {
        let new_direction = self.get_direction(&mut message);
        tracing::Span::current().record("new_direction", format!("{:?}", new_direction));
        match new_direction {
            PropagationDirection::Forward => { self.send_request(message).await; },
            PropagationDirection::Reverse => self.send_response(message).await,
            PropagationDirection::Final => self.execute_final_action(message).await,
            PropagationDirection::Stop => { trace!("Stopped") }
        };
    }

    async fn send_request(&mut self, mut message: Message) -> Option<Message> {
        let prev_sender = message.only_sender();
        message.set_direction(MessageDirection::Request);
        let dests = lock!(self.peer_ops).peers().into_iter().filter(|p| prev_sender.is_none() || prev_sender.unwrap().id != p.id);
        message = self.send_checked(dests, message, false).await?;
        Some(message)
    }

    async fn send_response(&mut self, mut message: Message) {
        let dest = option_early_return!(self.breadcrumb_service.get_dest(&message.id()));
        message.set_direction(MessageDirection::Response);
        if let Some(dest) = dest {
            self.send_checked(vec![Peer::from(dest)], message, false).await;
        }
        else {
            self.execute_final_action(message).await;
        }
    }

    #[instrument(level = "trace", skip_all, fields(myself = %self.outbound_gateway.myself.id, ?dests))]
    async fn send_checked(&mut self, dests: impl IntoIterator<Item = Peer> + Debug, mut message: Message, to_be_chunked: bool) -> Option<Message> {
        let mut remaining_dests = dests.into_iter();
        while let Some(dest) = remaining_dests.next() {
            message.replace_dest(dest);
            message.clear_senders();
            if !self.check_key_agreement(&message).await {
                let mut remaining_dests: Vec<Peer> = remaining_dests.collect();
                remaining_dests.push(dest);
                self.insert_cached_outbound_message(message, remaining_dests);
                return None;
            }
            self.outbound_gateway.send(&message, to_be_chunked, &mut self.key_store).await;
        }
        Some(message)
    }

    async fn check_key_agreement(&mut self, message: &Message) -> bool {
        if self.key_store.agreement_exists(&message.dest().id) {
            return true;
        }
        self.send_agreement(message.dest(), MessageDirection::Request).await;
        false
    }

    #[instrument(level = "trace", skip_all, fields(id = %message.id(), ?remaining_dests))]
    fn insert_cached_outbound_message(&mut self, message: Message, remaining_dests: Vec<Peer>) {
        self.cached_outbound_messages.set_timer(message.dest().id, TimerOptions::new(), "Stage:MessageCaching");
        let mut message_caching = lock!(self.cached_outbound_messages.collection().map());
        let cached_messages = message_caching.entry(message.dest().id).or_default();
        cached_messages.push((message, remaining_dests));
    }

    #[instrument(level = "trace", skip(self))]
    async fn send_agreement(&mut self, dest: Peer, direction: MessageDirection) {
        let public_key = option_early_return!(self.key_store.public_key(dest.id));
        self.outbound_gateway.send_agreement(dest, public_key, direction).await;
    }

    #[instrument(level = "trace", skip_all, fields(id = %from_http_handler.0.id(), host_name = from_http_handler.0.metadata().host_name(), myself = %self.outbound_gateway.myself.id))]
    async fn handle_http_request(&mut self, from_http_handler: (Message, mpsc::UnboundedSender<SerdeHttpResponse>)) {
        let (message, tx) = from_http_handler;
        let (id, host_name) = (message.id(), message.metadata().host_name().clone());
        self.to_http_handlers.insert(id, tx, "Staging:ToHttpHandlers");
        if self.stream_session_manager.source_active_retrieval(&host_name) {
            self.send_http_request(message, &host_name).await;
        } else {
            self.stream_session_manager.new_source_retrieval(host_name.clone(), self.outbound_channel_tx.clone());
            let search_message = Message::new_search_request(id, SearchMetadata::new(self.outbound_gateway.myself, host_name.clone(), SearchMetadataKind::Retrieval));
            self.initial_http_requests.insert(id, message, "Staging:InitialHttpRequests");
            self.send_outbound_message(search_message).await;
        }
    }

    async fn send_http_request(&mut self, message: Message, host_name: &str) {
        let message = option_early_return!(self.send_checked(self.stream_session_manager.get_destinations_source_retrieval(host_name), message, true).await);
        self.stream_session_manager.push_resource(message);
    }

    #[instrument(level = "trace", skip(self), fields(myself = %self.outbound_gateway.myself.id))]
    async fn handle_distribution_request(&mut self, id: NumId, metadata: DistributeMetadata) {
        if !self.stream_session_manager.host_installed(&metadata.host_name) {
            return;
        }
        if self.stream_session_manager.source_active_distribution(&metadata.host_name) {
            self.send_distribution_request(None, metadata.host_name).await;
        }
        else {
            self.stream_session_manager.new_source_distribution(metadata.host_name.clone(), self.outbound_channel_tx.clone(), id, metadata.hop_count).await;
            let search_message = Message::new_search_request(id, SearchMetadata::new(self.outbound_gateway.myself, metadata.host_name, SearchMetadataKind::Distribution));
            self.breadcrumb_service.try_add_breadcrumb(id, None, None, None);
            self.send_request(search_message).await;
        }
    }

    async fn send_distribution_request(&mut self, received_id: Option<NumId>, host_name: String) {
        let file = self.stream_session_manager.file_mut(&host_name);
        let (new_id, chunk) = result_early_return!(file.next_chunk_and_id(received_id).await);
        let dests = self.stream_session_manager.get_destinations_source_distribution(&host_name);
        let message = Message::new(Peer::default(), new_id, None, MetadataKind::Stream(StreamMetadata::new(StreamPayloadKind::DistributionRequest(chunk), host_name)), MessageDirection::Request);
        let message = option_early_return!(self.send_checked(dests, message, true).await);
        self.stream_session_manager.push_resource(message)
    }

    fn get_direction(&mut self, message: &mut Message) -> PropagationDirection {
        if let MetadataKind::Discover(metadata) = message.metadata() {
            if let DpMessageKind::IveGotSome = metadata.kind {
                for peer in metadata.peer_list.iter() {
                    self.add_new_peer(*peer);
                }
                return PropagationDirection::Stop;
            }
        }
        if let MessageDirection::Response = message.direction() {
            return PropagationDirection::Reverse;
        }
        let mut sender = message.only_sender();
        let (origin, early_return_context, ttl, reverse) = match message.metadata_mut() {
            MetadataKind::Search(metadata) => {
                if search::continue_propagating(self.outbound_gateway.myself, metadata, self.stream_session_manager.local_hosts()) { (metadata.origin, None, None, false) } else { (metadata.origin, None, None, true) }
            },
            MetadataKind::Discover(metadata) => {
                if sender.is_some_and(|s| s.id == metadata.origin.id) { sender = None; }
                metadata.peer_list.push(self.outbound_gateway.myself);                
                if self.discover_peer_processor.continue_propagating(metadata) { (metadata.origin, Some(EarlyReturnContext(self.outbound_channel_tx.clone(), message.clone())), Some(DPP_TTL_MILLIS), false) } else { (metadata.origin, None, Some(DPP_TTL_MILLIS), true) }
            },
            _ => return PropagationDirection::Final
        };

        if !self.breadcrumb_service.try_add_breadcrumb(message.id(), early_return_context, sender, ttl) {
            return PropagationDirection::Stop;
        }

        self.send_nat_heartbeats(origin);
        if reverse {
            return PropagationDirection::Reverse;
        }
        PropagationDirection::Forward
    }

    #[instrument(level = "trace", skip_all, fields(id = %message.id()))]
    async fn execute_final_action(&mut self, message: Message) {
        let (sender, id) = (message.only_sender(), message.id());
        let sender = if let Some(sender) = sender { sender } else { Sender::new(self.outbound_gateway.myself.endpoint_pair.private_endpoint, self.outbound_gateway.myself.id) };
        match message.into_metadata() {
            MetadataKind::Search(metadata) => {
                let SearchMetadata { origin, host_name, kind } = metadata;
                let sender = if sender.socket == origin.endpoint_pair.private_endpoint && sender.id == origin.id { Peer::from(sender) } else { origin };
                match kind {
                    SearchMetadataKind::Retrieval => {
                        if !self.stream_session_manager.add_destination_source_retrieval(&host_name, sender) { return; }
                        let initial_http_request = self.initial_http_requests.pop(&id).unwrap();
                        self.send_http_request(initial_http_request, &host_name).await;
                    },
                    SearchMetadataKind::Distribution => {
                        if !self.stream_session_manager.add_destination_source_distribution(&host_name, Peer::from(sender)) { return; }
                        self.send_distribution_request(None, host_name).await;
                    }
                };
            },
            MetadataKind::Discover(metadata) => {
                let peer_len_curr_max = self.discover_peer_processor.peer_len_curr_max(&id);
                if peer_len_curr_max == 0 {
                    self.discover_peer_processor.set_staging_early_return(self.outbound_channel_tx.clone(), id);
                }
                self.discover_peer_processor.stage_message(id, metadata, peer_len_curr_max);
            },
            MetadataKind::Stream(metadata) => {
                let response = match metadata.payload {
                    StreamPayloadKind::Request(payload) => {
                        if !self.stream_session_manager.sink_active_retrieval(&metadata.host_name) {
                            self.stream_session_manager.new_sink_retrieval(metadata.host_name.clone())
                        }
                        self.stream_session_manager.add_destination_sink(&metadata.host_name, Peer::from(sender));
                        self.stream_session_manager.retrieval_response_action(payload, metadata.host_name, id).await
                    },
                    StreamPayloadKind::Response(payload) => {
                        self.stream_session_manager.finalize_resource_retrieval(&metadata.host_name, &id);
                        return option_early_return!(self.to_http_handlers.pop(&id)).send(payload).unwrap();
                    },
                    StreamPayloadKind::DistributionRequest(bytes) => {
                        if !self.stream_session_manager.sink_active_distribution(&metadata.host_name) {
                            self.stream_session_manager.new_sink_distribution(metadata.host_name.clone());
                        }
                        self.stream_session_manager.distribution_response_action(bytes, metadata.host_name, id).await
                    },
                    StreamPayloadKind::DistributionResponse(response) => {
                        return match response {
                            DistributionResponse::Continue => self.send_distribution_request(Some(id), metadata.host_name).await,
                            DistributionResponse::InstallError => panic!(),
                            DistributionResponse::InstallOk => {
                                let (mut hop_count, distribution_id) = self.stream_session_manager.curr_hop_count_and_distribution_id(&metadata.host_name);
                                hop_count -= 1;
                                if hop_count <= 0 {
                                    return;
                                }
                                let distribution_message = Message::new(
                                    Peer::default(),
                                    distribution_id,
                                    None,
                                    MetadataKind::Distribute(DistributeMetadata::new(hop_count, metadata.host_name)),
                                    MessageDirection::Request);
                                self.send_request(distribution_message).await;
                            }
                        }
                    }
                };
                self.send_checked(vec![Peer::from(sender)], response, true).await;
            },
            MetadataKind::Distribute(metadata) => self.handle_distribution_request(id, metadata).await,
            _ => {}
        }
    }

    #[instrument(level = "trace", skip(self), fields(myself = %self.outbound_gateway.myself.id))]
    fn send_nat_heartbeats(&mut self, peer: Peer) {
        if peer.id == self.outbound_gateway.myself.id
            || self.unconfirmed_peers.contains_key(&peer.id)
            || lock!(self.peer_ops).has_peer(peer.id) {
            return;
        }
        // println!("Start sending nat heartbeats to peer {:?} at {:?}", peer, self.outbound_gateway.myself);
        self.unconfirmed_peers.insert(peer.id, peer.endpoint_pair, "Stage:UnconfirmedPeers");
        let unconfirmed_peers = self.unconfirmed_peers.collection().clone();
        let outbound_channel = self.outbound_channel_tx.clone();
        tokio::spawn(async move {
            while unconfirmed_peers.contains_key(&peer.id) {
                result_early_return!(outbound_channel.send(Message::new_heartbeat()));
                sleep(HEARTBEAT_INTERVAL_SECONDS).await;
            }
        });
    }

    #[instrument(level = "trace", skip(self), fields(myself = %self.outbound_gateway.myself.id))]
    fn add_new_peer(&mut self, peer: Peer) {
        let peer_endpoint = peer.endpoint_pair.public_endpoint;
        let mut peers = lock!(self.peer_ops);
        peers.add_peer(peer, DiscoverPeerProcessor::get_score(self.outbound_gateway.myself.endpoint_pair.public_endpoint, peer_endpoint));
        info!(peers = ?peers.peers().into_iter().map(|p| p.id).collect::<Vec<NumId>>());
    }

    #[instrument(level = "trace", skip_all, fields(message.peer_id))]
    async fn handle_key_agreement(&mut self, message: KeyAgreementMessage, sender: SocketAddrV4) {
        let KeyAgreementMessage { public_key, peer_id, direction } = message;
        let cached_messages = match direction {
            MessageDirection::Request => { self.send_agreement(Peer::from(Sender::new(sender, peer_id)), MessageDirection::Response).await; None },
            MessageDirection::Response => self.cached_outbound_messages.pop(&peer_id)
        };
        result_early_return!(self.key_store.agree(peer_id, public_key));
        if let Some(cached_messages) = cached_messages {
            debug!("Sending cached outbound messages");
            for (message, remaining_dests) in cached_messages {
                let (to_be_chunked, is_stream_request) = message.to_be_chunked();
                let message = option_early_return!(self.send_checked(remaining_dests, message, to_be_chunked).await);
                if is_stream_request {
                    self.stream_session_manager.push_resource(message);
                }
            }
        }
    }

    pub fn outbound_channel_tx(&self) -> &mpsc::UnboundedSender<Message> { &self.outbound_channel_tx }
    pub fn peer_ops(&self) -> &Arc<Mutex<PeerOps>> { &self.peer_ops }
}

#[derive(Debug)]
pub enum PropagationDirection {
    Forward,
    Reverse,
    Stop,
    Final
}