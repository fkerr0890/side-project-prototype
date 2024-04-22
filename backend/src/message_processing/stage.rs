use std::{collections::{HashMap, HashSet}, net::SocketAddrV4};
use ring::{aead, hmac::Key};
use tokio::{select, sync::mpsc, time::sleep};
use tracing::{debug, error, instrument, trace, warn};
use crate::{crypto::{Direction, Error, KeyStore}, http::SerdeHttpResponse, lock, message::{self, DiscoverMetadata, DiscoverPeerMessage, DistributeMetadata, DistributionMessage, DpMessageKind, Heartbeat, InboundMessage, KeyAgreementMessage, Message, MessageDirection, Messagea, MetadataKind, NumId, Peer, SearchMessage, SearchMessageKind, SearchMetadata, Sender, StreamMessage, StreamMessageKind, StreamMetadata, StreamPayloadKind}, message_processing::HEARTBEAT_INTERVAL_SECONDS, node::EndpointPair, option_early_return, result_early_return, utils::{ArcCollection, ArcMap, BidirectionalMpsc, TransientCollection}};

use super::{search::SearchRequestProcessor, stream::{SessionManagerDistribution, SessionManagerRetrieval}, stream2::StreamMessageProcessor, BreadcrumbService, DiscoverPeerProcessor, EarlyReturnContext, EmptyOption, OutboundGateway, SRP_TTL_SECONDS};

pub struct MessageStaging {
    from_gateway: mpsc::UnboundedReceiver<(SocketAddrV4, Vec<u8>)>,
    to_srp: mpsc::UnboundedSender<SearchMessage>,
    to_dpp: mpsc::UnboundedSender<DiscoverPeerMessage>,
    to_smp: mpsc::UnboundedSender<StreamMessage>,
    to_dsrp: mpsc::UnboundedSender<SearchMessage>,
    to_dsmp: mpsc::UnboundedSender<StreamMessage>,
    to_dh: mpsc::UnboundedSender<DistributionMessage>,
    message_staging: TransientCollection<ArcMap<NumId, HashMap<usize, InboundMessage>>>,
    cached_outbound_messages: TransientCollection<ArcMap<NumId, Vec<Messagea>>>,
    unconfirmed_peers: TransientCollection<ArcMap<NumId, EndpointPair>>,
    outbound_gateway: OutboundGateway,
    handlers: TransientCollection<ArcMap<NumId, mpsc::UnboundedSender<Messagea>>>,
    key_store: KeyStore,
    breadcrumb_service: BreadcrumbService,
    search_request_processor: SearchRequestProcessor,
    discover_peer_processor: DiscoverPeerProcessor,
    stream_message_processor: StreamMessageProcessor,
    outbound_channel_tx: mpsc::UnboundedSender<Messagea>,
    outbound_channel_rx: mpsc::UnboundedReceiver<Messagea>,
    session_manager: SessionManagerRetrieval,
    session_manager_distribution: SessionManagerDistribution,
    to_http_handlers: TransientCollection<ArcMap<NumId, mpsc::UnboundedSender<HttpSignal>>>,
    from_http_handlers: mpsc::UnboundedReceiver<(Messagea, mpsc::UnboundedSender<HttpSignal>)>
}

impl MessageStaging {
    pub fn new(
        from_gateway: mpsc::UnboundedReceiver<(SocketAddrV4, Vec<u8>)>,
        to_srp: mpsc::UnboundedSender<SearchMessage>,
        to_dpp: mpsc::UnboundedSender<DiscoverPeerMessage>,
        to_smp: mpsc::UnboundedSender<StreamMessage>,
        to_dsrp: mpsc::UnboundedSender<SearchMessage>,
        to_dsmp: mpsc::UnboundedSender<StreamMessage>,
        to_dh: mpsc::UnboundedSender<DistributionMessage>,
        outbound_gateway: OutboundGateway,
        breadcrumbs: TransientCollection<ArcMap<NumId, Option<Sender>>>,
        search_request_processor: SearchRequestProcessor,
        discover_peer_processor: DiscoverPeerProcessor,
        stream_message_processor: StreamMessageProcessor,
        from_http_handlers: mpsc::UnboundedReceiver<(Messagea, mpsc::UnboundedSender<HttpSignal>)>,
        local_hosts: HashMap<String, SocketAddrV4>) -> Self
    {
        let (early_return_trigger_tx, early_return_trigger_rx) = mpsc::unbounded_channel();
        Self {
            from_gateway,
            to_srp,
            to_dpp,
            to_smp,
            to_dsrp,
            to_dsmp,
            to_dh,
            message_staging: TransientCollection::new(SRP_TTL_SECONDS, false, ArcMap::new()),
            cached_outbound_messages: TransientCollection::new(SRP_TTL_SECONDS, false, ArcMap::new()),
            unconfirmed_peers: TransientCollection::new(HEARTBEAT_INTERVAL_SECONDS*2, false, ArcMap::new()),
            outbound_gateway,
            handlers: TransientCollection::new(SRP_TTL_SECONDS, false, ArcMap::new()),
            key_store: KeyStore::new(),
            breadcrumb_service: BreadcrumbService::new(SRP_TTL_SECONDS),
            search_request_processor,
            outbound_channel_tx: early_return_trigger_tx,
            outbound_channel_rx: early_return_trigger_rx,
            discover_peer_processor,
            stream_message_processor,
            session_manager: SessionManagerRetrieval::new(local_hosts.clone()),
            session_manager_distribution: SessionManagerDistribution::new(local_hosts),
            to_http_handlers: TransientCollection::new(SRP_TTL_SECONDS, false, ArcMap::new()),
            from_http_handlers
        }
    }

    pub async fn receive(&mut self) -> EmptyOption {
        let (sender_addr, message_bytes) = select! {
            message = self.from_gateway.recv() => message?,
            message = self.outbound_channel_rx.recv() => return Some(self.process_outbound_message(message?).await),
            message = self.from_http_handlers.recv() => return Some(self.send_http_request(message?).await)
        };
        if let Ok(message) = bincode::deserialize::<KeyAgreementMessage>(&message_bytes) {
            return Some(self.handle_key_agreement(message, sender_addr).await);
        }
        if let Some(message) = self.stage_message(message_bytes, sender_addr) {
            self.send_outbound_message(message).await;
        };
        Some(())
    }

    // #[instrument(level = "trace", skip_all, fields(inbound_message.sender = ?inbound_message.separate_parts().sender(), inbound_message.id = %inbound_message.separate_parts().id()))]
    fn stage_message(&mut self, mut message_bytes: Vec<u8>, sender_addr: SocketAddrV4) -> Option<Messagea> {
        // let (is_encrypted, sender) = (message_bytes.take_is_encrypted(), message_bytes.separate_parts().sender().socket);
        let mut suffix = message_bytes.split_off(message_bytes.len() - aead::NONCE_LEN - 16);
        let nonce = suffix.split_off(suffix.len() - aead::NONCE_LEN);
        let peer_id = NumId(u128::from_be_bytes(suffix.try_into().unwrap()));
        self.key_store.transform(peer_id, &mut message_bytes, Direction::Decode(nonce)).unwrap();
        let inbound_message: InboundMessage = result_early_return!(bincode::deserialize(&message_bytes), None);
        if inbound_message.separate_parts().sender().socket != sender_addr {
            warn!(sender = %inbound_message.separate_parts().sender().socket, actual_sender = %sender_addr, "Sender doesn't match actual sender");
            return None;
        }
        if let Some(endpoint_pair) = self.unconfirmed_peers.pop(&inbound_message.separate_parts().sender().id) {
            // println!("Confirmed peer {}", inbound_message.separate_parts().sender().id());
            self.outbound_gateway.add_new_peer(Peer::new(endpoint_pair, inbound_message.separate_parts().sender().id));
        }
        let (index, num_chunks) = inbound_message.separate_parts().position();
        if num_chunks == 1 {
            return Some(self.reassemble_message(vec![inbound_message]))
        }
        let id = inbound_message.separate_parts().id();
        let staged_messages_len = {
            self.message_staging.set_timer(id, "Stage:MessageStaging");
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

    fn reassemble_message(&mut self, message_parts: Vec<InboundMessage>) -> Messagea {
        let (message_bytes, senders, timestamp) = InboundMessage::reassemble_message(message_parts);
        let mut message = bincode::deserialize::<Messagea>(&message_bytes).unwrap();
        for sender in senders {
            message.set_sender(sender);
        }
        message.set_timestamp(timestamp);
        message
    }

    async fn process_outbound_message(&mut self, mut message: Messagea) {
        if let MetadataKind::Stream(StreamMetadata { payload: StreamPayloadKind::Request(_), host_name}) = message.metadata() {
            for dest in self.session_manager.get_destinations(host_name) {
                message = option_early_return!(self.send_checked(message, Some(Peer::from(dest)), true).await);
            }
        }
        else if let MetadataKind::Stream(StreamMetadata { payload: StreamPayloadKind::DistributionRequest(_), ..}) = message.metadata() {
            self.send_request2(message).await;
        }
        else {
            self.send_response(message).await;
        }
    }

    async fn send_outbound_message(&mut self, mut message: Messagea) {
        let new_direction = self.get_direction(&mut message);
        match new_direction {
            PropagationDirection::Forward => self.send_request2(message).await,
            PropagationDirection::Reverse => self.send_response(message).await,
            PropagationDirection::Final => self.execute_final_action(message).await,
            PropagationDirection::Stop => {}
        };
    }

    async fn send_request2(&mut self, mut message: Messagea) {
        let prev_sender = message.only_sender();
        message.set_direction(MessageDirection::Request);
        for peer in self.outbound_gateway.peer_ops.peers() {
            match prev_sender { Some(s) if s.id == peer.id => continue, _ => {}}
            message = option_early_return!(self.send_checked(message, Some(peer), false).await);
        }
    }

    async fn send_response(&mut self, mut message: Messagea) {
        let dest = option_early_return!(self.breadcrumb_service.get_dest(&message.id()));
        message.set_direction(MessageDirection::Response);
        if let Some(dest) = dest {
            self.send_checked(message, Some(Peer::from(dest)), false).await;
        }
        else {
            self.execute_final_action(message).await;
        }
    }

    async fn send_checked(&mut self, mut message: Messagea, dest: Option<Peer>, to_be_chunked: bool) -> Option<Messagea> {
        if let Some(dest) = dest {
            message.replace_dest(dest);
        }
        let mut message = self.check_key_agreement(message).await?;
        message.clear_senders();
        self.outbound_gateway.send(&message, to_be_chunked, &mut self.key_store).await;
        Some(message)
    }

    async fn check_key_agreement(&mut self, message: Messagea) -> Option<Messagea> {
        if self.key_store.agreement_exists(&message.dest().id) {
            return Some(message);
        }
        self.cached_outbound_messages.set_timer(message.dest().id, "Stage:MessageCaching");
        let dest = message.dest();
        {
            let mut message_caching = lock!(self.cached_outbound_messages.collection().map());
            let cached_messages = message_caching.entry(message.dest().id).or_default();
            cached_messages.push(message);
        }
        self.send_agreement(dest).await;
        None
    }

    async fn send_agreement(&mut self, dest: Peer) {
        let public_key = self.key_store.public_key(dest.id);
        self.outbound_gateway.send_agreement(dest, public_key, MessageDirection::Request).await;
    }

    async fn send_http_request(&mut self, from_http_handler: (Messagea, mpsc::UnboundedSender<HttpSignal>)) {
        let (mut message, tx) = from_http_handler;
        let (id, host_name) = (message.id(), message.metadata().host_name());
        if !self.session_manager.session_manager().session_active(host_name) {
            let search_message = Messagea::new_search_request(id, SearchMetadata::new(self.outbound_gateway.myself, host_name.clone()));
            self.send_outbound_message(search_message).await;
            return tx.send(HttpSignal::Wait(message)).unwrap();
        }
        self.to_http_handlers.set_timer(id, "Staging:ToHttpHandlers");
        lock!(self.to_http_handlers.collection().map()).insert(id, tx);
        for dest in self.session_manager.get_destinations(host_name) {
            message = option_early_return!(self.send_checked(message, Some(Peer::from(dest)), true).await);
        }
        self.session_manager.session_manager_mut().push_resource(message);
    }

    async fn send_distribution_request(&mut self, metadata: DistributeMetadata) {
        if !self.session_manager_distribution.session_manager().session_active(&metadata.host_name) {
            let mut dests = self.outbound_gateway.peer_ops.peers();
            self.session_manager_distribution.new_active_session(metadata.host_name, self.outbound_channel_tx.clone(), metadata.hop_count);
            // for dest in 
        }
    }

    // #[instrument(level = "trace", skip(self, message_bytes))]
    // fn deserialize_message(&mut self, message_bytes: &[u8], was_encrypted: bool, mut senders: HashSet<Sender>, timestamp: String) {
    //     if let Ok(mut message) = bincode::deserialize::<SearchMessage>(message_bytes) {
    //         debug!(id = %message.id(), curr_node = ?self.outbound_gateway.myself, "Received search message");
    //         if SearchMessage::ENCRYPTION_REQUIRED && !was_encrypted { return }
    //         message.set_sender(senders.drain().next().unwrap());
    //         self.traverse_nat(message.origin());
    //         message.set_timestamp(timestamp);
    //         result_early_return!(match message {
    //             SearchMessage { kind: SearchMessageKind::Resource(_), .. } => self.to_srp.send(message),
    //             SearchMessage { kind: SearchMessageKind::Distribution(_), ..} => self.to_dsrp.send(message)
    //         });
    //     }
    //     else if let Ok(mut message) = bincode::deserialize::<DiscoverPeerMessage>(message_bytes) {
    //         // debug!(id = %message.id(), curr_node = ?self.outbound_gateway.myself, "Received dp message");
    //         if DiscoverPeerMessage::ENCRYPTION_REQUIRED && !was_encrypted { return }
    //         if let DiscoverPeerMessage { kind: DpMessageKind::IveGotSome, .. } = message {
    //             for peer in message.into_peer_list() {
    //                 self.send_nat_heartbeats(peer);
    //             }
    //             return;
    //         }
    //         message.set_sender(senders.drain().next().unwrap());
    //         self.traverse_nat(message.origin());
    //         message.set_timestamp(timestamp);
    //         result_early_return!(self.to_dpp.send(message));
    //     }
    //     else if let Ok(mut message) = bincode::deserialize::<DistributionMessage>(message_bytes) {
    //         if DiscoverPeerMessage::ENCRYPTION_REQUIRED && !was_encrypted { return }
    //         message.set_sender(senders.drain().next().unwrap());
    //         message.set_timestamp(timestamp);
    //         result_early_return!(self.to_dh.send(message));
    //     }
    //     else if let Ok(mut message) = bincode::deserialize::<Heartbeat>(message_bytes) {
    //         if Heartbeat::ENCRYPTION_REQUIRED && !was_encrypted { return }
    //         message.set_sender(senders.drain().next().unwrap());
    //         message.set_timestamp(timestamp);
    //         // trace!(sender = %message.sender(), curr_node = ?self.outbound_gateway.myself, "Received heartbeat");
    //     }
    //     else if let Ok(mut message) = bincode::deserialize::<StreamMessage>(message_bytes) {
    //         // if StreamMessage::ENCRYPTION_REQUIRED && !was_encrypted { return Ok(()) }
    //         for sender in senders {
    //             message.set_sender(sender);
    //         }
    //         message.set_timestamp(timestamp);
    //         match message {
    //             StreamMessage { kind: StreamMessageKind::KeyAgreement, .. } => { debug!(id = %message.id(), curr_node = ?self.outbound_gateway.myself, "Received key agreement message"); self.handle_key_agreement(message) },
    //             StreamMessage { kind: StreamMessageKind::Resource(_), .. } => { debug!(id = %message.id(), curr_node = ?self.outbound_gateway.myself, "Received stream message"); result_early_return!(self.to_smp.send(message)); }
    //             StreamMessage { kind: StreamMessageKind::Distribution(_), .. } => { debug!(id = %message.id(), curr_node = ?self.outbound_gateway.myself, "Received stream dmessage"); result_early_return!(self.to_dsmp.send(message)); }
    //         }
    //     }
    //     else {
    //         error!("Unable to deserialize received message to a supported type");
    //     }
    // }

    fn get_direction(&mut self, message: &mut Messagea) -> PropagationDirection {
        if let MessageDirection::Response = message.direction() {
            return PropagationDirection::Reverse;
        }
        let dest = message.dest();
        let (origin, early_return_context) = match message.metadata_mut() {
            MetadataKind::Search(metadata) => {
                if self.search_request_processor.continue_propagating(dest, metadata) { (None, None) } else { (Some(metadata.origin), None) }
            },
            MetadataKind::Discover(metadata) => {
                metadata.peer_list.push(self.outbound_gateway.myself);                
                if self.discover_peer_processor.continue_propagating(metadata) { (None, Some(EarlyReturnContext(self.outbound_channel_tx.clone(), message.clone()))) } else { (Some(metadata.origin), None) }
            },
            _ => return PropagationDirection::Final
        };
        if !self.breadcrumb_service.try_add_breadcrumb(message.id(), early_return_context, message.only_sender()) {
            return PropagationDirection::Stop;
        }
        if let Some(origin) = origin {
            self.send_nat_heartbeats(origin);
            return PropagationDirection::Reverse;
        }
        PropagationDirection::Forward
    }

    async fn execute_final_action(&mut self, message: Messagea) {
        let (sender, id) = (message.only_sender(), message.id());
        let sender = if let Some(sender) = sender { sender } else { Sender::new(self.outbound_gateway.myself.endpoint_pair.private_endpoint, self.outbound_gateway.myself.id) };
        if let MetadataKind::Discover(DiscoverMetadata {kind: DpMessageKind::IveGotSome, .. }) = message.metadata() {
            self.send_checked(message, None, false).await;
            return;
        }
        match message.into_metadata() {
            MetadataKind::Search(metadata) => {
                let SearchMetadata { origin, host_name } = metadata;
                let sender = self.search_request_processor.execute_final_action(sender, origin);
                if !self.session_manager.session_manager().session_active(&host_name) {
                    self.session_manager.new_active_session(host_name, sender, self.outbound_channel_tx.clone());
                }
                else if !self.session_manager.add_destination(&host_name, sender) {
                    return;
                }
                lock!(self.to_http_handlers.collection().map()).get_mut(&id).unwrap().send(HttpSignal::Go).unwrap();
            },
            MetadataKind::Discover(metadata) => {
                let peer_len_curr_max = self.discover_peer_processor.peer_len_curr_max(&id);
                if peer_len_curr_max == 0 {
                    self.discover_peer_processor.set_staging_early_return(self.outbound_channel_tx.clone(), id);
                }
                self.discover_peer_processor.stage_message1(id, metadata, peer_len_curr_max);
            },
            MetadataKind::Stream(metadata) => {
                let response = match metadata.payload {
                    StreamPayloadKind::Request(payload) => {
                        if !self.session_manager.session_active_host(&metadata.host_name) {
                            self.session_manager.new_active_session_host(metadata.host_name.clone(), sender)
                        }
                        else {
                            self.session_manager.add_destination_host(&metadata.host_name, sender);
                        }
                        self.session_manager.http_response_action(payload, metadata.host_name, id).await
                    },
                    StreamPayloadKind::Response(payload) => {
                        self.session_manager.session_manager_mut().finalize_resource(&metadata.host_name, &id);
                        return lock!(self.to_http_handlers.collection().map()).get(&id).unwrap().send(HttpSignal::Ok(payload)).unwrap();
                    },
                    StreamPayloadKind::DistributionRequest(payload) => self.session_manager_distribution.distribution_response_action(payload, metadata.host_name, id).await,
                    StreamPayloadKind::DistributionResponse(_) => todo!()
                };
                self.send_checked(response, Some(Peer::from(sender)), true).await;
            },
            MetadataKind::Distribute(metadata) => self.send_distribution_request(metadata).await,
            _ => {}
        }
    }

    fn send_nat_heartbeats(&mut self, peer: Peer) {
        if peer.id == self.outbound_gateway.myself.id
            || self.unconfirmed_peers.contains_key(&peer.id)
            || self.outbound_gateway.peer_ops.has_peer(peer.id) {
            return;
        }
        // println!("Start sending nat heartbeats to peer {:?} at {:?}", peer, self.outbound_gateway.myself);
        self.unconfirmed_peers.set_timer(peer.id, "Stage:UnconfirmedPeers");
        lock!(self.unconfirmed_peers.collection().map()).insert(peer.id, peer.endpoint_pair);
        let unconfirmed_peers = self.unconfirmed_peers.collection().clone();
        let outbound_channel = self.outbound_channel_tx.clone();
        tokio::spawn(async move {
            while unconfirmed_peers.contains_key(&peer.id) {
                result_early_return!(outbound_channel.send(Messagea::new_heartbeat()));
                sleep(HEARTBEAT_INTERVAL_SECONDS).await;
            }
        });
    }

    #[instrument(level = "trace", skip_all, fields(message.peer_id))]
    async fn handle_key_agreement(&mut self, message: KeyAgreementMessage, sender: SocketAddrV4) {
        let KeyAgreementMessage { public_key, peer_id, direction } = message;
        let cached_messages = match direction {
            MessageDirection::Request => {
                let my_public_key = self.key_store.public_key(peer_id);
                return self.outbound_gateway.send_agreement(Peer::from(Sender::new(sender, peer_id)), my_public_key, direction).await;
            },
            MessageDirection::Response => self.cached_outbound_messages.pop(&peer_id)
        };
        result_early_return!(self.key_store.agree(peer_id, public_key));
        if let Some(cached_messages) = cached_messages {
            debug!("Sending cached outbound messages");
            for message in cached_messages {
                let (to_be_chunked, is_http_request) = message.to_be_chunked();
                let message = option_early_return!(self.send_checked(message, None, to_be_chunked).await);
                if is_http_request {
                    self.session_manager.session_manager_mut().push_resource(message);
                }
            }
        }
    }
}

pub enum HttpSignal {
    Wait(Messagea),
    Go,
    Ok(SerdeHttpResponse)
}

pub enum PropagationDirection {
    Forward,
    Reverse,
    Stop,
    Final
}