use std::{collections::{HashMap, HashSet}, net::SocketAddrV4};
use ring::aead;
use tokio::{select, sync::mpsc, time::sleep};
use tracing::{debug, error, instrument, trace, warn};
use crate::{crypto::{Direction, Error, KeyStore}, lock, message::{DiscoverPeerMessage, DistributionMessage, DpMessageKind, Heartbeat, InboundMessage, KeyAgreementMessage, Message, MessageDirection, Messagea, MetadataKind, NumId, Peer, SearchMessage, SearchMessageKind, SearchMetadata, Sender, StreamMessage, StreamMessageKind}, message_processing::HEARTBEAT_INTERVAL_SECONDS, node::EndpointPair, option_early_return, result_early_return, utils::{ArcCollection, ArcMap, TransientCollection}};

use super::{search::SearchRequestProcessor, BreadcrumbService, DiscoverPeerProcessor, EmptyOption, OutboundGateway, SRP_TTL_SECONDS};

pub struct MessageStaging {
    from_gateway: mpsc::UnboundedReceiver<(SocketAddrV4, Vec<u8>)>,
    to_srp: mpsc::UnboundedSender<SearchMessage>,
    to_dpp: mpsc::UnboundedSender<DiscoverPeerMessage>,
    to_smp: mpsc::UnboundedSender<StreamMessage>,
    to_dsrp: mpsc::UnboundedSender<SearchMessage>,
    to_dsmp: mpsc::UnboundedSender<StreamMessage>,
    to_dh: mpsc::UnboundedSender<DistributionMessage>,
    message_staging: TransientCollection<ArcMap<NumId, HashMap<usize, InboundMessage>>>,
    message_caching: TransientCollection<ArcMap<NumId, Vec<(SocketAddrV4, Vec<u8>)>>>,
    unconfirmed_peers: TransientCollection<ArcMap<NumId, EndpointPair>>,
    outbound_gateway: OutboundGateway,
    handlers: TransientCollection<ArcMap<NumId, mpsc::UnboundedSender<Messagea>>>,
    key_store: KeyStore,
    breadcrumb_service: BreadcrumbService,
    search_request_processor: SearchRequestProcessor,
    discover_peer_processor: DiscoverPeerProcessor,
    early_return_trigger_tx: mpsc::UnboundedSender<Messagea>,
    early_return_trigger_rx: mpsc::UnboundedReceiver<Messagea>
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
        discover_peer_processor: DiscoverPeerProcessor) -> Self
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
            message_caching: TransientCollection::new(SRP_TTL_SECONDS, false, ArcMap::new()),
            unconfirmed_peers: TransientCollection::new(HEARTBEAT_INTERVAL_SECONDS*2, false, ArcMap::new()),
            outbound_gateway,
            handlers: TransientCollection::new(SRP_TTL_SECONDS, false, ArcMap::new()),
            key_store: KeyStore::new(),
            breadcrumb_service: BreadcrumbService::new(SRP_TTL_SECONDS),
            search_request_processor,
            early_return_trigger_tx,
            early_return_trigger_rx,
            discover_peer_processor
        }
    }

    pub async fn receive(&mut self) -> EmptyOption {
        let (sender_addr, message_bytes) = select! {
            message = self.from_gateway.recv() => message?,
            outbound_message = self.early_return_trigger_rx.recv() => return Some(())
        };
        self.stage_message(message_bytes, sender_addr);
        Some(())
    }

    fn on_finished(&self) { unimplemented!() }

    // #[instrument(level = "trace", skip_all, fields(inbound_message.sender = ?inbound_message.separate_parts().sender(), inbound_message.id = %inbound_message.separate_parts().id()))]
    fn stage_message(&mut self, mut message_bytes: Vec<u8>, sender_addr: SocketAddrV4) {
        // let (is_encrypted, sender) = (message_bytes.take_is_encrypted(), message_bytes.separate_parts().sender().socket);
        let mut suffix = message_bytes.split_off(message_bytes.len() - aead::NONCE_LEN - 16);
        let nonce = suffix.split_off(suffix.len() - aead::NONCE_LEN);
        let peer_id = NumId(u128::from_be_bytes(suffix.try_into().unwrap()));
        let crypto_result = self.key_store.transform(peer_id, &mut message_bytes, Direction::Decode(nonce));
        match crypto_result {
            Ok(plaintext) => { message_bytes = plaintext },
            Err(Error::NoKey) => {
                self.message_caching.set_timer(peer_id, "Stage:MessageCaching");
                let mut message_caching = lock!(self.message_caching.collection().map());
                let cached_messages = message_caching.entry(peer_id).or_default();
                cached_messages.push((sender_addr, message_bytes));
                debug!("no key");
                return;
            },
            Err(error) => { error!(?error); return; }
        };
        let inbound_message: InboundMessage = result_early_return!(bincode::deserialize(&message_bytes));
        if inbound_message.separate_parts().sender().socket != sender_addr {
            warn!(sender = %inbound_message.separate_parts().sender().socket, actual_sender = %sender_addr, "Sender doesn't match actual sender");
            return;
        }
        if let Some(endpoint_pair) = self.unconfirmed_peers.pop(&inbound_message.separate_parts().sender().id) {
            // println!("Confirmed peer {}", inbound_message.separate_parts().sender().id());
            self.outbound_gateway.add_new_peer(Peer::new(endpoint_pair, inbound_message.separate_parts().sender().id));
        }
        let (index, num_chunks) = inbound_message.separate_parts().position();
        if num_chunks == 1 {
            // gateway::log_debug(&format!("Hash on the way back: {}", message.message_ext().hash()));
            return;
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
            let messages = option_early_return!(self.message_staging.pop(&id));
            let messages: Vec<InboundMessage> = messages.into_values().collect();
            self.reassemble_message(messages);
        }
    }

    fn reassemble_message(&mut self, message_parts: Vec<InboundMessage>) {
        let (message_bytes, senders, timestamp) = InboundMessage::reassemble_message(message_parts);
        let mut message = self.deserialize_message(&message_bytes, senders, timestamp);
        let continue_propogating = self.continue_propagating(&message);
        if continue_propogating {
            
            self.outbound_gateway.send_request2(&message, message.only_sender(), &self.key_store)
        }
        else {
            let dest = option_early_return!(self.breadcrumb_service.get_dest(&message.id()));
            if let Some(dest) = dest {
                self.outbound_gateway.send_individual2(dest, &message, false, &self.key_store);
            }
            else {
                todo!()
            }
        }
    }

    fn deserialize_message(&mut self, message_bytes: &[u8], mut senders: HashSet<Sender>, timestamp: String) -> Messagea {
        let mut message = bincode::deserialize::<Messagea>(message_bytes).unwrap();
        for sender in senders {
            message.set_sender(sender);
        }
        message.set_timestamp(timestamp);
        message
    }

    fn check_key_agreement(&self, message: Messagea) -> Option<Messagea> {
        if self.key_store.agreement_exists(&message.dest().id) {
            Some(message)
        }
        else {
            let public_key = self.key_store.public_key(message.dest().id);
            self.outbound_gateway.send_agreement(message.dest(), public_key, MessageDirection::Request);
            None
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

    fn continue_propagating(&self, message: &Messagea) -> bool {
        if let MessageDirection::Response = message.direction() {
            return false;
        }        
        let (origin, early_return_context) = match message.metadata() {
            MetadataKind::Search(metadata) => {
                if self.search_request_processor.continue_propagating(message.dest(), &metadata.host_name, metadata) { (None, None) } else { (Some(metadata.origin), None) }
            },
            MetadataKind::Discover(mut metadata) => {
                if self.discover_peer_processor.continue_propagating(&mut metadata) { (None, Some((self.early_return_trigger_tx.clone(), message.clone()))) } else { (Some(metadata.origin), None) }
            }
            MetadataKind::Stream(_) => (None, None),
            _ => todo!()
        };
        if !self.breadcrumb_service.try_add_breadcrumb(message.id(), early_return_context, message.only_sender()) {
            return false;
        }
        if let Some(origin) = origin {
            self.traverse_nat(origin);
            return false;
        }
        true
    }

    fn execute_final_action(&self, mut message: Messagea) {
        match message.into_metadata() {
            MetadataKind::Search(metadata) => {
                let SearchMetadata { origin, public_key, kind, host_name } = metadata;
                self.key_store.agree(message.id(), public_key);
                let sender = if let Some(sender) = message.only_sender() { Peer::from(sender) } else { self.outbound_gateway.myself };
                self.search_request_processor.execute_final_action(message.id(), sender, origin.unwrap(), host_name);
            },
            MetadataKind::Discover(metadata) => {
                let peer_len_curr_max = self.discover_peer_processor.peer_len_curr_max(&message.id());
                if peer_len_curr_max == 0 {
                    self.discover_peer_processor.set_staging_early_return(self.early_return_trigger_tx.clone(), message.id());
                }
                self.discover_peer_processor.stage_message1(message.id(), metadata, peer_len_curr_max);
            }
            _ => {}
        }
    }

    fn traverse_nat(&mut self, peer: Option<Peer>) {
        let Some(peer) = peer else { return };
        self.send_nat_heartbeats(peer);
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
        let (outbound_channel, my_node_id) = (self.early_return_trigger_tx.clone(), self.outbound_gateway.myself.id);
        tokio::spawn(async move {
            while unconfirmed_peers.contains_key(&peer.id) {
                result_early_return!(outbound_channel.send(Messagea::new_heartbeat(my_node_id)));
                sleep(HEARTBEAT_INTERVAL_SECONDS).await;
            }
        });
    }

    #[instrument(level = "trace", skip_all, fields(message.sender = ?message.senders()[0], message.host_name = message.host_name()))]
    fn handle_key_agreement(&mut self, mut message: StreamMessage) {
        let sender = message.only_sender().unwrap();
        let (id, peer_public_key) = message.into_hash_payload();
        result_early_return!(self.key_store.agree(sender.id, peer_public_key));
        let cached_messages = self.message_caching.pop(&id);
        if let Some(cached_messages) = cached_messages {
            debug!("Staging cached messages");
            for (sender_addr, message) in cached_messages {
                self.stage_message(message, sender_addr);
            }
        }
    }
}