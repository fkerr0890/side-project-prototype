use std::{collections::{HashMap, HashSet}, net::SocketAddrV4, time::Duration};
use tokio::{sync::mpsc, time::sleep};
use tracing::{debug, error, instrument, trace, warn};
use crate::{crypto::{Direction, Error}, message::{DiscoverPeerMessage, DistributionMessage, DpMessageKind, Heartbeat, InboundMessage, IsEncrypted, Message, NumId, Peer, SearchMessage, SearchMessageKind, Sender, StreamMessage, StreamMessageInnerKind, StreamMessageKind}, message_processing::HEARTBEAT_INTERVAL_SECONDS, node::EndpointPair, result_early_return, utils::{ArcMap, ArcCollection, TransientCollection, TtlType}};

use super::{EmptyOption, OutboundGateway, ToBeEncrypted, SRP_TTL_SECONDS};

pub struct MessageStaging {
    from_gateway: mpsc::UnboundedReceiver<(SocketAddrV4, InboundMessage)>,
    to_srp: mpsc::UnboundedSender<SearchMessage>,
    to_dpp: mpsc::UnboundedSender<DiscoverPeerMessage>,
    to_smp: mpsc::UnboundedSender<StreamMessage>,
    to_dsrp: mpsc::UnboundedSender<SearchMessage>,
    to_dsmp: mpsc::UnboundedSender<StreamMessage>,
    to_dh: mpsc::UnboundedSender<DistributionMessage>,
    message_staging: TransientCollection<ArcMap<NumId, HashMap<usize, InboundMessage>>>,
    message_caching: TransientCollection<ArcMap<NumId, Vec<InboundMessage>>>,
    unconfirmed_peers: TransientCollection<ArcMap<NumId, EndpointPair>>,
    outbound_gateway: OutboundGateway
}

impl MessageStaging {
    pub fn new(
        from_gateway: mpsc::UnboundedReceiver<(SocketAddrV4, InboundMessage)>,
        to_srp: mpsc::UnboundedSender<SearchMessage>,
        to_dpp: mpsc::UnboundedSender<DiscoverPeerMessage>,
        to_smp: mpsc::UnboundedSender<StreamMessage>,
        to_dsrp: mpsc::UnboundedSender<SearchMessage>,
        to_dsmp: mpsc::UnboundedSender<StreamMessage>,
        to_dh: mpsc::UnboundedSender<DistributionMessage>,
        outbound_gateway: OutboundGateway) -> Self
    {
        Self {
            from_gateway,
            to_srp,
            to_dpp,
            to_smp,
            to_dsrp,
            to_dsmp,
            to_dh,
            message_staging: TransientCollection::new(TtlType::Secs(SRP_TTL_SECONDS), false, ArcMap::new()),
            message_caching: TransientCollection::new(TtlType::Secs(SRP_TTL_SECONDS), false, ArcMap::new()),
            unconfirmed_peers: TransientCollection::new(TtlType::Secs(HEARTBEAT_INTERVAL_SECONDS*2), false, ArcMap::new()),
            outbound_gateway
        }
    }

    pub async fn receive(&mut self) -> EmptyOption {
        let (peer_addr, inbound_message) = self.from_gateway.recv().await?;
        if inbound_message.separate_parts().sender().socket != peer_addr {
            return Some(warn!(sender = %inbound_message.separate_parts().sender().socket, actual_sender = %peer_addr, "Sender doesn't match actual sender"));
        }
        if let Some(endpoint_pair) = self.unconfirmed_peers.pop(&inbound_message.separate_parts().sender().id) {
            // println!("Confirmed peer {}", inbound_message.separate_parts().sender().id());
            self.outbound_gateway.add_new_peer(Peer::new(endpoint_pair, inbound_message.separate_parts().sender().id));
        }
        let res = self.stage_message(inbound_message);
        if let Some(message_parts) = res {
            self.reassemble_message(message_parts);
        }
        Some(())
    }

    fn reassemble_message(&mut self, message_parts: Vec<InboundMessage>) {
        let (message_bytes, senders, timestamp) = InboundMessage::reassemble_message(message_parts);
        self.deserialize_message(&message_bytes, false, senders, timestamp);
    }

    fn traverse_nat(&mut self, peer: Option<Peer>) {
        let Some(peer) = peer else { return };
        self.send_nat_heartbeats(peer);
    }

    fn send_nat_heartbeats(&mut self, peer: Peer) {
        if peer.id == self.outbound_gateway.myself.id
            || self.unconfirmed_peers.contains_key(&peer.id)
            || self.outbound_gateway.peer_ops.lock().unwrap().has_peer(peer.id) {
            return;
        }
        // println!("Start sending nat heartbeats to peer {:?} at {:?}", peer, self.outbound_gateway.myself);
        self.unconfirmed_peers.set_timer(peer.id, "Stage:UnconfirmedPeers");
        self.unconfirmed_peers.collection().map().lock().unwrap().insert(peer.id, peer.endpoint_pair);
        let unconfirmed_peers = self.unconfirmed_peers.collection().clone();
        let (socket, myself) = (self.outbound_gateway.socket.clone(), self.outbound_gateway.myself);
        tokio::spawn(async move {
            while unconfirmed_peers.contains_key(&peer.id) {
                OutboundGateway::send_private_public_static(&socket, peer.endpoint_pair, myself, &mut Heartbeat::new(), ToBeEncrypted::False, true);
                sleep(Duration::from_secs(HEARTBEAT_INTERVAL_SECONDS)).await;
            }
        });
    }

    #[instrument(level = "trace", skip_all, fields(message.sender = ?message.senders()[0], message.host_name = message.host_name()))]
    fn handle_key_agreement(&mut self, mut message: StreamMessage) {
        let sender = message.only_sender().unwrap();
        let (id, peer_public_key) = message.into_hash_payload();
        result_early_return!(self.outbound_gateway.key_store.lock().unwrap().agree(sender.socket, peer_public_key));
        let cached_messages = self.message_caching.pop(&id);
        if let Some(cached_messages) = cached_messages {
            debug!("Staging cached messages");
            let mut res = None;
            for message in cached_messages {
                res = self.stage_message(message);
            }
            if let Some(message_parts) = res {
                self.reassemble_message(message_parts);
            }
        }
    }

    // #[instrument(level = "trace", skip(self, message_bytes))]
    fn deserialize_message(&mut self, message_bytes: &[u8], was_encrypted: bool, mut senders: HashSet<Sender>, timestamp: String) {
        if let Ok(mut message) = bincode::deserialize::<SearchMessage>(message_bytes) {
            debug!(id = %message.id(), curr_node = ?self.outbound_gateway.myself, "Received search message");
            if SearchMessage::ENCRYPTION_REQUIRED && !was_encrypted { return }
            message.set_sender(senders.drain().next().unwrap());
            self.traverse_nat(message.origin());
            message.set_timestamp(timestamp);
            result_early_return!(match message {
                SearchMessage { kind: SearchMessageKind::Resource(_), .. } => self.to_srp.send(message),
                SearchMessage { kind: SearchMessageKind::Distribution(_), ..} => self.to_dsrp.send(message)
            });
        }
        else if let Ok(mut message) = bincode::deserialize::<DiscoverPeerMessage>(message_bytes) {
            // debug!(id = %message.id(), curr_node = ?self.outbound_gateway.myself, "Received dp message");
            if DiscoverPeerMessage::ENCRYPTION_REQUIRED && !was_encrypted { return }
            if let DiscoverPeerMessage { kind: DpMessageKind::IveGotSome, .. } = message {
                for peer in message.into_peer_list() {
                    self.send_nat_heartbeats(peer);
                }
                return;
            }
            message.set_sender(senders.drain().next().unwrap());
            self.traverse_nat(message.origin());
            message.set_timestamp(timestamp);
            result_early_return!(self.to_dpp.send(message));
        }
        else if let Ok(mut message) = bincode::deserialize::<DistributionMessage>(message_bytes) {
            if DiscoverPeerMessage::ENCRYPTION_REQUIRED && !was_encrypted { return }
            message.set_sender(senders.drain().next().unwrap());
            message.set_timestamp(timestamp);
            result_early_return!(self.to_dh.send(message));
        }
        else if let Ok(mut message) = bincode::deserialize::<Heartbeat>(message_bytes) {
            if Heartbeat::ENCRYPTION_REQUIRED && !was_encrypted { return }
            message.set_sender(senders.drain().next().unwrap());
            message.set_timestamp(timestamp);
            // trace!(sender = %message.sender(), curr_node = ?self.outbound_gateway.myself, "Received heartbeat");
        }
        else if let Ok(mut message) = bincode::deserialize::<StreamMessage>(message_bytes) {
            // if StreamMessage::ENCRYPTION_REQUIRED && !was_encrypted { return Ok(()) }
            for sender in senders {
                message.set_sender(sender);
            }
            message.set_timestamp(timestamp);
            match message {
                StreamMessage { kind: StreamMessageKind::Resource(StreamMessageInnerKind::KeyAgreement) | StreamMessageKind::Distribution(StreamMessageInnerKind::KeyAgreement), .. } => { debug!(id = %message.id(), curr_node = ?self.outbound_gateway.myself, "Received key agreement message"); self.handle_key_agreement(message) },
                StreamMessage { kind: StreamMessageKind::Resource(_), .. } => { debug!(id = %message.id(), curr_node = ?self.outbound_gateway.myself, "Received stream message"); result_early_return!(self.to_smp.send(message)); }
                StreamMessage { kind: StreamMessageKind::Distribution(_), .. } => { debug!(id = %message.id(), curr_node = ?self.outbound_gateway.myself, "Received stream dmessage"); result_early_return!(self.to_dsmp.send(message)); }
            }
        }
        else {
            error!("Unable to deserialize received message to a supported type");
        }
    }

    // #[instrument(level = "trace", skip_all, fields(inbound_message.sender = ?inbound_message.separate_parts().sender(), inbound_message.id = %inbound_message.separate_parts().id()))]
    fn stage_message(&mut self, mut inbound_message: InboundMessage) -> Option<Vec<InboundMessage>> {
        let (is_encrypted, sender) = (inbound_message.take_is_encrypted(), inbound_message.separate_parts().sender().socket);
        if let IsEncrypted::True(nonce) = is_encrypted {
            let payload = inbound_message.payload_mut();
            let crypto_result = self.outbound_gateway.key_store.lock().unwrap().transform(sender, payload, Direction::Decode(nonce.clone()));
            match crypto_result {
                Ok(plaintext) => { *payload = plaintext },
                Err(Error::NoKey) => {
                    self.message_caching.set_timer(inbound_message.separate_parts().id(), "Stage:MessageCaching");
                    let mut message_caching = self.message_caching.collection().map().lock().unwrap();
                    let cached_messages = message_caching.entry(inbound_message.separate_parts().id()).or_default();
                    inbound_message.set_is_encrypted_true(nonce);
                    cached_messages.push(inbound_message);
                    debug!("no key");
                    return None;
                },
                Err(error) => { error!(?error); return None; }
            };
        }
        let (index, num_chunks) = inbound_message.separate_parts().position();
        if num_chunks == 1 {
            // gateway::log_debug(&format!("Hash on the way back: {}", message.message_ext().hash()));
            Some(vec![inbound_message])
        }
        else {
            let id = inbound_message.separate_parts().id();
            let staged_messages_len = {
                self.message_staging.set_timer(id, "Stage:MessageStaging");
                let mut message_staging = self.message_staging.collection().map().lock().unwrap();
                let staged_messages= message_staging.entry(id).or_insert(HashMap::with_capacity(num_chunks));
                staged_messages.insert(index, inbound_message);
                staged_messages.len()
            };
            if staged_messages_len == num_chunks {
                // gateway::log_debug("Collected all messages");
                let Some(messages) = self.message_staging.pop(&id) else { return None };
                let messages: Vec<InboundMessage> = messages.into_values().collect();
                return Some(messages);
            }
            None
        }
    }
}