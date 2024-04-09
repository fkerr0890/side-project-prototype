use std::{collections::{HashMap, HashSet}, net::SocketAddrV4, time::Duration};
use ring::aead;
use tokio::{sync::mpsc, time::sleep};
use tracing::{debug, error, instrument, trace, warn};
use crate::{crypto::{Direction, Error}, lock, message::{DiscoverPeerMessage, DistributionMessage, DpMessageKind, Heartbeat, InboundMessage, Message, NumId, Peer, SearchMessage, SearchMessageKind, Sender, StreamMessage, StreamMessageKind}, message_processing::HEARTBEAT_INTERVAL_SECONDS, node::EndpointPair, result_early_return, utils::{ArcCollection, ArcMap, TransientCollection, TtlType}};

use super::{EmptyOption, OutboundGateway, SRP_TTL_SECONDS};

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
    outbound_gateway: OutboundGateway
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
        let (sender_addr, message_bytes) = self.from_gateway.recv().await?;
        let res = self.stage_message(message_bytes, sender_addr);
        if let Some(message_parts) = res {
            self.reassemble_message(message_parts);
        }
        Some(())
    }

    // #[instrument(level = "trace", skip_all, fields(inbound_message.sender = ?inbound_message.separate_parts().sender(), inbound_message.id = %inbound_message.separate_parts().id()))]
    fn stage_message(&mut self, mut message_bytes: Vec<u8>, sender_addr: SocketAddrV4) -> Option<Vec<InboundMessage>> {
        // let (is_encrypted, sender) = (message_bytes.take_is_encrypted(), message_bytes.separate_parts().sender().socket);
        let mut suffix = message_bytes.split_off(message_bytes.len() - aead::NONCE_LEN - 16);
        let nonce = suffix.split_off(suffix.len() - aead::NONCE_LEN);
        let peer_id = NumId(u128::from_be_bytes(suffix.try_into().unwrap()));
        let crypto_result = lock!(self.outbound_gateway.key_store).transform(peer_id, &mut message_bytes, Direction::Decode(nonce));
        match crypto_result {
            Ok(plaintext) => { message_bytes = plaintext },
            Err(Error::NoKey) => {
                self.message_caching.set_timer(peer_id, "Stage:MessageCaching");
                let mut message_caching = lock!(self.message_caching.collection().map());
                let cached_messages = message_caching.entry(peer_id).or_default();
                cached_messages.push((sender_addr, message_bytes));
                debug!("no key");
                return None;
            },
            Err(error) => { error!(?error); return None; }
        };
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
            // gateway::log_debug(&format!("Hash on the way back: {}", message.message_ext().hash()));
            Some(vec![inbound_message])
        }
        else {
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
                let messages = self.message_staging.pop(&id)?;
                let messages: Vec<InboundMessage> = messages.into_values().collect();
                return Some(messages);
            }
            None
        }
    }

    fn reassemble_message(&mut self, message_parts: Vec<InboundMessage>) {
        let (message_bytes, senders, timestamp) = InboundMessage::reassemble_message(message_parts);
        self.deserialize_message(&message_bytes, false, senders, timestamp);
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
                StreamMessage { kind: StreamMessageKind::KeyAgreement, .. } => { debug!(id = %message.id(), curr_node = ?self.outbound_gateway.myself, "Received key agreement message"); self.handle_key_agreement(message) },
                StreamMessage { kind: StreamMessageKind::Resource(_), .. } => { debug!(id = %message.id(), curr_node = ?self.outbound_gateway.myself, "Received stream message"); result_early_return!(self.to_smp.send(message)); }
                StreamMessage { kind: StreamMessageKind::Distribution(_), .. } => { debug!(id = %message.id(), curr_node = ?self.outbound_gateway.myself, "Received stream dmessage"); result_early_return!(self.to_dsmp.send(message)); }
            }
        }
        else {
            error!("Unable to deserialize received message to a supported type");
        }
    }

    fn traverse_nat(&mut self, peer: Option<Peer>) {
        let Some(peer) = peer else { return };
        self.send_nat_heartbeats(peer);
    }

    fn send_nat_heartbeats(&mut self, peer: Peer) {
        if peer.id == self.outbound_gateway.myself.id
            || self.unconfirmed_peers.contains_key(&peer.id)
            || lock!(self.outbound_gateway.peer_ops).has_peer(peer.id) {
            return;
        }
        // println!("Start sending nat heartbeats to peer {:?} at {:?}", peer, self.outbound_gateway.myself);
        self.unconfirmed_peers.set_timer(peer.id, "Stage:UnconfirmedPeers");
        lock!(self.unconfirmed_peers.collection().map()).insert(peer.id, peer.endpoint_pair);
        let unconfirmed_peers = self.unconfirmed_peers.collection().clone();
        let (socket, myself, key_store) = (self.outbound_gateway.socket.clone(), self.outbound_gateway.myself, self.outbound_gateway.key_store.clone());
        tokio::spawn(async move {
            while unconfirmed_peers.contains_key(&peer.id) {
                OutboundGateway::send_private_public_static(&socket, peer, myself, &mut Heartbeat::new(), key_store.clone(), true);
                sleep(Duration::from_secs(HEARTBEAT_INTERVAL_SECONDS)).await;
            }
        });
    }

    #[instrument(level = "trace", skip_all, fields(message.sender = ?message.senders()[0], message.host_name = message.host_name()))]
    fn handle_key_agreement(&mut self, mut message: StreamMessage) {
        let sender = message.only_sender().unwrap();
        let (id, peer_public_key) = message.into_hash_payload();
        result_early_return!(lock!(self.outbound_gateway.key_store).agree(sender.id, peer_public_key));
        let cached_messages = self.message_caching.pop(&id);
        if let Some(cached_messages) = cached_messages {
            debug!("Staging cached messages");
            let mut res = None;
            for (sender_addr, message) in cached_messages {
                res = self.stage_message(message, sender_addr);
            }
            if let Some(message_parts) = res {
                self.reassemble_message(message_parts);
            }
        }
    }
}