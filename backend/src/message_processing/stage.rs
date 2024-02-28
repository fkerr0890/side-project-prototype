use std::{collections::{HashMap, HashSet}, net::SocketAddrV4, time::Duration};
use tokio::{sync::mpsc, time::sleep};
use crate::{crypto::{Direction, Error}, message::{DiscoverPeerMessage, DpMessageKind, Heartbeat, Id, InboundMessage, IsEncrypted, Message, Peer, SearchMessage, Sender, StreamMessage, StreamMessageKind}, message_processing::HEARTBEAT_INTERVAL_SECONDS, node::EndpointPair, utils::{TransientMap, TtlType}};

use super::{EmptyResult, OutboundGateway, SRP_TTL_SECONDS};

pub struct MessageStaging {
    from_gateway: mpsc::UnboundedReceiver<(SocketAddrV4, InboundMessage)>,
    to_srp: mpsc::UnboundedSender<SearchMessage>,
    to_dpp: mpsc::UnboundedSender<DiscoverPeerMessage>,
    to_smp: mpsc::UnboundedSender<StreamMessage>,
    message_staging: TransientMap<Id, HashMap<usize, InboundMessage>>,
    message_caching: TransientMap<Id, Vec<InboundMessage>>,
    unconfirmed_peers: TransientMap<String, EndpointPair>,
    outbound_gateway: OutboundGateway
}

impl MessageStaging {
    pub fn new(
        from_gateway: mpsc::UnboundedReceiver<(SocketAddrV4, InboundMessage)>,
        to_srp: mpsc::UnboundedSender<SearchMessage>,
        to_dpp: mpsc::UnboundedSender<DiscoverPeerMessage>,
        to_smp: mpsc::UnboundedSender<StreamMessage>,
        outbound_gateway: OutboundGateway) -> Self
    {
        Self {
            from_gateway,
            to_srp,
            to_dpp,
            to_smp,
            message_staging: TransientMap::new(TtlType::Secs(SRP_TTL_SECONDS)),
            message_caching: TransientMap::new(TtlType::Secs(SRP_TTL_SECONDS)),
            unconfirmed_peers: TransientMap::new(TtlType::Secs(HEARTBEAT_INTERVAL_SECONDS*2)),
            outbound_gateway
        }
    }

    pub async fn receive(&mut self) -> EmptyResult {
        let (peer_addr, inbound_message) = self.from_gateway.recv().await.ok_or("MessageStaging: failed to receive message from gateway")?;
        if inbound_message.separate_parts().sender().socket() != peer_addr {
            return Ok(())
        }
        if let Some(endpoint_pair) = self.unconfirmed_peers.map().lock().unwrap().remove(inbound_message.separate_parts().sender().uuid()) {
            // println!("Confirmed peer {}", inbound_message.separate_parts().sender().uuid());
            self.outbound_gateway.add_new_peer(Peer::new(endpoint_pair, inbound_message.separate_parts().sender().uuid().to_owned()));
        }
        let res = self.stage_message(inbound_message);
        if let Some(message_parts) = res {
            return self.reassemble_message(message_parts);
        }
        Ok(())
    }

    fn reassemble_message(&mut self, message_parts: Vec<InboundMessage>) -> EmptyResult {
        let (message_bytes, senders, timestamp) = InboundMessage::reassemble_message(message_parts);
        self.deserialize_message(&message_bytes, false, senders, timestamp)
    }

    fn traverse_nat(&self, peer: Option<&Peer>) {
        let Some(peer) = peer else { return };
        self.send_nat_heartbeats(peer);
    }

    fn send_nat_heartbeats(&self, peer: &Peer) {
        if peer.uuid() == self.outbound_gateway.myself.uuid()
            || self.unconfirmed_peers.map().lock().unwrap().contains_key(peer.uuid())
            || self.outbound_gateway.peer_ops.as_ref().unwrap().lock().unwrap().has_peer(peer.uuid()) {
            return;
        }
        // println!("Start sending nat heartbeats to peer {:?} at {:?}", peer, self.outbound_gateway.myself);
        let (peer_endpoint_pair, uuid) = (peer.endpoint_pair(), peer.uuid().to_owned());
        self.unconfirmed_peers.set_timer(uuid.clone());
        self.unconfirmed_peers.map().lock().unwrap().insert(uuid.clone(), peer_endpoint_pair);
        let unconfirmed_peers = self.unconfirmed_peers.map().clone();
        let (socket, key_store, myself) = (self.outbound_gateway.socket.clone(), self.outbound_gateway.key_store.clone(), self.outbound_gateway.myself.clone());
        tokio::spawn(async move {
            while unconfirmed_peers.lock().unwrap().contains_key(&uuid) {
                OutboundGateway::send_static(&socket, &key_store, peer_endpoint_pair.public_endpoint, &myself, &mut Heartbeat::new(), false, true).ok();
                OutboundGateway::send_static(&socket, &key_store, peer_endpoint_pair.private_endpoint, &myself, &mut Heartbeat::new(), false, true).ok();
                sleep(Duration::from_secs(HEARTBEAT_INTERVAL_SECONDS)).await;
            }
        });
    }

    fn handle_key_agreement(&mut self, mut message: StreamMessage) -> EmptyResult {
        let sender = message.only_sender();
        let (uuid, peer_public_key) = message.into_hash_payload();
        self.outbound_gateway.key_store.lock().unwrap().agree(sender, peer_public_key).map_err(|e| e.error_response(file!(), line!()))?;
        let cached_messages = self.message_caching.map().lock().unwrap().remove(&uuid);
        if let Some(cached_messages) = cached_messages {
            println!("Staging cached messages");
            let mut res = None;
            for message in cached_messages {
                res = self.stage_message(message);
            }
            if let Some(message_parts) = res {
                return self.reassemble_message(message_parts)
            }
        }
        Ok(())
    }

    fn deserialize_message(&mut self, message_bytes: &[u8], was_encrypted: bool, mut senders: HashSet<Sender>, timestamp: String) -> EmptyResult {
        if let Ok(mut message) = bincode::deserialize::<SearchMessage>(message_bytes) {
            println!("Received search message, uuid: {} at {:?}", message.id(), self.outbound_gateway.myself);
            if SearchMessage::ENCRYPTION_REQUIRED && !was_encrypted { return Ok(()) }
            message.set_sender(senders.drain().next().unwrap().socket());
            self.traverse_nat(message.origin());
            message.set_timestamp(timestamp);
            self.to_srp.send(message).map_err(|e| { e.to_string() } )
        }
        else if let Ok(mut message) = bincode::deserialize::<DiscoverPeerMessage>(message_bytes) {
            // println!("Received dp message, uuid: {} at {:?}, {:?}", message.id(), self.outbound_gateway.myself, message.kind);
            if DiscoverPeerMessage::ENCRYPTION_REQUIRED && !was_encrypted { return Ok(()) }
            if let DiscoverPeerMessage { kind: DpMessageKind::IveGotSome, .. } = message {
                for peer in message.into_peer_list() {
                    self.send_nat_heartbeats(&peer);
                }
                return Ok(());
            }
            message.set_sender(senders.drain().next().unwrap().socket());
            self.traverse_nat(message.origin());
            message.set_timestamp(timestamp);
            self.to_dpp.send(message).map_err(|e| { e.to_string() } )
        }
        else if let Ok(mut message) = bincode::deserialize::<Heartbeat>(message_bytes) {
            if Heartbeat::ENCRYPTION_REQUIRED && !was_encrypted { return Ok(()) }
            message.set_sender(senders.drain().next().unwrap().socket());
            message.set_timestamp(timestamp);
            // Ok(println!("{:?}", message))
            Ok(())
        }
        else if let Ok(mut message) = bincode::deserialize::<StreamMessage>(message_bytes) {
            // if StreamMessage::ENCRYPTION_REQUIRED && !was_encrypted { return Ok(()) }
            for sender in senders {
                message.set_sender(sender.socket());
            }
            message.set_timestamp(timestamp);
            match message {
                StreamMessage { kind: StreamMessageKind::KeyAgreement, .. } => { println!("Received key agreement message, uuid: {} at {:?}", message.id(), self.outbound_gateway.myself); self.handle_key_agreement(message) },
                _ => { println!("Received stream message, uuid: {} at {:?}", message.id(), self.outbound_gateway.myself); self.to_smp.send(message).map_err(|e| { e.to_string() } ) }
            }
        }
        else {
            Err(String::from("Unable to deserialize received message to a supported type"))
        }
    }

    fn stage_message(&mut self, mut inbound_message: InboundMessage) -> Option<Vec<InboundMessage>> {
        let (is_encrypted, sender) = (inbound_message.take_is_encrypted(), inbound_message.separate_parts().sender().socket());
        if let IsEncrypted::True(nonce) = is_encrypted {
            let payload = inbound_message.payload_mut();
            let crypto_result = self.outbound_gateway.key_store.lock().unwrap().transform(sender, payload, Direction::Decode(nonce.clone()));
            match crypto_result {
                Ok(plaintext) => { *payload = plaintext },
                Err(Error::NoKey) => {
                    self.message_caching.set_timer(inbound_message.separate_parts().id().to_owned());
                    let mut message_caching = self.message_caching.map().lock().unwrap();
                    let cached_messages = message_caching.entry(inbound_message.separate_parts().id().to_owned()).or_default();
                    inbound_message.set_is_encrypted_true(nonce);
                    cached_messages.push(inbound_message);
                    println!("no key");
                    return None;
                },
                Err(e) => { println!("{e}"); return None }
            };
        }
        let (index, num_chunks) = inbound_message.separate_parts().position();
        if num_chunks == 1 {
            // gateway::log_debug(&format!("Hash on the way back: {}", message.message_ext().hash()));
            Some(vec![inbound_message])
        }
        else {
            let uuid = inbound_message.separate_parts().id().to_owned();
            let staged_messages_len = {
                self.message_staging.set_timer(uuid.clone());
                let mut message_staging = self.message_staging.map().lock().unwrap();
                let staged_messages= message_staging.entry(uuid.clone()).or_insert(HashMap::with_capacity(num_chunks));
                staged_messages.insert(index, inbound_message);
                staged_messages.len()
            };
            if staged_messages_len == num_chunks {
                // gateway::log_debug("Collected all messages");
                let Some(messages) = self.message_staging.map().lock().unwrap().remove(&uuid) else { return None };
                let messages: Vec<InboundMessage> = messages.into_values().collect();
                return Some(messages);
            }
            None
        }
    }
}