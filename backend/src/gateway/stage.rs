use std::{collections::{HashMap, HashSet}, net::SocketAddrV4, sync::{Arc, Mutex}};
use tokio::sync::mpsc;
use crate::{crypto::{Direction, Error, KeyStore}, message::{DiscoverPeerMessage, Heartbeat, Id, InboundMessage, IsEncrypted, Message, SearchMessage, StreamMessage, StreamMessageKind}, node::EndpointPair, peer::PeerOps, utils::{TransientMap, TtlType}};

use super::{DiscoverPeerProcessor, EmptyResult, SRP_TTL_SECONDS};

pub struct MessageStaging {
    from_gateway: mpsc::UnboundedReceiver<(SocketAddrV4, InboundMessage)>,
    to_srp: mpsc::UnboundedSender<SearchMessage>,
    to_dpp: mpsc::UnboundedSender<DiscoverPeerMessage>,
    to_smp: mpsc::UnboundedSender<StreamMessage>,
    key_store: Arc<Mutex<KeyStore>>,
    peer_ops: Arc<Mutex<PeerOps>>,
    message_staging: TransientMap<Id, HashMap<usize, InboundMessage>>,
    cached_messages: TransientMap<Id, Vec<InboundMessage>>,
    endpoint_pair: EndpointPair
}

impl MessageStaging {
    pub fn new(
        from_gateway: mpsc::UnboundedReceiver<(SocketAddrV4, InboundMessage)>,
        to_srp: mpsc::UnboundedSender<SearchMessage>,
        to_dpp: mpsc::UnboundedSender<DiscoverPeerMessage>,
        to_smp: mpsc::UnboundedSender<StreamMessage>,
        key_store: &Arc<Mutex<KeyStore>>,
        peer_ops: Arc<Mutex<PeerOps>>,
        endpoint_pair: EndpointPair) -> Self
    {
        Self {
            from_gateway,
            to_srp,
            to_dpp,
            to_smp,
            key_store: key_store.clone(),
            peer_ops,
            message_staging: TransientMap::new(TtlType::Secs(SRP_TTL_SECONDS)),
            cached_messages: TransientMap::new(TtlType::Secs(SRP_TTL_SECONDS)),
            endpoint_pair,
        }
    }

    pub async fn receive(&mut self) -> EmptyResult {
        let (peer_addr, inbound_message) = self.from_gateway.recv().await.ok_or("MessageStaging: failed to receive message from gateway")?;
        if inbound_message.separate_parts().sender() != peer_addr {
            return Ok(())
        }
        let res = self.stage_message(inbound_message);
        if let Some(message_parts) = res {
            return self.reassemble_message(message_parts);
        }
        Ok(())
    }

    fn reassemble_message(&mut self, message_parts: Vec<InboundMessage>) -> EmptyResult {
        let (message_bytes, senders, timestamp) = InboundMessage::reassemble_message(message_parts);
        for sender in senders.iter() {
            self.add_new_peer(*sender);
        }
        self.deserialize_message(&message_bytes, false, senders, timestamp)
    }

    fn add_new_peer(&self, sender: SocketAddrV4) {
        if sender != EndpointPair::default_socket() && sender != self.endpoint_pair.public_endpoint {
            let endpoint_pair = EndpointPair::new(sender, sender);
            self.peer_ops.lock().unwrap().add_peer(endpoint_pair, DiscoverPeerProcessor::get_score(self.endpoint_pair.public_endpoint, endpoint_pair.public_endpoint))
        }
    }

    fn handle_key_agreement(&mut self, mut message: StreamMessage) -> EmptyResult {
        let sender = message.only_sender();
        let (uuid, peer_public_key) = message.into_hash_payload();
        self.key_store.lock().unwrap().agree(sender, peer_public_key).map_err(|e| e.error_response(file!(), line!()))?;
        let cached_messages = self.cached_messages.map().lock().unwrap().remove(&uuid);
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

    fn deserialize_message(&mut self, message_bytes: &[u8], was_encrypted: bool, mut senders: HashSet<SocketAddrV4>, timestamp: String) -> EmptyResult {
        if let Ok(mut message) = bincode::deserialize::<SearchMessage>(message_bytes) {
            println!("Received search message, uuid: {} at {}", message.id(), self.endpoint_pair.public_endpoint.to_string());
            if SearchMessage::ENCRYPTION_REQUIRED && !was_encrypted { return Ok(()) }
            message.set_sender(senders.drain().next().unwrap());
            message.set_timestamp(timestamp);
            self.to_srp.send(message).map_err(|e| { e.to_string() } )
        }
        else if let Ok(mut message) = bincode::deserialize::<DiscoverPeerMessage>(message_bytes) {
            // println!("Received dp message, uuid: {} at {}, {:?}", message.id(), self.endpoint_pair.public_endpoint.to_string(), message.kind);
            if DiscoverPeerMessage::ENCRYPTION_REQUIRED && !was_encrypted { return Ok(()) }
            message.set_sender(senders.drain().next().unwrap());
            message.set_timestamp(timestamp);
            self.to_dpp.send(message).map_err(|e| { e.to_string() } )
        }
        else if let Ok(mut message) = bincode::deserialize::<Heartbeat>(message_bytes) {
            if Heartbeat::ENCRYPTION_REQUIRED && !was_encrypted { return Ok(()) }
            message.set_sender(senders.drain().next().unwrap());
            message.set_timestamp(timestamp);
            // Ok(println!("{:?}", message))
            Ok(())
        }
        else if let Ok(mut message) = bincode::deserialize::<StreamMessage>(message_bytes) {
            // if StreamMessage::ENCRYPTION_REQUIRED && !was_encrypted { return Ok(()) }
            for sender in senders {
                message.set_sender(sender);
            }
            message.set_timestamp(timestamp);
            match message {
                StreamMessage { kind: StreamMessageKind::KeyAgreement, .. } => { println!("Received key agreement message, uuid: {} at {}", message.id(), self.endpoint_pair.public_endpoint.to_string()); self.handle_key_agreement(message) },
                _ => { println!("Received stream message, uuid: {} at {}", message.id(), self.endpoint_pair.public_endpoint.to_string()); self.to_smp.send(message).map_err(|e| { e.to_string() } ) }
            }
        }
        else {
            Err(String::from("Unable to deserialize received message to a supported type"))
        }
    }

    fn stage_message(&mut self, mut inbound_message: InboundMessage) -> Option<Vec<InboundMessage>> {
        let (is_encrypted, sender) = (inbound_message.take_is_encrypted(), inbound_message.separate_parts().sender());
        if let IsEncrypted::True(nonce) = is_encrypted {
            let payload = inbound_message.payload_mut();
            let crypto_result = self.key_store.lock().unwrap().transform(sender, payload, Direction::Decode(nonce.clone()));
            match crypto_result {
                Ok(plaintext) => { *payload = plaintext },
                Err(Error::NoKey) => {
                    self.cached_messages.set_timer(inbound_message.separate_parts().id().to_owned());
                    let mut cached_messages = self.cached_messages.map().lock().unwrap();
                    let cached_messages = cached_messages.entry(inbound_message.separate_parts().id().to_owned()).or_default();
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