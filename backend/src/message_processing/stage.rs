use std::{collections::{HashMap, HashSet}, net::SocketAddrV4, sync::{Arc, Mutex}, time::Duration};
use tokio::{sync::{mpsc, oneshot}, time::sleep};
use crate::{crypto::{Direction, Error, KeyStore}, gateway::EmptyResult, message::{DiscoverPeerMessage, Heartbeat, InboundMessage, IsEncrypted, Message, SearchMessage, SeparateParts, StreamMessage, StreamMessageKind}, node::EndpointPair};

use super::SRP_TTL;

pub struct MessageStaging {
    from_gateway: mpsc::UnboundedReceiver<(SocketAddrV4, InboundMessage)>,
    to_srp: mpsc::UnboundedSender<SearchMessage>,
    to_dpp: mpsc::UnboundedSender<DiscoverPeerMessage>,
    to_smp: mpsc::UnboundedSender<StreamMessage>,
    key_store: Arc<Mutex<KeyStore>>,
    message_staging: HashMap<String, HashMap<usize, StagedMessage>>,
    cached_messages: HashMap<String, Vec<StagedMessage>>,
    staging_ttls: HashMap<String, oneshot::Receiver<()>>,
    endpoint_pair: EndpointPair
}

impl MessageStaging {
    pub fn new(
        from_gateway: mpsc::UnboundedReceiver<(SocketAddrV4, InboundMessage)>,
        to_srp: mpsc::UnboundedSender<SearchMessage>,
        to_dpp: mpsc::UnboundedSender<DiscoverPeerMessage>,
        to_smp: mpsc::UnboundedSender<StreamMessage>,
        key_store: &Arc<Mutex<KeyStore>>,
        endpoint_pair: EndpointPair) -> Self
    {
        Self {
            from_gateway,
            to_srp,
            to_dpp,
            to_smp,
            key_store: key_store.clone(),
            message_staging: HashMap::new(),
            cached_messages: HashMap::new(),
            staging_ttls: HashMap::new(),
            endpoint_pair,
        }
    }

    pub async fn receive(&mut self) -> EmptyResult {
        let (peer_addr, inbound_message) = self.from_gateway.recv().await.ok_or("MessageStaging: failed to receive message from gateway")?;
        let (payload, is_encrypted, separate_parts) = inbound_message.into_parts();
        let res = self.stage_message(payload, is_encrypted, separate_parts);
        if let Some(message_parts) = res {
            return self.reassemble_message(message_parts);
        }
        Ok(())
    }

    fn reassemble_message(&mut self, message_parts: Vec<StagedMessage>) -> EmptyResult {
        let (message_bytes, senders) = StagedMessage::reassemble_message(message_parts);
        self.deserialize_message(&message_bytes, false, senders)
    }

    fn handle_key_agreement(&mut self, mut message: StreamMessage) -> EmptyResult {
        let sender = message.only_sender();
        let (uuid, peer_public_key) = message.into_uuid_payload();
        self.key_store.lock().unwrap().agree(sender, peer_public_key).map_err(|e| e.error_response(file!(), line!()))?;
        if let Some(cached_messages) = self.cached_messages.remove(&uuid) {
            let mut res = None;
            for message in cached_messages {
                let (payload, is_encrypted, sender, position) = message.into_parts();
                res = self.stage_message(payload, is_encrypted, SeparateParts::new(sender, uuid.clone()).set_position(position));
            }
            if let Some(message_parts) = res {
                return self.reassemble_message(message_parts)
            }
        }
        Ok(())
    }

    fn deserialize_message(&mut self, message_bytes: &[u8], was_encrypted: bool, mut senders: HashSet<SocketAddrV4>) -> EmptyResult {
        if let Ok(mut message) = bincode::deserialize::<SearchMessage>(message_bytes) {
            println!("Received search message, uuid: {} at {}", message.id(), self.endpoint_pair.public_endpoint.to_string());
            if SearchMessage::ENCRYPTION_REQUIRED && !was_encrypted { return Ok(()) }
            message.set_sender(senders.drain().next().unwrap());
            self.to_srp.send(message).map_err(|e| { e.to_string() } )
        }
        else if let Ok(mut message) = bincode::deserialize::<DiscoverPeerMessage>(message_bytes) {
            // println!("Received dp message, uuid: {} at {}, {:?}", message.id(), self.endpoint_pair.public_endpoint.to_string(), message.kind);
            if DiscoverPeerMessage::ENCRYPTION_REQUIRED && !was_encrypted { return Ok(()) }
            message.set_sender(senders.drain().next().unwrap());
            self.to_dpp.send(message).map_err(|e| { e.to_string() } )
        }
        else if let Ok(mut message) = bincode::deserialize::<Heartbeat>(message_bytes) {
            if Heartbeat::ENCRYPTION_REQUIRED && !was_encrypted { return Ok(()) }
            message.set_sender(senders.drain().next().unwrap());
            Ok(println!("{:?}", message))
        }
        else if let Ok(mut message) = bincode::deserialize::<StreamMessage>(message_bytes) {
            // if StreamMessage::ENCRYPTION_REQUIRED && !was_encrypted { return Ok(()) }
            for sender in senders {
                message.set_sender(sender);
            }
            match message {
                StreamMessage { kind: StreamMessageKind::KeyAgreement, .. } => { println!("Received key agreement message, uuid: {} at {}", message.id(), self.endpoint_pair.public_endpoint.to_string()); self.handle_key_agreement(message) },
                _ => { println!("Received stream message, uuid: {} at {}", message.id(), self.endpoint_pair.public_endpoint.to_string()); self.to_smp.send(message).map_err(|e| { e.to_string() } ) }
            }
        }
        else {
            Err(String::from("Unable to deserialize received message to a supported type"))
        }
    }

    fn stage_message(&mut self, mut payload: Vec<u8>, is_encrypted: IsEncrypted, separate_parts: SeparateParts) -> Option<Vec<StagedMessage>> {
        let (sender, uuid, position) = separate_parts.into_parts();
        if let IsEncrypted::True(nonce) = is_encrypted {
            let crypto_result = self.key_store.lock().unwrap().transform(sender, &mut payload, Direction::Decode(nonce.clone()));
            match crypto_result {
                Ok(plaintext) => { payload = plaintext },
                Err(Error::NoKey) => {
                    let cached_messages = self.cached_messages.entry(uuid).or_default();
                    cached_messages.push(StagedMessage::new(payload, IsEncrypted::True(nonce), sender, position));
                    println!("no key");
                    return None;
                },
                Err(e) => { println!("{e}"); return None }
            };
        }
        let (index, num_chunks) = position;
        if num_chunks == 1 {
            // gateway::log_debug(&format!("Hash on the way back: {}", message.message_ext().hash()));
            Some(vec![StagedMessage::new(payload, IsEncrypted::False, sender, position)])
        }
        else {
            let staged_messages_len = {
                let staged_messages= self.message_staging.entry(uuid.clone()).or_insert(HashMap::with_capacity(num_chunks));
                staged_messages.insert(index, StagedMessage::new(payload, IsEncrypted::False, sender, position));
                staged_messages.len()
            };
            if staged_messages_len == 1 {
                self.set_staging_ttl(&uuid, SRP_TTL);
            }
            if !self.check_staging_ttl(&uuid) {
                self.message_staging.remove(&uuid);
                return None;
            }
            if staged_messages_len == num_chunks {
                // gateway::log_debug("Collected all messages");
                let messages: Vec<StagedMessage> = self.message_staging.remove(&uuid).unwrap().into_values().collect();
                return Some(messages);
            }
            None
        }
    }

    fn set_staging_ttl(&mut self, id: &str, ttl_secs: u64) {
        let (tx, rx) = oneshot::channel();
        self.staging_ttls.insert(id.to_owned(), rx);
        tokio::spawn(async move {
            sleep(Duration::from_millis(ttl_secs)).await;
            tx.send(()).ok()
        });
    }

    fn check_staging_ttl(&mut self, id: &str) -> bool {
        let Some(rx) = self.staging_ttls.get_mut(id) else { return false };
        if let Ok(_) = rx.try_recv() {
            // gateway::log_debug("Ttl for message staging expired");
            self.staging_ttls.remove(id);
            false
        }
        else {
            true
        }
    }
}

pub struct StagedMessage {
    payload: Vec<u8>,
    is_encrypted: IsEncrypted,
    sender: SocketAddrV4,
    position: (usize, usize)
}

impl StagedMessage {
    pub fn new(payload: Vec<u8>, is_encrypted: IsEncrypted, sender: SocketAddrV4, position: (usize, usize)) -> Self { Self { payload, is_encrypted, sender, position } }
    pub fn into_parts(self) -> (Vec<u8>, IsEncrypted, SocketAddrV4, (usize, usize)) { (self.payload, self.is_encrypted, self.sender, self.position) }

    pub fn reassemble_message(mut messages: Vec<Self>) -> (Vec<u8>, HashSet<SocketAddrV4>) {
        messages.sort_by(|a, b| a.position.0.cmp(&b.position.0));
        let (bytes, senders): (Vec<Vec<u8>>, Vec<SocketAddrV4>) = messages
            .into_iter()
            .map(|m| { let parts = m.into_parts(); (parts.0, parts.2) })
            .unzip();
        (bytes.concat(), HashSet::from_iter(senders.into_iter()))
    }
}