use std::{collections::HashMap, net::SocketAddrV4, time::Duration};

use chrono::Utc;
use ring::{aead::{self, AES_256_GCM, BoundKey}, rand::SystemRandom, agreement, hkdf::{self, HKDF_SHA256, KeyType}};
use tokio::{sync::{oneshot, mpsc}, time::sleep};

use crate::message::{Heartbeat, self};

const INITIAL_SALT: [u8; 20] = [
    0xc3, 0xee, 0xf7, 0x12, 0xc7, 0x2e, 0xbb, 0x5a, 0x11, 0xa7, 0xd2, 0x43, 0x2b, 0xb4, 0x63, 0x65,
    0xbe, 0xf9, 0xf5, 0x02,
    ];
pub struct KeyStore {
    private_keys: HashMap<SocketAddrV4, (agreement::EphemeralPrivateKey, Option<oneshot::Sender<()>>)>,
    symmetric_keys: HashMap<SocketAddrV4, KeySet>,
    rng: SystemRandom
}
impl KeyStore {
    pub fn new() -> Self {
        Self {
            private_keys: HashMap::new(),
            symmetric_keys: HashMap::new(),
            rng: SystemRandom::new()
        }
    }

    fn get_key_aad(&mut self, peer_addr: SocketAddrV4) -> Result<(aead::Aad<Vec<u8>>, &mut KeySet), Error> {
        Ok((aead::Aad::from(peer_addr.ip().octets().to_vec()), self.symmetric_keys.get_mut(&peer_addr).ok_or(Error::NoKey)?))
    }

    fn generate_key_pair(&mut self, peer_addr: SocketAddrV4, tx: Option<oneshot::Sender<()>>) -> agreement::PublicKey {
        let my_private_key = agreement::EphemeralPrivateKey::generate(&agreement::X25519, &self.rng).unwrap();
        let public_key = my_private_key.compute_public_key().unwrap();
        self.private_keys.insert(peer_addr, (my_private_key, tx));
        public_key
    }

    pub fn host_public_key(&mut self, peer_addr: SocketAddrV4, egress: mpsc::UnboundedSender<Heartbeat>, sender: SocketAddrV4) -> agreement::PublicKey {
        let (tx, rx) = oneshot::channel();
        // send_rapid_heartbeats(rx, egress, sender, peer_addr);
        self.generate_key_pair(peer_addr, Some(tx))
    }

    pub fn requester_public_key(&mut self, peer_addr: SocketAddrV4) -> agreement::PublicKey {
        self.generate_key_pair(peer_addr, None)
    }

    pub fn transform<'a>(&'a mut self, peer_addr: SocketAddrV4, payload: &'a mut Vec<u8>, mode: Direction) -> Result<(), Error> {
        let (aad, key_set) = self.get_key_aad(peer_addr)?;
        match mode {
            Direction::Encode => key_set.sealing_key.seal_in_place_append_tag(aad, payload).map(|_| ()).or(Err(Error::Unspecified)),
            Direction::Decode => key_set.opening_key.open_in_place(aad, payload).map(|_| ()).or(Err(Error::Unspecified))
        }
    }

    pub fn agree(&mut self, peer_addr: SocketAddrV4, peer_public_key: Vec<u8>) -> Result<(), Error> {
        let peer_public_key = agreement::UnparsedPublicKey::new(&agreement::X25519, peer_public_key);
        let Some((my_private_key, stop_heartbeats)) = self.private_keys.remove(&peer_addr) else { return Err(Error::NoKey) };
        if let Some(tx) = stop_heartbeats {
            tx.send(()).ok();
        }
        let symmetric_key = agreement::agree_ephemeral(my_private_key, &peer_public_key, |key_material| {
            hkdf::Salt::new(HKDF_SHA256, &INITIAL_SALT).extract(key_material)
        }).or(Err(Error::Unspecified))?;
        let mut symmetric_key_bytes = vec![0u8; HKDF_SHA256.len()];
        let mut initial_nonce = vec![0u8; HKDF_SHA256.len()];
        symmetric_key.expand(&[b"sym"], HKDF_SHA256).unwrap().fill(&mut symmetric_key_bytes).unwrap();
        symmetric_key.expand(&[b"nonce_0"], HKDF_SHA256).unwrap().fill(&mut initial_nonce).unwrap();
        initial_nonce.truncate(16);
        let initial_value = u128::from_be_bytes(initial_nonce.try_into().unwrap());
        let opening_key = aead::OpeningKey::new(aead::UnboundKey::new(&AES_256_GCM, &symmetric_key_bytes).unwrap(), CurrentNonce(initial_value));
        let sealing_key = aead::SealingKey::new(aead::UnboundKey::new(&AES_256_GCM, &symmetric_key_bytes).unwrap(), CurrentNonce(initial_value));
        let key_set = KeySet { opening_key, sealing_key };
        self.symmetric_keys.insert(peer_addr, key_set);
        Ok(())
    }
}

struct KeySet { opening_key: aead::OpeningKey<CurrentNonce>, sealing_key: aead::SealingKey<CurrentNonce> }

struct CurrentNonce(u128);
impl aead::NonceSequence for CurrentNonce {
    fn advance(&mut self) -> Result<aead::Nonce, ring::error::Unspecified> {
        (self.0, _) = self.0.overflowing_add(1);
        let bytes = self.0.to_be_bytes();
        aead::Nonce::try_assume_unique_for_key(&bytes[bytes.len() - aead::NONCE_LEN..])
    }
}

#[derive(Debug)]
pub enum Error {
    NoKey,
    Unspecified
}

pub enum Direction {
    Encode,
    Decode
}

fn send_rapid_heartbeats(mut rx: oneshot::Receiver<()>, egress: mpsc::UnboundedSender<Heartbeat>, sender: SocketAddrV4, dest: SocketAddrV4) {
    println!("send_rapid_heartbeats called: dest: {dest}, sender: {sender}");
    tokio::spawn(async move {
        while let Err(_) = rx.try_recv() {
            let message = Heartbeat::new(dest, sender, message::datetime_to_timestamp(Utc::now()));
            println!("Sending heartbeat {:?}", message);
            egress.send(message).ok();
            sleep(Duration::from_millis(500)).await;
        }
    });
}