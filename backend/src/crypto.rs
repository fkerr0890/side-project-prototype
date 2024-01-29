use std::{collections::HashMap, net::SocketAddrV4, time::Duration, sync::mpsc, fmt::Display};

use chrono::Utc;
use ring::{aead::{self, AES_256_GCM, BoundKey}, rand::SystemRandom, agreement, hkdf::{self, HKDF_SHA256, KeyType}};
use tokio::{sync::oneshot, time::sleep};

use crate::message::{Heartbeat, self};

const INITIAL_SALT: [u8; 20] = [
    0xc3, 0xee, 0xf7, 0x12, 0xc7, 0x2e, 0xbb, 0x5a, 0x11, 0xa7, 0xd2, 0x43, 0x2b, 0xb4, 0x63, 0x65,
    0xbe, 0xf9, 0xf5, 0x02,
    ];
pub struct KeyStore {
    private_keys: HashMap<String, (agreement::EphemeralPrivateKey, Option<oneshot::Sender<()>>)>,
    symmetric_keys: HashMap<String, KeySet>,
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

    fn get_key_aad(&mut self, index: &str) -> Result<(aead::Aad<Vec<u8>>, &mut KeySet), Error> {
        Ok((aead::Aad::from("test".as_bytes().to_vec()), self.symmetric_keys.get_mut(index).ok_or(Error::NoKey)?))
    }

    fn generate_key_pair(&mut self, index: String, tx: Option<oneshot::Sender<()>>) -> agreement::PublicKey {
        let my_private_key = agreement::EphemeralPrivateKey::generate(&agreement::X25519, &self.rng).unwrap();
        let public_key = my_private_key.compute_public_key().unwrap();
        // println!("Inserting private key: {index}");
        self.private_keys.insert(index, (my_private_key, tx));
        public_key
    }

    pub fn host_public_key(&mut self, peer_addr: SocketAddrV4, egress: tokio::sync::mpsc::UnboundedSender<Heartbeat>, sender: SocketAddrV4, host_name: &str) -> agreement::PublicKey {
        let (tx, rx) = oneshot::channel();
        send_rapid_heartbeats(rx, egress, sender, peer_addr);
        self.generate_key_pair(peer_addr.to_string() + host_name, Some(tx))
    }

    pub fn requester_public_key(&mut self, peer_addr: SocketAddrV4, host_name: &str) -> agreement::PublicKey {
        self.generate_key_pair(peer_addr.to_string() + host_name, None)
    }

    pub fn transform<'a>(&'a mut self, peer_addr: SocketAddrV4, payload: &'a mut Vec<u8>, mode: Direction) -> Result<Vec<u8>, Error> {
        let (aad, key_set) = self.get_key_aad(&peer_addr.to_string())?;
        match mode {
            Direction::Encode => {
                key_set.sealing_key.seal_in_place_append_tag(aad, payload)?;
                key_set.nonce_rx.recv().and_then(|nonce| Ok(nonce.as_ref().to_vec())).or_else(|e| Err(Error::Generic(e.to_string())))
            },
            Direction::Decode(nonce_bytes) => {
                let nonce = aead::Nonce::try_assume_unique_for_key(&nonce_bytes)?;
                key_set.opening_key.open_in_place(nonce, aad, payload)?;
                Ok(Vec::with_capacity(0))
            }
        }
    }

    pub fn agree(&mut self, peer_addr: SocketAddrV4, peer_public_key: Vec<u8>) -> Result<(), Error> {
        let peer_public_key = agreement::UnparsedPublicKey::new(&agreement::X25519, peer_public_key);
        let index = peer_addr.to_string();
        let Some((my_private_key, stop_heartbeats)) = self.private_keys.remove(&index) else { return Err(Error::NoKey) };
        if let Some(tx) = stop_heartbeats {
            tx.send(()).ok();
        }
        let symmetric_key = agreement::agree_ephemeral(my_private_key, &peer_public_key, |key_material| {
            hkdf::Salt::new(HKDF_SHA256, &INITIAL_SALT).extract(key_material)
        })?;
        let mut symmetric_key_bytes = vec![0u8; HKDF_SHA256.len()];
        let mut initial_nonce = vec![0u8; HKDF_SHA256.len()];
        symmetric_key.expand(&[b"sym"], HKDF_SHA256).unwrap().fill(&mut symmetric_key_bytes).unwrap();
        symmetric_key.expand(&[b"nonce_0"], HKDF_SHA256).unwrap().fill(&mut initial_nonce).unwrap();
        initial_nonce.truncate(16);
        let initial_value = u128::from_be_bytes(initial_nonce.try_into().unwrap());
        let opening_key = aead::LessSafeKey::new(aead::UnboundKey::new(&AES_256_GCM, &symmetric_key_bytes).unwrap());
        let (nonce_tx, nonce_rx) = mpsc::channel();
        let sealing_key = aead::SealingKey::new(aead::UnboundKey::new(&AES_256_GCM, &symmetric_key_bytes).unwrap(), CurrentNonce(initial_value, nonce_tx));
        let key_set = KeySet { opening_key, sealing_key, nonce_rx };
        // println!("Inserting symmetric key: {index}");
        self.symmetric_keys.insert(index, key_set);
        Ok(())
    }
}

struct KeySet { opening_key: aead::LessSafeKey, sealing_key: aead::SealingKey<CurrentNonce>, nonce_rx: mpsc::Receiver<aead::Nonce> }

struct CurrentNonce(u128, mpsc::Sender<aead::Nonce>);
impl aead::NonceSequence for CurrentNonce {
    fn advance(&mut self) -> Result<aead::Nonce, ring::error::Unspecified> {
        (self.0, _) = self.0.overflowing_add(1);
        let bytes = self.0.to_be_bytes();
        let nonce = aead::Nonce::try_assume_unique_for_key(&bytes[bytes.len() - aead::NONCE_LEN..])?;
        let nonce_copy = aead::Nonce::try_assume_unique_for_key(&bytes[bytes.len() - aead::NONCE_LEN..])?;
        self.1.send(nonce_copy).ok();
        Ok(nonce)
    }
}

#[derive(Debug)]
pub enum Error {
    NoKey,
    Unspecified,
    Generic(String)
}
impl Error {
    pub fn error_response(&self, file: &str, line: u32) -> String {
        format!("{} {} {}", self.to_string(), file, line)
    }
}
impl From<ring::error::Unspecified> for Error {
    fn from(_value: ring::error::Unspecified) -> Self {
        Error::Unspecified
    }
}
impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoKey => write!(f, "crypto: Failed to find key"),
            Self::Unspecified => write!(f, "crypto: Unspecified error"),
            Self::Generic(e) => write!(f, "crypto: {}", e)
        }
    }
}

pub enum Direction {
    Encode,
    Decode(Vec<u8>)
}

fn send_rapid_heartbeats(mut rx: oneshot::Receiver<()>, egress: tokio::sync::mpsc::UnboundedSender<Heartbeat>, sender: SocketAddrV4, dest: SocketAddrV4) {
    tokio::spawn(async move {
        while let Err(_) = rx.try_recv() {
            let message = Heartbeat::new(dest, sender, message::datetime_to_timestamp(Utc::now()));
            egress.send(message).ok();
            sleep(Duration::from_millis(500)).await;
        }
    });
}