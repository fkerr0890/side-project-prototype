use std::{net::SocketAddrV4, sync::mpsc, fmt::Display};

use ring::{aead::{self, BoundKey, AES_256_GCM}, agreement, digest, hkdf::{self, KeyType, HKDF_SHA256}, rand::SystemRandom};

use crate::{message_processing::ACTIVE_SESSION_TTL_SECONDS, utils::{TransientMap, TtlType}};

const INITIAL_SALT: [u8; 20] = [
    0xc3, 0xee, 0xf7, 0x12, 0xc7, 0x2e, 0xbb, 0x5a, 0x11, 0xa7, 0xd2, 0x43, 0x2b, 0xb4, 0x63, 0x65,
    0xbe, 0xf9, 0xf5, 0x02,
    ];
pub struct KeyStore {
    private_keys: TransientMap<String, agreement::EphemeralPrivateKey>,
    symmetric_keys: TransientMap<String, KeySet>,
    rng: SystemRandom
}
impl KeyStore {
    pub fn new() -> Self {
        Self {
            private_keys: TransientMap::new(TtlType::Secs(ACTIVE_SESSION_TTL_SECONDS)),
            symmetric_keys: TransientMap::new(TtlType::Secs(ACTIVE_SESSION_TTL_SECONDS)),
            rng: SystemRandom::new()
        }
    }

    fn generate_key_pair(&mut self, index: String) -> agreement::PublicKey {
        let is_new_key = self.private_keys.set_timer(index.clone());
        let mut private_keys = self.private_keys.map().lock().unwrap();
        if is_new_key {
            let my_private_key = agreement::EphemeralPrivateKey::generate(&agreement::X25519, &self.rng).unwrap();
            let public_key = my_private_key.compute_public_key().unwrap();
            private_keys.insert(index, my_private_key);
            public_key
        }
        else {
            private_keys.get(&index).unwrap().compute_public_key().unwrap()
        }
    }

    pub fn host_public_key(&mut self, peer_addr: SocketAddrV4) -> agreement::PublicKey {
        self.generate_key_pair(peer_addr.to_string())
    }

    pub fn requester_public_key(&mut self, peer_addr: SocketAddrV4) -> agreement::PublicKey {
        self.generate_key_pair(peer_addr.to_string())
    }

    pub fn transform<'a>(&'a mut self, peer_addr: SocketAddrV4, payload: &'a mut Vec<u8>, mode: Direction) -> Result<Vec<u8>, Error> {
        let mut symmetric_keys = self.symmetric_keys.map().lock().unwrap();
        let (aad, key_set) = (aead::Aad::from("test".as_bytes().to_vec()), symmetric_keys.get_mut(&peer_addr.to_string()).ok_or(Error::NoKey)?);
        match mode {
            Direction::Encode => {
                key_set.sealing_key.seal_in_place_append_tag(aad, payload)?;
                key_set.nonce_rx.recv().and_then(|nonce| Ok(nonce.as_ref().to_vec())).or_else(|e| Err(Error::Generic(e.to_string())))
            },
            Direction::Decode(nonce_bytes) => {
                let nonce = aead::Nonce::try_assume_unique_for_key(&nonce_bytes)?;
                let res = key_set.opening_key.open_in_place(nonce, aad, payload)?.to_vec();
                Ok(res)
            }
        }
    }

    pub fn agree(&mut self, peer_addr: SocketAddrV4, peer_public_key: Vec<u8>) -> Result<(), Error> {
        // println!("{:?}, finding: {}", self.private_keys, peer_addr.to_string());
        let index = peer_addr.to_string();
        let Some(my_private_key) = self.private_keys.map().lock().unwrap().remove(&index) else { return Ok(()) };
        let peer_public_key = agreement::UnparsedPublicKey::new(&agreement::X25519, peer_public_key);
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
        if !self.symmetric_keys.set_timer(index.clone()) {
            return Ok(())
        }
        self.symmetric_keys.map().lock().unwrap().insert(index, key_set);
        Ok(())
    }
}

#[derive(Debug)]
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

pub fn digest_parts(parts: Vec<&[u8]>) -> Vec<u8> {
    let mut ctx = digest::Context::new(&digest::SHA1_FOR_LEGACY_USE_ONLY);
    for part in parts {
        ctx.update(part);
    }
    ctx.finish().as_ref().to_vec()
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