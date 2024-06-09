use std::fmt::Display;
use rustc_hash::FxHashMap;

use ring::{aead, agreement, digest, hkdf, rand::{SecureRandom, SystemRandom}};

use crate::{event::{TimeboundAction, TimeboundEventManager}, message::NumId, message_processing::{ACTIVE_SESSION_TTL_SECONDS, SRP_TTL_SECONDS}, option_early_return};

const INITIAL_SALT: [u8; 20] = [
    0xc3, 0xee, 0xf7, 0x12, 0xc7, 0x2e, 0xbb, 0x5a, 0x11, 0xa7, 0xd2, 0x43, 0x2b, 0xb4, 0x63, 0x65,
    0xbe, 0xf9, 0xf5, 0x02,
    ];
pub struct KeyStore {
    private_keys: FxHashMap<NumId, agreement::EphemeralPrivateKey>,
    symmetric_keys: FxHashMap<NumId, aead::LessSafeKey>,
    rng: SystemRandom
}
impl Default for KeyStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyStore {
    pub fn new() -> Self {
        let rng = SystemRandom::new();
        let mut bytes = vec![0u8; aead::NONCE_LEN];
        rng.fill(&mut bytes).ok();
        Self {
            private_keys: FxHashMap::default(),
            symmetric_keys: FxHashMap::default(),
            rng
        }
    }

    pub fn public_key(&mut self, peer_id: NumId, event_manager: &mut TimeboundEventManager) -> Option<Vec<u8>> {
        if self.symmetric_keys.contains_key(&peer_id) {
            return None
        }
        if !self.private_keys.contains_key(&peer_id) {
            let my_private_key = agreement::EphemeralPrivateKey::generate(&agreement::X25519, &self.rng).unwrap();
            let public_key = my_private_key.compute_public_key().unwrap();
            event_manager.put_event(TimeboundAction::RemovePrivateKey(peer_id), SRP_TTL_SECONDS);
            self.private_keys.insert(peer_id, my_private_key);
            Some(public_key.as_ref().to_vec())
        }
        else {
            Some(self.private_keys.get(&peer_id).unwrap().compute_public_key().unwrap().as_ref().to_vec())
        }
    }

    pub fn transform<'a>(&'a mut self, peer_id: NumId, payload: &'a mut Vec<u8>, mode: Direction) -> Result<Vec<u8>, Error> {
        let (aad, key) = (aead::Aad::from("test".as_bytes().to_vec()), self.symmetric_keys.get(&peer_id).ok_or(Error::NoKey)?);
        match mode {
            Direction::Encode => {
                let nonce_bytes = self.random_nonce_bytes()?;
                let nonce = aead::Nonce::try_assume_unique_for_key(&nonce_bytes)?;
                key.seal_in_place_append_tag(nonce, aad, payload)?;
                Ok(nonce_bytes)
            },
            Direction::Decode(nonce_bytes) => {
                let nonce = aead::Nonce::try_assume_unique_for_key(nonce_bytes)?;
                let res = key.open_in_place(nonce, aad, payload)?.to_vec();
                Ok(res)
            }
        }
    }

    pub fn agree(&mut self, peer_id: NumId, peer_public_key: Vec<u8>, event_manager: &mut TimeboundEventManager) -> Result<(), Error> {
        if self.symmetric_keys.contains_key(&peer_id) {
            return Ok(())
        }
        assert!(!peer_public_key.is_empty());
        let my_private_key = option_early_return!(self.private_keys.remove(&peer_id), Ok(()));
        let peer_public_key = agreement::UnparsedPublicKey::new(&agreement::X25519, peer_public_key);
        let symmetric_key = agreement::agree_ephemeral(my_private_key, &peer_public_key, |key_material| {
            hkdf::Salt::new(hkdf::HKDF_SHA256, &INITIAL_SALT).extract(key_material)
        })?;
        let mut symmetric_key_bytes = vec![0u8; hkdf::KeyType::len(&hkdf::HKDF_SHA256)];
        symmetric_key.expand(&[b"sym"], hkdf::HKDF_SHA256).unwrap().fill(&mut symmetric_key_bytes).unwrap();
        let symmetric_key = aead::LessSafeKey::new(aead::UnboundKey::new(&aead::AES_256_GCM, &symmetric_key_bytes).unwrap());
        event_manager.put_event(TimeboundAction::RemoveSymmetricKey(peer_id), ACTIVE_SESSION_TTL_SECONDS);
        self.symmetric_keys.insert(peer_id, symmetric_key);
        Ok(())
    }

    pub fn agreement_exists(&self, peer_id: &NumId) -> bool {
        self.symmetric_keys.contains_key(peer_id)
    }

    fn random_nonce_bytes(&self) -> Result<Vec<u8>, ring::error::Unspecified> {
        let mut bytes = vec![0u8; aead::NONCE_LEN];
        self.rng.fill(&mut bytes)?;
        Ok(bytes)
    }

    pub fn clear(&mut self) {
        self.symmetric_keys.clear();
        self.private_keys.clear();
    }

    pub fn remove_symmetric_key(&mut self, peer_id: &NumId) { self.symmetric_keys.remove(peer_id); }
    pub fn remove_private_key(&mut self, peer_id: &NumId) { self.private_keys.remove(peer_id); }
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

pub enum Direction<'a> {
    Encode,
    Decode(&'a[u8])
}
