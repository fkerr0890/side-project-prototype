use std::fmt::Display;

use ring::{aead, agreement, digest, hkdf, rand::{SecureRandom, SystemRandom}};

use crate::{lock, message::NumId, message_processing::{ACTIVE_SESSION_TTL_SECONDS, SRP_TTL_SECONDS}, utils::{ArcCollection, ArcMap, TimerOptions, TransientCollection}};

const INITIAL_SALT: [u8; 20] = [
    0xc3, 0xee, 0xf7, 0x12, 0xc7, 0x2e, 0xbb, 0x5a, 0x11, 0xa7, 0xd2, 0x43, 0x2b, 0xb4, 0x63, 0x65,
    0xbe, 0xf9, 0xf5, 0x02,
    ];
pub struct KeyStore {
    private_keys: TransientCollection<ArcMap<NumId, agreement::EphemeralPrivateKey>>,
    symmetric_keys: TransientCollection<ArcMap<NumId, aead::LessSafeKey>>,
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
            private_keys: TransientCollection::new(SRP_TTL_SECONDS, false, ArcMap::new()),
            symmetric_keys: TransientCollection::new(ACTIVE_SESSION_TTL_SECONDS, true, ArcMap::new()),
            rng
        }
    }

    pub fn public_key(&mut self, peer_id: NumId) -> Option<Vec<u8>> {
        if self.symmetric_keys.contains_key(&peer_id) {
            return None
        }
        let is_new_key = self.private_keys.set_timer(peer_id, TimerOptions::new(), "Crypto:PrivateKeys");
        let mut private_keys = lock!(self.private_keys.collection().map());
        if is_new_key {
            let my_private_key = agreement::EphemeralPrivateKey::generate(&agreement::X25519, &self.rng).unwrap();
            let public_key = my_private_key.compute_public_key().unwrap();
            private_keys.insert(peer_id, my_private_key);
            Some(public_key.as_ref().to_vec())
        }
        else {
            Some(private_keys.get(&peer_id).unwrap().compute_public_key().unwrap().as_ref().to_vec())
        }
    }

    pub fn transform<'a>(&'a mut self, peer_id: NumId, payload: &'a mut Vec<u8>, mode: Direction) -> Result<Vec<u8>, Error> {
        self.symmetric_keys.set_timer(peer_id, TimerOptions::new(), "Crypto:SymmetricKeysRenew");
        let mut symmetric_keys = lock!(self.symmetric_keys.collection().map());
        let (aad, key) = (aead::Aad::from("test".as_bytes().to_vec()), symmetric_keys.get_mut(&peer_id).ok_or(Error::NoKey)?);
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

    pub fn agree(&mut self, peer_id: NumId, peer_public_key: Vec<u8>) -> Result<(), Error> {
        if !self.symmetric_keys.set_timer(peer_id, TimerOptions::new(), "Crypto:SymmetricKeys") {
            return Ok(())
        }
        assert!(!peer_public_key.is_empty());
        let Some(my_private_key) = self.private_keys.pop(&peer_id) else { return Ok(()) };
        let peer_public_key = agreement::UnparsedPublicKey::new(&agreement::X25519, peer_public_key);
        let symmetric_key = agreement::agree_ephemeral(my_private_key, &peer_public_key, |key_material| {
            hkdf::Salt::new(hkdf::HKDF_SHA256, &INITIAL_SALT).extract(key_material)
        })?;
        let mut symmetric_key_bytes = vec![0u8; hkdf::KeyType::len(&hkdf::HKDF_SHA256)];
        symmetric_key.expand(&[b"sym"], hkdf::HKDF_SHA256).unwrap().fill(&mut symmetric_key_bytes).unwrap();
        let symmetric_key = aead::LessSafeKey::new(aead::UnboundKey::new(&aead::AES_256_GCM, &symmetric_key_bytes).unwrap());
        lock!(self.symmetric_keys.collection().map()).insert(peer_id, symmetric_key);
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
        format!("{} {} {}", self, file, line)
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

pub enum Direction<'a> {
    Encode,
    Decode(&'a[u8])
}