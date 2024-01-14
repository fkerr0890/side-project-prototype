use std::{collections::HashMap, net::SocketAddrV4};

use ring::{aead::{self, AES_256_GCM}, rand::SystemRandom, agreement, hkdf::{self, HKDF_SHA256, KeyType}};

const INITIAL_SALT: [u8; 20] = [
    0xc3, 0xee, 0xf7, 0x12, 0xc7, 0x2e, 0xbb, 0x5a, 0x11, 0xa7, 0xd2, 0x43, 0x2b, 0xb4, 0x63, 0x65,
    0xbe, 0xf9, 0xf5, 0x02,
    ];
pub struct KeyStore {
    symmetric_keys: HashMap<String, KeySet>,
    rng: SystemRandom
}
impl KeyStore {
    fn get_key_aad(&self, session_id: &str) -> Result<(aead::Aad<Vec<u8>>, &KeySet), Error> {
        Ok((aead::Aad::from(session_id.into()), self.symmetric_keys.get(session_id).ok_or(Error::NoKey)?))
    }

    pub fn transform(&self, session_id: &str, mut payload: Vec<u8>, mode: Direction) -> Result<Vec<u8>, Error> {
        let (aad, key_set) = self.get_key_aad(session_id)?;
        match mode {
            Direction::Encode => { key_set.key.seal_in_place_append_tag(key_set.nonce, aad, &mut payload).unwrap(); Ok(payload) },
            Direction::Decode => key_set.key.open_in_place(key_set.nonce, aad, &mut payload).map(|payload| payload.to_owned()).or(Err(Error::Unspecified))
        }
    }

    pub fn agree(&self, session_id: String, peer_public_key: Vec<u8>) -> Result<(), Error> {
        let peer_public_key = agreement::UnparsedPublicKey::new(&agreement::X25519, peer_public_key);
        let my_private_key = agreement::EphemeralPrivateKey::generate(&agreement::X25519, &self.rng).unwrap();
        let symmetric_key = agreement::agree_ephemeral(my_private_key, &peer_public_key, |key_material| {
            hkdf::Salt::new(HKDF_SHA256, &INITIAL_SALT).extract(key_material)
        }).or(Err(Error::Unspecified))?;
        let mut symmetric_key_bytes = vec![0u8; HKDF_SHA256.len()];
        let mut nonce = vec![0u8; HKDF_SHA256.len()];
        symmetric_key.expand(&[b"sym"], HKDF_SHA256).unwrap().fill(&mut symmetric_key_bytes).unwrap();
        symmetric_key.expand(&[b"nonce"], HKDF_SHA256).unwrap().fill(&mut nonce).unwrap();
        let symmetric_key = aead::LessSafeKey::new(aead::UnboundKey::new(&AES_256_GCM, &symmetric_key_bytes).unwrap());
        nonce.truncate(aead::NONCE_LEN);
        let nonce = aead::Nonce::try_assume_unique_for_key(&nonce).unwrap();
        let key_set = KeySet::new(nonce, symmetric_key, AES_256_GCM);
        self.symmetric_keys.insert(session_id, key_set);
        Ok(())
    }
}

pub struct KeySet {
    nonce: aead::Nonce,
    key: aead::LessSafeKey,
    algorithm: aead::Algorithm
}

impl KeySet {
    pub fn new(nonce: aead::Nonce, key: aead::LessSafeKey, algorithm: aead::Algorithm) -> Self { Self { nonce, key, algorithm } }
}

pub enum Error {
    NoKey,
    Unspecified
}

pub enum Direction {
    Encode,
    Decode
}