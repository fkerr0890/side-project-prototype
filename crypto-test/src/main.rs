use ring::aead::{UnboundKey, AES_256_GCM, LessSafeKey, NONCE_LEN, Aad, Nonce};
use ring::hkdf::{Salt, HKDF_SHA256, KeyType};
use ring::rand::{SystemRandom, SecureRandom};

fn main() {
    const INITIAL_SALT: [u8; 20] = [
    0xc3, 0xee, 0xf7, 0x12, 0xc7, 0x2e, 0xbb, 0x5a, 0x11, 0xa7, 0xd2, 0x43, 0x2b, 0xb4, 0x63, 0x65,
    0xbe, 0xf9, 0xf5, 0x02,
    ];
    let mut secret = vec!(0u8; 6);
    let rng = SystemRandom::new();
    rng.fill(&mut secret).unwrap();
    let prk = Salt::new(HKDF_SHA256, &INITIAL_SALT).extract(&secret);
    let prk2 = Salt::new(HKDF_SHA256, &INITIAL_SALT).extract(&secret);
    let mut nonce_bytes = vec!(0u8; HKDF_SHA256.len());
    let mut nonce_bytes2 = vec!(0u8; HKDF_SHA256.len());
    prk.expand(&[b"Testinfo"], HKDF_SHA256).unwrap().fill(&mut nonce_bytes).unwrap();
    prk2.expand(&[b"Testinfo"], HKDF_SHA256).unwrap().fill(&mut nonce_bytes2).unwrap();
    nonce_bytes.truncate(NONCE_LEN);
    nonce_bytes2.truncate(NONCE_LEN);
    println!("{:?}", secret);
    println!("{:?}", nonce_bytes);
    println!("{:?}", nonce_bytes2);
    // let mut nonce_bytes = vec!(0u8; NONCE_LEN);
    // // let mut nonce_bytes2 = vec!(0u8; NONCE_LEN);
    // rng.fill(&mut nonce_bytes).unwrap();
    // // rng.fill(&mut nonce_bytes2).unwrap();
    // let nonce =  Nonce::try_assume_unique_for_key(&nonce_bytes).unwrap();
    // // let nonce2 = Nonce::try_assume_unique_for_key(&nonce_bytes2).unwrap();
    // let key = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, &secret).unwrap());
    // let mut payload = Vec::from(String::from("Test"));
    // let aad = Aad::from(b"test_aad");
    // println!("{:?}", aad);
    // println!("{:?}", payload);
    // key.seal_in_place_append_tag(Nonce::try_assume_unique_for_key(&nonce_bytes).unwrap(), aad, &mut payload).unwrap();
    // println!("{:?}", secret);
    // println!("{:?}", aad);
    // println!("{:?}", payload);
    // let key2 = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, &secret).unwrap());
    // let decrypted = key2.open_in_place(nonce, aad, &mut payload).unwrap();
    // println!("{}", String::from_utf8(decrypted.to_vec()).unwrap())
}
