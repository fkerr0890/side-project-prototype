use ring::aead::{UnboundKey, AES_256_GCM, LessSafeKey, NONCE_LEN, Aad, Nonce};
use ring::rand::{SystemRandom, SecureRandom};

fn main() {
    let mut secret = vec!(0u8; AES_256_GCM.key_len());
    let mut nonce_bytes = vec!(0u8; NONCE_LEN);
    // let mut nonce_bytes2 = vec!(0u8; NONCE_LEN);
    let rng = SystemRandom::new();
    rng.fill(&mut secret).unwrap();
    rng.fill(&mut nonce_bytes).unwrap();
    // rng.fill(&mut nonce_bytes2).unwrap();
    let nonce =  Nonce::try_assume_unique_for_key(&nonce_bytes).unwrap();
    // let nonce2 = Nonce::try_assume_unique_for_key(&nonce_bytes2).unwrap();
    let key = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, &secret).unwrap());
    let mut payload = Vec::from(String::from("Test"));
    let aad = Aad::from(b"test_aad");
    println!("{:?}", secret);
    println!("{:?}", aad);
    println!("{:?}", payload);
    key.seal_in_place_append_tag(Nonce::try_assume_unique_for_key(&nonce_bytes).unwrap(), aad, &mut payload).unwrap();
    println!("{:?}", secret);
    println!("{:?}", aad);
    println!("{:?}", payload);
    let key2 = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, &secret).unwrap());
    let decrypted = key2.open_in_place(nonce, aad, &mut payload).unwrap();
    println!("{}", String::from_utf8(decrypted.to_vec()).unwrap())
}
