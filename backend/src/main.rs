use p2p::{message::NumId, node::Node};
use tokio::fs;
use uuid::Uuid;
use std::{env, panic, process};

#[tokio::main]
async fn main() {
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
        println!("{}", panic_info);
        process::exit(1);
    }));

    fs::remove_dir_all("../peer_info").await.unwrap();
    fs::create_dir("../peer_info").await.unwrap();

    let args: Vec<String> = env::args().collect();
    let (private_ip, private_port, public_ip, peer_ip, peer_port, is_start) = (args[1].clone(), args[2].clone(), args[3].clone(), args[4].clone(), args[5].clone(), args[6].parse::<bool>().unwrap());
    let (endpoint_pair, socket) = Node::get_socket(private_ip, private_port, &public_ip).await;
    Node::new().listen(is_start, !is_start, None, None, NumId(Uuid::new_v4().as_u128()), vec![(peer_ip + ":" + &peer_port, NumId(Uuid::new_v4().as_u128()))], endpoint_pair, socket).await
}