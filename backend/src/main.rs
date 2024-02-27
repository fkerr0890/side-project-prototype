use p2p::node::Node;
use tokio::fs;
use uuid::Uuid;
use std::{env, panic, process};

#[tokio::main]
async fn main() {
    // let socket = UdpSocket::bind("192.168.0.104:0").await.unwrap();
    // socket.send_to(b"hello", String::from("192.168.0.104:") + &socket.local_addr().unwrap().port().to_string()).await.unwrap();
    // let mut buf = [0u8; 1024];
    // socket.recv(&mut buf).await.unwrap();
    // println!("{:?}", buf);
    // return;
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
    Node::new().listen(is_start, !is_start, None, None, Uuid::new_v4().simple().to_string(), vec![(peer_ip + ":" + &peer_port, String::from("unimplemented"))], endpoint_pair, socket).await
}