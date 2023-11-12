use p2p::{node::Node, gateway, message_processing::TTL};
use rand::Rng;
use tokio::time::sleep;
use uuid::Uuid;
use std::{panic, process, time::Duration};

#[tokio::main]
async fn main() {
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
        gateway::log_debug(&panic_info.to_string());
        process::exit(1);
    }));

    let mut introducers = Vec::new();
    for _ in 0..100 {
        let introducer = if introducers.len() > 0 { Some(introducers.get(rand::thread_rng().gen_range(0..introducers.len())).unwrap()) } else { None };
        let node = Node::new("127.0.0.1:0", Uuid::new_v4(), introducer).await;
        introducers.push(node.endpoint_pair());
        tokio::spawn(async move { node.listen().await });
        sleep(Duration::from_millis(TTL*3)).await;
        println!()
    }
    loop {}
}