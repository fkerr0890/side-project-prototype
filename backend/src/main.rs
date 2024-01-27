use p2p::node::Node;
use tokio::fs;
use uuid::Uuid;
use std::{panic, process, env};

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
    let (my_ip, my_port, peer_ip, peer_port, is_start) = (args[1].clone(), args[2].clone(), args[3].clone(), args[4].clone(), args[5].parse::<bool>().unwrap());
    let node = Node::new(my_ip, my_port, Uuid::new_v4(), None, Some(vec![peer_ip + ":" + &peer_port])).await;
    node.listen(is_start, !is_start).await
}