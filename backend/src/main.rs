use p2p::{node::Node, gateway, message_processing::TTL};
use rand::Rng;
use rand::seq::IteratorRandom;
use tokio::{time::sleep, fs};
use uuid::Uuid;
use std::collections::HashSet;
use std::{panic, process, time::Duration};
use std::future;

#[tokio::main]
async fn main() {
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
        gateway::log_debug(&panic_info.to_string());
        process::exit(1);
    }));

    fs::remove_dir_all("../peer_info").await.unwrap();
    fs::create_dir("../peer_info").await.unwrap();

    let mut introducers = Vec::new();
    let num_hosts = 10;
    let num_nodes: u16 = 100;
    let mut rng = rand::thread_rng();
    let mut indices = (0..num_nodes).choose_multiple(&mut rng, num_hosts + 1);
    let start = indices.pop().unwrap();
    let host_indices = HashSet::<u16>::from_iter(indices.into_iter());
    for i in 0..num_nodes {
        let introducer = if introducers.len() > 0 { Some(introducers.get(rand::thread_rng().gen_range(0..introducers.len())).unwrap()) } else { None };
        let node = Node::new("127.0.0.1:0", Uuid::new_v4(), introducer).await;
        introducers.push(node.endpoint_pair());
        let is_end = host_indices.contains(&i);
        let is_start = start == i;
        tokio::spawn(async move { node.listen(is_start, is_end).await });
        sleep(Duration::from_millis(TTL*6)).await;
        println!();
    }
    future::pending::<()>().await;
}