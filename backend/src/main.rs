use p2p::{node::Node, gateway, message_processing::TTL};
use rand::Rng;
use tokio::{time::sleep, fs};
use uuid::Uuid;
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
    let mut start_chosen = false;
    let mut num_hosts = 0;
    let num_nodes: u16 = 50;
    for _ in 0..num_nodes {
        let introducer = if introducers.len() > 0 { Some(introducers.get(rand::thread_rng().gen_range(0..introducers.len())).unwrap()) } else { None };
        let node = Node::new("127.0.0.1:0", Uuid::new_v4(), introducer).await;
        introducers.push(node.endpoint_pair());
        let is_end = num_hosts < 10 && rand::thread_rng().gen_range(0..(num_nodes/5+1 as u16)) == num_nodes/10 as u16;
        if is_end {
            num_hosts += 1;
        }
        let is_start = !is_end && !start_chosen && rand::thread_rng().gen_range(0..(num_nodes/2+1 as u16)) == num_nodes/4 as u16;
        start_chosen = start_chosen || is_start;
        tokio::spawn(async move { node.listen(is_start, is_end).await });
        sleep(Duration::from_millis(TTL*3)).await;
        println!()
    }
    println!("{}, {}", start_chosen, num_hosts);
    future::pending::<()>().await;
}