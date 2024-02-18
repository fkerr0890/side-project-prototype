use std::{collections::HashSet, time::Duration, panic, process, future};

use p2p::{self, node::Node, message_processing::DPP_TTL};
use rand::{seq::IteratorRandom, Rng};
use tokio::{time::sleep, fs};
use uuid::Uuid;

#[tokio::test]
async fn basic() {
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
        println!("{}", panic_info);
        process::exit(1);
    }));
    
    fs::remove_dir_all("../peer_info").await.unwrap();
    fs::create_dir("../peer_info").await.unwrap();

    let mut introducers = Vec::new();
    let num_hosts = 1;
    let num_nodes: u16 = 2;
    let mut rng = rand::thread_rng();
    let mut indices = (0..num_nodes).choose_multiple(&mut rng, num_hosts + 1);
    let start = indices.pop().unwrap();
    let host_indices = HashSet::<u16>::from_iter(indices.into_iter());
    for i in 0..num_nodes {
        let introducer = if introducers.len() > 0 { Some(introducers.get(rand::thread_rng().gen_range(0..introducers.len())).unwrap()) } else { None };
        let node = Node::new(String::from("127.0.0.1"), String::from("0"), "127.0.0.1", Uuid::new_v4(), introducer, None).await;
        introducers.push(node.endpoint_pair());
        let is_end = host_indices.contains(&i);
        let is_start = start == i;
        tokio::spawn(async move { node.listen(is_start, is_end).await });
        sleep(Duration::from_millis(DPP_TTL*6)).await;
        println!();
    }
    future::pending::<()>().await;
}