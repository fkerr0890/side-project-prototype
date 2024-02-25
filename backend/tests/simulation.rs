use std::{collections::HashSet, time::Duration, panic, process, future};

use p2p::{self, message_processing::DPP_TTL_MILLIS, node::{EndpointPair, Node}};
use rand::{seq::IteratorRandom, Rng};
use tokio::{fs, sync::mpsc, time::sleep};
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
    let regenerate = false;
    if regenerate {
        fs::remove_dir_all("../peer_info").await.unwrap();
        fs::create_dir("../peer_info").await.unwrap();
    
        let mut introducers: Vec<(EndpointPair, mpsc::Sender<()>)> = Vec::new();
        let num_hosts = 10;
        let num_nodes: u16 = 200;
        let mut rng = rand::thread_rng();
        let mut indices = (0..num_nodes).choose_multiple(&mut rng, num_hosts + 1);
        let start = indices.pop().unwrap();
        let host_indices = HashSet::<u16>::from_iter(indices.into_iter());
        for i in 0..num_nodes {
            let introducer = if introducers.len() > 0 { Some(introducers.get(rand::thread_rng().gen_range(0..introducers.len())).unwrap().0) } else { None };
            let node = Node::new(String::from("127.0.0.1"), String::from("0"), "127.0.0.1", Uuid::new_v4(), introducer, None).await;
            let (tx, rx) = mpsc::channel(1);
            introducers.push((node.endpoint_pair(), tx));
            let is_end = host_indices.contains(&i);
            let is_start = start == i;
            tokio::spawn(async move { node.listen(is_start, is_end, Some(rx)).await });
            sleep(Duration::from_millis(DPP_TTL_MILLIS*6)).await;
            println!();
        }
        for (_, tx) in introducers {
            tx.send(()).await.unwrap();
        }
    }
    future::pending::<()>().await;
}