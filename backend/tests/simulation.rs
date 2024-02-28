use std::{collections::HashSet, future, net::SocketAddrV4, panic, process, sync::Arc, time::Duration};

use p2p::{self, message::Peer, message_processing::{DPP_TTL_MILLIS, HEARTBEAT_INTERVAL_SECONDS}, node::{EndpointPair, Node, NodeInfo}};
use rand::{seq::IteratorRandom, Rng};
use tokio::{fs, net::UdpSocket, sync::mpsc, time::sleep};
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
    let regenerate = true;
    if regenerate {
        fs::remove_dir_all("../peer_info").await.unwrap();
        fs::create_dir("../peer_info").await.unwrap();
    
        let mut introducers: Vec<(Peer, mpsc::Sender<()>)> = Vec::new();
        let num_hosts = 10;
        let num_nodes: u16 = 30;
        let mut rng = rand::thread_rng();
        let mut indices = (0..num_nodes).choose_multiple(&mut rng, num_hosts + 1);
        let start = indices.pop().unwrap();
        let host_indices = HashSet::<u16>::from_iter(indices.into_iter());
        for i in 0..num_nodes {
            let introducer = if introducers.len() > 0 { Some(introducers.get(rand::thread_rng().gen_range(0..introducers.len())).unwrap().clone().0) } else { None };
            let (tx, rx) = mpsc::channel(1);
            let (endpoint_pair, socket) = Node::get_socket(String::from("127.0.0.1"), String::from("0"), "127.0.0.1").await;
            let uuid =  Uuid::new_v4().simple().to_string();
            introducers.push((Peer::new(endpoint_pair, uuid.clone()), tx));
            let is_end = host_indices.contains(&i);
            let is_start = start == i;
            tokio::spawn(async move { Node::new().listen(is_start, is_end, Some(rx), introducer, uuid, Vec::with_capacity(0), endpoint_pair, socket).await });
            sleep(Duration::from_millis(DPP_TTL_MILLIS*6)).await;
            println!();
        }
        sleep(Duration::from_secs(HEARTBEAT_INTERVAL_SECONDS)).await;
        for (_, tx) in introducers {
            tx.send(()).await.unwrap();
        }
    }
    else {
        let mut paths = fs::read_dir("../peer_info").await.unwrap();
        let mut senders = Vec::new();
        while let Some(path) = paths.next_entry().await.unwrap() {
            let node_info: NodeInfo = serde_json::from_slice(&fs::read(path.path()).await.unwrap()).unwrap();
            let (port, uuid, peers, is_start, is_end) = Node::read_node_info(node_info);
            let socket = Arc::new(UdpSocket::bind(String::from("127.0.0.1:") + &port.to_string()).await.unwrap());
            let endpoint_pair = EndpointPair::new(SocketAddrV4::new("127.0.0.1".parse().unwrap(), port), SocketAddrV4::new("127.0.0.1".parse().unwrap(), port));
            let (tx, rx) = mpsc::channel(1);
            senders.push(tx);
            tokio::spawn(async move { Node::new().listen(is_start, is_end, Some(rx), None, uuid, peers, endpoint_pair, socket).await });
        }
        sleep(Duration::from_secs(1)).await;
        for tx in senders {
            tx.send(()).await.unwrap();
        }
        println!("Setup complete");
    }
    future::pending::<()>().await;
}