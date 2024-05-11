use std::{collections::HashSet, net::SocketAddrV4, panic, process, sync::Arc, time::Duration};

use crate::{http::ServerContext, message::{NumId, Peer}, message_processing::{stage::ClientApiRequest, DPP_TTL_MILLIS, HEARTBEAT_INTERVAL_SECONDS}, node::{EndpointPair, Node, NodeInfo}, MAX_TIME};
use rand::{seq::IteratorRandom, Rng};
use tokio::{fs, net::UdpSocket, sync::mpsc, time::sleep};
use tracing::{debug, Level};
use tracing_subscriber::fmt::format::FmtSpan;
use uuid::Uuid;

pub fn setup(tracing_level: Level) {
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
        println!("{}", panic_info);
        process::exit(1);
    }));

    tracing_subscriber::fmt()
        .with_file(true)
        .with_line_number(true)
        .with_span_events(FmtSpan::NEW)
        .with_max_level(tracing_level)
        .init();
    // console_subscriber::init();
}

pub async fn regenerate_nodes(num_hosts: usize, num_nodes: u16) {
    fs::remove_dir_all("../peer_info").await.unwrap();
    fs::create_dir("../peer_info").await.unwrap();

    let mut introducers: Vec<(Peer, mpsc::Sender<()>)> = Vec::new();
    let mut rng = rand::thread_rng();
    let start = (0..num_nodes).choose(&mut rng).unwrap();
    let host_indices = HashSet::<u16>::from_iter((0..num_nodes).choose_multiple(&mut rng, num_hosts).into_iter());
    for i in 0..num_nodes {
        let introducer = if introducers.len() > 0 { Some(introducers.get(rand::thread_rng().gen_range(0..introducers.len())).unwrap().clone().0) } else { None };
        let (tx, rx) = mpsc::channel(1);
        let (endpoint_pair, socket) = Node::get_socket(String::from("127.0.0.1"), String::from("0"), "127.0.0.1").await;
        let id =  NumId(Uuid::new_v4().as_u128());
        introducers.push((Peer::new(endpoint_pair, id), tx));
        let is_end = host_indices.contains(&i);
        let is_start = start == i;
        let (http_handler_tx, http_handler_rx) = mpsc::unbounded_channel();
        let (client_api_tx, client_api_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move { Node::new().listen(is_start, is_end, Some(rx), introducer, id, Vec::with_capacity(0), endpoint_pair, socket, ServerContext::new(http_handler_tx), http_handler_rx, client_api_tx, client_api_rx).await });
        sleep(DPP_TTL_MILLIS*6).await;
        println!();
    }
    sleep(HEARTBEAT_INTERVAL_SECONDS * 2).await;
    println!("****************************************************************************");
    for (_, tx) in introducers {
        tx.send(()).await.unwrap();
    }
}

pub async fn load_nodes_from_file() -> (ServerContext, Vec<mpsc::UnboundedSender<ClientApiRequest>>) {
    let mut paths = fs::read_dir("../peer_info").await.unwrap();
    let mut server_context = None;
    let mut client_api_txs = Vec::new();
    while let Some(path) = paths.next_entry().await.unwrap() {
        let node_info: NodeInfo = serde_json::from_slice(&fs::read(path.path()).await.unwrap()).unwrap();
        let (port, id, peers, is_start, is_end) = Node::read_node_info(node_info);
        let socket = Arc::new(UdpSocket::bind(String::from("127.0.0.1:") + &port.to_string()).await.unwrap());
        let endpoint_pair = EndpointPair::new(SocketAddrV4::new("127.0.0.1".parse().unwrap(), port), SocketAddrV4::new("127.0.0.1".parse().unwrap(), port));
        let (http_handler_tx, http_handler_rx) = mpsc::unbounded_channel();
        let context = ServerContext::new(http_handler_tx);
        let (client_api_tx, client_api_rx) = mpsc::unbounded_channel();
        client_api_txs.push(client_api_tx.clone());
        if is_start {
            server_context = Some(context.clone());
        }
        tokio::spawn(async move { Node::new().listen(is_start, is_end, None, None, id, peers, endpoint_pair, socket, context, http_handler_rx, client_api_tx, client_api_rx).await });
    }
    sleep(Duration::from_secs(1)).await;
    debug!("Setup complete");
    (server_context.unwrap(), client_api_txs)
}

pub fn measure_lock_time() {
    tokio::spawn(async move {
        sleep(Duration::from_secs(15)).await;
        let (max, sum, count, ref at) = *MAX_TIME.lock().unwrap();
        println!("Max: {:.2?}, Avg: {:.2?},{}", max, if count > 0 { sum / count } else { Duration::ZERO }, at);
    });
}