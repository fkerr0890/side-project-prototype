use rustc_hash::FxHashSet;
use std::{net::SocketAddrV4, panic, process, sync::Arc, time::Duration};

use crate::{
    http::{SerdeHttpResponse, ServerContext},
    message::{DistributeMetadata, Message, MessageDirection, MetadataKind, NumId, Peer},
    message_processing::{
        stage::{ClientApiRequest, MessageStaging},
        CipherSender, DPP_TTL_MILLIS, HEARTBEAT_INTERVAL_SECONDS,
    },
    node::{EndpointPair, Node, NodeInfo},
    MAX_TIME,
};
use rand::{seq::IteratorRandom, Rng};
use tokio::{
    fs,
    net::UdpSocket,
    sync::mpsc::{self, UnboundedSender},
    time::sleep,
};
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
    let host_indices = FxHashSet::<u16>::from_iter(
        (0..num_nodes)
            .choose_multiple(&mut rng, num_hosts)
            .into_iter(),
    );
    for i in 0..num_nodes {
        let introducer = if introducers.len() > 0 {
            Some(
                introducers
                    .get(rand::thread_rng().gen_range(0..introducers.len()))
                    .unwrap()
                    .clone()
                    .0,
            )
        } else {
            None
        };
        let (tx, rx) = mpsc::channel(1);
        let (endpoint_pair, socket) =
            Node::get_socket(String::from("127.0.0.1"), String::from("0"), "127.0.0.1").await;
        let id = NumId(Uuid::new_v4().as_u128());
        introducers.push((Peer::new(endpoint_pair, id), tx));
        let is_end = host_indices.contains(&i);
        let is_start = start == i;
        let (message_staging, to_staging, myself, _, http_handler_tx) = setup_staging(
            is_end,
            id,
            Vec::with_capacity(0),
            endpoint_pair,
            socket.clone(),
        );
        tokio::spawn(async move {
            Node::new()
                .listen(
                    message_staging,
                    myself,
                    to_staging,
                    socket,
                    is_start,
                    is_end,
                    Some(rx),
                    introducer,
                    id,
                    endpoint_pair,
                    ServerContext::new(http_handler_tx),
                )
                .await
        });
        sleep(DPP_TTL_MILLIS * 2).await;
        println!();
    }
    sleep(HEARTBEAT_INTERVAL_SECONDS * 2).await;
    println!("****************************************************************************");
    for (_, tx) in introducers {
        tx.send(()).await.unwrap();
    }
}

pub async fn load_nodes_from_file(
    directory: &str,
) -> (ServerContext, Vec<mpsc::UnboundedSender<ClientApiRequest>>) {
    let mut paths = fs::read_dir(format!("../{directory}")).await.unwrap();
    let mut server_context = None;
    let mut client_api_txs = Vec::new();
    while let Some(path) = paths.next_entry().await.unwrap() {
        let node_info: NodeInfo =
            serde_json::from_slice(&fs::read(path.path()).await.unwrap()).unwrap();
        let (port, id, peers, is_start, is_end) = Node::read_node_info(node_info);
        let socket = Arc::new(
            UdpSocket::bind(String::from("127.0.0.1:") + &port.to_string())
                .await
                .unwrap(),
        );
        let endpoint_pair = EndpointPair::new(
            SocketAddrV4::new("127.0.0.1".parse().unwrap(), port),
            SocketAddrV4::new("127.0.0.1".parse().unwrap(), port),
        );
        let (message_staging, to_staging, myself, client_api_tx, http_handler_tx) =
            setup_staging(is_end, id, peers, endpoint_pair, socket.clone());
        let context = ServerContext::new(http_handler_tx);
        if is_start {
            server_context = Some(context.clone());
        }
        client_api_txs.push(client_api_tx.clone());
        tokio::spawn(async move {
            Node::new()
                .listen(
                    message_staging,
                    myself,
                    to_staging,
                    socket,
                    is_start,
                    is_end,
                    None,
                    None,
                    id,
                    endpoint_pair,
                    context,
                )
                .await
        });
    }
    sleep(Duration::from_secs(1)).await;
    debug!("Setup complete");
    (server_context.unwrap(), client_api_txs)
}

pub fn start_distribution(txs: Vec<mpsc::UnboundedSender<ClientApiRequest>>, host_name: String) {
    let mut rng = rand::thread_rng();
    let tx = txs.into_iter().choose(&mut rng).unwrap();
    tx.send(ClientApiRequest::AddHost(host_name.clone()))
        .unwrap();
    let dmessage = Message::new(
        Peer::default(),
        NumId(Uuid::new_v4().as_u128()),
        None,
        MetadataKind::Distribute(DistributeMetadata::new(1, host_name)),
        MessageDirection::Request,
    );
    tx.send(ClientApiRequest::Message(dmessage)).unwrap();
}

pub fn measure_lock_time() {
    tokio::spawn(async move {
        sleep(Duration::from_secs(15)).await;
        let (max, sum, count, ref at) = *MAX_TIME.lock().unwrap();
        println!(
            "Max: {:.2?}, Avg: {:.2?},{}",
            max,
            if count > 0 {
                sum / count
            } else {
                Duration::ZERO
            },
            at
        );
    });
}

pub fn setup_staging(
    is_end: bool,
    id: NumId,
    peers: Vec<(String, NumId)>,
    endpoint_pair: EndpointPair,
    socket: Arc<UdpSocket>,
) -> (
    MessageStaging,
    CipherSender,
    Peer,
    UnboundedSender<ClientApiRequest>,
    UnboundedSender<(Message, UnboundedSender<SerdeHttpResponse>)>,
) {
    let (http_handler_tx, http_handler_rx) = mpsc::unbounded_channel();
    let (client_api_tx, client_api_rx) = mpsc::unbounded_channel();
    let (message_staging, to_staging, myself) = Node::setup_staging(
        is_end,
        id,
        peers,
        endpoint_pair,
        socket,
        http_handler_rx,
        client_api_tx.clone(),
        client_api_rx,
    );
    (
        message_staging,
        to_staging,
        myself,
        client_api_tx,
        http_handler_tx,
    )
}
