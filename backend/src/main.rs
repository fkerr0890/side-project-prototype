use p2p::{http::ServerContext, message::NumId, node::Node, test_utils};
use std::env;
use tokio::sync::mpsc;
use tracing::Level;

#[tokio::main]
async fn main() {
    test_utils::setup(Level::INFO);
    let args: Vec<String> = env::args().collect();
    let (private_ip, private_port, public_ip, peer_ip, peer_port, is_start) = (
        args[1].clone(),
        args[2].clone(),
        args[3].clone(),
        args[4].clone(),
        args[5].clone(),
        args[6].parse::<bool>().unwrap(),
    );
    let (endpoint_pair, socket) = Node::get_socket(private_ip, private_port, &public_ip).await;
    let (http_handler_tx, http_handler_rx) = mpsc::unbounded_channel();
    let (client_api_tx, client_api_rx) = mpsc::unbounded_channel();
    let (my_id, peer_id) = if is_start {
        (NumId(0), NumId(1))
    } else {
        (NumId(1), NumId(0))
    };
    Node::new()
        .listen(
            is_start,
            !is_start,
            None,
            None,
            my_id,
            vec![(peer_ip + ":" + &peer_port, peer_id)],
            endpoint_pair,
            socket,
            ServerContext::new(http_handler_tx),
            http_handler_rx,
            client_api_tx,
            client_api_rx,
        )
        .await
}
