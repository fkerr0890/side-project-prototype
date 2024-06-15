use p2p::{http::ServerContext, message::NumId, node::Node, test_utils};
use std::env;
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
    let (my_id, peer_id) = if is_start {
        (NumId(0), NumId(1))
    } else {
        (NumId(1), NumId(0))
    };
    let (message_staging, to_staging, myself, _, http_handler_tx) = test_utils::setup_staging(
        !is_start,
        my_id,
        vec![(peer_ip + ":" + &peer_port, peer_id)],
        endpoint_pair,
        socket.clone(),
    );
    Node::new()
        .listen(
            message_staging,
            myself,
            to_staging,
            socket,
            is_start,
            !is_start,
            None,
            None,
            my_id,
            endpoint_pair,
            ServerContext::new(http_handler_tx),
        )
        .await;
}
