use p2p::test_utils;
use tracing::Level;
use std::future;

#[tokio::test]
async fn basic() {
    test_utils::setup(Level::INFO);
    // test_utils::regenerate_nodes(1, 10).await;
    let (_, txs) = test_utils::load_nodes_from_file("peer_info").await;
    // test_utils::start_distribution(txs, String::from("Apple Cover Letter.pdf"));
    future::pending::<()>().await;
}
