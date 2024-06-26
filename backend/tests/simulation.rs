use p2p::test_utils;
use std::future;
use tracing::Level;

#[tokio::test]
async fn basic() {
    test_utils::setup(Level::INFO);
    // test_utils::regenerate_nodes(1, 5).await;
    let (_, txs) = test_utils::load_nodes_from_file("../peer_info.json").await;
    // test_utils::start_distribution(txs, String::from("/home/fred/test/screenshot.png.gz"));
    future::pending::<()>().await;
}
