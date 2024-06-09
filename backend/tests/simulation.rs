use p2p::test_utils;
use tracing::Level;
use std::future;

#[tokio::test]
async fn basic() {
    test_utils::setup(Level::INFO);
    // test_utils::regenerate_nodes(1, 5).await;
    let (_, txs) = test_utils::load_nodes_from_file("peer_info").await;
    // test_utils::start_distribution(txs, String::from("gh_2.49.2_linux_amd64/LICENSE"));
    future::pending::<()>().await;
}
