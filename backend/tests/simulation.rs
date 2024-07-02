use p2p::test_utils;
use std::future;
use tracing::{span, Level, Subscriber};
use tracing_subscriber::{
    layer::{Context, SubscriberExt},
    EnvFilter, Layer, Registry,
};

#[tokio::test]
async fn basic() {
    test_utils::setup(Some(Level::INFO));
    // test_utils::regenerate_nodes(1, 5).await;
    let (_, txs) = test_utils::load_nodes_from_file("../peer_info.json").await;
    // test_utils::start_distribution(txs, String::from("/home/fred/test/screenshot.png.gz"));
    future::pending::<()>().await;
}

#[tokio::test]
async fn layer_test() {
    test_utils::setup(None);
    let filter = EnvFilter::try_new("p2p=trace").unwrap();
    let subscriber = Registry::default().with(filter).with(MyLayer);
    tracing::subscriber::set_global_default(subscriber).unwrap();
    // test_utils::regenerate_nodes(1, 5).await;
    test_utils::load_nodes_from_file("../peer_info.json").await;
    future::pending::<()>().await;
}

struct MyLayer;

impl<S> Layer<S> for MyLayer
where
    S: Subscriber,
    Self: 'static,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        println!("Span {:?}, id: {:?}", attrs, id);
    }
}
