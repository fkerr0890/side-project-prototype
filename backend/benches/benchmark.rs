use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use hyper::{Body, Request};
use p2p::{http::{self, ServerContext}, message_processing::stage::ClientApiRequest, test_utils};
use tokio::{sync::mpsc::UnboundedSender, time::sleep};
use tracing::Level;

fn bench_http(c: &mut Criterion) {
    test_utils::setup(Level::DEBUG);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let parameter = runtime.block_on(test_utils::load_nodes_from_file());
    c.bench_with_input(BenchmarkId::new("bench_http", "shitty"), &parameter, |b, p| b.to_async(tokio::runtime::Runtime::new().unwrap()).iter(|| { send_request(p) }));
}

async fn send_request(arg: &(ServerContext, Vec<UnboundedSender<ClientApiRequest>>)) {
    let (server_context, txs) = arg;
    let request = Request::get("/~example").body(Body::empty()).unwrap();
    http::handle_request(server_context.clone(), request).await.unwrap();
    sleep(Duration::from_millis(1000)).await;
    for tx in txs {
        tx.send(ClientApiRequest::ClearActiveSessions).unwrap();
    }
    sleep(Duration::from_millis(200)).await;
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10).measurement_time(Duration::from_secs(60));
    targets = bench_http
}
criterion_main!(benches);
