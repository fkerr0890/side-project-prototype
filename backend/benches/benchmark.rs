use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use hyper::{Body, Request};
use p2p::{http, test_utils};
use tokio::time::sleep;
use tracing::Level;

fn bench_http(c: &mut Criterion) {
    test_utils::setup(Level::WARN);
    c.bench_function("fuck", |b| b.to_async(tokio::runtime::Runtime::new().unwrap()).iter(|| { send_request() }));
}

async fn send_request() {
    let (server_context, ct, task_tracker) = test_utils::load_nodes_from_file().await;
    let request = Request::get("/~example").body(Body::empty()).unwrap();
    http::handle_request(server_context, request).await.unwrap();
    ct.cancel();
    task_tracker.wait().await;
    sleep(Duration::from_millis(100)).await;
}

criterion_group!{
    name = benches;
    config = Criterion::default().sample_size(10).measurement_time(Duration::from_secs(60));
    targets = bench_http
}
criterion_main!(benches);
