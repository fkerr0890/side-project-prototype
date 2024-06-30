use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use hyper::{Body, Request};
use p2p::{
    http::{self, ServerContext},
    test_utils, time,
};
use tracing::Level;

fn bench_retrieval(c: &mut Criterion) {
    test_utils::setup(Level::INFO);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (server_context, _) =
        runtime.block_on(test_utils::load_nodes_from_file("../peer_info_benchmarking.json"));
    time!(
        { runtime.block_on(send_request(&server_context)) },
        Some(Level::INFO)
    );
    c.bench_with_input(
        BenchmarkId::new("bench_retrieval", "with server context"),
        &server_context,
        |b, p| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| send_request(p))
        },
    );
}

async fn send_request(server_context: &ServerContext) {
    let request = Request::get("/~example")
        .body(Body::from(vec![128u8; 523]))
        .unwrap();
    black_box(
        http::handle_request(server_context.clone(), request, true)
            .await
            .unwrap(),
    );
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10).measurement_time(Duration::from_secs(60));
    targets = bench_retrieval
}
criterion_main!(benches);
