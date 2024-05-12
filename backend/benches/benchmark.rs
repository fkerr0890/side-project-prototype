use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use hyper::{Body, Request};
use p2p::{http::{self, ServerContext}, message::{DistributeMetadata, Message, MessageDirection, MetadataKind, NumId, Peer}, message_processing::stage::ClientApiRequest, test_utils, time};
use rand::seq::IteratorRandom;
use tracing::Level;
use uuid::Uuid;

fn bench_retrieval(c: &mut Criterion) {
    test_utils::setup(Level::INFO);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (server_context, _) = runtime.block_on(test_utils::load_nodes_from_file());
    time!({ runtime.block_on(send_request(&server_context)) }, Some(Level::INFO));
    c.bench_with_input(BenchmarkId::new("bench_http", "shitty"), &server_context, |b, p| b.to_async(tokio::runtime::Runtime::new().unwrap()).iter(|| black_box(send_request(p)) ));
}

fn bench_distribution(c: &mut Criterion) {
    test_utils::setup(Level::INFO);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (_, txs) = runtime.block_on(test_utils::load_nodes_from_file());
    let start_tx = txs.iter().choose(&mut rand::thread_rng()).unwrap();
    start_tx.send(ClientApiRequest::AddHost(String::from("Apple Cover Letter.pdf"))).unwrap();
    c.bench_with_input(BenchmarkId::new("bench_distribution", "shitty"), &start_tx, |b, p| b.to_async(tokio::runtime::Runtime::new().unwrap()).iter(|| async {
        let dmessage = Message::new(Peer::default(), NumId(Uuid::new_v4().as_u128()), None, MetadataKind::Distribute(DistributeMetadata::new(1, String::from("Apple Cover Letter.pdf"))), MessageDirection::Request);
        //TODO: Fix
        p.send(ClientApiRequest::Message(dmessage)).unwrap();
    }));
}

async fn send_request(server_context: &ServerContext) {
    let request = Request::get("/~example").body(Body::from(vec![128u8; 523])).unwrap();
    http::handle_request(server_context.clone(), request, true).await.unwrap();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10).measurement_time(Duration::from_secs(60));
    targets = bench_retrieval
}
criterion_main!(benches);
