use tokio::{fs, sync::mpsc};

use crate::{http::ServerContext, message::{Message, SearchMessage, StreamMessage, StreamMessageInnerKind, StreamMessageKind}, node::EndpointPair};

impl ServerContext {
    pub async fn distribute_app(&self) {
        let payload = fs::read("C:/Users/fredk/Downloads/chucknorris.tar.gz").await.unwrap();
        let search_message = SearchMessage::initial_search_request(String::from("chucknorris"));
        let mut stream_message = StreamMessage::new(search_message.host_name().clone(), search_message.id().clone(), StreamMessageKind::Distribution(StreamMessageInnerKind::Request), payload);
        stream_message.set_sender(EndpointPair::default_socket());
        let (tx, mut rx) = mpsc::unbounded_channel();
        self.tx_to_smp.send(tx).unwrap();
        self.to_smp.send(stream_message).unwrap();
        self.to_srp.send(search_message).unwrap();
        rx.recv().await.unwrap();
        println!("Distribution complete");
    }
}