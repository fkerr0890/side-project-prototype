use tokio::{fs, sync::mpsc};
use uuid::Uuid;

use crate::{http::ServerContext, message::{Id, Message, SearchMessage, StreamMessage, StreamMessageInnerKind, StreamMessageKind}, node::EndpointPair};

impl ServerContext {
    pub async fn distribute_app(&self) {
        let payload = fs::read("C:/Users/fredk/Downloads/chucknorris.tar.gz").await.unwrap();
        let search_message = SearchMessage::initial_search_request(String::from("chucknorris"), false);
        let chunks = payload.chunks(1024);
        let chunks_len = chunks.len();
        let mut sent_search = false;
        for (i, chunk) in chunks.enumerate() {
            let id = if sent_search { Id(Uuid::new_v4().as_bytes().to_vec()) } else { search_message.id().clone() };
            let mut stream_message = StreamMessage::new(search_message.host_name().clone(), id, StreamMessageKind::Distribution(StreamMessageInnerKind::Request), chunk.to_vec());
            stream_message.set_sender(EndpointPair::default_socket());
            let (tx, mut rx) = mpsc::unbounded_channel();
            self.tx_to_smp.send(tx).unwrap();
            self.to_smp.send(stream_message).unwrap();
            if !sent_search {
                self.to_srp.send(search_message.clone()).unwrap();
                sent_search = true;
            }
            rx.recv().await.unwrap();
            println!("Distributed {} of {}", i, chunks_len)
        }
        let (tx, mut rx) = mpsc::unbounded_channel();
        self.tx_to_smp.send(tx).unwrap();
        let mut final_message = StreamMessage::new(search_message.host_name().clone(), Id(Uuid::new_v4().as_bytes().to_vec()), StreamMessageKind::Distribution(StreamMessageInnerKind::Request), Vec::with_capacity(0));
        final_message.set_sender(EndpointPair::default_socket());
        self.to_smp.send(final_message).unwrap();
        let result = rx.recv().await.unwrap().unwrap_distribution().0[0];
        if result == 1 {
            println!("Distribution complete");
        }
        else {
            println!("Distribution error {result}");
        }
    }
}