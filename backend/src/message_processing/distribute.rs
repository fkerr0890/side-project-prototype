use tokio::{fs, sync::mpsc};
use uuid::Uuid;

use crate::{http::ServerContext, message::{DistributionMessage, Id, Message, SearchMessage, StreamMessage, StreamMessageInnerKind, StreamMessageKind}, node::EndpointPair};

use super::{stream::StreamResponseType, EmptyResult, OutboundGateway};

pub struct DistributionHandler {
    from_staging: mpsc::UnboundedReceiver<DistributionMessage>,
    to_srp: mpsc::UnboundedSender<SearchMessage>,
    to_smp: mpsc::UnboundedSender<StreamMessage>,
    tx_to_smp: mpsc::UnboundedSender<mpsc::UnboundedSender<StreamResponseType>>,
    outbound_gateway: OutboundGateway
}
impl DistributionHandler {
    pub fn new(from_staging: mpsc::UnboundedReceiver<DistributionMessage>, to_srp: mpsc::UnboundedSender<SearchMessage>, to_smp: mpsc::UnboundedSender<StreamMessage>, tx_to_smp: mpsc::UnboundedSender<mpsc::UnboundedSender<StreamResponseType>>, outbound_gateway: OutboundGateway) -> Self {
        Self { from_staging, to_srp, to_smp, tx_to_smp, outbound_gateway }
    }
    
    pub async fn receive(&mut self) -> EmptyResult  {
        let message = self.from_staging.recv().await.ok_or("DistributionHandler: failed to receive message from gateway")?;
        if message.hop_count() <= 0 || !self.outbound_gateway.try_add_breadcrumb(None, message.id(), message.sender()) {
            return Ok(())
        }
        let context = ServerContext::new(self.to_srp.clone(), self.to_smp.clone(), self.tx_to_smp.clone());
        let (host_name, mut hop_count, uuid) = message.into_host_name_hop_count_uuid();
        context.distribute_app(host_name.clone(), uuid.clone()).await?;
        hop_count -= 1;
        if hop_count > 0 {
            println!("Propagating distribution");
            self.outbound_gateway.send_request(&mut DistributionMessage::new(uuid, hop_count, host_name), None)?;
        }
        Ok(())
    }
}

impl ServerContext {
    pub async fn distribute_app(&self, host_name: String, uuid: Id) -> EmptyResult {
        let payload = fs::read(format!("C:/Users/fredk/Downloads/{host_name}.gz")).await.unwrap();
        let search_message = SearchMessage::initial_search_request(host_name, false, Some(uuid));
        let chunks = payload.chunks(1024);
        let chunks_len = chunks.len();
        let mut sent_search = false;
        for (i, chunk) in chunks.enumerate() {
            let id = if sent_search { Id(Uuid::new_v4().as_bytes().to_vec()) } else { search_message.id().clone() };
            let mut stream_message = StreamMessage::new(search_message.host_name().clone(), id.clone(), StreamMessageKind::Distribution(StreamMessageInnerKind::Request), chunk.to_vec());
            stream_message.set_sender(EndpointPair::default_socket());
            let (tx, mut rx) = mpsc::unbounded_channel();
            self.tx_to_smp.send(tx).unwrap();
            self.to_smp.send(stream_message).unwrap();
            if !sent_search {
                self.to_srp.send(search_message.clone()).unwrap();
                sent_search = true;
            }
            let response_id = rx.recv().await.unwrap().unwrap_distribution();
            if id != response_id {
                return Err(String::from("Id mismatch"));
            }
            println!("Distributed {} of {}", i, chunks_len)
        }
        let (tx, mut rx) = mpsc::unbounded_channel();
        self.tx_to_smp.send(tx).unwrap();
        let mut final_message = StreamMessage::new(search_message.host_name().clone(), Id(Uuid::new_v4().as_bytes().to_vec()), StreamMessageKind::Distribution(StreamMessageInnerKind::Request), Vec::with_capacity(0));
        final_message.set_sender(EndpointPair::default_socket());
        self.to_smp.send(final_message).unwrap();
        let result = rx.recv().await.unwrap().unwrap_distribution().0[0];
        if result == 1 {
            Ok(())
        }
        else {
            Err(format!("Distribution error {result}"))
        }
    }
}