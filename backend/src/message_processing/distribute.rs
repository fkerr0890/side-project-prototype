use std::fmt::Display;

use tokio::{fs, sync::mpsc};
use tracing::{debug, error, instrument};
use uuid::Uuid;

use crate::{http::ServerContext, message::{DistributionMessage, NumId, Message, SearchMessage, StreamMessage, StreamMessageInnerKind, StreamMessageKind}, node::EndpointPair, result_early_return};

use super::{stream::StreamResponseType, EmptyOption, OutboundGateway};

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
    
    #[instrument(level = "trace", skip(self))]
    pub async fn receive(&mut self) -> EmptyOption {
        let message = self.from_staging.recv().await?;
        if message.hop_count() <= 0 || !self.outbound_gateway.try_add_breadcrumb(None, message.id(), message.sender()) {
            return Some(());
        }
        let context = ServerContext::new(self.to_srp.clone(), self.to_smp.clone(), self.tx_to_smp.clone());
        let (host_name, mut hop_count, id) = message.into_host_name_hop_count_id();
        result_early_return!(context.distribute_app(host_name.clone(), id).await, Some(()));
        hop_count -= 1;
        if hop_count > 0 {
            debug!("Propagating distribution");
            self.outbound_gateway.send_request(&mut DistributionMessage::new(id, hop_count, host_name));
        }
        Some(())
    }
}

impl ServerContext {
    #[instrument(level = "trace", skip(self))]
    pub async fn distribute_app(&self, host_name: String, id: NumId) -> Result<(), Error> {
        let payload = fs::read(format!("C:/Users/fredk/Downloads/{host_name}.gz")).await.unwrap();
        let search_message = SearchMessage::initial_search_request(host_name, false, Some(id));
        let chunks = payload.chunks(1024);
        let chunks_len = chunks.len();
        let mut sent_search = false;
        for (i, chunk) in chunks.enumerate() {
            let id = if sent_search { NumId(Uuid::new_v4().as_u128()) } else { search_message.id() };
            let mut stream_message = StreamMessage::new(search_message.host_name().clone(), id, StreamMessageKind::Distribution(StreamMessageInnerKind::Request), chunk.to_vec());
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
                return Err(Error::IdMismatch);
            }
            debug!("Distributed {} of {}", i, chunks_len)
        }
        let (tx, mut rx) = mpsc::unbounded_channel();
        self.tx_to_smp.send(tx).unwrap();
        let mut final_message = StreamMessage::new(search_message.host_name().clone(), NumId(Uuid::new_v4().as_u128()), StreamMessageKind::Distribution(StreamMessageInnerKind::Request), Vec::with_capacity(0));
        final_message.set_sender(EndpointPair::default_socket());
        self.to_smp.send(final_message).unwrap();
        let result = rx.recv().await.unwrap().unwrap_distribution().0;
        if result == 1 {
            Ok(())
        }
        else {
            Err(Error::HostInstall)
        }
    }
}

#[derive(Debug)]
enum Error {
    IdMismatch,
    HostInstall,
}
impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}