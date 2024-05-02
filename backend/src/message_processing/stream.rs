use std::{collections::{HashMap, HashSet, VecDeque}, net::{Ipv4Addr, SocketAddrV4}, time::Duration};

use serde::{Serialize, Deserialize};
use tokio::{fs, sync::mpsc, task::AbortHandle, time::sleep};
use tracing::{debug, warn};

use crate::{http::{self, SerdeHttpRequest}, message::{MessageDirection, Message, MetadataKind, NumId, Peer, StreamMetadata, StreamPayloadKind}, option_early_return, result_early_return};

use super::{distribute::ChunkedFileHandler, SRP_TTL_SECONDS};

pub struct StreamSessionManager {
    sources_retrieval: HashMap<String, StreamSource>,
    sources_distribution: HashMap<String, StreamSourceDistribution>,
    sinks: HashMap<String, StreamSink>,
    local_hosts: HashMap<String, SocketAddrV4>
}

impl StreamSessionManager {
    pub fn new(local_hosts: HashMap<String, SocketAddrV4>) -> Self {
        Self {
            sources_retrieval: HashMap::new(),
            sources_distribution: HashMap::new(),
            sinks: HashMap::new(),
            local_hosts
        }
    }

    pub fn source_active_retrieval(&self, host_name: &str) -> bool { self.sources_retrieval.contains_key(host_name) }

    pub fn source_active_distribution(&self, host_name: &str) -> bool { self.sources_distribution.contains_key(host_name) }

    pub fn sink_active_retrieval(&self, host_name: &str) -> bool { matches!(self.sinks.get(host_name), Some(StreamSink::Retrieval(_))) }

    pub fn sink_active_distribution(&self, host_name: &str) -> bool { matches!(self.sinks.get(host_name), Some(StreamSink::Distribution(_))) }

    pub fn get_destinations_source_retrieval(&self, host_name: &str) -> HashSet<Peer> { self.sources_retrieval.get(host_name).unwrap().active_dests.clone() }
    
    pub fn get_all_destinations_source_retrieval(&self) -> Vec<Peer> { self.sources_retrieval.values().flat_map(|s| s.active_dests.clone()).collect() }

    pub fn get_destinations_source_distribution(&self, host_name: &str) -> HashSet<Peer> { self.sources_distribution.get(host_name).unwrap().stream_source.active_dests.clone() }

    pub fn get_all_destinations_source_distribution(&self) -> Vec<Peer> { self.sources_distribution.values().flat_map(|s| s.stream_source.active_dests.clone()).collect() }

    pub fn get_destinations_sink(&mut self, host_name: &str) -> HashSet<Peer> { self.sinks.get_mut(host_name).unwrap().unwrap_retrieval().clone() }

    pub fn get_all_destinations_sink(&mut self) -> Vec<Peer> { self.sinks.values().filter_map(|s| if let StreamSink::Retrieval(dests) = s { Some(dests.clone()) } else { None }).flatten().collect() }

    pub fn add_destination_source_retrieval(&mut self, host_name: &str, dest: Peer) -> bool {
        self.sources_retrieval.get_mut(host_name).unwrap().active_dests.insert(dest)
    }

    pub fn add_destination_source_distribution(&mut self, host_name: &str, dest: Peer) -> bool {
        self.sources_distribution.get_mut(host_name).unwrap().stream_source.active_dests.insert(dest)
    }

    pub fn add_destination_sink(&mut self, host_name: &str, dest: Peer) -> bool {
        self.sinks.get_mut(host_name).unwrap().unwrap_retrieval().insert(dest)
    }

    pub fn host_installed(&self, host_name: &str) -> bool { self.local_hosts.contains_key(host_name) }

    pub fn new_source_retrieval(&mut self, host_name: String, outbound_channel: mpsc::UnboundedSender<Message>) {
        assert!(self.sources_retrieval.insert(host_name, StreamSource::new(outbound_channel, 5)).is_none());
    }

    pub async fn new_source_distribution(&mut self, host_name: String, outbound_channel: mpsc::UnboundedSender<Message>, id: NumId, hop_count: u16) {
        let file = ChunkedFileHandler::new(&host_name).await;
        assert!(self.sources_distribution.insert(host_name, StreamSourceDistribution::new(StreamSource::new(outbound_channel, 5), id, hop_count, file).await).is_none());
    }

    pub fn new_sink_retrieval(&mut self, host_name: String) {
        assert!(self.sinks.insert(host_name, StreamSink::Retrieval(HashSet::new())).is_none());
    }

    pub fn new_sink_distribution(&mut self, host_name: String) {
        assert!(self.sinks.insert(host_name, StreamSink::Distribution(DistributionStreamSink::new())).is_none());
    }

    pub fn push_resource(&mut self, message: Message) {
        match message.metadata() {
            MetadataKind::Stream(StreamMetadata { payload: StreamPayloadKind::Request(_), host_name }) => self.sources_retrieval.get_mut(host_name).unwrap().push_resource(message),
            MetadataKind::Stream(StreamMetadata { payload: StreamPayloadKind::DistributionRequest(_), host_name }) => self.sources_distribution.get_mut(host_name).unwrap().stream_source.push_resource(message),
            _ => panic!()
        }
    }

    pub fn finalize_resource_retrieval(&mut self, host_name: &str, id: &NumId) {
        self.sources_retrieval.get_mut(host_name).unwrap().finalize_resource(id);
    }

    pub fn finalize_resource_distribution(&mut self, host_name: &str, id: &NumId) {
        self.sources_distribution.get_mut(host_name).unwrap().stream_source.finalize_resource(id);
    }

    pub async fn retrieval_response_action(&self, request: SerdeHttpRequest, host_name: String, id: NumId) -> Message {
        let socket = self.local_hosts.get(&host_name).unwrap();
        let response = http::make_request(request, &socket.to_string()).await;
        Message::new(
            Peer::default(),
            id,
            None,
            MetadataKind::Stream(StreamMetadata::new(StreamPayloadKind::Response(response), host_name)),
            MessageDirection::OneHop
        )
    }

    pub async fn distribution_response_action(&mut self, bytes: Vec<u8>, host_name: String, id: NumId) -> Message {
        let result = self.sinks.get_mut(&host_name).unwrap().unwrap_distribution().stage_message(bytes).await;
        if let DistributionResponse::InstallOk = result {
            self.local_hosts.insert(host_name.clone(), SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 3000));
        }
        Message::new(
            Peer::default(),
            id,
            None,
            MetadataKind::Stream(StreamMetadata::new(StreamPayloadKind::DistributionResponse(result), host_name)),
            MessageDirection::OneHop
        )
    }

    pub fn file_mut(&mut self, host_name: &str) -> &mut ChunkedFileHandler { &mut self.sources_distribution.get_mut(host_name).unwrap().file }
    
    pub fn local_hosts(&self) -> &HashMap<String, SocketAddrV4> { &self.local_hosts }

    pub fn curr_hop_count_and_distribution_id(&self, host_name: &str) -> (u16, NumId) {
        let source = self.sources_distribution.get(host_name).unwrap();
        (source.hop_count, source.id)
    }
}
struct StreamSource {
    active_dests: HashSet<Peer>,
    resource_queue: VecDeque<NumId>,
    abort_handlers: HashMap<NumId, AbortHandle>,
    outbound_channel: mpsc::UnboundedSender<Message>,
    cached_size_max: usize
}

impl StreamSource {
    fn new(outbound_channel: mpsc::UnboundedSender<Message>, cached_size_max: usize) -> Self {
        assert!(cached_size_max >= 1);
        Self {
            active_dests: HashSet::new(),
            resource_queue: VecDeque::new(),
            abort_handlers: HashMap::new(),
            outbound_channel,
            cached_size_max
        }
    }

    fn push_resource(&mut self, message: Message) {
        let id = message.id();
        if self.abort_handlers.contains_key(&id) {
            warn!(id = %message.id(), "ActiveSessionInfo: Attempted to insert duplicate request");
            return;
        }
        if self.resource_queue.len() == self.cached_size_max {
            self.abort_handlers.remove(&self.resource_queue.pop_front().unwrap()).unwrap().abort();
        }
        self.resource_queue.push_back(id);
        let abort_handle = self.start_follow_ups(message);
        self.abort_handlers.insert(id, abort_handle);
    }

    fn start_follow_ups(&self, message: Message) -> AbortHandle {
        let outbound_channel = self.outbound_channel.clone();
        tokio::spawn(async move {
            let num_retries = SRP_TTL_SECONDS.as_secs() / 2;
            for _ in 0..num_retries {
                sleep(Duration::from_secs(2)).await;
                result_early_return!(outbound_channel.send(message.clone()));
            }
        }).abort_handle()
    }

    fn finalize_resource(&mut self, id: &NumId) {
        let  abort_handler = option_early_return!(self.abort_handlers.remove(id));
        abort_handler.abort();
        let index = self.resource_queue.iter().enumerate().filter(|(_, curr_id) | *curr_id == id).last().unwrap().0;
        debug!(id = %self.resource_queue.remove(index).unwrap(), "Popped from queue");
    }
}

struct StreamSourceDistribution {
    stream_source: StreamSource,
    id: NumId,
    hop_count: u16,
    file: ChunkedFileHandler
}

impl StreamSourceDistribution {
    async fn new(stream_source: StreamSource, id: NumId, hop_count: u16, file: ChunkedFileHandler) -> Self {
        Self { stream_source, id, hop_count, file }
    }
}

enum StreamSink {
    Retrieval(HashSet<Peer>),
    Distribution(DistributionStreamSink)
}

impl StreamSink {
    fn unwrap_retrieval(&mut self) -> &mut HashSet<Peer> { if let Self::Retrieval(ref mut dests) = self { dests } else { panic!() } }
    fn unwrap_distribution(&mut self) -> &mut DistributionStreamSink { if let Self::Distribution(ref mut chunks) = self { chunks } else { panic!() } }
}

struct DistributionStreamSink {
    chunks: Vec<Vec<u8>>
}

impl DistributionStreamSink {
    fn new() -> Self {
        Self { chunks: Vec::new() }
    }

    async fn stage_message(&mut self, payload: Vec<u8>) -> DistributionResponse {
        if !payload.is_empty() {
            self.chunks.push(payload);
            DistributionResponse::Continue
        }
        else {
            if fs::write("C:/Users/fredk/Downloads/p2p-dump.tar.gz", self.chunks.concat()).await.is_ok() { DistributionResponse::InstallOk } else { DistributionResponse::InstallError }
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum DistributionResponse {
    Continue,
    InstallOk,
    InstallError
}