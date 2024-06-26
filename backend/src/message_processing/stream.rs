use rustc_hash::{FxHashMap, FxHashSet};
use std::{
    collections::VecDeque,
    net::{Ipv4Addr, SocketAddrV4},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use tokio::{fs, sync::mpsc, task::AbortHandle, time::sleep};
use tracing::{debug, error};

use crate::{
    http::{self, SerdeHttpRequest},
    message::{
        Message, MessageDirection, MetadataKind, NumId, Peer, StreamMetadata, StreamPayloadKind,
    },
    node::EndpointPair,
    option_early_return, result_early_return,
};

use super::{distribute::ChunkedFileHandler, SRP_TTL_SECONDS};

pub struct StreamSessionManager {
    sources_retrieval: FxHashMap<String, StreamSource>,
    sources_distribution: FxHashMap<String, StreamSourceDistribution>,
    sinks: FxHashMap<String, StreamSink>,
    local_hosts: FxHashMap<String, SocketAddrV4>,
    follow_up_tx: mpsc::UnboundedSender<NumId>,
    follow_up_rx: mpsc::UnboundedReceiver<NumId>,
}

impl StreamSessionManager {
    pub fn new(local_hosts: FxHashMap<String, SocketAddrV4>) -> Self {
        let (follow_up_tx, follow_up_rx) = mpsc::unbounded_channel();
        Self {
            sources_retrieval: FxHashMap::default(),
            sources_distribution: FxHashMap::default(),
            sinks: FxHashMap::default(),
            local_hosts,
            follow_up_tx,
            follow_up_rx,
        }
    }

    pub fn source_active_retrieval(&self, host_name: &str) -> bool {
        self.sources_retrieval.contains_key(host_name)
    }

    pub fn source_active_distribution(&self, host_name: &str) -> bool {
        self.sources_distribution.contains_key(host_name)
    }

    pub fn sink_active_retrieval(&self, host_name: &str) -> bool {
        matches!(self.sinks.get(host_name), Some(StreamSink::Retrieval(_)))
    }

    pub fn sink_active_distribution(&self, host_name: &str) -> bool {
        matches!(self.sinks.get(host_name), Some(StreamSink::Distribution(_)))
    }

    pub fn get_destinations_source_retrieval(&self, host_name: &str) -> FxHashSet<Peer> {
        option_early_return!(self.sources_retrieval.get(host_name), FxHashSet::default())
            .active_dests
            .clone()
    }

    pub fn get_all_destinations_source_retrieval(&self) -> Vec<Peer> {
        self.sources_retrieval
            .values()
            .flat_map(|s| s.active_dests.clone())
            .collect()
    }

    pub fn get_destinations_source_distribution(&self, host_name: &str) -> FxHashSet<Peer> {
        option_early_return!(
            self.sources_distribution.get(host_name),
            FxHashSet::default()
        )
        .dests_remaining
        .iter()
        .map(|d| Peer::new(*d.1, *d.0))
        .collect()
    }

    pub fn get_all_destinations_source_distribution(&self) -> Vec<Peer> {
        self.sources_distribution
            .values()
            .flat_map(|s| s.stream_source.active_dests.clone())
            .collect()
    }

    pub fn get_destinations_sink(&mut self, host_name: &str) -> FxHashSet<Peer> {
        option_early_return!(self.sinks.get_mut(host_name), FxHashSet::default())
            .unwrap_retrieval()
            .clone()
    }

    pub fn get_all_destinations_sink(&self) -> Vec<Peer> {
        self.sinks
            .values()
            .filter_map(|s| {
                if let StreamSink::Retrieval(dests) = s {
                    Some(dests.clone())
                } else {
                    None
                }
            })
            .flatten()
            .collect()
    }

    pub fn add_destination_source_retrieval(&mut self, host_name: &str, dest: Peer) -> bool {
        option_early_return!(self.sources_retrieval.get_mut(host_name), false)
            .active_dests
            .insert(dest)
    }

    pub fn add_destination_source_distribution(&mut self, host_name: &str, dest: Peer) -> bool {
        option_early_return!(self.sources_distribution.get_mut(host_name), false)
            .add_destination(dest)
    }

    pub fn add_destination_sink(&mut self, host_name: &str, dest: Peer) -> bool {
        option_early_return!(self.sinks.get_mut(host_name), false)
            .unwrap_retrieval()
            .insert(dest)
    }

    pub fn host_installed(&self, host_name: &str) -> bool {
        self.local_hosts.contains_key(host_name)
    }

    pub fn new_source_retrieval(&mut self, host_name: String) {
        assert!(self
            .sources_retrieval
            .insert(host_name, StreamSource::new(self.follow_up_tx.clone(), 5))
            .is_none());
    }

    pub async fn new_source_distribution(&mut self, host_name: String, id: NumId, hop_count: u16) {
        let file = ChunkedFileHandler::new(&host_name).await;
        assert!(self
            .sources_distribution
            .insert(
                host_name,
                StreamSourceDistribution::new(
                    StreamSource::new(self.follow_up_tx.clone(), 5),
                    id,
                    hop_count,
                    file
                )
                .await
            )
            .is_none());
    }

    pub fn new_sink_retrieval(&mut self, host_name: String) {
        assert!(self
            .sinks
            .insert(host_name, StreamSink::Retrieval(FxHashSet::default()))
            .is_none());
    }

    pub fn new_sink_distribution(&mut self, host_name: String) {
        assert!(self
            .sinks
            .insert(
                host_name,
                StreamSink::Distribution(StreamSinkDistribution::new())
            )
            .is_none());
    }

    pub fn push_resource_retrieval(&mut self, host_name: &str, id: NumId) {
        option_early_return!(self.sources_retrieval.get_mut(host_name)).push_resource(id);
    }

    pub fn push_resource_distribution(&mut self, host_name: &str, id: NumId) {
        option_early_return!(self.sources_distribution.get_mut(host_name)).push_resource(id);
    }

    pub fn set_dests_remaining_distribution(&mut self, host_name: &str) {
        option_early_return!(self.sources_distribution.get_mut(host_name)).set_dests_remaining()
    }

    pub fn remove_dest_remaining_distribution(&mut self, host_name: &str, sender_id: &NumId) {
        option_early_return!(self.sources_distribution.get_mut(host_name))
            .dests_remaining
            .remove(sender_id);
    }

    pub fn finalize_resource_retrieval(&mut self, host_name: &str, id: &NumId) {
        option_early_return!(self.sources_retrieval.get_mut(host_name)).finalize_resource(id);
    }

    pub fn finalize_resource_distribution(&mut self, host_name: &str, id: &NumId) -> bool {
        option_early_return!(self.sources_distribution.get_mut(host_name), false)
            .finalize_resource(id)
    }

    pub fn finalize_all_resources_retrieval(&mut self, host_name: &str) {
        option_early_return!(self.sources_retrieval.get_mut(host_name)).finalize_all_resources()
    }

    pub fn finalize_all_resources_distribution(&mut self, host_name: &str) {
        option_early_return!(self.sources_distribution.get_mut(host_name))
            .stream_source
            .finalize_all_resources()
    }

    pub fn lock_dests_distribution(&mut self, host_name: &str) {
        let stream_source_distribution =
            option_early_return!(self.sources_distribution.get_mut(host_name));
        stream_source_distribution.dests_locked = true;
        stream_source_distribution.stream_source.active_dests = stream_source_distribution
            .stream_source
            .active_dests
            .iter()
            .filter(|d| {
                !stream_source_distribution
                    .dests_remaining
                    .contains_key(&d.id)
            })
            .map(|d| *d)
            .collect();
    }

    pub fn dests_locked_distribution(&self, host_name: &str) -> bool {
        option_early_return!(self.sources_distribution.get(host_name), false).dests_locked
    }

    pub async fn retrieval_response_action(
        &self,
        request: SerdeHttpRequest,
        host_name: String,
        id: NumId,
    ) -> Option<Message> {
        let socket = self.local_hosts.get(&host_name)?;
        let response = http::make_request(request, &socket.to_string()).await;
        Some(Message::new(
            Peer::default(),
            id,
            None,
            MetadataKind::Stream(StreamMetadata::new(
                StreamPayloadKind::Response(response),
                host_name,
            )),
            MessageDirection::OneHop,
        ))
    }

    pub async fn distribution_response_action(
        &mut self,
        bytes: Vec<u8>,
        host_name: String,
        id: NumId,
    ) -> Option<Message> {
        let result = self
            .sinks
            .get_mut(&host_name)?
            .unwrap_distribution()
            .stage_message(bytes, id, &host_name)
            .await?;
        if let DistributionResponse::InstallOk = result {
            self.local_hosts.insert(
                host_name.clone(),
                SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 3000),
            );
        }
        Some(Message::new(
            Peer::default(),
            id,
            None,
            MetadataKind::Stream(StreamMetadata::new(
                StreamPayloadKind::DistributionResponse(result),
                host_name,
            )),
            MessageDirection::OneHop,
        ))
    }

    pub fn clear_all_sources_sinks(&mut self) {
        self.sources_retrieval
            .drain()
            .for_each(|(_, mut stream_source)| stream_source.finalize_all_resources());
        self.sources_retrieval = FxHashMap::default();
        self.sources_distribution
            .drain()
            .for_each(|(_, mut source)| source.stream_source.finalize_all_resources());
        self.sources_distribution = FxHashMap::default();
    }

    pub fn file_mut(&mut self, host_name: &str) -> Option<&mut ChunkedFileHandler> {
        Some(&mut self.sources_distribution.get_mut(host_name)?.file)
    }

    pub fn local_hosts(&self) -> &FxHashMap<String, SocketAddrV4> {
        &self.local_hosts
    }

    pub fn add_local_host(&mut self, host_name: String, endpoint: SocketAddrV4) {
        self.local_hosts.insert(host_name, endpoint);
    }

    pub fn curr_hop_count_and_distribution_id(&self, host_name: &str) -> Option<(u16, NumId)> {
        let source = self.sources_distribution.get(host_name)?;
        Some((source.hop_count, source.id))
    }

    pub fn follow_up_rx(&mut self) -> &mut mpsc::UnboundedReceiver<NumId> {
        &mut self.follow_up_rx
    }
}

#[cfg(test)]
impl StreamSessionManager {
    pub fn remove_local_host(&mut self, local_host: &str) {
        self.local_hosts.remove(local_host);
    }
}

struct StreamSource {
    active_dests: FxHashSet<Peer>,
    resource_queue: VecDeque<NumId>,
    abort_handlers: FxHashMap<NumId, AbortHandle>,
    cached_size_max: usize,
    follow_up_tx: mpsc::UnboundedSender<NumId>,
}

impl StreamSource {
    fn new(follow_up_tx: mpsc::UnboundedSender<NumId>, cached_size_max: usize) -> Self {
        assert!(cached_size_max >= 1);
        Self {
            active_dests: FxHashSet::default(),
            resource_queue: VecDeque::new(),
            abort_handlers: FxHashMap::default(),
            follow_up_tx,
            cached_size_max,
        }
    }

    fn push_resource(&mut self, id: NumId) {
        assert!(!self.abort_handlers.contains_key(&id));
        if self.resource_queue.len() == self.cached_size_max {
            self.abort_handlers
                .remove(&self.resource_queue.pop_front().unwrap())
                .unwrap()
                .abort();
        }
        self.resource_queue.push_back(id);
        let abort_handle = self.start_follow_ups(id);
        self.abort_handlers.insert(id, abort_handle);
    }

    fn start_follow_ups(&self, id: NumId) -> AbortHandle {
        let outbound_channel = self.follow_up_tx.clone();
        tokio::spawn(async move {
            let num_retries = SRP_TTL_SECONDS.as_secs() / 2;
            for _ in 0..num_retries {
                sleep(Duration::from_secs(2)).await;
                debug!(%id, "Sending follow up");
                result_early_return!(outbound_channel.send(id));
            }
        })
        .abort_handle()
    }

    fn finalize_resource(&mut self, id: &NumId) {
        let abort_handler = option_early_return!(self.abort_handlers.remove(id));
        abort_handler.abort();
        let index = self
            .resource_queue
            .iter()
            .enumerate()
            .filter(|(_, curr_id)| *curr_id == id)
            .last()
            .unwrap()
            .0;
        let id = self.resource_queue.remove(index).unwrap();
        debug!(%id, "Popped from queue");
    }

    fn finalize_all_resources(&mut self) {
        for id in self.resource_queue.drain(..) {
            self.abort_handlers.remove(&id).unwrap().abort();
        }
    }
}

struct StreamSourceDistribution {
    stream_source: StreamSource,
    id: NumId,
    hop_count: u16,
    file: ChunkedFileHandler,
    dests_remaining: FxHashMap<NumId, EndpointPair>,
    dests_locked: bool,
}

impl StreamSourceDistribution {
    async fn new(
        stream_source: StreamSource,
        id: NumId,
        hop_count: u16,
        file: ChunkedFileHandler,
    ) -> Self {
        Self {
            stream_source,
            id,
            hop_count,
            file,
            dests_remaining: FxHashMap::default(),
            dests_locked: false,
        }
    }

    fn add_destination(&mut self, dest: Peer) -> bool {
        if self.dests_locked {
            return false;
        }
        self.dests_remaining.insert(dest.id, dest.endpoint_pair);
        self.stream_source.active_dests.insert(dest)
    }

    fn set_dests_remaining(&mut self) {
        self.dests_remaining = FxHashMap::from_iter(
            self.stream_source
                .active_dests
                .iter()
                .map(|d| (d.id, d.endpoint_pair)),
        );
    }

    fn push_resource(&mut self, id: NumId) {
        self.stream_source.push_resource(id);
    }

    fn finalize_resource(&mut self, id: &NumId) -> bool {
        debug!(dests_remaining = ?self.dests_remaining);
        if !self.dests_remaining.is_empty() {
            return false;
        }
        self.stream_source.finalize_resource(id);
        true
    }
}

enum StreamSink {
    Retrieval(FxHashSet<Peer>),
    Distribution(StreamSinkDistribution),
}

impl StreamSink {
    fn unwrap_retrieval(&mut self) -> &mut FxHashSet<Peer> {
        if let Self::Retrieval(ref mut dests) = self {
            dests
        } else {
            panic!()
        }
    }
    fn unwrap_distribution(&mut self) -> &mut StreamSinkDistribution {
        if let Self::Distribution(ref mut chunks) = self {
            chunks
        } else {
            panic!()
        }
    }
}

struct StreamSinkDistribution {
    chunks: Vec<Vec<u8>>,
    prev_id: Option<NumId>,
}

impl StreamSinkDistribution {
    fn new() -> Self {
        Self {
            chunks: Vec::new(),
            prev_id: None,
        }
    }

    async fn stage_message(
        &mut self,
        payload: Vec<u8>,
        id: NumId,
        host_name: &str,
    ) -> Option<DistributionResponse> {
        if self
            .prev_id
            .is_some_and(|prev_id| id.0 <= prev_id.0 && id.0 != 0)
        {
            return None;
        }
        self.prev_id = Some(id);
        Some(if !payload.is_empty() {
            self.chunks.push(payload);
            DistributionResponse::Continue
        } else {
            let last_slash = host_name.rfind('/').unwrap();
            let path = host_name
                .chars()
                .take(host_name.len() - last_slash - 1)
                .collect::<String>()
                + "p2pdump.gz";
            if let Err(e) = fs::write(path, self.chunks.concat()).await {
                error!(%e);
                DistributionResponse::InstallError
            } else {
                DistributionResponse::InstallOk
            }
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum DistributionResponse {
    Continue,
    InstallOk,
    InstallError,
}
