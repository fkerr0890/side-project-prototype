use std::{collections::{HashMap, HashSet, VecDeque}, net::{Ipv4Addr, SocketAddrV4}, time::Duration};

use serde::{Serialize, Deserialize};
use tokio::{fs, sync::mpsc, task::AbortHandle, time::sleep};
use tracing::{debug, warn};

use crate::{http::{self, SerdeHttpRequest}, lock, message::{MessageDirection, Messagea, MetadataKind, NumId, Peer, Sender, StreamMetadata, StreamPayloadKind}, option_early_return, result_early_return, utils::{ArcCollection, ArcMap, TransientCollection}};

use super::SRP_TTL_SECONDS;

pub struct SessionManagerRetrieval {
    session_manager: SessionManager,
    active_sessions_host: HashMap<String, HashSet<Sender>>
}

impl SessionManagerRetrieval {
    pub fn new(local_hosts: HashMap<String, SocketAddrV4>) -> Self {
        Self { session_manager: SessionManager::new(local_hosts), active_sessions_host: HashMap::new() }
    }

    pub fn new_active_session(&mut self, host_name: String, outbound_channel: mpsc::UnboundedSender<Messagea>) {
        self.session_manager.new_active_session(host_name, outbound_channel);
    }

    pub fn session_active_host(&self, host_name: &str) -> bool { self.active_sessions_host.contains_key(host_name) }

    pub fn new_active_session_host(&mut self, host_name: String, initial_dest: Sender) {
        self.active_sessions_host.insert(host_name, HashSet::from([initial_dest]));
    }

    pub fn add_destination_host(&mut self, host_name: &str, dest: Sender) {
        self.active_sessions_host.get_mut(host_name).unwrap().insert(dest);
    }

    pub async fn http_response_action(&self, request: SerdeHttpRequest, host_name: String, id: NumId) -> Messagea {
        let socket = self.session_manager.local_hosts.get(&host_name).unwrap();
        let response = http::make_request(request, &socket.to_string()).await;
        Messagea::new(
            Peer::default(),
            id,
            None,
            MetadataKind::Stream(StreamMetadata::new(StreamPayloadKind::Response(response), host_name)),
            MessageDirection::Response
        )
    }

    pub fn session_manager(&self) -> &SessionManager { &self.session_manager }
    pub fn session_manager_mut(&mut self) -> &mut SessionManager { &mut self.session_manager }
}

pub struct SessionManagerDistribution {
    session_manager: SessionManager,
    dmessage_staging: DMessageStaging,
    curr_hop_count: u16,
    id: NumId
}

impl SessionManagerDistribution {
    pub fn new(local_hosts: HashMap<String, SocketAddrV4>) -> Self {
        Self { session_manager: SessionManager::new(local_hosts), dmessage_staging: DMessageStaging::new(), curr_hop_count: 0, id: NumId(0) }
    }

    pub fn new_active_session(&mut self, host_name: String, outbound_channel: mpsc::UnboundedSender<Messagea>, hop_count: u16, id: NumId) {
        self.curr_hop_count = hop_count;
        self.id = id;
        self.session_manager.new_active_session(host_name, outbound_channel);
    }
    
    pub async fn distribution_response_action(&mut self, bytes: Vec<u8>, host_name: String, id: NumId) -> Messagea {
        let result = if self.session_manager.local_hosts.contains_key(&host_name) { DistributionResponse::NotModified } else { self.dmessage_staging.stage_message(bytes, host_name.clone()).await };
        if let DistributionResponse::InstallOk = result {
            self.session_manager.local_hosts.insert(host_name.clone(), SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 3000));
        }
        Messagea::new(
            Peer::default(),
            id,
            None,
            MetadataKind::Stream(StreamMetadata::new(StreamPayloadKind::DistributionResponse(result), host_name)),
            MessageDirection::Response
        )
    }

    pub fn curr_hop_count(&self) -> u16 { self.curr_hop_count }
    pub fn session_manager(&self) -> &SessionManager { &self.session_manager }
    pub fn session_manager_mut(&mut self) -> &mut SessionManager { &mut self.session_manager }
}

pub struct SessionManager {
    active_sessions_client: HashMap<String, ActiveSessionInfo>,
    local_hosts: HashMap<String, SocketAddrV4>
}

impl SessionManager {
    pub fn new(local_hosts: HashMap<String, SocketAddrV4>) -> Self {
        Self {
            active_sessions_client: HashMap::new(),
            local_hosts
        }
    }

    pub fn session_active(&self, host_name: &str) -> bool { self.active_sessions_client.contains_key(host_name) }

    pub fn get_destinations(&self, host_name: &str) -> HashSet<Peer> { self.active_sessions_client.get(host_name).unwrap().active_dests.clone() }

    pub fn add_destination(&mut self, host_name: &str, dest: Peer) -> bool {
        self.active_sessions_client.get_mut(host_name).unwrap().active_dests.insert(dest)
    }

    pub fn host_installed(&self, host_name: &str) -> bool { self.local_hosts.contains_key(host_name) }

    pub fn new_active_session(&mut self, host_name: String, outbound_channel: mpsc::UnboundedSender<Messagea>) {
        self.active_sessions_client.insert(host_name, ActiveSessionInfo::new(outbound_channel, 5));
    }

    pub fn push_resource(&mut self, message: Messagea) {
        self.active_sessions_client.get_mut(message.metadata().host_name()).unwrap().push_resource(message);
    }

    pub fn finalize_resource(&mut self, host_name: &str, id: &NumId) {
        self.active_sessions_client.get_mut(host_name).unwrap().finalize_resource(id);
    }
}

struct ActiveSessionInfo {
    active_dests: HashSet<Peer>,
    resource_queue: VecDeque<NumId>,
    abort_handlers: HashMap<NumId, AbortHandle>,
    outbound_channel: mpsc::UnboundedSender<Messagea>,
    cached_size_max: usize
}

impl ActiveSessionInfo {
    fn new(outbound_channel: mpsc::UnboundedSender<Messagea>, cached_size_max: usize) -> Self {
        assert!(cached_size_max >= 1);
        Self {
            active_dests: HashSet::new(),
            resource_queue: VecDeque::new(),
            abort_handlers: HashMap::new(),
            outbound_channel,
            cached_size_max
        }
    }

    fn push_resource(&mut self, message: Messagea) {
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

    fn start_follow_ups(&self, message: Messagea) -> AbortHandle {
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
struct DMessageStaging {
    message_staging: TransientCollection<ArcMap<String, Vec<Vec<u8>>>>
}

impl DMessageStaging {
    fn new() -> Self {
        Self { message_staging: TransientCollection::new(Duration::from_secs(1800), false, ArcMap::new()) }
    }

    async fn stage_message(&mut self, payload: Vec<u8>, host_name: String) -> DistributionResponse {
        if !payload.is_empty() {
            lock!(self.message_staging.collection().map()).entry(host_name).or_default().push(payload);
            DistributionResponse::Continue
        }
        else {
            let Some(contents) = self.message_staging.pop(&host_name) else { return DistributionResponse::InstallError };
            if fs::write("C:/Users/fredk/Downloads/p2p-dump.tar.gz", &contents.concat()).await.is_ok() { DistributionResponse::InstallOk } else { DistributionResponse::InstallError }
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum DistributionResponse {
    Continue,
    NotModified,
    InstallOk,
    InstallError
}