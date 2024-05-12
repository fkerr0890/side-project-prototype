use std::{collections::HashMap, net::SocketAddrV4, time::Duration};

use tracing::{debug, instrument};

use crate::message::{Message, NumId, Peer, SearchMetadata, SearchMetadataKind};

use super::stage::PropagationDirection;

#[instrument(level = "trace", skip_all, fields(dest, host_name))]
pub fn logic(id: NumId, myself: Peer, metadata: &mut SearchMetadata, local_hosts: &HashMap<String, SocketAddrV4>) -> (Peer, Option<Message>, Option<Duration>, PropagationDirection) {
    let should_stop = match metadata.kind {
        SearchMetadataKind::Retrieval => local_hosts.contains_key(&metadata.host_name),
        SearchMetadataKind::Distribution => myself.id != metadata.origin.id && !local_hosts.contains_key(&metadata.host_name)
    };
    if !should_stop {
        (metadata.origin, None, None, PropagationDirection::Forward)
    } else {
        debug!(%id, ?myself, "Stopped propagating search request");
        let prev_origin = metadata.origin;
        metadata.origin = myself;
        (prev_origin, None, None, PropagationDirection::Reverse)
    }
}