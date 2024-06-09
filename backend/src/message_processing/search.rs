use std::{net::SocketAddrV4, time::Duration};
use rustc_hash::FxHashMap;

use tracing::{debug, instrument};

use crate::message::{NumId, Peer, SearchMetadata, SearchMetadataKind};

use super::stage::PropagationDirection;

#[instrument(level = "trace", skip_all, fields(dest, host_name))]
pub fn logic(id: NumId, myself: Peer, metadata: &mut SearchMetadata, local_hosts: &FxHashMap<String, SocketAddrV4>) -> (Peer, Option<Duration>, PropagationDirection) {
    let should_stop = match metadata.kind {
        SearchMetadataKind::Retrieval => local_hosts.contains_key(&metadata.host_name),
        SearchMetadataKind::Distribution => myself.id != metadata.origin.id && !local_hosts.contains_key(&metadata.host_name)
    };
    if !should_stop {
        (metadata.origin, None, PropagationDirection::Forward)
    } else {
        debug!(%id, ?myself, "Stopped propagating search request");
        metadata.hairpin = Some(myself);
        (metadata.origin, None, PropagationDirection::Reverse)
    }
}
