use std::{collections::HashMap, net::SocketAddrV4};

use tracing::{debug, instrument};

use crate::message::{Peer, SearchMetadata, SearchMetadataKind};

#[instrument(level = "trace", skip_all, fields(dest, host_name))]
pub fn continue_propagating(myself: Peer, metadata: &SearchMetadata, local_hosts: &HashMap<String, SocketAddrV4>) -> bool {
    let should_stop = match metadata.kind {
        SearchMetadataKind::Retrieval => local_hosts.contains_key(&metadata.host_name),
        SearchMetadataKind::Distribution => myself.id != metadata.origin.id && !local_hosts.contains_key(&metadata.host_name)
    };
    if !should_stop {
        return true;
    }
    debug!("Stopped propagating search request");
    false
}