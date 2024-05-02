use std::fmt::Display;

use tokio::{fs::File, io::AsyncReadExt};
use tracing::info;
use uuid::Uuid;

use crate::message::NumId;

pub struct ChunkedFileHandler {
    file: File,
    sent_id: Option<NumId>,
    bytes_read: usize
}
impl ChunkedFileHandler {
    pub async fn new(host_name: &str) -> Self {
        Self { file: File::open(format!("C:/Users/fredk/Downloads/{host_name}.gz")).await.unwrap(), sent_id: None, bytes_read: 0 }
    }

    pub async fn next_chunk_and_id(&mut self, received_id: Option<NumId>) -> Result<(NumId, Vec<u8>), Error> {
        if received_id != self.sent_id {
            return Err(Error::IdMismatch);
        }
        let mut buffer = [0; 1024];
        let n = self.file.read(&mut buffer).await.unwrap();
        self.bytes_read += n;
        info!(bytes_read = self.bytes_read, "Distribution");
        let id = NumId(Uuid::new_v4().as_u128());
        self.sent_id = Some(id);
        Ok((id, if n == 0 { Vec::with_capacity(0) } else { buffer[..n].to_vec() }))
    }
}

#[derive(Debug)]
pub enum Error {
    IdMismatch,
    HostInstall,
}
impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}