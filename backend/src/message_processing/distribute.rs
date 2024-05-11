use std::fmt::Display;

use tokio::{fs::File, io::AsyncReadExt};
use tracing::info;

use crate::message::NumId;

pub struct ChunkedFileHandler {
    file: File,
    sent_id: Option<NumId>,
    pub bytes_read: usize
}
impl ChunkedFileHandler {
    pub async fn new(host_name: &str) -> Self {
        Self { file: File::open(format!("C:/Users/fredk/Downloads/{host_name}.gz")).await.unwrap(), sent_id: None, bytes_read: 0 }
    }

    pub async fn next_chunk_and_id(&mut self, received_id: NumId) -> Result<(NumId, Vec<u8>), Error> {
        if self.sent_id.is_some_and(|id| id != received_id) {
            return Err(Error::IdMismatch);
        }
        let mut buffer = [0; 1024];
        let n = self.file.read(&mut buffer).await.unwrap();
        self.bytes_read += n;
        info!(bytes_read = self.bytes_read, "Distribution");
        let id = NumId(u128::overflowing_add(received_id.0 , 1).0);
        self.sent_id = Some(id);
        Ok((id, if n == 0 { Vec::with_capacity(0) } else { buffer[..n].to_vec() }))
    }

    pub fn bytes_read(&self) -> usize { self.bytes_read }
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