use crate::downloader::ControlConfig;
use crate::utils::H3Get;
use std::sync::Arc;

#[derive(Clone)]
pub struct Quiche(H3Get);

impl Quiche {
    #[inline]
    pub fn new(url: url::Url, port: u16) -> Self {
        Quiche(H3Get::new(url, port))
    }

    pub async fn download_chunk(
        self,
        start: usize,
        end: usize,
        tx: tokio::sync::mpsc::Sender<(usize, bytes::Bytes)>,
        semaphore: Arc<ControlConfig>,
    ) {
        let _permit = semaphore.acquire_semaphore().await;

        let range = format!("bytes={}-{}", start, end);

        let buf = self
            .0
            .send_with_header(vec![
                tokio_quiche::quiche::h3::Header::new(b":range", range.as_bytes())
            ])
            .await;

        tx.send((start, bytes::Bytes::copy_from_slice(&buf)))
            .await
            .expect("Failed to send buffer");
    }
}
