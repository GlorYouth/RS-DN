use crate::downloader::ControlConfig;
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

#[derive(Clone)]
pub struct Request {
    client: reqwest::Client,
    url: Arc<String>,
}

impl Request {
    pub fn new(client: reqwest::Client, url: String) -> Self {
        Self {
            client,
            url: Arc::new(url),
        }
    }

    pub async fn download_chunk(
        self,
        start: usize,
        end: usize,
        tx: Sender<(usize, Bytes)>,
        control: Arc<ControlConfig>,
    ) {
        use futures::TryFutureExt;

        let _ = control.acquire_semaphore();
        let range = format!("bytes={}-{}", start, end);
        let mut retries = 0;
        while IntoFuture::into_future(
            self.client
                .get(self.url.as_str())
                .header("Range", &range)
                .send()
                .and_then(async |response| {
                    let buffer = response.bytes().await.expect("Failed to read chunk");
                    tx.send((start, buffer))
                        .await
                        .expect("Failed to send buffer");
                    Ok(())
                }),
        )
        .await
        .is_err()
        {
            if retries == 3 {
                panic!("Too many retries left");
            }
            tokio::time::sleep(std::time::Duration::from_secs(retries)).await;
            retries += 1;
        }
    }
}

