use crate::utils::{BlockInfo, ChunkedBuffer, Quiche, Request};
use bytes::Bytes;
use lazy_static::lazy_static;
use regex::Regex;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::{Semaphore, SemaphorePermit};

const ELEMENT_SIZE: usize = 6 * 1024 * 1024; // 每个最小元素大小为6MB

#[derive(Clone)]
pub enum Downloader {
    Request(Request),
    Quiche(Quiche),
}

impl Downloader {
    #[inline]
    async fn download_chunk(
        self,
        start: usize,
        end: usize,
        tx: Sender<(usize, Bytes)>,
        control: Arc<ControlConfig>,
    ) {
        match self {
            Downloader::Request(v) => v.download_chunk(start, end, tx, control).await,
            Downloader::Quiche(v) => v.download_chunk(start, end, tx, control).await,
        }
    }
}

#[derive(Clone)]
pub struct ParallelDownloader {
    output_path: Arc<String>,
    downloader: Downloader,
    info: Arc<BlockInfo>,
}

impl ParallelDownloader {
    pub async fn new(url: String, output_path: String) -> Self {
        let client = reqwest::Client::new();
        let response = client
            .head(url.as_str())
            .send()
            .await
            .expect("error during request");

        let downloader = match response.headers().get("alt-svc") {
            None => Downloader::Request(Request::new(client, url)),
            Some(value) => {
                lazy_static! {
                    static ref HASHTAG_REGEX: Regex =
                        Regex::new(r#"h3=":([1-9][0-9]{1,3}|[1-9]|[1-5][0-9]{4}|6553[0-5])""#)
                            .unwrap();
                }
                let port = HASHTAG_REGEX.captures(value.to_str().unwrap()).unwrap()[1]
                    .parse::<u16>()
                    .unwrap();
                Downloader::Quiche(Quiche::new(response.url().to_owned(), port))
            }
        };
        let total_size = response
            .headers()
            .get("Content-Length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok())
            .expect("Content-Length is invalid");
        ParallelDownloader {
            output_path: Arc::new(output_path),
            downloader,
            info: Arc::new(BlockInfo::new(ELEMENT_SIZE, 16, total_size)),
        }
    }

    pub async fn start(self, threads: usize) {
        let total_size = self.info.get_total_size();
        let total_element_counts = (total_size + ELEMENT_SIZE - 1) / ELEMENT_SIZE; // 相当于有余数则+1

        let info = Arc::new(BlockInfo::new(ELEMENT_SIZE, 16, total_size));

        let mut config = ControlConfig::new();
        config.set_threads(threads);
        let config = Arc::new(config);

        let (tx, rx) = tokio::sync::mpsc::channel::<(usize, bytes::Bytes)>(50);
        let mut tasks = Vec::with_capacity(total_element_counts + 1);

        let write = Self::write(self.output_path.clone(), rx, info).await;

        for i in 0..total_element_counts {
            let start = i * ELEMENT_SIZE;
            let end = std::cmp::min(start + ELEMENT_SIZE - 1, total_size - 1);

            // 异步下载块
            let task = tokio::spawn(self.downloader.clone().download_chunk(
                start,
                end,
                tx.clone(),
                config.clone(),
            ));
            tasks.push(task);
        }

        futures::future::join_all(tasks).await;
        drop(tx);
        write.await.expect("Write await error");
        println!("Done");
    }

    #[inline]
    async fn write(
        output_path: Arc<String>,
        mut rx: tokio::sync::mpsc::Receiver<(usize, bytes::Bytes)>,
        info: Arc<BlockInfo>,
    ) -> tokio::task::JoinHandle<()> {
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(output_path.as_str())
            .await
            .expect("Failed to open output file");

        tokio::task::spawn(async move {
            let mut buffer = ChunkedBuffer::new(info);
            while let Some((start, bytes)) = rx.recv().await {
                buffer.insert(start, bytes);

                while let Some(v) = buffer.take_first_full_chunk() {
                    v.write_file(&mut file).await;
                }
            }

            while let Some(v) = buffer.take_first_not_full_chunk() {
                v.write_file(&mut file).await;
            }
        })
    }
}

pub struct ControlConfig {
    semaphore: Option<Semaphore>,
}

impl ControlConfig {
    #[inline]
    pub fn new() -> Self {
        Self { semaphore: None }
    }

    #[inline]
    pub fn set_threads(&mut self, threads: usize) {
        self.semaphore = Some(Semaphore::new(threads));
    }

    #[inline]
    pub(crate) async fn acquire_semaphore(&self) -> Result<SemaphorePermit<'_>, AcquireError> {
        match &self.semaphore {
            None => Err(AcquireError::NoSemaphore),
            Some(v) => Ok(v.acquire().await?),
        }
    }
}

pub(crate) enum AcquireError {
    NoSemaphore,
    AcquireError(tokio::sync::AcquireError),
}

impl From<tokio::sync::AcquireError> for AcquireError {
    #[inline]
    fn from(value: tokio::sync::AcquireError) -> Self {
        Self::AcquireError(value)
    }
}

#[cfg(test)]
mod tests {
    use lazy_static::lazy_static;
    use regex::Regex;

    #[test]
    fn test_parse() {
        lazy_static! {
            static ref HASHTAG_REGEX: Regex =
                Regex::new(r#"h3=":([1-9][0-9]{1,3}|[1-9]|[1-5][0-9]{4}|6553[0-5])""#).unwrap();
        }

        for i in -1000000_i64..=1000000_i64 {
            let str = format!(r#"h3=":{}""#, i);
            if let Some(v) = HASHTAG_REGEX
                .captures(str.as_str())
                .and_then(|m| m.get(1))
                .and_then(|m| {
                    if i > 0 && i <= u16::MAX as i64 {
                        m.as_str().parse::<u16>().ok().map(|u| u as i64)
                    } else {
                        (!m.as_str().parse::<u16>().is_ok()).then_some(i)
                    }
                })
            {
                assert_eq!(v, i);
            }
        }
    }
}
