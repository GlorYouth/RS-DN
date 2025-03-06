// use lazy_static::lazy_static;
// use regex::Regex;
use crate::utils::{BlockInfo, ChunkedBuffer};
use std::sync::Arc;

const ELEMENT_SIZE: usize = 6 * 1024 * 1024; // 每个最小元素大小为6MB

pub struct ParallelDownloader {
    url: Arc<String>,
    client: reqwest::Client,
    output_path: Arc<String>,
}

impl ParallelDownloader {
    #[inline]
    pub fn new(url: String, output_path: String) -> Self {
        ParallelDownloader {
            url: Arc::new(url),
            client: reqwest::Client::new(),
            output_path: Arc::new(output_path),
        }
    }

    pub async fn start(self, threads: usize) {
        let total_size = self.get_size().await;
        let total_element_counts = (total_size + ELEMENT_SIZE - 1) / ELEMENT_SIZE; // 相当于有余数则+1

        let info = Arc::new(BlockInfo::new(ELEMENT_SIZE, 16, total_size));

        let (tx, rx) = tokio::sync::mpsc::channel::<(usize, bytes::Bytes)>(50);
        let mut tasks = Vec::with_capacity(total_element_counts + 1);

        let write = Self::write(self.output_path.clone(), rx, info).await;

        let semaphore = Arc::new(tokio::sync::Semaphore::new(threads)); // 限制为20并发

        for i in 0..total_element_counts {
            let start = i * ELEMENT_SIZE;
            let end = std::cmp::min(start + ELEMENT_SIZE - 1, total_size - 1);

            // 异步下载块
            let task = tokio::spawn(self.clone().download_chunk(
                start,
                end,
                tx.clone(),
                semaphore.clone(),
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

    #[inline]
    async fn get_size(&self) -> usize {
        let response = self
            .client
            .head(self.url.as_str())
            .send()
            .await
            .expect("error during request");
        response
            .headers()
            .get("Content-Length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok())
            .expect("Content-Length is invalid")
    }

    async fn download_chunk(
        self,
        start: usize,
        end: usize,
        tx: tokio::sync::mpsc::Sender<(usize, bytes::Bytes)>,
        semaphore: Arc<tokio::sync::Semaphore>,
    ) {
        use futures::TryFutureExt;

        let _permit = semaphore.acquire().await;
        let range = format!("bytes={}-{}", start, end);
        let mut retries = 0;
        while std::future::IntoFuture::into_future(
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

impl Clone for ParallelDownloader {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            url: self.url.clone(),
            client: self.client.clone(),
            output_path: self.output_path.clone(),
        }
    }
}
