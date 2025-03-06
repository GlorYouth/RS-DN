// use lazy_static::lazy_static;
// use regex::Regex;
use crate::utils::{BlockInfo, ChunkedBuffer};
use lazy_static::lazy_static;
use regex::Regex;
use std::sync::Arc;

const ELEMENT_SIZE: usize = 6 * 1024 * 1024; // 每个最小元素大小为6MB

#[derive(Clone)]
enum Downloader {
    Reqwest(crate::utils::Request),
    Quinn(crate::utils::Quinn),
}

impl Downloader {
    #[inline]
    async fn download_chunk(
        self,
        start: usize,
        end: usize,
        tx: tokio::sync::mpsc::Sender<(usize, bytes::Bytes)>,
        semaphore: Arc<tokio::sync::Semaphore>,
    ) {
        match self {
            Downloader::Reqwest(req) => {
                req.download_chunk(start, end, tx, semaphore).await;
            }
            Downloader::Quinn(quinn) => {
                quinn.download_chunk(start, end, tx, semaphore).await;
            }
        }
    }
}

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
            None => Downloader::Reqwest(crate::utils::Request::new(client, url)),
            Some(value) => {
                lazy_static! {
                    static ref HASHTAG_REGEX: Regex =
                        Regex::new(r#"h3=":([1-9][0-9]{1,3}|[1-9]|[1-5][0-9]{4}|6553[0-5])""#)
                            .unwrap();
                }
                let port = HASHTAG_REGEX.captures(value.to_str().unwrap()).unwrap()[1]
                    .parse::<u16>()
                    .unwrap();
                url::Url::parse(url.as_str()).expect("URL is invalid");
                Downloader::Quinn(crate::utils::Quinn::new(
                    response.remote_addr().expect("No remote addr").ip(),
                    port,
                    url
                ))
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

        let (tx, rx) = tokio::sync::mpsc::channel::<(usize, bytes::Bytes)>(50);
        let mut tasks = Vec::with_capacity(total_element_counts + 1);

        let write = Self::write(self.output_path.clone(), rx, info).await;

        let semaphore = Arc::new(tokio::sync::Semaphore::new(threads)); // 限制为20并发

        for i in 0..total_element_counts {
            let start = i * ELEMENT_SIZE;
            let end = std::cmp::min(start + ELEMENT_SIZE - 1, total_size - 1);

            // 异步下载块
            let task = tokio::spawn(self.downloader.clone().download_chunk(
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
}

impl Clone for ParallelDownloader {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            output_path: self.output_path.clone(),
            downloader: self.downloader.clone(),
            info: self.info.clone(),
        }
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
