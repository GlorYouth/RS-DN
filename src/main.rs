mod utils;

use std::io::Read;
// use lazy_static::lazy_static;
// use regex::Regex;
use crate::utils::ChunkedBuffer;
use md5::{Digest, Md5};
use std::sync::Arc;
use futures::executor::block_on;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

const BLOCK_SIZE: usize = 6 * 1024 * 1024; // 每个块6MB

struct ParallelDownloader {
    url: Arc<String>,
    client: reqwest::Client,
    output_path: Arc<String>,
}

impl ParallelDownloader {
    #[inline]
    fn new(url: String, output_path: String) -> Self {
        ParallelDownloader {
            url: Arc::new(url),
            client: reqwest::Client::new(),
            output_path: Arc::new(output_path),
        }
    }

    async fn start(self, threads: usize) {
        let total_size = self.get_size().await;
        let (tx, rx) = tokio::sync::mpsc::channel::<(usize, bytes::Bytes)>(50);
        let num_chunks = (total_size + BLOCK_SIZE - 1) / BLOCK_SIZE; // 相当于有余数则+1

        let mut tasks = Vec::with_capacity(num_chunks + 1);
        let write = Self::write(self.output_path.clone(), rx).await;

        let semaphore = Arc::new(tokio::sync::Semaphore::new(threads)); // 限制为20并发

        for i in 0..num_chunks {
            let start = i * BLOCK_SIZE;
            let end = std::cmp::min(start + BLOCK_SIZE - 1, total_size - 1);

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
    ) -> tokio::task::JoinHandle<()> {
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(output_path.as_str())
            .await
            .expect("Failed to open output file");
        let mut file = tokio::io::BufWriter::with_capacity(BLOCK_SIZE, file);

        tokio::task::spawn(async move {
            let mut buffer = ChunkedBuffer::new(16);
            while let Some((start, bytes)) = rx.recv().await {
                buffer.insert(start, bytes);
                let mut iter = buffer.iter_full();
                while iter.write_file(&mut file).await.is_some() {}
            }
            let block_size = buffer.get_block_size();
            while buffer.take_first_chunk(false).map(|(block_index,chunk)| {
                let start = block_index * block_size;
                block_on(async {
                    for (index, bytes) in chunk.non_full_bytes().iter() {
                        file.seek(std::io::SeekFrom::Start((start + *index) as u64))
                            .await
                            .expect("Failed to seek");
                        file.write_all(bytes.as_ref())
                            .await
                            .expect("Failed to write chunk");
                    }
                    Some(())
                });
            }).is_some() {}
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

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(
            std::thread::available_parallelism() // 系统线程数
                .expect("Fail to get thread counts")
                .get(),
        )
        .enable_all() // 可在runtime中使用所有功能
        .build() // 创建runtime
        .expect("Failed to build tokio runtime");
    rt.block_on(async {
        let path = "output.mp4";
        ParallelDownloader::new(
            "https://ddns.gloryouth.com:10053/d/outer/local/output.mp4".into(),
            path.into(),
        )
        .start(20)
        .await;
        let mut hasher = Md5::new();
        let file = std::fs::File::open(path).expect("Failed to open output file");
        let mut reader = std::io::BufReader::new(file);
        let mut buf = [0; 4096];
        loop {
            let n = reader.read(&mut buf).expect("Failed to read from file");
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        println!("Hash: {:x}", hasher.finalize());
        // c682de7c1dafda005525f1bc0a282d7d
    });
    // match response.headers().get("alt-svc") {
    //     None => {
    //         let v = response
    //             .headers()
    //             .get("Content-Length")
    //             .and_then(|v| v.to_str().ok());
    //         println!("{:?}", v);
    //     }
    //     Some(str) => {
    //         lazy_static! {
    //         static ref HASHTAG_REGEX: Regex =
    //             Regex::new(r#"h3="([1-9][0-9]{1,3}|[1-9]|[1-5][0-9]{4}|6553[0-5])""#).unwrap();
    //     }
    //         println!(
    //             "{}",
    //             HASHTAG_REGEX.find(&str.to_str().unwrap()).unwrap().as_str()
    //         );
    //     }
    // }
    return;
}

#[cfg(test)]
mod tests {
    use lazy_static::lazy_static;
    use regex::Regex;

    #[test]
    fn test_parse() {
        lazy_static! {
            static ref HASHTAG_REGEX: Regex =
                Regex::new(r#"h3="([1-9][0-9]{1,3}|[1-9]|[1-5][0-9]{4}|6553[0-5])""#).unwrap();
        }

        for i in -1000000_i64..=1000000_i64 {
            let str = format!(r#"h3="{}""#, i);
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
