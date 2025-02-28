use futures::future::join_all;
// use lazy_static::lazy_static;
// use regex::Regex;
use reqwest::Client;
use std::io::SeekFrom;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};
use tokio::sync::Semaphore;

const BLOCK_SIZE: usize = 6 * 1024 * 1024; // 每个块6MB

async fn download_chunk(
    client: Client,
    url: &str,
    start: usize,
    end: usize,
    tx: tokio::sync::mpsc::Sender<(usize, bytes::Bytes)>,
    semaphore: Arc<Semaphore>,
) {
    let _permit = semaphore.acquire().await;
    let range = format!("bytes={}-{}", start, end);
    let mut retries = 0;
    loop {
        let _ = client
            .get(url)
            .header("Range", &range)
            .send()
            .await.map(async |response| {
            let buffer = response.bytes().await.expect("Failed to read chunk");
            tx.send((start,buffer)).await.expect("Failed to send buffer");
        }).map_err(async |_| {
            retries += 1;
            tokio::time::sleep(Duration::from_secs(retries)).await
        });
        if retries >= 3 {
            panic!("Too many retries left");
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
        let client = Client::new();
        let url = "https://alist.gloryouth.com/d/EDU/%E5%AF%B9%E5%A4%96/%E6%88%90%E7%89%87/%E8%8A%82%E7%9B%AE1_%E6%94%B9_ai.mp4";
        let response = client
            .head(url)
            .send()
            .await.expect("error during request");
        let total_size = response
            .headers()
            .get("Content-Length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok())
            .expect("Content-Length is invalid");
        let (tx, mut rx) = tokio::sync::mpsc::channel::<(usize, bytes::Bytes)>(50);
        let num_chunks = (total_size + BLOCK_SIZE - 1) / BLOCK_SIZE; // 相当于有余数则+1
        let output_path = "output.mp4";
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(output_path).await.expect("Failed to open output file");
        let mut file = BufWriter::with_capacity(BLOCK_SIZE, file);
        let mut tasks = Vec::with_capacity(num_chunks);

        let mut buffer = std::collections::BTreeMap::new();
        let mut current_pos = 0;

        let write = tokio::spawn(async move {
            while let Some((start, bytes)) = rx.recv().await {
                buffer.insert(start, bytes);
                // 检查是否有连续块可写入
                while let Some((&next_start, bytes)) = buffer.first_key_value() {
                    if next_start == current_pos {
                        file.seek(SeekFrom::Start(current_pos as u64)).await.expect("Failed to seek");
                        file.write_all(bytes).await.expect("Failed to write to file");
                        current_pos += bytes.len();
                        buffer.remove(&next_start);
                    } else {
                        break;
                    }
                }
            }
        });

        let semaphore = Arc::new(Semaphore::new(20)); // 限制为20并发

        for i in 0..num_chunks {
            let start = i * BLOCK_SIZE;
            let end = std::cmp::min(start + BLOCK_SIZE - 1, total_size - 1);

            // 异步下载块
            let task = tokio::spawn(download_chunk(client.clone(),url, start, end, tx.clone(), semaphore.clone()));
            tasks.push(task);
        }

        join_all(tasks).await;
        drop(tx);
        write.await.expect("Failed to write chunk");
        println!("Done");

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
    });
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
