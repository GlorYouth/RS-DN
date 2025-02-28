use std::io::SeekFrom;
use std::sync::Arc;
use lazy_static::lazy_static;
use regex::Regex;
use reqwest::Client;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

const BLOCK_SIZE: usize = 8 * 1024 * 1024; // 每个块8MB

async fn download_chunk(url: &str, start: usize, end: usize, file_mutex: Arc<Mutex<tokio::fs::File>>) {
    let range = format!("bytes={}-{}", start, end);
    let response = Client::new().get(url).header("Range", range).send().await.expect("Failed to download chunk");
    let buffer = response.bytes().await.expect("Failed to read chunk");

    let mut file = file_mutex.lock().await;
    file.seek(SeekFrom::Start(start as u64)).await.expect("Failed to seek");
    file.write_all(&buffer).await.expect("Failed to write chunk");
}

#[tokio::main]
async fn main() {
    let client = Client::new();
    let thread_count: u16 = 16;
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
    let num_chunks = (total_size + BLOCK_SIZE - 1) / BLOCK_SIZE; // 相当于有余数则+1
    let output_path = "output.zip";
    let file = OpenOptions::new().write(true).create(true).open(output_path).await.expect("Failed to create output file");
    let file_mutex = Arc::new(Mutex::new(file));
    let mut tasks = Vec::with_capacity(thread_count as usize);

    for i in 0..num_chunks {
        let start = i * BLOCK_SIZE;
        let end = std::cmp::min(start + BLOCK_SIZE - 1, total_size - 1);
        let file_mutex = Arc::clone(&file_mutex);

        // 异步下载块
        let task = tokio::spawn(download_chunk(url, start, end, file_mutex));
        tasks.push(task);
    }
    for task in tasks {
        task.await.expect("Task failed");
    }
    return;
    match response.headers().get("alt-svc") {
        None => {
            let v = response
                .headers()
                .get("Content-Length")
                .and_then(|v| v.to_str().ok());
            println!("{:?}", v);
        }
        Some(str) => {
            lazy_static! {
                static ref HASHTAG_REGEX: Regex =
                    Regex::new(r#"h3="([1-9][0-9]{1,3}|[1-9]|[1-5][0-9]{4}|6553[0-5])""#).unwrap();
            }
            println!(
                "{}",
                HASHTAG_REGEX.find(&str.to_str().unwrap()).unwrap().as_str()
            );
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
