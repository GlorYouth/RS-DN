use md5::{Digest, Md5};
use rs_dn::ParallelDownloader;
use std::io::Read;

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
            "http://192.168.2.3:5244/d/outer/local/output.mp4".into(), // https://alist.gloryouth.com/d/outer/edu/%E6%88%90%E7%89%87/%E8%8A%82%E7%9B%AE1_%E6%94%B9_ai.mp4
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
        assert_eq!(
            format!("{:x}", hasher.finalize()),
            "c682de7c1dafda005525f1bc0a282d7d"
        );
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
