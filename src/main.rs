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
            "https://alist.gloryouth.com/d/outer/edu/%E6%88%90%E7%89%87/%E8%8A%82%E7%9B%AE1_%E6%94%B9_ai.mp4".into(), // https://alist.gloryouth.com/d/outer/edu/%E6%88%90%E7%89%87/%E8%8A%82%E7%9B%AE1_%E6%94%B9_ai.mp4
            path.into(),
        ).await
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

    return;
}
