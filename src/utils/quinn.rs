use bytes::Buf;
use std::sync::Arc;

#[derive(Clone)]
pub struct Quinn {
    remote_addr: std::net::SocketAddr,
    url: Arc<String>,
}

static ALPN: &[u8] = b"h3";

impl Quinn {
    #[inline]
    pub fn new(remote_addr: std::net::IpAddr, remote_port: u16, url: String) -> Self {
        Quinn {
            remote_addr: std::net::SocketAddr::new(remote_addr, remote_port),
            url: Arc::new(url),
        }
    }

    pub async fn download_chunk(
        self,
        start: usize,
        end: usize,
        tx: tokio::sync::mpsc::Sender<(usize, bytes::Bytes)>,
        semaphore: Arc<tokio::sync::Semaphore>,
    ) {
        let _permit = semaphore.acquire().await;

        let addr = match self.remote_addr {
            std::net::SocketAddr::V4(_) => "0.0.0.0".parse::<std::net::IpAddr>().unwrap(),
            std::net::SocketAddr::V6(_) => "::".parse().unwrap(),
        };
        let mut client_endpoint =
            h3_quinn::Endpoint::client(std::net::SocketAddr::new(addr, 0))
                .expect("Failed to create client");

        let url = url::Url::parse(&self.url).expect("Failed to parse url");

        let range = format!("bytes={}-{}", start, end);

        let mut roots = rustls::RootCertStore::empty();
        rustls_native_certs::load_native_certs().expect("Failed to load platform certificates").into_iter().for_each(
            |cert| {
                roots.add(cert).expect("Failed to add certificate");
            }
        );

        let mut tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();

        tls_config.enable_early_data = true;
        tls_config.alpn_protocols = vec![ALPN.into()];

        let client_config = quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(tls_config).expect("Failed to create quic client config"),
        ));

        client_endpoint.set_default_client_config(client_config);
        
        let conn = client_endpoint.connect(self.remote_addr, url.domain().expect("Parse domain error")).unwrap().await.expect("Connect error");

        let quinn_conn = h3_quinn::Connection::new(conn);
        
        let (mut driver, mut send_request) = h3::client::new(quinn_conn)
            .await
            .expect("Failed to create client");
        

        let drive = async move {
            futures::future::poll_fn(|cx| driver.poll_close(cx))
                .await
                .expect("Failed to close connection");
        };
        let request = async move {
            let req = http::Request::builder()
                .uri(self.url.as_str())
                .header("Range", range)
                .body(()).expect("Failed to build request body");

            let mut stream = send_request.send_request(req).await.expect("Failed to send request");
            stream.finish().await.expect("Failed to finish request");

            let resp = stream.recv_response().await.expect("Failed to read response");

            let mut buf = Vec::with_capacity(resp.headers().len());
            while let Some(chunk) = stream.recv_data().await.expect("Failed to read data") {
                buf.extend_from_slice(chunk.chunk());
            }
            tx.send((start, bytes::Bytes::copy_from_slice(&buf)))
                .await
                .expect("Failed to send buffer");
        };

        let _ = tokio::join!(request, drive);

        client_endpoint.wait_idle().await;
    }
}
