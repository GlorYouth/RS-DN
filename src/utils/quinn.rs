use bytes::Buf;
use rustls::crypto;
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

        let auth = {
            let uri = self.url.parse::<http::Uri>().expect("Failed to parse url");

            if uri.scheme() != Some(&http::uri::Scheme::HTTPS) {
                panic!("Only HTTPS URI are supported");
            }
            uri.authority().expect("Url must have an authority").clone()
        };

        let remote_addr = tokio::net::lookup_host(format!("{}:{}",auth.host(),443)).await.expect("Failed to lookup host").next().unwrap();
        
        let mut client_endpoint = {
            let addr = match remote_addr {
                std::net::SocketAddr::V4(_) => "0.0.0.0".parse::<std::net::IpAddr>().unwrap(),
                std::net::SocketAddr::V6(_) => "::".parse().unwrap(),
            };
            h3_quinn::Endpoint::client(std::net::SocketAddr::new(addr, 0))
                .expect("Failed to create client")
        };
        
        let mut roots = rustls::RootCertStore::empty();
        rustls_native_certs::load_native_certs()
            .expect("Failed to load platform certificates")
            .into_iter()
            .for_each(|cert| {
                roots.add(cert).expect("Failed to add certificate");
            });

        let mut tls_config = rustls::ClientConfig::builder_with_provider(Arc::from(
            crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("Failed to create TLS config")
        .with_root_certificates(roots)
        .with_no_client_auth();

        tls_config.enable_early_data = true;
        tls_config.alpn_protocols = vec![ALPN.into()];

        let client_config = quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)
                .expect("Failed to create quic client config"),
        ));

        client_endpoint.set_default_client_config(client_config);

        let conn = client_endpoint
            .connect(remote_addr, auth.host())
            .unwrap()
            .await
            .expect("Connect error");

        let quinn_conn = h3_quinn::Connection::new(conn);

        let (mut driver, mut send_request) = h3::client::new(quinn_conn)
            .await
            .expect("Failed to create client");

        let drive = async move {
            futures::future::poll_fn(|cx| driver.poll_close(cx))
                .await
                .expect("Failed to close connection");
        };

        let range = format!("bytes={}-{}", start, end);
        
        let request = async move {
            let req = http::Request::builder()
                .uri(self.url.as_str())
                .header("Range", range)
                .body(())
                .expect("Failed to build request body");

            let mut stream = send_request
                .send_request(req)
                .await
                .expect("Failed to send request");
            stream.finish().await.expect("Failed to finish request");

            let resp = stream
                .recv_response()
                .await
                .expect("Failed to read response");

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

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn download() {
        let url = "https://alist.gloryouth.com/d/";

        let uri = url.parse::<http::Uri>().expect("Failed to parse url");

        if uri.scheme() != Some(&http::uri::Scheme::HTTPS) {
            panic!("Only HTTPS URI are supported");
        }

        let auth = uri.authority().expect("Url must have an authority").clone();

        let remote_addr = tokio::net::lookup_host(format!("{}:{}",auth.host(),443)).await.expect("Failed to lookup host").next().unwrap();
        let mut client_endpoint = {
            let addr = match remote_addr {
                std::net::SocketAddr::V4(_) => "0.0.0.0".parse::<std::net::IpAddr>().unwrap(),
                std::net::SocketAddr::V6(_) => "::".parse().unwrap(),
            };
            h3_quinn::Endpoint::client(std::net::SocketAddr::new(addr, 0))
                .expect("Failed to create client")
        };


        let mut roots = rustls::RootCertStore::empty();
        rustls_native_certs::load_native_certs()
            .expect("Failed to load platform certificates")
            .into_iter()
            .for_each(|cert| {
                roots.add(cert).expect("Failed to add certificate");
            });

        let mut tls_config = rustls::ClientConfig::builder_with_provider(Arc::from(
            crypto::ring::default_provider(),
        ))
            .with_safe_default_protocol_versions()
            .expect("Failed to create TLS config")
            .with_root_certificates(roots)
            .with_no_client_auth();

        tls_config.enable_early_data = true;
        tls_config.alpn_protocols = vec![ALPN.into()];

        let client_config = quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)
                .expect("Failed to create quic client config"),
        ));

        client_endpoint.set_default_client_config(client_config);

        let conn = client_endpoint
            .connect(remote_addr, auth.host())
            .unwrap()
            .await
            .expect("Connect error");

        let quinn_conn = h3_quinn::Connection::new(conn);

        let (mut driver, mut send_request) = h3::client::new(quinn_conn)
            .await
            .expect("Failed to create client");

        let drive = async move {
            futures::future::poll_fn(|cx| driver.poll_close(cx))
                .await
                .expect("Failed to close connection");
        };
        // 
        // let range = format!("bytes={}-{}", start, end);
        // 
        // let request = async move {
        //     let req = http::Request::builder()
        //         .uri(url)
        //         .header("Range", range)
        //         .body(())
        //         .expect("Failed to build request body");
        // 
        //     let mut stream = send_request
        //         .send_request(req)
        //         .await
        //         .expect("Failed to send request");
        //     stream.finish().await.expect("Failed to finish request");
        // 
        //     let resp = stream
        //         .recv_response()
        //         .await
        //         .expect("Failed to read response");
        // 
        //     let mut buf = Vec::with_capacity(resp.headers().len());
        //     while let Some(chunk) = stream.recv_data().await.expect("Failed to read data") {
        //         buf.extend_from_slice(chunk.chunk());
        //     }
        //     tx.send((start, bytes::Bytes::copy_from_slice(&buf)))
        //         .await
        //         .expect("Failed to send buffer");
        // };
        // 
        // let _ = tokio::join!(request, drive);
        // 
        // client_endpoint.wait_idle().await;
    }
}