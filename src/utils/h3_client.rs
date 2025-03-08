use hickory_resolver::config::{ResolverConfig, ResolverOpts};
use hickory_resolver::lookup_ip::LookupIp;
use hickory_resolver::TokioAsyncResolver;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio_quiche::http3::driver::{ClientH3Event, H3Event, InboundFrame, IncomingH3Headers};
use tokio_quiche::quiche::h3;
use tokio_quiche::settings::{Hooks, QuicSettings};
use tokio_quiche::{ClientH3Driver, ConnectionParams};

struct Inner {
    headers: Vec<h3::Header>,
    stream_id: AtomicU64,
    resolver: TokioAsyncResolver,
    url: url::Url,
    port: u16,
}

#[derive(Clone)]
pub struct H3Get {
    inner: Arc<Inner>,
}

impl H3Get {
    pub fn new(url: url::Url, port: u16) -> Self {
        let headers = vec![
            h3::Header::new(b":authority", url.authority().as_ref()),
            h3::Header::new(b":path", url.path().as_ref()),
            h3::Header::new(b":method", b"GET"),
            h3::Header::new(b":scheme", b"https"),
            h3::Header::new(b":accept", b"*/*"),
        ];
        Self {
            inner: Arc::new(Inner {
                headers,
                stream_id: AtomicU64::new(0),
                resolver: TokioAsyncResolver::tokio(
                    ResolverConfig::default(),
                    ResolverOpts::default(),
                ),
                url,
                port,
            }),
        }
    }

    pub async fn send_with_header(&self, vec: Vec<h3::Header>) -> Vec<u8> {
        let ip = self.lookup_ip().await;
        while let Some(ip) = ip.iter().next() {
            let socket = match ip {
                std::net::IpAddr::V4(_) => tokio::net::UdpSocket::bind("0.0.0.0:0"),
                std::net::IpAddr::V6(_) => tokio::net::UdpSocket::bind("[::]:0"),
            }
            .await
            .expect("Bind address failed");

            socket
                .connect(SocketAddr::new(ip, self.inner.port))
                .await
                .expect("Connect failed");
            let param = ConnectionParams::new_client(QuicSettings::default(),None,Hooks::default());
            let mut driver = ClientH3Driver::new(Default::default());
            let conn = tokio_quiche::quic::connect_with_config(socket, self.inner.url.domain(), &param, driver.0)
                .await
                .expect("Connect failed");

            driver.1
                .request_sender()
                .send(tokio_quiche::http3::driver::NewClientRequest {
                    request_id: self
                        .inner
                        .stream_id
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                    headers: Vec::from_iter(
                        vec.into_iter().chain(self.inner.headers.iter().cloned()),
                    ),
                    body_writer: None,
                })
                .unwrap();

            let mut buf = Vec::new();

            while let Some(event) = driver.1.event_receiver_mut().recv().await {
                match event {
                    // 处理响应头事件，并进入 body 的处理分支
                    ClientH3Event::Core(H3Event::IncomingHeaders(IncomingH3Headers {
                        stream_id: _,
                        headers: _,
                        mut recv,
                        ..
                    })) => {
                        'body: while let Some(frame) = recv.recv().await {
                            match frame {
                                InboundFrame::Body(pooled, fin) => {
                                    println!("body: {:?}", pooled);
                                    buf.extend_from_slice(&pooled);
                                    if fin {
                                        break 'body;
                                    }
                                }
                                InboundFrame::Datagram(_) => {
                                    println!("received datagram");
                                }
                            }
                        }
                    }
                    // 如果收到 BodyBytesReceived 且 fin 为 true，则认为整个响应结束
                    ClientH3Event::Core(H3Event::BodyBytesReceived { fin: true, .. }) => {
                        println!("received body bytes");
                        break;
                    }
                    // 其它 Core 事件仅做日志输出
                    ClientH3Event::Core(event) => {
                        println!("received core event: {:?}", event);
                    }
                    // 对于新发起的出站请求，也打印相关信息
                    ClientH3Event::NewOutboundRequest {
                        stream_id: _,
                        request_id,
                    } => {
                        println!("new outbound request: {:?}", request_id);
                    }
                }
            }
            drop(conn);
            return buf;
        }
        Vec::new()
    }

    #[inline]
    pub async fn lookup_ip(&self) -> LookupIp {
        self.inner
            .resolver
            .lookup_ip(self.inner.url.domain().expect("Failed to get domain"))
            .await
            .expect("Failed to lookup IP")
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test() {
        let vec = H3Get::new("".parse().unwrap(), 443).send_with_header(vec![
            h3::Header::new(b":range", b"bytes=0-100")
        ]).await;
    println!("{:#?}", String::from_utf8_lossy(&vec));
    }
}
