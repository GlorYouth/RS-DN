use crate::downloader::ControlConfig;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio_quiche::http3::driver::{ClientH3Event, H3Event, InboundFrame, IncomingH3Headers};
use tokio_quiche::quiche::h3;

struct Inner {
    stream_id: AtomicU64,
    remote_addr: std::net::SocketAddr,
    url: url::Url,
}

#[derive(Clone)]
pub struct Quiche(Arc<Inner>);

impl Quiche {
    #[inline]
    pub fn new(remote_addr: std::net::IpAddr, remote_port: u16, url: String) -> Self {
        let url = url::Url::parse(&url).expect("url parse error");
        Quiche(Arc::from(Inner {
            stream_id: AtomicU64::new(0),
            remote_addr: std::net::SocketAddr::new(remote_addr, remote_port),
            url,
        }))
    }

    pub async fn download_chunk(
        self,
        start: usize,
        end: usize,
        tx: tokio::sync::mpsc::Sender<(usize, bytes::Bytes)>,
        semaphore: Arc<ControlConfig>,
    ) {
        let _permit = semaphore.acquire_semaphore().await;

        let socket = match self.0.remote_addr {
            std::net::SocketAddr::V4(_) => {
                tokio::net::UdpSocket::bind("0.0.0.0:0")
            }
            std::net::SocketAddr::V6(_) => {
                tokio::net::UdpSocket::bind("[::]:0")
            }
        }
            .await
            .expect("Bind address failed");

        socket
            .connect(self.0.remote_addr)
            .await
            .expect("Connect failed");
        let (_, mut controller) = tokio_quiche::quic::connect(socket, self.0.url.domain())
            .await
            .expect("Connect failed");

        controller
            .request_sender()
            .send(tokio_quiche::http3::driver::NewClientRequest {
                request_id: self.0.stream_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                headers: vec![
                    h3::Header::new(b":path", self.0.url.path().as_ref()),
                    h3::Header::new(b":method", b"GET"),
                ],
                body_writer: None,
            })
            .unwrap();

        let mut buf = Vec::with_capacity(end - start);

        while let Some(event) = controller.event_receiver_mut().recv().await {
            match event {
                // 处理响应头事件，并进入 body 的处理分支
                ClientH3Event::Core(
                    H3Event::IncomingHeaders(
                        IncomingH3Headers {
                            stream_id: _,
                            headers: _,
                            mut recv,
                            ..
                        },
                    ),
                ) => {
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
                ClientH3Event::Core(
                    H3Event::BodyBytesReceived { fin: true, .. },
                ) => {
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
        tx.send((start, bytes::Bytes::copy_from_slice(&buf)))
            .await
            .expect("Failed to send buffer");
    }
}
