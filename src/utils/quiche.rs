use crate::downloader::ControlConfig;
use std::sync::Arc;
use tokio_quiche::http3::driver::{ClientH3Event, H3Event, InboundFrame, IncomingH3Headers};
use tokio_quiche::quiche::h3;

#[derive(Clone)]
pub struct Quiche {
    remote_addr: std::net::SocketAddr,
    url: Arc<url::Url>,
}
impl Quiche {
    #[inline]
    pub fn new(remote_addr: std::net::IpAddr, remote_port: u16, url: String) -> Self {
        let url = url::Url::parse(&url).expect("url parse error");
        Quiche {
            remote_addr: std::net::SocketAddr::new(remote_addr, remote_port),
            url: Arc::new(url),
        }
    }

    pub async fn download_chunk(
        self,
        start: usize,
        end: usize,
        tx: tokio::sync::mpsc::Sender<(usize, bytes::Bytes)>,
        semaphore: Arc<ControlConfig>,
    ) {
        let _permit = semaphore.acquire_semaphore().await;

        let socket = tokio::net::UdpSocket::bind("0.0.0.0:0")
            .await
            .expect("Bind address failed");

        socket
            .connect(self.remote_addr)
            .await
            .expect("Connect failed");
        let (_, mut controller) = tokio_quiche::quic::connect(socket, None)
            .await
            .expect("Connect failed");

        controller
            .request_sender()
            .send(tokio_quiche::http3::driver::NewClientRequest {
                request_id: 0,
                headers: vec![h3::Header::new(b":method", b"GET")],
                body_writer: None,
            })
            .unwrap();

        let mut buf = Vec::with_capacity(end - start);

        while let Some(event) = controller.event_receiver_mut().recv().await {
            match event {
                ClientH3Event::Core(H3Event::IncomingHeaders(IncomingH3Headers {
                    stream_id: _,
                    headers: _,
                    mut recv,
                    ..
                })) => {
                    'body: while let Some(frame) = recv.recv().await {
                        match frame {
                            InboundFrame::Body(pooled, fin) => {
                                if fin {
                                    println!("received full body, exiting");
                                    break 'body;
                                }
                                buf.extend_from_slice(&pooled[..]);
                            }
                            InboundFrame::Datagram(_) => {
                                panic!("received datagram");
                            }
                        }
                    }
                }
                ClientH3Event::Core(H3Event::BodyBytesReceived { fin: true, .. }) => {
                    panic!("inbound body bytes received, exiting");
                }
                _ => panic!("Received unknown body event"),
            }
        }
        tx.send((start, bytes::Bytes::copy_from_slice(&buf)))
            .await
            .expect("Failed to send buffer");
    }
}
