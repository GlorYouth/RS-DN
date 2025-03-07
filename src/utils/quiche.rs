use std::sync::Arc;
use tokio_quiche::http3::driver::{InboundFrame, IncomingH3Headers};

#[derive(Clone)]
pub struct Quiche {
    url: Arc<String>,
}

async fn new() {
    use tokio_quiche::http3::driver::{ClientH3Event, H3Event};
    use tokio_quiche::quiche::h3;

    let socket = tokio::net::UdpSocket::bind("0.0.0.0:0")
        .await
        .expect("Bind address failed");
    socket
        .connect("127.0.0.1:4043")
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
                            println!("inbound body: {:?}", std::str::from_utf8(&pooled)
                                "fin" => fin,
                                "len" => pooled.len()
                            );
                            if fin {
                                println!("received full body, exiting");
                                break 'body;
                            }
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
}
