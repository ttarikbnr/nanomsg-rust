use std::{pin::Pin, task::{Poll, Context}, io};
use std::ops::Deref;
use std::net::SocketAddr;
use futures::{Sink, Stream, StreamExt};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::codec::{Encoder, Decoder, Framed};
use bytes::{BytesMut, BufMut, Buf};
use pin_project::*;
use super::reconnect::Reconnectable;
use futures::{FutureExt, future::BoxFuture, TryFutureExt};

use crate::options::SocketOptions;

const BUS_HANDSHAKE_PACKET : [u8; 8] = [0x00, 0x53, 0x50, 0x00, 0x00, 0x70, 0x00, 0x00];

#[pin_project]
pub struct NanomsgBus {
    address: Option<String>,
    #[pin]
    inner : tokio_util::codec::Framed<TcpStream, NanomsgBusCodec>
}

impl NanomsgBus {
    pub async fn connect(address: String) -> std::io::Result<Self> {
        Self::connect_with_socket_options(address, SocketOptions::default()).await
    }

    pub async fn connect_with_socket_options(address: String,
                                             socket_options: SocketOptions) -> std::io::Result<Self> {

        let mut tcp_stream = tokio::net::TcpStream::connect(address.clone()).await?;


        socket_options.apply_to_tcpstream(&tcp_stream)?;


        tcp_stream.write_all(&BUS_HANDSHAKE_PACKET[..]).await?;
        
        let mut incoming_handshake= [0u8; 8];

        tcp_stream.read_exact(&mut incoming_handshake).await?;

        if incoming_handshake != BUS_HANDSHAKE_PACKET {
            return Err(std::io::Error::from(std::io::ErrorKind::InvalidData))
        }

        let framed_parts = tokio_util::codec::FramedParts::new::<&[u8]>(tcp_stream, NanomsgBusCodec::new());
        let framed = Framed::from_parts(framed_parts);

        Ok(Self {
            address: Some(address),
            inner: framed
        })
    }

    pub async fn listen(address: &str) -> std::io::Result<impl Stream<Item = std::io::Result<(SocketAddr, NanomsgBus)>> + Unpin> {
        let tcp_socket = tokio::net::TcpSocket::new_v4()?;
        let socket_addr = if let Ok(addr) = address.parse() {
            addr
        } else {
            return Err(std::io::Error::from(std::io::ErrorKind::AddrNotAvailable))
        };

        tcp_socket.bind(socket_addr)?;
        let listener = tcp_socket.listen(1024)?;

        Ok(Box::pin(listener.then(|stream| async {
            let mut stream = stream?;
            stream.write_all(&BUS_HANDSHAKE_PACKET[..]).await?;
            let address = stream.peer_addr()?;

            let mut incoming_handshake= [0u8; 8];
            stream.read_exact(&mut incoming_handshake).await?;
    
            if incoming_handshake != BUS_HANDSHAKE_PACKET {
                return Err(std::io::Error::from(std::io::ErrorKind::InvalidData))
            }

            let framed_parts = tokio_util::codec::FramedParts::new::<&[u8]>(stream, NanomsgBusCodec::new());
            let framed = Framed::from_parts(framed_parts);
            Ok((address, NanomsgBus {
                address : None,
                inner: framed
            }))
        })))
    }
}


impl Reconnectable for NanomsgBus {
    type ConnectFut = BoxFuture<'static, Result<Self, (Self, io::Error)>>;

    fn reconnect(self) -> Self::ConnectFut {

        // Since bind method doesn't return Reconnect<T> 
        // We can safely unwrap the address here
        let address = self.address
                          .clone()
                          .unwrap();


        NanomsgBus::connect(address)
                    .map_err(|io_err|{
                        (self, io_err)
                    })
                    .boxed()

    }
}


impl Stream for NanomsgBus {
    type Item = std::io::Result<Vec<u8>>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.inner.poll_next(cx)
    }
}


impl <I> Sink<I> for NanomsgBus 
    where I: Deref<Target=[u8]>{
    type Error = std::io::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        <Framed<TcpStream, NanomsgBusCodec> as Sink<I>>::poll_ready(this.inner, cx)
    }

    fn start_send(self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        let this = self.project();
        this.inner.start_send(item)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        <Framed<TcpStream, NanomsgBusCodec> as Sink<I>>::poll_flush(this.inner, cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        <Framed<TcpStream, NanomsgBusCodec> as Sink<I>>::poll_close(this.inner, cx)
    }
}


enum DecodingState {
    Size,
    Payload(usize)
}

struct NanomsgBusCodec {
    decoding_state: DecodingState,
}

impl NanomsgBusCodec {
    pub fn new() -> Self {
        Self {
            decoding_state: DecodingState::Size,
        }
    }
}

impl <T>Encoder<T> for NanomsgBusCodec 
    where T: std::ops::Deref<Target = [u8]> {
    type Error = std::io::Error;

    fn encode(&mut self, item: T, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.reserve(item.len() + 8);
        dst.put_u64(item.len() as _);
        dst.put(item.as_ref());
        Ok(())
    }
}

impl Decoder for NanomsgBusCodec {
    type Item = Vec<u8>;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        use std::mem::replace;

        loop {
            match self.decoding_state {
                DecodingState::Size => {
                    if src.remaining() >= 8 {
                        let size = src.get_u64() as _;
                        let _ = replace(&mut self.decoding_state, DecodingState::Payload(size));
                    } else {
                        return Ok(None)
                    }
                }
                DecodingState::Payload(size) => {
                    if src.remaining() >= size {
                        let payload = src.split_to(size);
                        let _ = replace(&mut self.decoding_state, DecodingState::Size);
                        return Ok(Some(payload.to_vec()))
                    } else {
                        return Ok(None)
                    }
                }
            }
        }
    }
}

