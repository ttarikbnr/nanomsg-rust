use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use futures::{Sink, Stream, StreamExt};
use std::{pin::Pin, task::{Poll, Context}};
use bytes::{BytesMut, BufMut, Buf};
use pin_project::*;
use tokio_util::codec::{Encoder, Decoder, Framed};
use std::ops::Deref;
use std::net::SocketAddr;
use super::options::SocketOptions;


const PAIR_HANDSHAKE_PACKET : [u8; 8] = [0x00, 0x53, 0x50, 0x00, 0x00, 0x10, 0x00, 0x00];

#[pin_project]
pub struct NanomsgPair {
    #[pin]
    inner : tokio_util::codec::Framed<TcpStream, NanomsgPairCodec>
}

impl NanomsgPair {
    pub async fn connect<A: ToSocketAddrs>(address: A) -> std::io::Result<Self> {
        Self::connect_with_socket_options(address, SocketOptions::default()).await
    }

    pub async fn connect_with_socket_options<A>(address: A,
                                                socket_options: SocketOptions) -> std::io::Result<Self>
        where A: ToSocketAddrs {

        let mut tcp_stream = tokio::net::TcpStream::connect(address).await?;

        socket_options.apply_to_tcpstream(&tcp_stream)?;


        tcp_stream.write_all(&PAIR_HANDSHAKE_PACKET[..]).await?;
        
        let mut incoming_handshake= [0u8; 8];
        tcp_stream.read_exact(&mut incoming_handshake).await?;

        if incoming_handshake != PAIR_HANDSHAKE_PACKET {
            return Err(std::io::Error::from(std::io::ErrorKind::InvalidData))
        }

        let codec = NanomsgPairCodec::new();

        let framed_parts = tokio_util::codec::FramedParts::new::<&[u8]>(tcp_stream, codec);
        let framed = Framed::from_parts(framed_parts);
        Ok(Self {
            inner: framed
        })
    }


    pub async fn listen(address: &str) -> std::io::Result<impl Stream<Item = std::io::Result<(SocketAddr, NanomsgPair)>> + Unpin> {
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
            stream.write_all(&PAIR_HANDSHAKE_PACKET[..]).await?;
            let address = stream.peer_addr()?;

            let mut incoming_handshake= [0u8; 8];
            stream.read_exact(&mut incoming_handshake).await?;
    
            if incoming_handshake != PAIR_HANDSHAKE_PACKET {
                return Err(std::io::Error::from(std::io::ErrorKind::InvalidData))
            }
            let codec = NanomsgPairCodec::new();

            let framed_parts = tokio_util::codec::FramedParts::new::<&[u8]>(stream, codec);
            let framed = Framed::from_parts(framed_parts);
            Ok((address, NanomsgPair {
                inner: framed
            }))
        })))
    }
}



impl Stream for NanomsgPair {
    type Item = std::io::Result<Vec<u8>>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.inner.poll_next(cx)
    }
}


impl <I> Sink<I> for NanomsgPair 
    where I: Deref<Target=[u8]>{
    type Error = std::io::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        <Framed<TcpStream, NanomsgPairCodec> as Sink<I>>::poll_ready(this.inner, cx)
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
        <Framed<TcpStream, NanomsgPairCodec> as Sink<I>>::poll_flush(this.inner, cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        <Framed<TcpStream, NanomsgPairCodec> as Sink<I>>::poll_close(this.inner, cx)
    }
}

enum DecodingState {
    Size,
    Payload(usize)
}

struct NanomsgPairCodec {
    decoding_state: DecodingState,
}

impl NanomsgPairCodec {
    pub fn new() -> Self {
        Self {
            decoding_state: DecodingState::Size,
        }
    }
}

impl <T>Encoder<T> for NanomsgPairCodec 
    where T: std::ops::Deref<Target = [u8]> {
    type Error = std::io::Error;

    fn encode(&mut self, item: T, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.reserve(item.len() + 8);
        dst.put_u64(item.len() as _);
        dst.put(item.as_ref());
        Ok(())
    }
}

impl Decoder for NanomsgPairCodec {
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

