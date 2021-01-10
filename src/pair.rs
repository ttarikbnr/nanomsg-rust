use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use futures::{Sink, Stream, StreamExt};
use std::{io, pin::Pin, task::{Poll, Context}};
use pin_project::*;
use tokio_util::codec::{Framed};
use std::ops::Deref;
use std::net::SocketAddr;
use super::options::SocketOptions;
use super::size_payload_codec::SizePayloadCodec;


const PAIR_HANDSHAKE_PACKET : [u8; 8] = [0x00, 0x53, 0x50, 0x00, 0x00, 0x10, 0x00, 0x00];

#[pin_project]
pub struct NanomsgPair {
    #[pin]
    inner : tokio_util::codec::Framed<TcpStream, SizePayloadCodec>
}

impl NanomsgPair {
    pub async fn connect<A: ToSocketAddrs>(address: A) -> io::Result<Self> {
        Self::connect_with_socket_options(address, SocketOptions::default()).await
    }

    pub async fn connect_with_socket_options<A>(address: A,
                                                socket_options: SocketOptions) -> io::Result<Self>
        where A: ToSocketAddrs {

        let mut tcp_stream = tokio::net::TcpStream::connect(address).await?;

        socket_options.apply_to_tcpstream(&tcp_stream)?;


        tcp_stream.write_all(&PAIR_HANDSHAKE_PACKET[..]).await?;
        
        let mut incoming_handshake= [0u8; 8];
        tcp_stream.read_exact(&mut incoming_handshake).await?;

        if incoming_handshake != PAIR_HANDSHAKE_PACKET {
            return Err(io::Error::from(io::ErrorKind::InvalidData))
        }

        let codec = SizePayloadCodec::new();

        let framed_parts = tokio_util::codec::FramedParts::new::<&[u8]>(tcp_stream, codec);
        let framed = Framed::from_parts(framed_parts);
        Ok(Self {
            inner: framed
        })
    }


    pub async fn listen<A>(address: A) -> io::Result<impl Stream<Item = io::Result<(SocketAddr, NanomsgPair)>> + Unpin>
        where A: ToSocketAddrs {

        let listener = tokio::net::TcpListener::bind(address).await?;

        Ok(Box::pin(listener.then(|stream| async {
            let mut stream = stream?;
            stream.write_all(&PAIR_HANDSHAKE_PACKET[..]).await?;
            let address = stream.peer_addr()?;

            let mut incoming_handshake= [0u8; 8];
            stream.read_exact(&mut incoming_handshake).await?;
    
            if incoming_handshake != PAIR_HANDSHAKE_PACKET {
                return Err(io::Error::from(io::ErrorKind::InvalidData))
            }
            let codec = SizePayloadCodec::new();

            let framed_parts = tokio_util::codec::FramedParts::new::<&[u8]>(stream, codec);
            let framed = Framed::from_parts(framed_parts);
            Ok((address, NanomsgPair {
                inner: framed
            }))
        })))
    }
}



impl Stream for NanomsgPair {
    type Item = io::Result<Vec<u8>>;

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
    type Error = io::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        <Framed<TcpStream, SizePayloadCodec> as Sink<I>>::poll_ready(this.inner, cx)
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
        <Framed<TcpStream, SizePayloadCodec> as Sink<I>>::poll_flush(this.inner, cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        <Framed<TcpStream, SizePayloadCodec> as Sink<I>>::poll_close(this.inner, cx)
    }
}