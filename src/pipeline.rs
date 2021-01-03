use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::{UnboundedSender,
                        UnboundedReceiver,
                        unbounded_channel};
use futures::{Sink, Stream, StreamExt, SinkExt};
use std::{io, pin::Pin, task::{Poll, Context}};
use bytes::{BytesMut, BufMut, Buf};
use pin_project::*;
use tokio_util::codec::{Encoder, Decoder, Framed, FramedParts};
use std::ops::Deref;
use std::net::SocketAddr;
use super::options::SocketOptions;
use std::sync::Arc;

const PIPELINE_HANDSHAKE_PACKET : [u8; 8] = [0x00, 0x53, 0x50, 0x00, 0x00, 0x50, 0x00, 0x00];


pub struct NanomsgPush {
    ind    : usize,
    // Vector of frameds
    senders : Vec<UnboundedSender<Arc<Vec<u8>>>>,
}


impl NanomsgPush {
    pub fn new() -> Self {
        Self {
            ind     : 0,
            senders : Vec::new()
        }
    }

    pub async fn connect<A>(&mut self,
                            address: A) -> io::Result<()> 
        where A: ToSocketAddrs + Send + Sync + 'static {

        self.connect_with_socket_options(address, SocketOptions::default()).await
    }


    pub async fn connect_with_socket_options<A>(&mut self,
                                                address: A,
                                                socket_options: SocketOptions) -> io::Result<()> 
        where A: ToSocketAddrs + Send + Sync + 'static  {
        

        let framed = connect_push(&address,
                                  &socket_options).await?;

        let (sender, recv) = unbounded_channel();

        self.senders.push(sender);

        spawn_push_socket(Some(address),
                          socket_options,
                          framed,
                          recv);
        Ok(())
    }

}

fn spawn_push_socket<A>(address         : Option<A>,
                        socket_options  : SocketOptions,
                        framed          : Framed<TcpStream, NanomsgPipelineCodec>,
                        recv            : UnboundedReceiver<Arc<Vec<u8>>>)
    where A: ToSocketAddrs + Send + Sync + 'static {

    // TODO spawn socket
    tokio::spawn(async move {
        // Implement reconnect
        let mut socket = NanomsgPushSocket::new(framed, recv);

        'outer: loop {
            let recv = match socket.run().await {
                Some(recv) => recv, 
                None => break 'outer,
            };

            // Reconnection
            'reconnect: loop {
                if let Some(ref address) = address {
                    log::info!("Trying to reconnect.");

                    match connect_push(address,
                                       &socket_options).await {
                        Ok(framed) => {
                            socket = NanomsgPushSocket::new(framed, recv);

                            continue 'outer
                        }
                        Err(err) => {
                            log::error!("Can't reconnect to pull socket.");

                            // When reconnection fails take a breath
                            tokio::time::sleep(std::time::Duration::from_secs(4)).await;

                            continue 'reconnect
                        }
                    }
                } else {
                    break 'outer
                }
            }
        }

    });
   
}

async fn connect_push<A>(address        : &A,
                         socket_options : &SocketOptions) -> io::Result<Framed<TcpStream, NanomsgPipelineCodec>>
    where A: ToSocketAddrs + Send + Sync + 'static {
    let mut tcp_stream = tokio::net::TcpStream::connect(address).await?;

    socket_options.apply_to_tcpstream(&tcp_stream)?;

    tcp_stream.write_all(&PIPELINE_HANDSHAKE_PACKET[..]).await?;
    
    let mut incoming_handshake= [0u8; 8];
    tcp_stream.read_exact(&mut incoming_handshake).await?;

    if incoming_handshake != PIPELINE_HANDSHAKE_PACKET {
        return Err(io::Error::from(io::ErrorKind::InvalidData))
    }

    let codec = NanomsgPipelineCodec::new();

    let framed_parts = FramedParts::new::<&[u8]>(tcp_stream, codec);
    Ok(Framed::from_parts(framed_parts))
}



struct NanomsgPushSocket {
    framed  : Framed<TcpStream, NanomsgPipelineCodec>,
    recv    : UnboundedReceiver<Arc<Vec<u8>>>
}

impl NanomsgPushSocket {
    fn new(framed   : Framed<TcpStream, NanomsgPipelineCodec>,
           recv     : UnboundedReceiver<Arc<Vec<u8>>>) -> Self {
        Self {
            framed,
            recv
        }
    }


    // Return true if reconnection is neccessary
    async fn run(mut self) -> Option<UnboundedReceiver<Arc<Vec<u8>>>> {
        // Read from recv
            // Write to socket
        while let Some(packet) = self.recv.next().await {
            if self.framed
                    .send(&packet[..])
                    .await
                    .is_err() {

                return Some(self.recv)
            }
        }

        return None
    }
}



#[pin_project]
pub struct NanomsgPipeline {
    #[pin]
    inner : Framed<TcpStream, NanomsgPipelineCodec>
}

impl NanomsgPipeline {
    pub async fn connect<A: ToSocketAddrs>(address: A) -> io::Result<Self> {
        Self::connect_with_socket_options(address, SocketOptions::default()).await
    }

    pub async fn connect_with_socket_options<A>(address: A,
                                                socket_options: SocketOptions) -> io::Result<Self>
        where A: ToSocketAddrs {

        let mut tcp_stream = tokio::net::TcpStream::connect(address).await?;

        socket_options.apply_to_tcpstream(&tcp_stream)?;


        tcp_stream.write_all(&PIPELINE_HANDSHAKE_PACKET[..]).await?;
        
        let mut incoming_handshake= [0u8; 8];
        tcp_stream.read_exact(&mut incoming_handshake).await?;

        if incoming_handshake != PIPELINE_HANDSHAKE_PACKET {
            return Err(io::Error::from(io::ErrorKind::InvalidData))
        }

        let codec = NanomsgPipelineCodec::new();

        let framed_parts = FramedParts::new::<&[u8]>(tcp_stream, codec);
        let framed = Framed::from_parts(framed_parts);
        Ok(Self {
            inner: framed
        })
    }

    pub async fn listen<A>(address: A) -> io::Result<impl Stream<Item = io::Result<(SocketAddr, NanomsgPipeline)>> + Unpin>
        where A: ToSocketAddrs {

        let listener = tokio::net::TcpListener::bind(address).await?;

        Ok(Box::pin(listener.then(|stream| async {
            let mut stream = stream?;
            stream.write_all(&PIPELINE_HANDSHAKE_PACKET[..]).await?;
            let address = stream.peer_addr()?;

            let mut incoming_handshake= [0u8; 8];
            stream.read_exact(&mut incoming_handshake).await?;
    
            if incoming_handshake != PIPELINE_HANDSHAKE_PACKET {
                return Err(io::Error::from(io::ErrorKind::InvalidData))
            }
            let codec = NanomsgPipelineCodec::new();

            let framed_parts = FramedParts::new::<&[u8]>(stream, codec);
            let framed = Framed::from_parts(framed_parts);
            Ok((address, NanomsgPipeline {
                inner: framed
            }))
        })))
    }
}

impl Stream for NanomsgPipeline {
    type Item = io::Result<Vec<u8>>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.inner.poll_next(cx)
    }
}

impl <I> Sink<I> for NanomsgPipeline 
    where I: Deref<Target=[u8]>{
    type Error = io::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        <Framed<TcpStream, NanomsgPipelineCodec> as Sink<I>>::poll_ready(this.inner, cx)
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
        <Framed<TcpStream, NanomsgPipelineCodec> as Sink<I>>::poll_flush(this.inner, cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        <Framed<TcpStream, NanomsgPipelineCodec> as Sink<I>>::poll_close(this.inner, cx)
    }
}


enum DecodingState {
    Size,
    Payload(usize)
}

struct NanomsgPipelineCodec {
    decoding_state: DecodingState,
}

impl NanomsgPipelineCodec {
    pub fn new() -> Self {
        Self {
            decoding_state: DecodingState::Size,
        }
    }
}

impl <T>Encoder<T> for NanomsgPipelineCodec 
    where T: std::ops::Deref<Target = [u8]> {

    type Error = io::Error;

    fn encode(&mut self, item: T, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.reserve(item.len() + 8);
        dst.put_u64(item.len() as _);
        dst.put(item.as_ref());
        Ok(())
    }
}

impl Decoder for NanomsgPipelineCodec {
    type Item = Vec<u8>;
    type Error = io::Error;

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