use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::prelude::*;
use tokio::time::timeout;
use std::{ops::Deref, pin::Pin, task::{Context, Poll}, time::Duration};
use std::io::ErrorKind;
use futures::{Future, SinkExt, StreamExt};
use tokio_util::codec::Framed;
use futures::{Sink, Stream};
use pin_project::*;
use super::options::SocketOptions;
use super::request_reply_codec::RequestReplyCodec;

const HANDSHAKE_WRITE_TIMEOUT_MS : u64 = 250;
const REP_HANDSHAKE_PACKET : [u8; 8] = [0x00, 0x53, 0x50, 0x00, 0x00, 0x31, 0x00, 0x00];
const REQ_HANDSHAKE_PACKET : [u8; 8] = [0x00, 0x53, 0x50, 0x00, 0x00, 0x30, 0x00, 0x00];

pub struct NanomsgReply<S, F> {
    listener            : Option<TcpListener>,
    service_fn          : S,
    spawn_local         : bool,
    socket_options      : SocketOptions,
    _f                  : std::marker::PhantomData<F> 
}

impl <S, F>NanomsgReply<S, F>
    where S: FnMut(Vec<u8>) -> F + Clone + Send + 'static,
          F: Future<Output=Vec<u8>> + Send + 'static {

    pub fn new(service_fn   : S,
               spawn_local  : bool) -> Self {

        Self::new_with_socket_options(service_fn,
                                      SocketOptions::default(),
                                      spawn_local)
    }

    pub fn new_with_socket_options(service_fn       : S,
                                   socket_options   : SocketOptions,
                                   spawn_local      : bool) -> Self {
        Self {
            service_fn,
            listener        : None,
            spawn_local,
            socket_options,
            _f              : std::marker::PhantomData
        }
    }

    pub async fn bind<A>(&mut self, address: A) -> std::io::Result<()> 
        where A: ToSocketAddrs{
        let listener = TcpListener::bind(address).await?;

        self.listener = Some(listener);
        Ok(())
    }

    async fn accept(&mut self) -> std::io::Result<NanomsgReplySocket<S, F>> {
        if let Some(listener) = self.listener.as_mut() {
            let (mut tcpstream, _socket_addr) = listener.accept().await?;

            self.socket_options.apply_to_tcpstream(&tcpstream)?;

            tcpstream.write_all(&REP_HANDSHAKE_PACKET[..]).await?;

            let duration = Duration::from_millis(HANDSHAKE_WRITE_TIMEOUT_MS);

            let mut handshake = [0u8; 8];

            match timeout(duration, tcpstream.read_exact(handshake.as_mut())).await {
                Ok(_res) => {
                    // Check handshake packet
                    if handshake == REQ_HANDSHAKE_PACKET {
                        let nanomsg_rep_socket = NanomsgReplySocket::new(tcpstream, self.service_fn.clone());
                        Ok(nanomsg_rep_socket)
                    } else {
                        Err(wrong_protocol_err())
                    }
                },
                Err(_err) => {
                    Err(timeout_err("Handshake reading timedout."))
                }
            }
        } else {
            panic!("No socket listener.")
        }
    }

    pub async fn serve(mut self) -> std::io::Result<()> {
        loop {
            match self.accept().await {
                Ok(reply_socket) => {
                    if self.spawn_local {
                        tokio::task::spawn_local(reply_socket.reply());
                    } else {
                        tokio::spawn(reply_socket.reply());
                    }
                }
                Err(err) => {
                    log::error!("Got error while accepting socket. {}", err);
                }
            }
        }
    }
    
}


fn timeout_err(error_msg: &'static str) -> std::io::Error {
    std::io::Error::new(ErrorKind::TimedOut, error_msg)
}

fn wrong_protocol_err() -> std::io::Error {
    std::io::Error::new(ErrorKind::Other, "Wrong protocol.")
}


struct NanomsgReplySocket<S, F> {
    socket      : NanomsgReplyStreamSink,
    service_fn  : S,
    _f          : std::marker::PhantomData<F> 
}

impl <S, F> NanomsgReplySocket <S, F>
    where S: FnMut(Vec<u8>) -> F,
          F: Future<Output=Vec<u8>> + Send + 'static {

    fn new(socket     : TcpStream,
           service_fn : S) -> Self {
        Self {
            socket      : NanomsgReplyStreamSink::new(socket),
            service_fn,
            _f          : std::marker::PhantomData
        }
    }

    async fn reply(mut self) -> std::io::Result<()> {
        // Read request
        while let Some(request_res) = self.socket.next().await {
            let (request_id, payload) = request_res?;

            // Run service function
            let reply = (self.service_fn)(payload).await;

            // Write reply
            self.socket.send((request_id, reply)).await?;
        }

        Ok(())
    }
}



#[pin_project]
struct NanomsgReplyStreamSink {
    #[pin]
    inner           : tokio_util::codec::Framed<TcpStream, RequestReplyCodec>,
}

impl NanomsgReplyStreamSink {
    fn new(tcpstream: TcpStream) -> Self {
        Self {
            inner: Framed::new(tcpstream, RequestReplyCodec::new())
        }
    }
}



impl Stream for NanomsgReplyStreamSink {
    type Item = std::io::Result<(u32, Vec<u8>)>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.inner.poll_next(cx)
    }
}


impl <I> Sink<(u32, I)> for NanomsgReplyStreamSink 
    where I: Deref<Target=[u8]>{
    type Error = std::io::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        <Framed<TcpStream, RequestReplyCodec> as Sink<(u32, I)>>::poll_ready(this.inner, cx)
    }

    fn start_send(self: Pin<&mut Self>, item: (u32, I)) -> Result<(), Self::Error> {
        let this = self.project();
        this.inner.start_send(item)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        <Framed<TcpStream, RequestReplyCodec> as Sink<(u32, I)>>::poll_flush(this.inner, cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        <Framed<TcpStream, RequestReplyCodec> as Sink<(u32, I)>>::poll_close(this.inner, cx)
    }
}