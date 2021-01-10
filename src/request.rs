use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{oneshot, mpsc};
use tokio::prelude::*;
use std::{ops::Deref, io, pin::Pin, task::Context};
use std::io::ErrorKind;
use std::task::Poll;
use std::future::Future;
use std::sync::Arc;
use futures::{Sink, Stream, future};
use std::collections::HashMap;
use tokio_util::codec::Framed;
use pin_project::*;
use super::options::SocketOptions;
use super::request_reply_codec::RequestReplyCodec;

const REP_HANDSHAKE_PACKET : [u8; 8] = [0x00, 0x53, 0x50, 0x00, 0x00, 0x31, 0x00, 0x00];
const REQ_HANDSHAKE_PACKET : [u8; 8] = [0x00, 0x53, 0x50, 0x00, 0x00, 0x30, 0x00, 0x00];

use std::sync::atomic::{AtomicUsize, Ordering};

const STATE_STARTING        : usize = 0;
const STATE_RECONNECTING    : usize = 1;
const STATE_CONNECTED       : usize = 2;

#[derive(Clone)]
struct BackgroundTaskStateAtomic{
    state : Arc<AtomicUsize>, 
}

impl BackgroundTaskStateAtomic {
    fn new() -> Self {
        Self {
            state: Arc::new(AtomicUsize::new(STATE_STARTING))
        }
    }

    fn set_connected(&self) {
        self.state.store(STATE_CONNECTED, Ordering::Release);
    }

    fn set_reconnecting(&self) {
        self.state.store(STATE_RECONNECTING, Ordering::Release);
    }
    
    fn get_state(&self) -> BackgroundTaskState {
        let state = self.state.load(Ordering::Acquire);
        match state {
            STATE_STARTING      => BackgroundTaskState::Starting,
            STATE_CONNECTED     => BackgroundTaskState::Connected,
            STATE_RECONNECTING  => BackgroundTaskState::Reconnecting,
            _                   => unreachable!(),
        }
    }
}

struct Request {
    payload         : Vec<u8>,
    reply_sender    : oneshot::Sender<Vec<u8>>,
}

use BackgroundTaskState::*;
#[derive(Debug, Clone, Copy)]
enum BackgroundTaskState {
    Starting,
    Connected,
    Reconnecting,
}

pub struct NanomsgRequest {

    #[allow(unused)]
    address                     : String,
    request_chan                : mpsc::UnboundedSender<Request>,
    background_task_cancel_tx   : Option<oneshot::Sender<()>>,

    #[allow(unused)]
    socket_options              : SocketOptions,
    bg_task_state               : BackgroundTaskStateAtomic,
}

impl NanomsgRequest {
    pub fn new(address: String, spawn_local: bool) -> Self {

        let socket_options = SocketOptions::default();
        Self::new_with_socket_options(address, socket_options, spawn_local)
    }

    pub fn new_with_socket_options(address          : String,
                                   socket_options   : SocketOptions,
                                   spawn_local      : bool) -> Self {
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let (bg_closer_tx, bg_closer_rx) = oneshot::channel();
        let bg_task_state = BackgroundTaskStateAtomic::new();

        spawn_background_task(address.clone(),
                              request_rx, 
                              bg_closer_rx, 
                              socket_options.clone(),
                              bg_task_state.clone(),
                              spawn_local
                            );

        Self {
            address,
            socket_options,
            bg_task_state,
            request_chan              : request_tx,
            background_task_cancel_tx : Some(bg_closer_tx),
        }
    }

    pub fn request(&self, payload: Vec<u8>) -> impl Future<Output=io::Result<Vec<u8>>> + Send + 'static {
        let (tx, rx) = oneshot::channel();
        let request = Request { payload, reply_sender: tx};
        let bg_task_state = self.bg_task_state.get_state();

        match bg_task_state {
            Starting | Reconnecting => {
                if let Err(err) = self.request_chan.send(request) {
                    let err_msg = format!("Couldn't send request to background task. {}", err);
                    future::Either::Left(future::err(other_err(err_msg)))
                } else {
                    future::Either::Right(reply_fut(rx))
                }
            }
            Connected => {
                if let Err(err) = self.request_chan.send(request) {
                    let err_msg = format!("Couldn't send request to background task. {}", err);
                    future::Either::Left(future::err(other_err(err_msg)))
                } else {
                    future::Either::Right(reply_fut(rx))
                }
            }
        }
    }
}

impl Drop for NanomsgRequest {
    fn drop(&mut self) {
        if let Some(tx) = self.background_task_cancel_tx.take() {
            let _ = tx.send(());
        }
    }
}


async fn reply_fut(rx       : oneshot::Receiver<Vec<u8>>) -> io::Result<Vec<u8>> {
    rx.await.map_err(|err| {
        other_err(format!("Got error on reply channel. {}", err))
    })
}

fn spawn_background_task(address                 : String,
                         request_rx              : mpsc::UnboundedReceiver<Request>,
                         background_task_closer  : oneshot::Receiver<()>,
                         socket_options          : SocketOptions, 
                         bg_task_state           : BackgroundTaskStateAtomic,
                         spawn_local             : bool) {
    let background_task = async move {
        tokio::select!{
            reason = run_socket_task(address, socket_options, request_rx, bg_task_state) => {
                log::debug!("Background task is dropped. Caused by inner error. {:?}", reason);
            },
            _ = background_task_closer => {
                log::debug!("Background task is dropped by close signal!");
            }
        }
    };
    if spawn_local {
        tokio::task::spawn_local(background_task);
    } else {
        tokio::spawn(background_task);
    }
}


async fn handshake(socket: &mut TcpStream) -> io::Result<()> {
    socket.write_all(&REQ_HANDSHAKE_PACKET[..]).await?;
    let mut rep_handshake = [0; 8];
    socket.read_exact(&mut rep_handshake).await?;
    if rep_handshake != REP_HANDSHAKE_PACKET {
        Err(wrong_protocol_err())
    } else {
        Ok(())
    }
}


// TODO request id must be produced randomly
fn produce_request_id() -> u32 {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    rng.gen::<u32>() & 0x7FFFFFFF
}

fn other_err<E>(error_msg: E) -> std::io::Error 
    where E: Into<Box<dyn std::error::Error + Send + Sync>>{
    std::io::Error::new(ErrorKind::Other, error_msg)
}

fn wrong_protocol_err() -> std::io::Error {
    std::io::Error::new(ErrorKind::Other, "Wrong protocol.")
}


async fn run_socket_task(address            : String,
                         socket_options     : SocketOptions,
                         request_recv       : mpsc::UnboundedReceiver<Request>,
                         bg_task_state      : BackgroundTaskStateAtomic) {
    use futures::{StreamExt, SinkExt};
    let mut request_recv = request_recv.fuse();
    'outer: loop {
        let mut request_sock = match NanomsgRequestSocket::connect(address.clone(), &socket_options).await {
            Ok(request_socket) => {
                bg_task_state.set_connected();
                request_socket
            }
            Err(err) => {
                log::error!("Can't connect to the request socket. {}", err);
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                bg_task_state.set_reconnecting();
                continue;
            }
        };

        let mut otf_requests = HashMap::new();
        let mut request_id = produce_request_id();
        loop {
            tokio::select! {
                Request{payload, reply_sender} = request_recv.select_next_some() => {
                    advance_request_id(&mut request_id);

                    otf_requests.insert(request_id, reply_sender);

                    if let Err(err) = request_sock.send((request_id, payload)).await {
                        log::error!("Got error while sending to request socket. {}", err);
                        bg_task_state.set_reconnecting();
                        continue 'outer
                    }
                }
                incoming = request_sock.next() => {
                    match incoming {
                        Some(Ok((inc_request_id, payload))) => {
                            if let Some(sender) = otf_requests.remove(&(inc_request_id)) {
                                let _ = sender.send(payload);
                            }
                        }
                        Some(Err(err)) => {
                            log::error!("Got error while reading from request socket. {}", err);
                            bg_task_state.set_reconnecting();
                            continue 'outer
                        }
                        None => {
                            log::error!("Got error while reading from request socket");
                            bg_task_state.set_reconnecting();
                            continue 'outer
                        }
                    }
                }
            }
        }

    }
}

fn advance_request_id(request_id: &mut u32) {
    if *request_id == 0x7FFFFFFF {
        *request_id = 0;
    } else {
        *request_id += 1;
    }
}

#[pin_project]
struct NanomsgRequestSocket {
    #[pin]
    inner           : tokio_util::codec::Framed<TcpStream, RequestReplyCodec>,
}

impl NanomsgRequestSocket {
    pub async fn connect<A: ToSocketAddrs>(address          : A,
                                           socket_options   : &SocketOptions) -> std::io::Result<Self> {
        let mut tcp_stream = tokio::net::TcpStream::connect(address).await?;

        socket_options.apply_to_tcpstream(&tcp_stream)?;

        tokio::time::timeout(socket_options.get_connect_timeout(),
                             handshake(&mut tcp_stream)).await??;

        let framed_parts = tokio_util::codec::FramedParts::new::<(u32, &[u8])>(tcp_stream, RequestReplyCodec::new());
        let framed = Framed::from_parts(framed_parts);
        Ok(Self {
            inner: framed,
        })
    }    
}


impl Stream for NanomsgRequestSocket {
    type Item = std::io::Result<(u32, Vec<u8>)>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.inner.poll_next(cx)
    }
}


impl <I> Sink<(u32, I)> for NanomsgRequestSocket 
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