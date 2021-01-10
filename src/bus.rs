use std::{pin::Pin, task::{Poll, Context}, io, sync::Arc, ops::Deref};
use futures::{Sink, Stream, StreamExt, SinkExt};
use tokio::net::{TcpStream, TcpListener, ToSocketAddrs};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::codec::{Framed, FramedParts};
use tokio::time::sleep;
use tokio::sync::mpsc::{unbounded_channel,
                        UnboundedReceiver,
                        UnboundedSender};
use tokio::sync::broadcast;
use super::size_payload_codec::SizePayloadCodec;
                    
use pin_project::*;

use crate::options::SocketOptions;

const BUS_HANDSHAKE_PACKET : [u8; 8] = [0x00, 0x53, 0x50, 0x00, 0x00, 0x70, 0x00, 0x00];

#[pin_project]
pub struct NanomsgBus {
    socket_tx       : UnboundedSender<Vec<u8>>,

    #[pin]
    rx              : UnboundedReceiver<Vec<u8>>,

    broadcaster     : broadcast::Sender<Arc<Vec<u8>>>,

    bound           : bool
}

impl NanomsgBus {
    pub fn new() -> Self {
        let (bc_sender, _)  = broadcast::channel(2048);
        let (sender, recv)  = unbounded_channel();

        Self {
            socket_tx   : sender,

            rx          : recv,

            broadcaster : bc_sender,

            bound       : false,
        }
    } 

    pub async fn connect<A>(&self, address: A) -> io::Result<()>
        where A: ToSocketAddrs + Sync + Send + 'static {

        self.connect_with_socket_options(address, SocketOptions::default()).await
    }

    pub async fn connect_with_socket_options<A>(&self,
                                                address        : A,
                                                socket_options : SocketOptions) -> io::Result<()>
        where A: ToSocketAddrs + Sync + Send + 'static {

        let framed = connect(&address, &socket_options).await?;
        spawn_socket(framed,
                     self.socket_tx.clone(),
                     self.broadcaster.clone(),
                     socket_options.clone(),
                     Some(address));
        Ok(())
    }

    pub async fn bind<A>(&mut self, address: A) -> io::Result<()> 
        where A: ToSocketAddrs {
        
        if self.bound {
            let kind = io::ErrorKind::Other;
            return Err(io::Error::new(kind, "Socket is already bound."));
        }

        let tcp_listener = TcpListener::bind(address).await?;

        let listener = NanomsgBusListener::new(tcp_listener,
                                               SocketOptions::default(),
                                               self.socket_tx.clone(),
                                               self.broadcaster.clone());
        self.bound = true;

        tokio::spawn(async move {
            listener.run().await;
        });

        Ok(())
    }

    // TODO return result 
    pub fn send(&self, packet: Vec<u8>) {
        let _ = self.broadcaster.send(Arc::new(packet));
    }
}

struct NanomsgBusListener {
    listener        : TcpListener,
    socket_options  : SocketOptions,
    tx_channel      : UnboundedSender<Vec<u8>>,
    broadcaster     : broadcast::Sender<Arc<Vec<u8>>>,
}

impl NanomsgBusListener {
    fn new(listener        : TcpListener,
           socket_options  : SocketOptions,
           tx_channel      : UnboundedSender<Vec<u8>>,
           broadcaster     : broadcast::Sender<Arc<Vec<u8>>>) -> Self {
        
        Self {
            listener,
            socket_options,
            tx_channel,
            broadcaster,
        }
    }


    // TODO Check if bus is alive
    async fn run(mut self) {
        while let Some(tcpstream_res) = self.listener
                                            .next()
                                            .await {
            match tcpstream_res {
                Ok(tcpstream) => {
                    let tx_channel = self.tx_channel.clone();
                    let broadcaster = self.broadcaster.clone();
                    let socket_options = self.socket_options.clone();
                    tokio::spawn(async move {
                        match accept(tcpstream, &socket_options).await {
                            Ok(framed) => {
                                spawn_socket::<String>(framed,
                                             tx_channel,
                                             broadcaster,
                                             socket_options,
                                             None)
                            }
                            Err(err) => {
                                log::error!("Can't handshake. {}", err);
                            }
                        }
                    });
                }
                Err(err) => {
                    log::error!("Can't accept tcpstream. {}", err);
                }
            }
        }

        log::error!("Bus listener is ended unexpectedly.")
    }
}


// If address is None the socket won't try to reconnect
fn spawn_socket<A>(framed           : Framed<TcpStream, SizePayloadCodec>,
                   rx_channel       : UnboundedSender<Vec<u8>>,
                   broadcaster      : broadcast::Sender<Arc<Vec<u8>>>,
                   socket_options   : SocketOptions,
                   address          : Option<A>) 
    where A: ToSocketAddrs + Sync + Send + 'static {

    tokio::spawn(async move {
        let mut socket = NanomsgBusSocket::new(framed, rx_channel.clone(), broadcaster.subscribe());

        'outer:
        loop {
            if !socket.run().await {
                return
            }
            // TODO reconnecting log

            // Reconnect loop
            'reconnect_loop:
            loop {
                let reconn_delay = socket_options.get_reconnect_try_interval();

                sleep(reconn_delay).await;

                if let Some(ref address) = address {
                    match connect(address, &socket_options).await {
                        Ok(framed) => {
                            let tx_channel = broadcaster.subscribe();
                            let rx_channel = rx_channel.clone();
                            socket = NanomsgBusSocket::new(framed, rx_channel, tx_channel);
                            continue 'outer
                        }
                        Err(_) => {
                            continue 'reconnect_loop
                        }
                    }
                } else {
                    return 
                }
            }
        }
    });
}

async fn connect<A>(address         : &A,
                    socket_options  : &SocketOptions) -> io::Result<Framed<TcpStream, SizePayloadCodec>> 
    where A: ToSocketAddrs  {

    let mut tcp_stream = tokio::net::TcpStream::connect(address).await?;

    socket_options.apply_to_tcpstream(&tcp_stream)?;


    tcp_stream.write_all(&BUS_HANDSHAKE_PACKET[..]).await?;
    
    let mut incoming_handshake= [0u8; 8];

    tcp_stream.read_exact(&mut incoming_handshake).await?;

    if incoming_handshake != BUS_HANDSHAKE_PACKET {
        return Err(io::Error::from(io::ErrorKind::InvalidData))
    }

    let framed_parts = FramedParts::new::<&[u8]>(tcp_stream, SizePayloadCodec::new());

    Ok(Framed::from_parts(framed_parts))
}

async fn accept(mut tcp_stream   : TcpStream,
                socket_options  : &SocketOptions) -> io::Result<Framed<TcpStream, SizePayloadCodec>> {
    tcp_stream.write_all(&BUS_HANDSHAKE_PACKET[..]).await?;

    socket_options.apply_to_tcpstream(&tcp_stream)?;

    let mut incoming_handshake = [0u8; 8];
    tcp_stream.read_exact(&mut incoming_handshake).await?;

    if incoming_handshake != BUS_HANDSHAKE_PACKET {
        return Err(io::Error::from(io::ErrorKind::InvalidData))
    }

    let framed_parts = FramedParts::new::<&[u8]>(tcp_stream, SizePayloadCodec::new());

    Ok(Framed::from_parts(framed_parts))
}


struct NanomsgBusSocket {
    framed              : Framed<TcpStream, SizePayloadCodec>,
    rx_channel          : UnboundedSender<Vec<u8>>,
    tx_channel          : broadcast::Receiver<Arc<Vec<u8>>>
}

impl NanomsgBusSocket {
    fn new(framed           : Framed<TcpStream, SizePayloadCodec>,
           rx_channel       : UnboundedSender<Vec<u8>>,
           tx_channel       : broadcast::Receiver<Arc<Vec<u8>>>) -> Self {

        Self {
            framed,
            rx_channel,
            tx_channel,
        }
    }

    // Returns true if we should try reconnect
    async fn run(mut self) -> bool {
        use tokio::sync::broadcast::error::RecvError;

        loop {
            tokio::select!{
                Some(Ok(packet)) = self.framed.next() => {
                    if self.rx_channel.send(packet).is_err() {
                        return false
                    }
                },
                packet_res = self.tx_channel.recv() => {
                    match packet_res {
                        Ok(packet) => {
                            if self.framed.send(&packet[..]).await.is_err() {
                                return true
                            }
                        }
                        Err(RecvError::Closed) => {
                            return false
                        }
                        Err(RecvError::Lagged(_)) => {
                            continue
                        }
                    }

                },
                else => {
                    return true
                }
            };
        }
    }
}


impl Stream for NanomsgBus {
    type Item = Vec<u8>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match futures::ready!(this.rx.poll_next(cx)) {
            Some(packet) => { Poll::Ready(Some(packet))}
            None => { Poll::Pending } // TODO check if are there any reconnecting socket exist
        }
    }
}

impl <I> Sink<I> for NanomsgBus 
    where I: Deref<Target=[u8]>{
    type Error = io::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(())) // TODO
    }

    fn start_send(self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        let _ = self.broadcaster.send(Arc::new(item.to_vec())); // TODO error handling
        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(())) // TODO
    }

    fn poll_close(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(())) // TODO
    }
}