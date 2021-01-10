use tokio::net::{TcpStream, TcpListener, ToSocketAddrs};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::{UnboundedSender,
                        UnboundedReceiver,
                        unbounded_channel,
                        error::SendError};
use tokio::time::sleep;
use futures::{ StreamExt, SinkExt };
use std::{sync::Arc, io, collections::VecDeque };
use tokio_util::codec::{ Framed, FramedParts};
use tokio::sync::Mutex;
use super::options::SocketOptions;
use super::size_payload_codec::SizePayloadCodec;

const PIPELINE_HANDSHAKE_PACKET : [u8; 8] = [0x00, 0x53, 0x50, 0x00, 0x00, 0x50, 0x00, 0x00];

type Senders = Arc<Mutex<VecDeque<UnboundedSender<Vec<u8>>>>>;

pub struct NanomsgPush {
    senders : Senders,
    bound   : bool
}

impl NanomsgPush {
    pub fn new() -> Self {
        Self {
            senders : Arc::new(Mutex::new(VecDeque::new())),
            bound: false
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

        self.senders.lock().await.push_front(sender);

        spawn_push_socket(Some(address),
                          socket_options,
                          framed,
                          recv);
        Ok(())
    }

    pub async fn bind<A>(&mut self, address: A) -> io::Result<()> 
        where A: ToSocketAddrs {

        if self.bound {
            let kind = io::ErrorKind::Other;
            return Err(io::Error::new(kind, "Socket is already bound."));
        }

        let tcp_listener = TcpListener::bind(address).await?;

        let senders = self.senders.clone();

        let listener = NanomsgPushListener::new(tcp_listener,
                                                SocketOptions::default(),
                                                senders);
        self.bound = true;

        tokio::spawn(async move {
            listener.run().await;
        });

        Ok(())
    }

    pub async fn push(&mut self, packet: Vec<u8>) -> io::Result<()> {
        let mut item = Some(packet);

        loop {
            // Pop a sender from back of the double ended vec
            if let Some(sender) = self.senders
                                      .lock()
                                      .await
                                      .pop_back() {
                
                // In case of send error get back the packet and drop the sender
                if let Err(SendError(packet)) = sender.send(item.take()
                                                                .unwrap()) {// Unwrapping here is ok
                    item = Some(packet);
                    // Try sending with another sender
                    continue
                }

                self.senders.lock().await.push_front(sender);

                return Ok(())
            } else {

                // We don't have any available socket
                return Ok(())
            }
        }
    }
}

struct NanomsgPushSocket {
    framed  : Framed<TcpStream, SizePayloadCodec>,
    recv    : UnboundedReceiver<Vec<u8>>
}

impl NanomsgPushSocket {
    fn new(framed   : Framed<TcpStream, SizePayloadCodec>,
           recv     : UnboundedReceiver<Vec<u8>>) -> Self {
        Self {
            framed,
            recv
        }
    }

    // Return some if reconnection is neccessary
    async fn run(mut self) -> Option<UnboundedReceiver<Vec<u8>>> {
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

        None
    }
}

struct NanomsgPushListener {
    listener        : TcpListener,
    socket_options  : SocketOptions,
    senders         : Senders
}

impl NanomsgPushListener {
    fn new(listener        : TcpListener,
           socket_options  : SocketOptions,
           senders         : Senders) -> Self {
        Self {
                listener,
                socket_options,
                senders
        }
    }

    async fn run(mut self) {
        while let Some(tcpstream_res) = self.listener
                                            .next()
                                            .await {
            match tcpstream_res {
                Ok(tcpstream) => {
                    let socket_options = self.socket_options.clone();
                    let senders = self.senders.clone();
                    
                    tokio::spawn(async move {
                        match accept_push(tcpstream, &socket_options).await {
                            Ok(framed) => {     
                                let (sender, recv) = unbounded_channel();
                                senders.lock().await.push_front(sender);
                                spawn_push_socket::<String>(None, socket_options, framed, recv);
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

        log::error!("NanomsgPush listener is ended unexpectedly.")
    }
}

async fn connect_push<A>(address        : &A,
                         socket_options : &SocketOptions) -> io::Result<Framed<TcpStream, SizePayloadCodec>>
where A: ToSocketAddrs + Send + Sync + 'static {
    let mut tcp_stream = tokio::net::TcpStream::connect(address).await?;

    socket_options.apply_to_tcpstream(&tcp_stream)?;

    tcp_stream.write_all(&PIPELINE_HANDSHAKE_PACKET[..]).await?;

    let mut incoming_handshake= [0u8; 8];
    tcp_stream.read_exact(&mut incoming_handshake).await?;

    if incoming_handshake != PIPELINE_HANDSHAKE_PACKET {
    return Err(io::Error::from(io::ErrorKind::InvalidData))
    }

    let codec = SizePayloadCodec::new();

    let framed_parts = FramedParts::new::<&[u8]>(tcp_stream, codec);
    Ok(Framed::from_parts(framed_parts))
}

fn spawn_push_socket<A>(address         : Option<A>,
                        socket_options  : SocketOptions,
                        framed          : Framed<TcpStream, SizePayloadCodec>,
                        recv            : UnboundedReceiver<Vec<u8>>)
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
                            log::error!("Can't reconnect to pull socket. {}", err);

                            // When reconnection fails take a breath
                            sleep(socket_options.get_reconnect_try_interval()).await;
                        
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

async fn accept_push(mut tcp_stream: TcpStream,
                     socket_options: &SocketOptions) -> io::Result<Framed<TcpStream, SizePayloadCodec>> {
    tcp_stream.write_all(&PIPELINE_HANDSHAKE_PACKET[..]).await?;

    socket_options.apply_to_tcpstream(&tcp_stream)?;

    let mut incoming_handshake = [0u8; 8];
    tcp_stream.read_exact(&mut incoming_handshake).await?;

    if incoming_handshake != PIPELINE_HANDSHAKE_PACKET {
        return Err(io::Error::from(io::ErrorKind::InvalidData))
    }

    let framed_parts = FramedParts::new::<&[u8]>(tcp_stream, SizePayloadCodec::new());

    Ok(Framed::from_parts(framed_parts))
}