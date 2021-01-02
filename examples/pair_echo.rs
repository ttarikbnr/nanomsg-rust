use nanomsg_rust::NanomsgPair;
use futures::{SinkExt, StreamExt};
use std::io::{self, Error, ErrorKind};


#[tokio::main]
async fn main() {
    tokio::spawn( async {
        if let Err(err) = listener("127.0.0.1:4536").await {
            eprintln!("Listener Err: {}", err);
        }
    });

    if let Err(err) = run_client("localhost:4536").await {
        eprintln!("Client Err: {}", err);
    }
}


async fn listener(address: &str) -> io::Result<()> {
    let mut pair_listener = NanomsgPair::listen(address)
                                         .await?;
    while let Some((socket_addr, pair_socket)) = pair_listener.next()
                                                              .await
                                                              .transpose()? {
        println!("Incoming pair socket. Address {}", socket_addr);

        tokio::spawn(run_echo(pair_socket));
    }

    let error = Error::new(ErrorKind::Other, "Pair listener ended unexpectedly.");

    Err(error)
}

async fn run_echo(mut pair_socket: NanomsgPair) -> io::Result<()> {
    while let Some(packet) = pair_socket.next()
                                        .await
                                        .transpose()? {

        println!("Server Incoming Packet: {}", String::from_utf8_lossy(&packet));

        if let Err(_) = pair_socket.send(packet).await {
            break
        }
    }

    let error = Error::new(ErrorKind::Other, "None came.");

    Err(error)
}

async fn run_client(address: &str) -> io::Result<()> {
    let mut socket = NanomsgPair::connect(address).await?;

    socket.send(&b"hello"[..]).await?;

    // Read incoming packet
    match socket.next()
                .await
                .transpose()? {
        Some(packet) => {
            println!("Client Incoming Packet: {}", String::from_utf8_lossy(&packet));
        }
        None => {
            let error = Error::new(ErrorKind::Other, "Client socket ended unexpectedly.");
            return Err(error)
        }
    }

    Ok(())
}

