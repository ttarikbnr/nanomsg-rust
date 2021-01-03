use nanomsg_rust::NanomsgPipeline;
use futures::{SinkExt, StreamExt};
use std::io::{self, Error, ErrorKind};

#[tokio::main]
async fn main() {
    tokio::spawn( async {
        if let Err(err) = consumer("127.0.0.1:4536").await {
            eprintln!("Consumer Err: {}", err);
        }
    });

    if let Err(err) = producer("localhost:4536").await {
        eprintln!("Producer Err: {}", err);
    }
}

async fn consumer(address: &str) -> io::Result<()> {
    let mut pipeline_listener = NanomsgPipeline::listen(address).await?;

    while let Some((socket_addr, pipeline_socket)) = pipeline_listener.next()
                                                              .await
                                                              .transpose()? {
        println!("Incoming pipeline socket. Address {}", socket_addr);

        tokio::spawn(run_echo(pipeline_socket));
    }

    let error = Error::new(ErrorKind::Other, "Pipeline listener ended unexpectedly.");

    Err(error)
}

async fn run_echo(mut pipeline_socket: NanomsgPipeline) -> io::Result<()> {
    while let Some(packet) = pipeline_socket.next()
                                        .await
                                        .transpose()? {

        println!("Producer incoming packet: {}", String::from_utf8_lossy(&packet));
    }

    let error = Error::new(ErrorKind::Other, "None came.");

    Err(error)
}

async fn producer(address: &str) -> io::Result<()> {
    let mut pipeline_socket = NanomsgPipeline::connect(address).await?;

    pipeline_socket.send(&b"hello"[..]).await?;

    Ok(())
}