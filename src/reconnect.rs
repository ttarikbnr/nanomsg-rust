use futures::{Future, Sink, Stream, pin_mut, ready};
use std::{io, pin::Pin, task::{Poll, Context}};
use pin_project::*;
use std::ops::Deref;

pub trait Reconnectable: Sized{
    // Since there is a possibility of failure of reconnnection
    // We should get underlying socket to try again in case of an Error
    type ConnectFut : Future<Output = Result< Self, (Self, std::io::Error)>> + Unpin;

    // TODO: how can we make this to take by ref
    fn reconnect(self) -> Self::ConnectFut;
}


#[pin_project(project = ReconnectProj)]
pub enum Reconnect<T>
    where T : Reconnectable {
    
    // In order to change state from Connected to Reconnecting 
    // we should be able to call Reconnactable::reconnect which takes self
    // So we wrap inner socket by Option and use take().unwrap() to get inner
    Connected( Option<T>),

    Reconnecting( #[pin] <T as Reconnectable>::ConnectFut )
}

fn reconnect<T>(socket: T) -> Reconnect<T> 
    where T : Reconnectable {
    let connect_fut = socket.reconnect();
    Reconnect::Reconnecting(connect_fut)
}


impl <T> Stream for Reconnect<T> 
    where T : Reconnectable,
          T : Stream< Item = io::Result<Vec<u8>>> + Unpin {

    // In case of an error we try to reconnect
    // There is no need to return a Result for now
    type Item = Vec<u8>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            let new_state = match self.as_mut().project() {
                ReconnectProj::Connected(inner) => {
                    let ready = match (*inner).as_mut() {
                        Some(socket) => {
                            pin_mut!(socket);
                            ready!(socket.poll_next(cx))
                        }
                        None => {
                            // If we are in connected state
                            // there is no way we have None
                            unreachable!()
                        }
                    };


                    match ready {
                        Some(ready) => {
                            match ready {
                                Ok(data) => return Poll::Ready(Some(data)),
                                Err(err) => {
                                    log::error!("Got error from socket. {}", err);
                                    log::info!("Reconnecting...");
                                }
                            }
                        }
                        None => {
                            log::error!("None came from socket.");
                            log::info!("Reconnecting...");
                        }
                    }
                    
                    let socket = (*inner).take().unwrap();

                    reconnect(socket)
                }
                ReconnectProj::Reconnecting(connect_fut) => {

                    match ready!(connect_fut.poll(cx)) {
                        Ok(socket) => {
                            Reconnect::Connected( Some(socket) )
                        }
                        Err((inner, io_err)) => {
                            log::error!("Can't reconnect. {}", io_err);
                            log::info!("Reconnecting...");

                            reconnect(inner)
                        }
                    }
                }
            };

            self.set(new_state);
        }

    }
}


// Here we are just delegating to inner types sink methods.
// We don't drive reconnection mechanism
impl <I, T> Sink<I> for Reconnect<T> 
    where I : Deref<Target=[u8]>,
          T : Reconnectable,
          T : Sink<I, Error=io::Error> + Unpin{
    type Error = io::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>
    ) -> Poll<Result<(), Self::Error>> {
        match self.project() {
            ReconnectProj::Connected(inner) => {
                match inner {
                    Some(socket) => {
                        pin_mut!(socket);
                        return socket.poll_ready(cx)
                    }
                    None => {
                        return Poll::Pending
                    }
                }
            }
            ReconnectProj::Reconnecting(_) => {
                return Poll::Pending
            }
        }
    }

    // Items that pushed in reconnecting state will be dropped
    fn start_send(self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        match self.project() {
            ReconnectProj::Connected(inner) => {
                match inner {
                    Some(socket) => {
                        pin_mut!(socket);
                        return socket.start_send(item)
                    }
                    None => {
                        // If we are in reconnect phase item will be dropped
                        return Ok(())
                    }
                }
            }
            ReconnectProj::Reconnecting(_) => {
                // If we are in reconnect phase item will be dropped
                return Ok(())
            }
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>
    ) -> Poll<Result<(), Self::Error>> {
        match self.project() {
            ReconnectProj::Connected(inner) => {
                match inner {
                    Some(socket) => {
                        pin_mut!(socket);
                        return socket.poll_flush(cx)
                    }
                    None => {
                        return Poll::Pending
                    }
                }
            }
            ReconnectProj::Reconnecting(_) => {
                return Poll::Pending
            }
        }
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>
    ) -> Poll<Result<(), Self::Error>> {
        match self.project() {
            ReconnectProj::Connected(inner) => {
                match inner {
                    Some(socket) => {
                        pin_mut!(socket);
                        return socket.poll_close(cx)
                    }
                    None => {
                        return Poll::Pending
                    }
                }
            }
            ReconnectProj::Reconnecting(_) => {
                return Poll::Pending
            }
        }
    }
}
