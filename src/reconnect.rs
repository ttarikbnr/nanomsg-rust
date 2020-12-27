use futures::{Future, Stream, ready, pin_mut};
use std::{io, pin::Pin, task::{Poll, Context}};
use pin_project::*;

pub trait Reconnectable: Sized{
    type ConnectFut : Future<Output = Result< Self, (Self, std::io::Error)>> + Unpin;

    fn reconnect(self) -> Self::ConnectFut;
}


#[pin_project(project = ReconnectProj)]
pub enum Reconnect<T>
    where T : Reconnectable {
    
    // In order to change state from Connected to Reconnecting 
    // we should be able to call reconnect which moves self
    // So we wrap inner socket by Option
    Connected( #[pin] Option<T>),

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

    type Item = Vec<u8>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            let new_state = match self.as_mut().project() {
                ReconnectProj::Connected(mut inner) => {
                
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
                                Err(_io_err) => {
                                    // TODO log

                                    let socket = (*inner).take().unwrap();

                                    reconnect(socket)
                                }
                            }
                        }
                        None => {
                            // TODO log
                            
                            let socket = (*inner).take().unwrap();

                            reconnect(socket)
                        }
                    }
                }
                ReconnectProj::Reconnecting(connect_fut) => {

                    match ready!(connect_fut.poll(cx)) {
                        Ok(socket) => {
                            Reconnect::Connected( Some(socket) )
                        }
                        Err((inner, io_err)) => {
                            log::error!("Can't reconnect. {}", io_err);

                            reconnect(inner)
                        }
                    }
                }
            };

            self.set(new_state);
        }


        // // If we are in reconnection state poll connection future to get a
        // // new connection
        // if let Some(connect_fut) = this.connect_fut.get_mut() {
        //     pin_mut!(connect_fut);

        //     match ready!(connect_fut.poll(cx)) {
        //         Ok(socket) => { this.inner.set(socket)}
        //         Err(err) => {
        //             let connect_fut = this.inner.connect();
        //             this.connect_fut.set(Some(connect_fut));
        //         }
        //     }
        // }

        // match ready!(this.inner.poll_next(cx)) {
        //     Some(ready) => {
        //         match ready {
        //             Ok(data) => return Poll::Ready(Some(Ok(data))),
        //             Err(err) => {
        //                 unimplemented!()
        //             }
        //         }
        //     }
        //     None => {
        //         unimplemented!()
        //     }
        // }
    }
}
