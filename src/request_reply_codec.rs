use tokio_util::codec::{ Encoder, Decoder };
use bytes::{BytesMut, BufMut, Buf};

enum DecodingState {
    Size,
    RequestId(usize),
    Payload(usize, u32)
}

pub struct RequestReplyCodec {
    decoding_state  : DecodingState
}

impl RequestReplyCodec {
    pub fn new() -> Self {
        Self {
            decoding_state  : DecodingState::Size,
        }
    }
}

impl <T>Encoder<(u32, T)> for RequestReplyCodec
    where T: std::ops::Deref<Target = [u8]> {
    type Error = std::io::Error;

    fn encode(&mut self, item: (u32, T), dst: &mut BytesMut) -> Result<(), Self::Error> {
        let payload_size = item.1.len() as u64;
        dst.put_u64(payload_size + 4);
        dst.put_u32(item.0 | 0x80000000);
        dst.put(item.1.as_ref());
        Ok(())
    }
}

impl Decoder for RequestReplyCodec {
    type Item = (u32, Vec<u8>);
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {

        use std::mem::replace;

        loop {
            match self.decoding_state {
                DecodingState::Size => {
                    if src.remaining() >= 8 {
                        let size = src.get_u64() as usize;
                        let _ = replace(&mut self.decoding_state, DecodingState::RequestId(size - 4));
                    } else {
                        return Ok(None)
                    }
                }
                DecodingState::RequestId(payload_size) => {
                    if src.remaining() >= 4 {
                        let request_id = src.get_u32() & 0x7FFFFFFF;
                        let _ = replace(&mut self.decoding_state, DecodingState::Payload(payload_size, request_id));
                    } else {
                        return Ok(None)
                    }
                }
                DecodingState::Payload(payload_size, request_id) => {
                    if src.remaining() >= payload_size {
                        let payload = src.split_to(payload_size);
                        let _ = replace(&mut self.decoding_state, DecodingState::Size);
                        return Ok(Some((request_id, payload.to_vec())))
                    } else {
                        return Ok(None)
                    }
                }
            }
        }
    }
}