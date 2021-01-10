use tokio_util::codec::{ Encoder, Decoder };
use bytes::{BytesMut, BufMut, Buf};

enum DecodingState {
    Size,
    Payload(usize)
}

pub struct SizePayloadCodec {
    decoding_state: DecodingState,
}

impl SizePayloadCodec {
    pub fn new() -> Self {
        Self {
            decoding_state: DecodingState::Size,
        }
    }
}

impl <T>Encoder<T> for SizePayloadCodec
    where T: std::ops::Deref<Target = [u8]> {
    type Error = std::io::Error;

    fn encode(&mut self, item: T, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.reserve(item.len() + 8);
        dst.put_u64(item.len() as _);
        dst.put(item.as_ref());
        Ok(())
    }
}

impl Decoder for SizePayloadCodec {
    type Item = Vec<u8>;
    type Error = std::io::Error;

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