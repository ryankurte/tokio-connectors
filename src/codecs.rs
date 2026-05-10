use std::fmt::Debug;

use serde::{Serialize, de::DeserializeOwned};
use tracing::{error, trace};

use crate::error::Error;

/// JSON streaming codec
pub struct Json;

/// Postcard + COBS codec
pub struct PostcardCobs;

/// Null codec, discards all outgoing messages and produces no incoming messages
pub struct Null;

/// Bytes codec, passes through raw bytes without any framing or serialization
pub struct Bytes;

/// An abstract codec for encoding and decoding messages to/from byte streams.
///
/// These use an accumulator to allow for chunks of data to be accumulated until a complete message (or messages)
/// can be parsed from the buffer.
pub trait Codec<OUT: Send + 'static, IN: Send + 'static>: Send + Sized + 'static {
    /// Encode an outgoing message into a byte vector for transmission
    fn encode(item: &OUT) -> Result<Vec<u8>, Error>;

    /// Try to decode a complete message from the accumulated input buffer
    ///
    /// This should drain the consumed bytes from the buffer.
    fn try_decode(buff: &mut Vec<u8>) -> Result<Option<IN>, Error>;
}

/// [Codec] implementation for JSON serialization using serde_json
impl<OUT: Serialize + Debug + Send + 'static, IN: DeserializeOwned + Debug + Send + 'static>
    Codec<OUT, IN> for Json
{
    fn encode(item: &OUT) -> Result<Vec<u8>, Error> {
        let b = serde_json::to_vec(item).map_err(|e| Error::Json(e))?;

        trace!("Encoded {item:?} to bytes: {:?}", b);

        Ok(b)
    }

    fn try_decode(buff: &mut Vec<u8>) -> Result<Option<IN>, Error> {
        // Create a stream deserializer from the accumulated buffer and try to parse a complete message
        let mut deserializer = serde_json::Deserializer::from_slice(&buff).into_iter::<IN>();

        let res = match deserializer.next() {
            Some(Ok(cmd)) => {
                // Successfully parsed a complete message, remove the consumed bytes from the buffer
                Ok(Some(cmd))
            }
            Some(Err(e)) if e.is_eof() => {
                // Not enough data yet
                Ok(None)
            }
            Some(Err(e)) => {
                error!("Failed to deserialize message: {:?}", e);
                trace!("Buffer: {:?}", buff);
                Err(Error::Json(e))
            }
            None => Ok(None), // No more messages in the buffer
        };

        let consumed = deserializer.byte_offset();
        buff.drain(..consumed);

        res
    }
}

/// [Codec] implementation for Postcard serialization with COBS framing
impl<OUT: Serialize + Debug + Send + 'static, IN: DeserializeOwned + Debug + Send + 'static>
    Codec<OUT, IN> for PostcardCobs
{
    fn encode(item: &OUT) -> Result<Vec<u8>, Error> {
        let b = postcard::to_allocvec_cobs(item).map_err(|e| Error::Postcard(e))?;

        trace!("Encoded {item:?} to bytes: {:?}", b);

        Ok(b)
    }

    fn try_decode(buff: &mut Vec<u8>) -> Result<Option<IN>, Error> {
        // Try to parse complete messages from the accumulator
        // by looking for COBS frame boundaries (0x00) and deserializing with postcard
        let pos = match buff.iter().position(|&b| b == 0) {
            Some(pos) => pos,
            None => return Ok(None), // No complete frame yet
        };

        let mut frame = buff.drain(..=pos).collect::<Vec<u8>>();

        match postcard::from_bytes_cobs::<IN>(&mut frame) {
            Ok(cmd) => Ok(Some(cmd)),
            Err(e) => {
                error!("Failed to deserialize message: {:?}", e);
                trace!("Buffer: {:?}", frame);
                Err(Error::Postcard(e))
            }
        }
    }
}

/// [Codec] implementation for a null codec that discards all outgoing messages and produces no incoming messages
impl<OUT: Send + 'static, IN: Send + 'static> Codec<OUT, IN> for Null {
    fn encode(_item: &OUT) -> Result<Vec<u8>, Error> {
        Ok(Vec::new())
    }

    fn try_decode(_buff: &mut Vec<u8>) -> Result<Option<IN>, Error> {
        Ok(None)
    }
}

/// [Codec] implementation for a bytes codec that passes through raw bytes without any
/// framing or serialization
impl Codec<Vec<u8>, Vec<u8>> for Bytes {
    fn encode(item: &Vec<u8>) -> Result<Vec<u8>, Error> {
        Ok(item.clone())
    }

    fn try_decode(buff: &mut Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        if buff.is_empty() {
            Ok(None)
        } else {
            Ok(Some(buff.split_off(0)))
        }
    }
}
