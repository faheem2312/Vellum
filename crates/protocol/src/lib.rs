//! Vellum's binary wire protocol, loosely inspired by Nasdaq's ITCH/OUCH.
//!
//! Design goals:
//! - Fixed-width fields, little-endian, no external serialization crate.
//! - Every message starts with a 1-byte tag so the reader knows how to parse it.
//! - No heap allocation required to encode/decode a single message.
//!
//! Real HFT protocols look almost exactly like this in spirit: dense
//! fixed-width binary framing instead of a text protocol like FIX, because
//! parsing text is slow and allocates memory.

use std::io::{self, Read, Write};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy = 0,
    Sell = 1,
}

impl Side {
    fn from_u8(b: u8) -> io::Result<Self> {
        match b {
            0 => Ok(Side::Buy),
            1 => Ok(Side::Sell),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "bad side byte")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// Client -> Server: submit a new order.
    NewOrder {
        order_id: u64,
        side: Side,
        price: u64, // price in integer ticks (e.g. cents) — never use floats for money
        qty: u32,
    },
    /// Client -> Server: cancel a previously submitted order.
    Cancel { order_id: u64 },
    /// Server -> Client: order accepted into the book.
    Ack { order_id: u64 },
    /// Server -> Client: order rejected (e.g. unknown id on cancel).
    Reject { order_id: u64, reason: u8 },
    /// Server -> Client: a trade occurred.
    Trade {
        resting_order_id: u64,
        incoming_order_id: u64,
        price: u64,
        qty: u32,
    },
}

const TAG_NEW_ORDER: u8 = 1;
const TAG_CANCEL: u8 = 2;
const TAG_ACK: u8 = 3;
const TAG_REJECT: u8 = 4;
const TAG_TRADE: u8 = 5;

impl Message {
    /// Encode this message into `buf`, returning the number of bytes written.
    /// Never allocates on the heap.
    pub fn encode(&self, buf: &mut [u8]) -> usize {
        match self {
            Message::NewOrder { order_id, side, price, qty } => {
                buf[0] = TAG_NEW_ORDER;
                buf[1..9].copy_from_slice(&order_id.to_le_bytes());
                buf[9] = *side as u8;
                buf[10..18].copy_from_slice(&price.to_le_bytes());
                buf[18..22].copy_from_slice(&qty.to_le_bytes());
                22
            }
            Message::Cancel { order_id } => {
                buf[0] = TAG_CANCEL;
                buf[1..9].copy_from_slice(&order_id.to_le_bytes());
                9
            }
            Message::Ack { order_id } => {
                buf[0] = TAG_ACK;
                buf[1..9].copy_from_slice(&order_id.to_le_bytes());
                9
            }
            Message::Reject { order_id, reason } => {
                buf[0] = TAG_REJECT;
                buf[1..9].copy_from_slice(&order_id.to_le_bytes());
                buf[9] = *reason;
                10
            }
            Message::Trade { resting_order_id, incoming_order_id, price, qty } => {
                buf[0] = TAG_TRADE;
                buf[1..9].copy_from_slice(&resting_order_id.to_le_bytes());
                buf[9..17].copy_from_slice(&incoming_order_id.to_le_bytes());
                buf[17..25].copy_from_slice(&price.to_le_bytes());
                buf[25..29].copy_from_slice(&qty.to_le_bytes());
                29
            }
        }
    }

    /// The number of bytes `encode` will write for this message.
    pub fn encoded_len(&self) -> usize {
        match self {
            Message::NewOrder { .. } => 22,
            Message::Cancel { .. } => 9,
            Message::Ack { .. } => 9,
            Message::Reject { .. } => 10,
            Message::Trade { .. } => 29,
        }
    }

    /// Decode a message from `buf`. Returns the message and bytes consumed.
    pub fn decode(buf: &[u8]) -> io::Result<(Message, usize)> {
        if buf.is_empty() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "empty buffer"));
        }
        let tag = buf[0];
        let need = |n: usize| -> io::Result<()> {
            if buf.len() < n {
                Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short buffer"))
            } else {
                Ok(())
            }
        };
        match tag {
            TAG_NEW_ORDER => {
                need(22)?;
                let order_id = u64::from_le_bytes(buf[1..9].try_into().unwrap());
                let side = Side::from_u8(buf[9])?;
                let price = u64::from_le_bytes(buf[10..18].try_into().unwrap());
                let qty = u32::from_le_bytes(buf[18..22].try_into().unwrap());
                Ok((Message::NewOrder { order_id, side, price, qty }, 22))
            }
            TAG_CANCEL => {
                need(9)?;
                let order_id = u64::from_le_bytes(buf[1..9].try_into().unwrap());
                Ok((Message::Cancel { order_id }, 9))
            }
            TAG_ACK => {
                need(9)?;
                let order_id = u64::from_le_bytes(buf[1..9].try_into().unwrap());
                Ok((Message::Ack { order_id }, 9))
            }
            TAG_REJECT => {
                need(10)?;
                let order_id = u64::from_le_bytes(buf[1..9].try_into().unwrap());
                let reason = buf[9];
                Ok((Message::Reject { order_id, reason }, 10))
            }
            TAG_TRADE => {
                need(29)?;
                let resting_order_id = u64::from_le_bytes(buf[1..9].try_into().unwrap());
                let incoming_order_id = u64::from_le_bytes(buf[9..17].try_into().unwrap());
                let price = u64::from_le_bytes(buf[17..25].try_into().unwrap());
                let qty = u32::from_le_bytes(buf[25..29].try_into().unwrap());
                Ok((Message::Trade { resting_order_id, incoming_order_id, price, qty }, 29))
            }
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "unknown tag")),
        }
    }

    /// Convenience: write directly to any `Write` (e.g. a TcpStream).
    pub fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        let mut buf = [0u8; 32];
        let n = self.encode(&mut buf);
        w.write_all(&buf[..n])
    }
}

/// Reads exactly one message from a `Read` by first peeking the tag byte to
/// know how many more bytes to pull. Phase 2 will replace this with a
/// proper length-delimited framing + reusable read buffer for efficiency.
pub fn read_message<R: Read>(r: &mut R) -> io::Result<Message> {
    let mut tag = [0u8; 1];
    r.read_exact(&mut tag)?;
    let body_len: usize = match tag[0] {
        TAG_NEW_ORDER => 21,
        TAG_CANCEL => 8,
        TAG_ACK => 8,
        TAG_REJECT => 9,
        TAG_TRADE => 28,
        _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "unknown tag")),
    };
    let mut full = [0u8; 32];
    full[0] = tag[0];
    r.read_exact(&mut full[1..1 + body_len])?;
    let (msg, _) = Message::decode(&full[..1 + body_len])?;
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_new_order() {
        let msg = Message::NewOrder {
            order_id: 42,
            side: Side::Buy,
            price: 10_050,
            qty: 100,
        };
        let mut buf = [0u8; 32];
        let n = msg.encode(&mut buf);
        assert_eq!(n, msg.encoded_len());
        let (decoded, consumed) = Message::decode(&buf[..n]).unwrap();
        assert_eq!(consumed, n);
        assert_eq!(decoded, msg);
    }

    #[test]
    fn round_trip_all_variants() {
        let msgs = vec![
            Message::NewOrder { order_id: 1, side: Side::Sell, price: 999, qty: 5 },
            Message::Cancel { order_id: 1 },
            Message::Ack { order_id: 1 },
            Message::Reject { order_id: 1, reason: 7 },
            Message::Trade { resting_order_id: 1, incoming_order_id: 2, price: 999, qty: 5 },
        ];
        for m in msgs {
            let mut buf = [0u8; 32];
            let n = m.encode(&mut buf);
            let (decoded, _) = Message::decode(&buf[..n]).unwrap();
            assert_eq!(decoded, m);
        }
    }
}