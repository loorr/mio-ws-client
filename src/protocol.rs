use std::borrow::Cow;
use std::io::Cursor;
use byteorder::{BigEndian, ReadBytesExt};

#[derive(Debug, PartialEq)]
pub enum Opcode {
    ContinuationFrame = 0x0,
    TextFrame = 0x1,
    BinaryFrame = 0x2,
    // %x3-7 are reserved for further non-control frames
    ReservedNonControlFrame,
    ConnectionClose = 0x8,
    Ping = 0x9,
    Pong = 0xA,
    // %xB-F are reserved for further control frames
    ReservedControlFrame,
}

#[derive(Debug)]
pub struct Frame<'a> {
    slice: &'a [u8],
    is_masked: bool,
    mask_offset: usize,
    pub payload: &'a [u8],
}

impl<'a> Frame<'a> {
    pub fn from_slice(slice: &'a [u8]) -> Option<Self> {
        let slice_length = slice.len();

        if slice_length < 2
            || slice[1] & 0x7F == 126 && slice_length < 4
            || slice[1] & 0x7F == 127 && slice_length < 10
        {
            return None;
        }

        let is_masked = slice[1] & 0x80 != 0;
        let payload_offset = if is_masked { 6 } else { 2 };
        let (payload_offset, payload_length, mask_offset) = match slice[1] & 0x7F {
            126 => (
                payload_offset + 2,
                Cursor::new(&slice[2..4]).read_u16::<BigEndian>().unwrap() as usize,
                4,
            ),
            127 => (
                payload_offset + 8,
                Cursor::new(&slice[2..10]).read_u64::<BigEndian>().unwrap() as usize,
                10,
            ),
            payload_length => (payload_offset, payload_length as usize, 2),
        };
        let frame_length = payload_offset + payload_length;

        if slice_length >= frame_length {
            Some(Frame {
                slice: &slice[0..frame_length],
                is_masked,
                mask_offset,
                payload: &slice[payload_offset..frame_length],
            })
        } else {
            None
        }
    }

    pub fn is_fin(&self) -> bool {
        self.slice[0] & 0x80 != 0
    }

    pub fn is_rsv1(&self) -> bool {
        self.slice[0] & 0x40 != 0
    }

    pub fn is_rsv2(&self) -> bool {
        self.slice[0] & 0x20 != 0
    }

    pub fn is_rsv3(&self) -> bool {
        self.slice[0] & 0x10 != 0
    }

    pub fn opcode(&self) -> Opcode {
        match self.slice[0] & 0xF {
            0x0 => Opcode::ContinuationFrame,
            0x1 => Opcode::TextFrame,
            0x2 => Opcode::BinaryFrame,
            opcode if (0x3..=0x7).contains(&opcode) => Opcode::ReservedNonControlFrame,
            0x8 => Opcode::ConnectionClose,
            0x9 => Opcode::Ping,
            0xA => Opcode::Pong,
            _ => Opcode::ReservedControlFrame,
        }
    }

    pub fn is_masked(&self) -> bool {
        self.is_masked
    }

    pub fn payload(&self) -> Cow<'a, [u8]> {
        if self.is_masked() {
            Cow::Owned(
                self.payload
                    .iter()
                    .enumerate()
                    .map(|(i, octet)| octet ^ self.slice[self.mask_offset + i % 4])
                    .collect(),
            )
        } else {
            Cow::Borrowed(self.payload)
        }
    }

    pub fn length(&self) -> usize {
        self.slice.len()
    }
}