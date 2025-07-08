use std::fmt;

pub mod protocol;
pub mod solt_map;

/// WebSocket message opcode as in RFC 6455.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum OpCode {
    /// Data (text or binary).
    Data(Data),
    /// Control message (close, ping, pong).
    Control(Control),
}

/// Data opcodes as in RFC 6455
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Data {
    /// 0x0 denotes a continuation frame
    Continue,
    /// 0x1 denotes a text frame
    Text,
    /// 0x2 denotes a binary frame
    Binary,
    /// 0x3-7 are reserved for further non-control frames
    Reserved(u8),
}

/// Control opcodes as in RFC 6455
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Control {
    /// 0x8 denotes a connection close
    Close,
    /// 0x9 denotes a ping
    Ping,
    /// 0xa denotes a pong
    Pong,
    /// 0xb-f are reserved for further control frames
    Reserved(u8),
}

impl fmt::Display for Data
where
    Self: Sized,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Data::Continue => write!(f, "CONTINUE"),
            Data::Text => write!(f, "TEXT"),
            Data::Binary => write!(f, "BINARY"),
            Data::Reserved(x) => write!(f, "RESERVED_DATA_{x}"),
        }
    }
}

impl fmt::Display for Control
where
    Self: Sized,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Control::Close => write!(f, "CLOSE"),
            Control::Ping => write!(f, "PING"),
            Control::Pong => write!(f, "PONG"),
            Control::Reserved(x) => write!(f, "RESERVED_CONTROL_{x}"),
        }
    }
}

impl fmt::Display for OpCode
where
    Self: Sized,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            OpCode::Data(d) => d.fmt(f),
            OpCode::Control(c) => c.fmt(f),
        }
    }
}

impl From<OpCode> for u8 {
    fn from(code: OpCode) -> Self {
        use self::{
            Control::{Close, Ping, Pong},
            Data::{Binary, Continue, Text},
            OpCode::*,
        };
        match code {
            Data(Continue) => 0,
            Data(Text) => 1,
            Data(Binary) => 2,
            Data(self::Data::Reserved(i)) => i,

            Control(Close) => 8,
            Control(Ping) => 9,
            Control(Pong) => 10,
            Control(self::Control::Reserved(i)) => i,
        }
    }
}

impl From<u8> for OpCode {
    fn from(byte: u8) -> OpCode {
        use self::{
            Control::{Close, Ping, Pong},
            Data::{Binary, Continue, Text},
            OpCode::*,
        };
        match byte {
            0 => Data(Continue),
            1 => Data(Text),
            2 => Data(Binary),
            i @ 3..=7 => Data(self::Data::Reserved(i)),
            8 => Control(Close),
            9 => Control(Ping),
            10 => Control(Pong),
            i @ 11..=15 => Control(self::Control::Reserved(i)),
            _ => panic!("Bug: OpCode out of range"),
        }
    }
}
