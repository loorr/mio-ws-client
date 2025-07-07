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
