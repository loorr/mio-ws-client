use byteorder::{BigEndian, ReadBytesExt};
use std::borrow::Cow;
use std::io::Cursor;
use std::net::{SocketAddr, ToSocketAddrs};

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

#[derive(Debug, PartialEq)]
pub enum WsProtocol {
    Wss,
    Ws,
}

#[derive(Debug)]
pub struct HostInfo {
    pub protocol: WsProtocol,
    pub domain: String,
    pub ip: String,
    pub port: u16,
    pub path: String,
}

impl HostInfo {
    pub fn parse_url(url: &str) -> Option<Self> {
        let (protocol, rest) = if url.starts_with("wss://") {
            (WsProtocol::Wss, url.trim_start_matches("wss://"))
        } else if url.starts_with("ws://") {
            (WsProtocol::Ws, url.trim_start_matches("ws://"))
        } else {
            return None;
        };

        let mut parts = rest.splitn(2, '/');
        let domain_and_port = parts.next().unwrap_or("");
        let path = format!("/{}", parts.next().unwrap_or(""));

        let mut domain_parts = domain_and_port.split(':');
        let domain = domain_parts.next().unwrap_or("");
        let port = domain_parts
            .next()
            .unwrap_or(if protocol == WsProtocol::Wss {
                "443"
            } else {
                "80"
            })
            .parse::<u16>()
            .unwrap_or(if protocol == WsProtocol::Wss { 443 } else { 80 });

        let ip = (domain, port)
            .to_socket_addrs()
            .ok()
            .and_then(|mut addrs| addrs.find(|x| x.is_ipv4()))
            .map(|addr| addr.ip().to_string())
            .unwrap_or_else(|| "parse error".to_string());

        Some(HostInfo {
            protocol,
            domain: domain.to_string(),
            ip,
            port,
            path,
        })
    }

    pub fn socat_addr(&self) -> SocketAddr {
        SocketAddr::new(self.ip.parse().unwrap(), self.port)
    }
}

#[test]
fn test_host_info_parsing() {
    let url = "wss://example.com:8080/path/to/resource";
    let host_info = HostInfo::parse_url(url).unwrap();
    println!("url: {:?}:\n{:#?}", url, host_info);

    let url = "wss://fstream.binance.com/ws/btcusdt@trade";
    let host_info = HostInfo::parse_url(url).unwrap();
    println!("url: {:?}:\n{:#?}", url, host_info);

    let url = "ws://127.0.0.1:8080/some/path";
    let host_info = HostInfo::parse_url(url).unwrap();
    println!("url: {:?}:\n{:#?}", url, host_info);

    let url = "ws://127.0.0.1:8080";
    let host_info = HostInfo::parse_url(url).unwrap();
    println!("url: {:?}:\n{:#?}", url, host_info);

    let url = "ws://127.0.0.1";
    let host_info = HostInfo::parse_url(url).unwrap();
    println!("url: {:?}:\n{:#?}", url, host_info);
}
