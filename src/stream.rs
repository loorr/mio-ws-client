use std::io;
use std::io::{Read, Write};
use mio::{Interest, Poll, Token};
use mio::net::TcpStream;
use rustls::{ClientConnection, StreamOwned};

pub enum MaybeTlsStream {
    Plain(TcpStream),
    NativeTls(StreamOwned<ClientConnection, TcpStream>),
}


impl Read for MaybeTlsStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            MaybeTlsStream::Plain(stream) => stream.read(buf),
            MaybeTlsStream::NativeTls(stream) => stream.read(buf),
        }
    }
}

impl Write for MaybeTlsStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            MaybeTlsStream::Plain(stream) => stream.write(buf),
            MaybeTlsStream::NativeTls(stream) => stream.write(buf),
        }
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        match self {
            MaybeTlsStream::Plain(stream) => stream.write_all(buf),
            MaybeTlsStream::NativeTls(stream) => stream.write_all(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            MaybeTlsStream::Plain(stream) => stream.flush(),
            MaybeTlsStream::NativeTls(stream) => stream.flush(),
        }
    }
}

impl MaybeTlsStream {
    pub fn register(&mut self, token: Token, poll: &Poll) -> std::io::Result<()> {
        let stream = match self {
            MaybeTlsStream::Plain(stream) => stream,
            MaybeTlsStream::NativeTls(stream) => &mut stream.sock,
        };
        poll.registry().register(
            stream,
            token,
            Interest::WRITABLE | Interest::READABLE,
        )
    }

    pub fn deregister(&mut self, poll: &Poll) -> io::Result<()> {
        let stream = match self {
            MaybeTlsStream::Plain(stream) => stream,
            MaybeTlsStream::NativeTls(stream) => &mut stream.sock,
        };
        poll.registry().deregister(stream)
    }
}
