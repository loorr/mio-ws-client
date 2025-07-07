use bytes::{Buf, BytesMut};
use crossbeam_channel::{TryRecvError, bounded, unbounded};
use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token, Waker};
use mio_ws::protocol;
use mio_ws::protocol::HostInfo;
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};
use std::cmp::PartialEq;
use std::collections::HashMap;
use std::io::{Cursor, Read, Write};
use std::sync::Arc;
use std::time::Duration;
use std::{io, thread};
use tungstenite::protocol::frame::FrameHeader;
use webpki_roots::TLS_SERVER_ROOTS;

const CLIENT_TOKEN: Token = Token(0);
const WAKER_TOKEN: Token = Token(usize::MAX - 1);

fn main() {
    println!("Hello, world!");

    let tx = handshake_thread(1);
    for i in 0..1000 {
        tx.send("wss://fstream.binance.com/ws/btcusdt@bookTicker".to_string())
            .unwrap();
        thread::sleep(Duration::from_secs(1));
        tx.send("wss://fstream.binance.com/ws/ethusdt@bookTicker".to_string())
            .unwrap();
        thread::sleep(Duration::from_secs(1));
    }

    // join
    loop {
        thread::sleep(Duration::from_secs(1));
    }
}

// PartialEq
#[derive(PartialEq)]
enum HandshakeState {
    Initial,
    WaitingForResponse,
    Completed,
    Failed
}

pub enum MaybeTlsStream {
    Plain(TcpStream),
    NativeTls(StreamOwned<ClientConnection, TcpStream>),
}

impl MaybeTlsStream {
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        match self {
            MaybeTlsStream::Plain(stream) => stream.write_all(buf),
            MaybeTlsStream::NativeTls(stream) => stream.write_all(buf),
        }
    }

    fn register(&mut self, token: Token, poll: &Poll) -> std::io::Result<()> {
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

    fn deregister(&mut self, poll: &Poll) -> io::Result<()> {
        let stream = match self {
            MaybeTlsStream::Plain(stream) => stream,
            MaybeTlsStream::NativeTls(stream) => &mut stream.sock,
        };
        poll.registry().deregister(stream)
    }

    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            MaybeTlsStream::Plain(stream) => stream.read(buf),
            MaybeTlsStream::NativeTls(stream) => stream.read(buf),
        }
    }
}



pub struct WsClient<Callback>
where
    Callback: FnMut(String),
{
    pub client_token: Token,
    pub stream: MaybeTlsStream,
    pub handshake_state: HandshakeState,
    pub host_info: HostInfo,

    pub header: Option<(FrameHeader, u64)>,
    pub in_buffer: BytesMut,
    pub receive_callback: Callback,
}

impl<Callback> WsClient<Callback>
where
    Callback: FnMut(String),
{
    pub fn new(
        client_token: Token,
        stream: MaybeTlsStream,
        host_info: HostInfo,
        callback: Callback,
    ) -> Self {
        WsClient {
            client_token,
            stream,
            handshake_state: HandshakeState::Initial,
            host_info,
            header: None,
            in_buffer: BytesMut::with_capacity(8192),
            receive_callback: callback,
        }
    }

    fn send_handshake(&mut self) -> io::Result<()> {
        let handshake = format!(
            "GET {} HTTP/1.1\r\n\
             Host: server.example.com\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
             Origin: http://example.com\r\n\
             Sec-WebSocket-Protocol: chat, superchat\r\n\
             Sec-WebSocket-Version: 13\r\n\
             \r\n",
            self.host_info.path
        );
        self.stream.write_all(handshake.as_bytes())?;
        self.handshake_state = HandshakeState::WaitingForResponse;
        Ok(())
    }

    fn register(&mut self, poll: &Poll) -> std::io::Result<()> {
        self.stream.register(self.client_token, poll)
    }

    fn deregister(&mut self, poll: &Poll) -> io::Result<()> {
        self.stream.deregister(poll)
    }

    /// fn read_all(stream: &mut TcpStream, buffer: &mut BytesMut) -> io::Result<()> {
    fn read_all(&mut self) -> io::Result<()> {
        let mut buf = [0u8; 4096];
        loop {
            match self.stream.read(&mut buf) {
                Ok(n) => {
                    if n == 0 {
                        println!("服务器关闭连接");
                        return Ok(());
                    }
                    self.in_buffer.extend_from_slice(&buf[..n]);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => {
                    eprintln!("读取数据时发生错误: {}", e);
                    return Err(e);
                }
            };
        }
        Ok(())
    }

    fn encode_packets(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // println!("{}", self.in_buffer.len());
        while self.in_buffer.len() >= 2 {
            // 读取 payload
            let payload = loop {
                {
                    if self.header.is_none() {
                        let mut cursor = Cursor::new(&mut self.in_buffer);
                        self.header = FrameHeader::parse(&mut cursor)?;
                        let advanced = cursor.position();
                        bytes::Buf::advance(&mut self.in_buffer, advanced as _);
                    }

                    if let Some((header, len)) = &self.header {
                        let len = *len as usize;
                        if len <= self.in_buffer.len() {
                            self.header = None;
                            break self.in_buffer.split_to(len);
                        }
                    }
                }
                // Not enough data in buffer.
                self.in_buffer.reserve(self.header.as_ref().map(|(_, l)| *l as usize).unwrap_or(6));
            };
            // 打印payload
            let s = String::from_utf8_lossy(&payload);
            // println!("{}", s);
            (self.receive_callback)(s.to_string())
        }

        Ok(())
    }
}

/// fn read_all(stream: &mut TcpStream, buffer: &mut BytesMut) -> io::Result<()> {
#[warn(dead_code)]
fn read_all(stream: &mut MaybeTlsStream, buffer: &mut BytesMut) -> io::Result<()> {
    let mut buf = [0u8; 4096];
    loop {
        match stream.read(&mut buf) {
            Ok(n) => {
                if n == 0 {
                    println!("服务器关闭连接");
                    return Ok(());
                }
                buffer.extend_from_slice(&buf[..n]);
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) => {
                eprintln!("读取数据时发生错误: {}", e);
                return Err(e);
            }
        };
    }
    Ok(())
}

fn decode_packets(buf: &mut BytesMut) {
    loop {
        if buf.len() < 2 {
            break; // 不足以读长度字段
        }

        let len = u16::from_be_bytes([buf[0], buf[1]]) as usize;
        if buf.len() < 2 + len {
            break; // 不足以读完整 payload
        }

        buf.advance(2); // 丢掉长度字段
        let payload = buf.split_to(len);

        handle_payload(&payload);
    }
}

fn handle_payload(payload: &[u8]) {
    println!("Got message: {:?}", payload);
}

fn handshake_thread(worker_num: usize) -> crossbeam_channel::Sender<String> {
    let mut worker_tx_vec = Vec::with_capacity(worker_num);
    let mut worker_wakers = Vec::with_capacity(worker_num);

    for index in 0..worker_num {
        let (tx, rx) = bounded::<WsClient<_>>(10);
        let mut poll = Poll::new().unwrap();
        let waker = Waker::new(poll.registry(), WAKER_TOKEN).unwrap();
        thread::Builder::new()
            .name(format!("work_{}_th", index).to_string())
            .spawn(move || -> io::Result<()> {
                let mut events = Events::with_capacity(2048);
                let mut client_token_map: HashMap<Token, WsClient<_>> = HashMap::new();

                println!("工作线程开始: {}", index);

                loop {
                    poll.poll(&mut events, None)?;
                    for event in events.iter() {
                        if event.token() == WAKER_TOKEN {
                            while let Ok(mut ws_client) = rx.try_recv() {
                                println!(
                                    "工作线程 {} 接收到客户端: {:?}",
                                    index, ws_client.client_token
                                );
                                ws_client.register(&mut poll).unwrap();
                                client_token_map.insert(ws_client.client_token, ws_client);
                            }
                        } else {
                            let client = match client_token_map.get_mut(&event.token()) {
                                Some(client) => client,
                                None => {
                                    eprintln!("未找到对应的客户端: {:?}", event.token());
                                    continue;
                                }
                            };

                            if event.is_readable() {
                                client.read_all().unwrap();

                                // 处理接收到的数据, 返回解码的数据
                                let _ = client.encode_packets();
                            }
                        }
                    }
                }
            })
            .expect(format!("<UNK>: {}", worker_num).as_str());

        worker_tx_vec.push(tx);
        worker_wakers.push(waker);
    }

    let (handshake_tx, handshake_rx) = unbounded::<String>();
    thread::Builder::new()
        .name("handshake_th".to_string())
        .spawn(move || -> std::io::Result<()> {
            println!("握手线程开始");
            let mut poll = Poll::new().unwrap();
            let mut events = Events::with_capacity(12);
            let mut client_token_map: HashMap<Token, WsClient<_>> = HashMap::new();
            let mut client_id = 0;

            loop {
                match handshake_rx.try_recv() {
                    Ok(url) => {
                        println!("url: {}", url);
                        if let Some(host_info) = HostInfo::parse_url(&url) {
                            println!("握手线程解析域名: {:?}", host_info);
                            let maybe_tls_stream = match host_info.protocol {
                                protocol::WsProtocol::Wss => {
                                    let maybe_tls_stream = match tls_connect(&host_info) {
                                        Ok(stream) => MaybeTlsStream::NativeTls(stream),
                                        Err(e) => {
                                            eprintln!("TLS 连接失败: {}", e);
                                            continue;
                                        }
                                    };
                                    maybe_tls_stream
                                }
                                protocol::WsProtocol::Ws => {
                                    let stream = match TcpStream::connect(host_info.socat_addr()) {
                                        Ok(stream) => stream,
                                        Err(e) => {
                                            eprintln!("TCP 连接失败: {}", e);
                                            continue;
                                        }
                                    };
                                    MaybeTlsStream::Plain(stream)
                                }
                            };
                            client_id += 1;
                            let mut ws_client =
                                WsClient::new(Token(client_id), maybe_tls_stream, host_info, |s| println!("{}", s));

                            println!("握手线程注册客户端: {:?}", ws_client.client_token);
                            ws_client.register(&mut poll).unwrap();
                            client_token_map.insert(ws_client.client_token, ws_client);
                        } else {
                            eprintln!("无法解析 URL: {}", url);
                            continue;
                        }
                    }
                    Err(TryRecvError::Disconnected) => return Ok(()),
                    Err(TryRecvError::Empty) => {}
                };

                poll.poll(&mut events, Some(Duration::from_millis(100)))?;
                for event in events.iter() {
                    if event.is_writable() {
                        if let Some(client) = client_token_map.get_mut(&event.token()) {
                            if client.handshake_state == HandshakeState::Initial {
                                println!("发送握手请求");
                                client.send_handshake()?;
                                client.handshake_state = HandshakeState::WaitingForResponse;
                                println!("发送握手请求done");
                            }
                        }
                    } else if event.is_readable() {
                        if let Some(mut client) = client_token_map.remove(&event.token()) {
                            let mut buf = [0u8; 4096];
                            loop {
                                let n = match client.stream.read(&mut buf) {
                                    Ok(n) => n,
                                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                                        // 非阻塞操作，继续等待
                                        break;
                                    }
                                    Err(e) => {
                                        eprintln!("读取数据时发生错误: {}", e);
                                        return Err(e);
                                    }
                                };
                                if n == 0 {
                                    println!("服务器关闭连接");
                                    continue;
                                }
                                client.in_buffer.extend_from_slice(&buf[..n]);
                            }
                            if client.in_buffer.len() == 0 {
                                println!("没有读取到数据，继续等待");
                                client_token_map.insert(client.client_token, client);
                                continue;
                            }

                            let separator = b"\r\n\r\n";
                            if let Some(end_of_headers_pos) = client
                                .in_buffer
                                .windows(4)
                                .position(|window| window == separator)
                            {
                                // 找到了边界！
                                let handshake_end = end_of_headers_pos + 4;

                                // 将缓冲区分割成两部分：握手响应 和 剩余数据
                                let handshake_data = client.in_buffer.split_to(handshake_end);

                                // 使用 from_utf8_lossy 检查握手响应
                                let response_str = String::from_utf8_lossy(&handshake_data);

                                println!("收到完整的握手响应: \n{}", response_str);

                                if response_str.contains("HTTP/1.1 101 Switching Protocols") {
                                    client.handshake_state = HandshakeState::Completed;
                                    client.deregister(&mut poll).unwrap();

                                    println!("握手成功，WebSocket 连接已建立");
                                    println!("缓冲区中剩余数据 {} 字节，将传递给工作线程", client.in_buffer.len());

                                    // 现在 client 对象包含了 read_buf，里面是第一个WebSocket帧的数据
                                    // 直接将这个 client 发送给工作线程
                                    worker_tx_vec[0].send(client).unwrap();
                                    worker_wakers[0].wake().unwrap();
                                } else {
                                    println!("握手失败，响应内容：\n{}", response_str);
                                    // 可以在这里关闭连接
                                }
                            } else {
                                // 没有找到边界，说明握手响应还没接收完
                                println!("握手数据不完整，等待更多数据...");
                                // 把 client 放回 map 中，等待下一次 readable 事件
                                client_token_map.insert(client.client_token, client);
                            }
                            
                            // 
                            // 
                            // 
                            // 
                            // println!("payload size: {}", client.in_buffer.len());
                            // let response = String::from_utf8_lossy(client.in_buffer.as_ref());
                            // println!("收到服务器响应：\n{:?}", client.in_buffer);
                            // if response.contains("HTTP/1.1 101 Switching Protocols") {
                            //     client.handshake_state = HandshakeState::Completed;
                            //     client.deregister(&mut poll).unwrap();
                            // 
                            //     println!("握手成功，WebSocket 连接已建立");
                            //     worker_tx_vec[0].send(client).unwrap();
                            //     worker_wakers[0].wake().unwrap();
                            // } else {
                            //     println!("握手失败，响应内容：\n{}", response);
                            //     return Ok(());
                            // }
                        }
                    }
                }
            }
        })
        .expect("Failed to spawn handshake thread");
    handshake_tx
}

fn tls_connect(host_info: &HostInfo) -> io::Result<StreamOwned<ClientConnection, TcpStream>> {
    let mut socket = TcpStream::connect(host_info.socat_addr())?;

    // 注册 poll 和 socket
    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(16);
    poll.registry()
        .register(&mut socket, CLIENT_TOKEN, Interest::WRITABLE)?;

    // 等待 socket 可写（TCP 连接完成）
    poll.poll(&mut events, Some(Duration::from_secs(3)))?;
    for event in events.iter() {
        if event.token() == CLIENT_TOKEN && event.is_writable() {
            poll.registry().reregister(
                &mut socket,
                CLIENT_TOKEN,
                Interest::READABLE | Interest::WRITABLE,
            )?;
        }
    }

    let mut root_store = RootCertStore::empty();
    root_store.extend(TLS_SERVER_ROOTS.iter().cloned());

    // 配置 rustls TLS 客户端
    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    let config = Arc::new(config);
    let server_name = ServerName::try_from(host_info.domain.clone()).unwrap();
    let mut tls_conn = ClientConnection::new(config, server_name).unwrap();

    // 进入非阻塞 TLS 握手 loop
    loop {
        // rustls 需要读 socket 的数据
        if tls_conn.wants_read() {
            match tls_conn.read_tls(&mut socket) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "EOF during handshake",
                    ));
                }
                Ok(_) => {
                    tls_conn
                        .process_new_packets()
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // 非阻塞操作，继续等待
                    poll.poll(&mut events, Some(Duration::from_millis(200)))?;
                    // continue;
                }
                Err(e) => {
                    println!("TLS read error: {}", e);
                    return Err(e);
                }
            }
        }

        // rustls 需要写数据到 socket
        if tls_conn.wants_write() {
            match tls_conn.write_tls(&mut socket) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // 非阻塞操作，继续等待
                    poll.poll(&mut events, Some(Duration::from_millis(100)))?;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        if !tls_conn.is_handshaking() {
            break;
        }

        poll.poll(&mut events, Some(Duration::from_millis(100)))?;
    }

    println!("✅ TLS handshake completed with {:?}", host_info.domain);
    // 取消register
    poll.registry().deregister(&mut socket)?;
    // 返回 TLS 流（可用于 WebSocket、HTTPS 等）
    Ok(StreamOwned::new(tls_conn, socket))
}
