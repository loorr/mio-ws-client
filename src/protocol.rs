use std::net::{SocketAddr, ToSocketAddrs};


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
