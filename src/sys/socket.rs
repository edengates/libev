/*
 * Copyright 2026 Ada Morgan, John Regan and the libev contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use bytes::BytesMut;
use rustix::io::Errno;
#[cfg(not(target_os = "macos"))]
use rustix::net::SocketFlags;
use socket2::{Domain, SockAddr, SockRef, Socket, Type};
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

fn new_socket(domain: Domain) -> io::Result<Socket> {
    #[cfg(target_os = "macos")]
    {
        let socket = Socket::new(domain, Type::STREAM, None)?;
        socket.set_nonblocking(true)?;
        Ok(socket)
    }
    #[cfg(not(target_os = "macos"))]
    Socket::new(domain, Type::STREAM.nonblocking(), None)
}

pub(crate) fn listener(
    address: &SocketAddr,
    backlog: i32,
    is_address_shared: bool,
    is_fallback_enabled: bool,
) -> io::Result<TcpListener> {
    let listener = new_socket(Domain::for_address(*address))?;
    listener.set_reuse_address(true)?;
    if is_address_shared {
        listener.set_reuse_port(true)?;
    }
    if address.is_ipv6() {
        listener.set_only_v6(!is_fallback_enabled)?;
    }
    listener.bind(&SockAddr::from(*address))?;
    listener.listen(backlog)?;
    Ok(TcpListener::from(listener))
}

pub(crate) fn client(address: &SocketAddr) -> io::Result<TcpStream> {
    let socket = new_socket(Domain::for_address(*address))?;
    if address.is_ipv6() {
        if let Err(error) = socket.set_only_v6(false) {
            if error.raw_os_error() != Some(Errno::AFNOSUPPORT.raw_os_error()) {
                return Err(error);
            }
        }
    }
    Ok(TcpStream::from(socket))
}

pub(crate) fn connect(stream: &TcpStream, address: &SocketAddr) -> io::Result<bool> {
    let address = SockAddr::from(*address);
    loop {
        match SockRef::from(stream).connect(&address) {
            Ok(()) => return Ok(true),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) if error.raw_os_error() == Some(Errno::INPROGRESS.raw_os_error()) => return Ok(false),
            Err(error) => return Err(error),
        }
    }
}

pub(crate) fn connected(stream: &TcpStream) -> io::Result<bool> {
    match SockRef::from(stream).take_error()? {
        None => Ok(true),
        Some(error) if error.raw_os_error() == Some(Errno::INPROGRESS.raw_os_error()) => Ok(false),
        Some(error) => Err(error),
    }
}

pub(crate) fn accept(listener: &TcpListener) -> io::Result<TcpStream> {
    loop {
        #[cfg(target_os = "macos")]
        let data = SockRef::from(listener).accept().and_then(|(socket, _)| {
            socket.set_nonblocking(true)?;
            socket.set_nosigpipe(true)?;
            Ok(socket)
        });
        #[cfg(not(target_os = "macos"))]
        let data = rustix::net::accept_with(listener, SocketFlags::NONBLOCK | SocketFlags::CLOEXEC)
            .map(Socket::from)
            .map_err(io::Error::from);
        match data {
            Ok(socket) => return Ok(TcpStream::from(socket)),
            Err(error) if error.kind() != io::ErrorKind::Interrupted => return Err(error),
            Err(_) => {}
        }
    }
}

pub(crate) fn receive(stream: &TcpStream, buffer: &mut BytesMut) -> io::Result<usize> {
    loop {
        match SockRef::from(stream).recv(buffer.spare_capacity_mut()) {
            Ok(length) => {
                unsafe { buffer.set_len(buffer.len() + length) };
                return Ok(length);
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

pub(crate) fn init(stream: &TcpStream, tcp_no_delay: bool, close_timeout: Option<Duration>) -> io::Result<()> {
    if tcp_no_delay {
        stream.set_nodelay(true)?;
    }
    if close_timeout.is_some() {
        SockRef::from(stream).set_linger(close_timeout)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustix::event::{PollFd, PollFlags, Timespec, poll};
    use std::os::fd::AsFd;

    fn await_ready(fd: &impl AsFd, events: PollFlags) {
        let timeout = Timespec { tv_sec: 5, tv_nsec: 0 };
        assert_eq!(poll(&mut [PollFd::new(fd, events)], Some(&timeout)).unwrap(), 1);
    }

    #[test]
    fn init() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let _client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        super::init(&server, true, Some(Duration::from_secs(5))).unwrap();

        assert!(server.nodelay().unwrap());
        assert_eq!(SockRef::from(&server).linger().unwrap(), Some(Duration::from_secs(5)));
    }

    #[test]
    fn listener() {
        let any = "127.0.0.1:0".parse().unwrap();
        let owned = super::listener(&any, 128, false, true).unwrap();
        assert!(super::listener(&owned.local_addr().unwrap(), 128, true, true).is_err());

        let shared = super::listener(&any, 128, true, true).unwrap();
        assert!(super::listener(&shared.local_addr().unwrap(), 128, true, true).is_ok());
    }

    #[test]
    fn fallback() {
        let any = "[::]:0".parse().unwrap();
        let dual = super::listener(&any, 128, false, true).unwrap();
        let port = dual.local_addr().unwrap().port();
        assert!(TcpStream::connect(("127.0.0.1", port)).is_ok());

        let single = super::listener(&any, 128, false, false).unwrap();
        assert!(SockRef::from(&single).only_v6().unwrap());
    }

    #[test]
    fn connect() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let client = client(&address).unwrap();
        if !super::connect(&client, &address).unwrap() {
            await_ready(&client, PollFlags::OUT);
        }
        assert!(connected(&client).unwrap());
        let (server, _) = listener.accept().unwrap();
        assert_eq!(client.peer_addr().unwrap(), address);
        assert_eq!(server.peer_addr().unwrap(), client.local_addr().unwrap());
    }

    #[test]
    fn accept() {
        let listener = super::listener(&"127.0.0.1:0".parse().unwrap(), 128, false, true).unwrap();
        assert_eq!(super::accept(&listener).unwrap_err().kind(), io::ErrorKind::WouldBlock);
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        await_ready(&listener, PollFlags::IN);
        let server = super::accept(&listener).unwrap();
        assert_eq!(server.peer_addr().unwrap(), client.local_addr().unwrap());
    }

    #[test]
    fn receive() {
        use std::io::Write;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        client.write_all(b"hello world").unwrap();

        let mut buffer = BytesMut::with_capacity(5);
        assert_eq!(super::receive(&server, &mut buffer).unwrap(), 5);
        assert_eq!(&buffer[..], b"hello");
        buffer.reserve(16);
        assert_eq!(super::receive(&server, &mut buffer).unwrap(), 6);
        assert_eq!(&buffer[..], b"hello world");

        drop(client);
        assert_eq!(super::receive(&server, &mut BytesMut::with_capacity(4)).unwrap(), 0);
    }
}
