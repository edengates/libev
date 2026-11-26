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

use crate::handle::event::Event;
use bytes::BytesMut;
use socket2::{Domain, SockAddr, SockRef, Socket, Type};
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::windows::io::AsRawSocket;
use std::ptr;
use std::slice;
use std::time::Duration;
use windows_sys::Win32::Networking::WinSock::{
    SOCKET, SOCKET_ERROR, SOL_SOCKET, SO_EXCLUSIVEADDRUSE, SO_UPDATE_ACCEPT_CONTEXT, SO_UPDATE_CONNECT_CONTEXT,
    WSAGetLastError, setsockopt,
};

pub(crate) fn somaxconn() -> i32 {
    0x7fffffff
}

pub(crate) fn listener(
    address: &SocketAddr,
    backlog: i32,
    is_address_shared: bool,
    is_fallback_enabled: bool,
) -> io::Result<TcpListener> {
    let listener = socket(address.is_ipv4())?;
    if is_address_shared {
        listener.set_reuse_address(true)?;
    } else {
        set_option(&listener, SO_EXCLUSIVEADDRUSE, Some(&1i32))?;
    }
    if address.is_ipv6() {
        listener.set_only_v6(!is_fallback_enabled)?;
    }
    listener.bind(&SockAddr::from(*address))?;
    listener.listen(backlog)?;
    Ok(TcpListener::from(listener))
}

pub(crate) fn client(address: &SocketAddr) -> io::Result<TcpStream> {
    Ok(TcpStream::from(socket(address.is_ipv4())?))
}

pub(crate) fn accepted(listener: &TcpListener, packet: &mut Event) -> io::Result<TcpStream> {
    let socket = packet.take_socket().ok_or(io::ErrorKind::InvalidInput)?;
    set_option(&socket, SO_UPDATE_ACCEPT_CONTEXT, Some(&(listener.as_raw_socket() as SOCKET)))?;
    Ok(TcpStream::from(socket))
}

pub(crate) fn connected(stream: &TcpStream) -> io::Result<()> {
    set_option::<()>(stream, SO_UPDATE_CONNECT_CONTEXT, None)
}

fn set_option<T>(socket: &impl AsRawSocket, name: i32, value: Option<&T>) -> io::Result<()> {
    let (value, length) = match value {
        Some(value) => ((value as *const T).cast(), size_of::<T>() as i32),
        None => (ptr::null(), 0),
    };
    let data = unsafe { setsockopt(socket.as_raw_socket() as SOCKET, SOL_SOCKET, name, value, length) };
    if data == SOCKET_ERROR {
        Err(last_error())
    } else {
        Ok(())
    }
}

pub(crate) fn socket(ipv4: bool) -> io::Result<Socket> {
    Socket::new(if ipv4 { Domain::IPV4 } else { Domain::IPV6 }, Type::STREAM, None)
}

pub(crate) fn socket_address(address: &SocketAddr) -> BytesMut {
    let address = SockAddr::from(*address);
    BytesMut::from(unsafe { slice::from_raw_parts(address.as_ptr().cast::<u8>(), address.len() as usize) })
}

pub(crate) fn receive(stream: &TcpStream, buffer: &mut BytesMut) -> io::Result<usize> {
    let length = SockRef::from(stream).recv(buffer.spare_capacity_mut())?;
    unsafe { buffer.set_len(buffer.len() + length) };
    Ok(length)
}

pub(crate) fn init(
    stream: &TcpStream,
    tcp_no_delay: bool,
    is_keep_alive_enabled: bool,
    keep_alive: Option<Duration>,
    close_timeout: Option<Duration>,
) -> io::Result<()> {
    super::option::init(stream, tcp_no_delay, is_keep_alive_enabled, keep_alive, close_timeout)?;
    stream.set_nonblocking(true)
}

pub(crate) fn last_error() -> io::Error {
    io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init() {
        use std::io::Read;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let _client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (mut server, _) = listener.accept().unwrap();
        super::init(&server, false, false, None, None).unwrap();

        assert_eq!(server.read(&mut [0; 1]).unwrap_err().kind(), io::ErrorKind::WouldBlock);
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
