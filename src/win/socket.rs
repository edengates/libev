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
use socket2::{Domain, SockAddr, SockRef, Socket, Type};
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::windows::io::AsRawSocket;
use std::ptr;
use std::slice;
use std::time::Duration;
use windows_sys::Win32::Networking::WinSock::{
    SOCKET, SOCKET_ERROR, SOL_SOCKET, SO_EXCLUSIVEADDRUSE, SO_UPDATE_CONNECT_CONTEXT, WSAGetLastError, setsockopt,
};

/// Creates a new socket and binds it to the provided address.
///
/// If `is_address_shared` is `true`, the address can be reused by another socket (`SO_REUSEADDR`), otherwise no other
/// socket can be bound to the same address (`SO_EXCLUSIVEADDRUSE`). For an IPv6 address, IPv4 connections are also
/// accepted if `is_fallback_enabled` is `true`. The socket starts to listen with the provided `backlog`.
///
/// # Arguments
///
/// - `address` — The address of the socket
/// - `backlog` — The maximum amount of pending connections
/// - `is_address_shared` — Whether the address can be reused by another socket
/// - `is_fallback_enabled` — Whether IPv4 connections are also accepted for an IPv6 address
///
/// # Errors
///
/// Returns an error:
/// - If the socket could not be created
/// - If the socket could not be configured
/// - If the socket could not be bound
/// - If the socket could not start to listen
///
/// # Returns
///
/// The bound socket.
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
        let data = unsafe {
            setsockopt(
                listener.as_raw_socket() as SOCKET,
                SOL_SOCKET,
                SO_EXCLUSIVEADDRUSE,
                (&1i32 as *const i32).cast(),
                size_of::<i32>() as i32,
            )
        };
        if data == SOCKET_ERROR {
            return Err(last_error());
        }
    }
    if address.is_ipv6() {
        listener.set_only_v6(!is_fallback_enabled)?;
    }
    listener.bind(&SockAddr::from(*address))?;
    listener.listen(backlog)?;
    Ok(TcpListener::from(listener))
}

/// Creates a new socket, which can connect to the provided address.
///
/// The socket has the same IP version as the address and is **not** bound or connected yet, see
/// [`socket_packet::connect`](crate::handle::socket_packet::connect).
///
/// # Arguments
///
/// - `address` — The address to connect to
///
/// # Errors
///
/// Returns an error if the socket could not be created.
///
/// # Returns
///
/// The new socket.
pub(crate) fn client(address: &SocketAddr) -> io::Result<TcpStream> {
    Ok(TcpStream::from(socket(address.is_ipv4())?))
}

/// Updates the socket after the connection has been established.
///
/// This **must** be called once the `ConnectEx` operation has completed (`SO_UPDATE_CONNECT_CONTEXT`), otherwise
/// functions like [`TcpStream::peer_addr`] can **not** be used.
///
/// # Arguments
///
/// - `stream` — The connection, which has been established with `ConnectEx`
///
/// # Errors
///
/// Returns an error if the socket could not be updated.
///
/// # See also
///
/// - [`socket_packet::connect`](crate::handle::socket_packet::connect)
pub fn connected(stream: &TcpStream) -> io::Result<()> {
    let data = unsafe {
        setsockopt(
            stream.as_raw_socket() as SOCKET,
            SOL_SOCKET,
            SO_UPDATE_CONNECT_CONTEXT,
            ptr::null(),
            0,
        )
    };
    if data == SOCKET_ERROR {
        Err(last_error())
    } else {
        Ok(())
    }
}

/// Creates a new socket for IPv4 or IPv6.
///
/// The socket is a stream socket, which can be registered with the
/// [`SocketQueueManager`](crate::handle::SocketQueueManager).
///
/// # Arguments
///
/// - `ipv4` — Whether the socket should use IPv4, otherwise IPv6
///
/// # Errors
///
/// Returns an error if the socket could not be created.
///
/// # Returns
///
/// The new socket.
pub(super) fn socket(ipv4: bool) -> io::Result<Socket> {
    Socket::new(if ipv4 { Domain::IPV4 } else { Domain::IPV6 }, Type::STREAM, None)
}

/// Converts the provided address to the bytes, which are used by `ConnectEx`.
///
/// # Arguments
///
/// - `address` — The address to convert
///
/// # Returns
///
/// The bytes of the address.
pub(super) fn socket_address(address: &SocketAddr) -> BytesMut {
    let address = SockAddr::from(*address);
    BytesMut::from(unsafe { slice::from_raw_parts(address.as_ptr().cast::<u8>(), address.len() as usize) })
}

/// Receives the next bytes into the provided buffer.
///
/// **Only** the free capacity of the buffer is used, the received bytes are added to the buffer.
///
/// # Arguments
///
/// - `stream` — The connection
/// - `buffer` — The buffer, which receives the bytes
///
/// # Errors
///
/// Returns:
/// - [`WouldBlock`](io::ErrorKind::WouldBlock) if no bytes are available yet and the socket does not block, see
///   [`init`]
/// - An error if no bytes could be received
///
/// # Returns
///
/// The amount of received bytes, `0` if the connection has been closed.
pub(crate) fn receive(stream: &TcpStream, buffer: &mut BytesMut) -> io::Result<usize> {
    let length = SockRef::from(stream).recv(buffer.spare_capacity_mut())?;
    unsafe { buffer.set_len(buffer.len() + length) };
    Ok(length)
}

/// Initializes the socket with the provided configuration.
///
/// If `tcp_no_delay` is `true`, `TCP_NODELAY` is enabled. If `close_timeout` is set, it is used as `SO_LINGER` of
/// the socket. Then the socket does **not** block the current thread anymore, if no bytes are available.
///
/// # Arguments
///
/// - `stream` — The connection
/// - `tcp_no_delay` — Whether `TCP_NODELAY` should be enabled
/// - `close_timeout` — The timeout, which is used as `SO_LINGER`, or `None`
///
/// # Errors
///
/// Returns an error if the configuration could not be set.
///
/// # See also
///
/// - [`receive`]
pub(crate) fn init(stream: &TcpStream, tcp_no_delay: bool, close_timeout: Option<Duration>) -> io::Result<()> {
    if tcp_no_delay {
        stream.set_nodelay(true)?;
    }
    if close_timeout.is_some() {
        SockRef::from(stream).set_linger(close_timeout)?;
    }
    stream.set_nonblocking(true)
}

/// Returns the last error of the current thread.
///
/// This is the result of `WSAGetLastError`.
pub(super) fn last_error() -> io::Error {
    io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
}

#[cfg(test)]
mod tests {
    use super::*;

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
