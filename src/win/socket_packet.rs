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

use crate::handle::socket::{last_error, socket, socket_address};
use socket2::{SockAddr, SockRef};
use bytes::{Buf, BufMut, BytesMut};
use std::io;
use std::mem;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener, TcpStream};
use std::os::windows::io::{AsRawSocket, OwnedSocket};
use std::ptr;
use windows_sys::Win32::Foundation::{NTSTATUS, RtlNtStatusToDosError};
use windows_sys::Win32::Networking::WinSock::{
    AcceptEx, LPFN_CONNECTEX, SIO_GET_EXTENSION_FUNCTION_POINTER,
    SOCKADDR_STORAGE, SOCKET, SOCKET_ERROR, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT, WSABUF, WSAGetLastError,
    WSAID_CONNECTEX, WSAIoctl, WSARecv, WSASend, WSA_IO_PENDING, setsockopt,
};
use windows_sys::Win32::System::IO::{OVERLAPPED, OVERLAPPED_ENTRY};
use windows_sys::core::GUID;

/// The type of an operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Type {
    /// Accepts a new connection.
    Accept,
    /// Connects to the remote address.
    Connect,
    /// Receives bytes from the connection.
    Read,
    /// Sends bytes over the connection.
    Write,
}

/// An operation, which is scheduled to the [`SocketQueueManager`](crate::handle::SocketQueueManager).
///
/// It holds the buffers of the operation until the operation has completed.
///
/// <div class="warning">
///
/// Once the operation has been started, the packet is held by Windows and **must not** be moved or released until the
/// operation has completed, it is returned by [`handle_success`](Self::handle_success).
///
/// </div>
///
/// The `OVERLAPPED` of the operation is the first field, so the memory address of the `OVERLAPPED` is also the memory
/// address of the packet.
///
/// **Note:** For an accept operation, the packet also holds the new socket of the connection.
///
/// # See also
///
/// - [`SocketQueueManager::poll`](crate::handle::SocketQueueManager::poll)
/// - [`Type`]
#[repr(C)]
pub struct SocketPacket {
    overlapped: OVERLAPPED,
    buffers: Vec<BytesMut>,
    length: usize,
    socket: Option<OwnedSocket>,
    method: Type,
}

/// The size of the buffer for each address of an accepted connection.
///
/// `AcceptEx` needs 16 more bytes than the size of the address.
const ADDRESS_LENGTH: usize = size_of::<SOCKADDR_STORAGE>() + 16;

impl SocketPacket {
    /// Creates a new packet with a buffer of the provided capacity, or without any buffer if the capacity is `0`.
    ///
    /// A read operation with a packet without buffer does **not** receive any bytes, it **only** completes once bytes
    /// can be received.
    ///
    /// # Arguments
    ///
    /// - `capacity` — The capacity of the buffer
    ///
    /// # See also
    ///
    /// - [`wrap`](Self::wrap)
    pub fn new(capacity: usize) -> Box<Self> {
        if capacity == 0 {
            Self::wrap(Vec::new())
        } else {
            Self::wrap(vec![BytesMut::with_capacity(capacity)])
        }
    }

    /// Creates a new packet with the provided buffers.
    ///
    /// A write operation sends the bytes of **all** buffers in order, a read operation **only** uses the free capacity
    /// of the first buffer.
    ///
    /// # Arguments
    ///
    /// - `buffers` — The buffers of the packet
    ///
    /// # See also
    ///
    /// - [`new`](Self::new)
    pub fn wrap(buffers: Vec<BytesMut>) -> Box<Self> {
        Box::new(Self {
            overlapped: unsafe { mem::zeroed() },
            buffers,
            length: 0,
            socket: None,
            method: Type::Read,
        })
    }

    #[cfg(test)]
    pub fn buffer(&self) -> &BytesMut {
        &self.buffers[0]
    }

    /// The amount of bytes, which have been sent or received.
    pub fn length(&self) -> usize {
        self.length
    }

    /// The type of the operation.
    pub fn method(&self) -> Type {
        self.method
    }

    #[cfg(test)]
    pub fn into_body(self) -> BytesMut {
        self.buffers.into_iter().next().unwrap_or_default()
    }

    /// Removes the sent bytes from the buffers.
    ///
    /// Each buffer, which has been sent completely, is removed, while a buffer, which has been sent only in part,
    /// keeps its remaining bytes. The amount of sent bytes is reset to `0`.
    ///
    /// # Returns
    ///
    /// `true` if some bytes have not been sent yet.
    ///
    /// # See also
    ///
    /// - [`write`](fn@write)
    pub(crate) fn reallocate(&mut self) -> bool {
        let mut length = mem::take(&mut self.length);
        for buffer in &mut self.buffers {
            let step = length.min(buffer.len());
            buffer.advance(step);
            length -= step;
        }
        self.buffers.retain(|buffer| buffer.has_remaining());
        !self.buffers.is_empty()
    }

    /// Returns the packet of the provided completed operation, or the error of the operation.
    ///
    /// The packet is held by the caller again. For a read operation, the received bytes are added to the first
    /// buffer.
    ///
    /// # Safety
    ///
    /// The entry **must** have been returned by [`SocketQueueManager::poll`](crate::handle::SocketQueueManager::poll) for
    /// an operation, which has been scheduled with [`read`], [`write`](fn@write), [`accept`] or [`connect`] of this
    /// module. Each entry **must** only be handled once, as its packet is released by this call. An entry without
    /// packet, see [`SocketQueueManager::enqueue`](crate::handle::SocketQueueManager::enqueue), **must not** be provided.
    ///
    /// # Arguments
    ///
    /// - `entry` — The completed operation
    ///
    /// # Errors
    ///
    /// Returns the error of the operation, for example if the socket has been closed while the operation was
    /// pending. The packet is released in this case.
    ///
    /// # Returns
    ///
    /// The packet of the operation.
    pub(crate) unsafe fn handle_success(entry: &OVERLAPPED_ENTRY) -> io::Result<Box<Self>> {
        let data = unsafe { Box::from_raw(entry.lpOverlapped as *mut Self) };
        let status = data.overlapped.Internal as NTSTATUS;
        if status != 0 {
            Err(io::Error::from_raw_os_error(
                unsafe { RtlNtStatusToDosError(status) } as i32,
            ))
        } else {
            Self::on_success(data, entry)
        }
    }

    /// Updates the packet with the amount of sent or received bytes.
    ///
    /// For a read operation, the received bytes are added to the first buffer.
    ///
    /// # Arguments
    ///
    /// - `packet` — The packet of the operation
    /// - `entry` — The completed operation
    ///
    /// # Returns
    ///
    /// The updated packet.
    fn on_success(
        mut packet: Box<SocketPacket>,
        entry: &OVERLAPPED_ENTRY,
    ) -> io::Result<Box<Self>> {
        packet.length = entry.dwNumberOfBytesTransferred as usize;
        if packet.method == Type::Read {
            let length = packet.length();
            if let Some(buffer) = packet.buffers.first_mut() {
                unsafe { buffer.advance_mut(length) }
            }
        }
        Ok(packet)
    }
}

/// Schedules a read operation for the provided socket.
///
/// The packet is held by the operation until it has completed. **Only** the free capacity of the first buffer is used,
/// a packet without buffer does **not** receive any bytes, the operation **only** completes once bytes can be received.
///
/// # Arguments
///
/// - `socket` — The registered socket
/// - `packet` — The packet, which receives the bytes
///
/// # Errors
///
/// Returns an error if the operation could not be started, the packet is released in this case.
///
/// # See also
///
/// - [`write`](fn@write)
/// - [`ChannelManager::read`](crate::handle::ChannelManager::read)
pub(crate) fn read<S: AsRawSocket>(socket: &S, packet: Box<SocketPacket>) -> io::Result<()> {
    submit(packet, Type::Read, |buffers, overlapped| {
        let mut flags = 0;
        unsafe {
            WSARecv(
                socket.as_raw_socket() as SOCKET,
                buffers.as_ptr(),
                buffers.len() as u32,
                ptr::null_mut(),
                &mut flags,
                overlapped,
                None,
            )
        }
    })
}

/// Schedules a write operation for the provided socket.
///
/// The packet is held by the operation until it has completed. The bytes of all buffers are sent in order, the
/// operation *may* complete once only a part of the bytes has been sent, see [`SocketPacket::reallocate`].
///
/// # Arguments
///
/// - `socket` — The registered socket
/// - `packet` — The packet with the bytes to send
///
/// # Errors
///
/// Returns an error if the operation could not be started, the packet is released in this case.
///
/// # See also
///
/// - [`read`]
/// - [`ChannelManager::write`](crate::handle::ChannelManager::write)
pub(crate) fn write<S: AsRawSocket>(socket: &S, packet: Box<SocketPacket>) -> io::Result<()> {
    submit(packet, Type::Write, |buffers, overlapped| unsafe {
        WSASend(
            socket.as_raw_socket() as SOCKET,
            buffers.as_ptr(),
            buffers.len() as u32,
            ptr::null_mut(),
            0,
            overlapped,
            None,
        )
    })
}

/// Schedules an accept operation for the provided socket.
///
/// A new socket for the connection is created with the same IP version as the provided socket and is held by the
/// packet, together with the local and remote address of the connection. Once the operation has completed, the
/// connection can be retrieved with [`accepted`].
///
/// # Arguments
///
/// - `listener` — The registered and bound socket
///
/// # Errors
///
/// Returns an error:
/// - If the local address of the provided socket could not be retrieved
/// - If the new socket could not be created
/// - If the operation could not be started
///
/// # See also
///
/// - [`ChannelManager::accept`](crate::handle::ChannelManager::accept)
pub(crate) fn accept(listener: &TcpListener) -> io::Result<()> {
    let connect_node = socket(listener.local_addr()?.is_ipv4())?;
    let socket = connect_node.as_raw_socket() as SOCKET;
    let mut packet = SocketPacket::new(2 * ADDRESS_LENGTH);
    packet.socket = Some(connect_node.into());
    submit(packet, Type::Accept, |buffers, overlapped| {
        let mut received = 0;
        let data = unsafe {
            AcceptEx(
                listener.as_raw_socket() as SOCKET,
                socket,
                buffers[0].buf.cast(),
                0,
                ADDRESS_LENGTH as u32,
                ADDRESS_LENGTH as u32,
                &mut received,
                overlapped,
            )
        };
        if data == 0 { SOCKET_ERROR } else { 0 }
    })
}

/// Returns the accepted connection of the provided packet.
///
/// The connection is updated with the provided socket (`SO_UPDATE_ACCEPT_CONTEXT`), so that functions like
/// [`TcpStream::peer_addr`] can be used.
///
/// # Arguments
///
/// - `listener` — The bound socket, which has accepted the connection
/// - `packet` — The packet of the completed accept operation
///
/// # Errors
///
/// Returns:
/// - [`InvalidInput`](io::ErrorKind::InvalidInput) if the packet does not hold a connection
/// - An error if the connection could not be updated, the connection is closed in this case
///
/// # See also
///
/// - [`accept`]
pub fn accepted(listener: &TcpListener, mut packet: SocketPacket) -> io::Result<TcpStream> {
    let socket = packet.socket.take().ok_or(io::ErrorKind::InvalidInput)?;
    let parent = listener.as_raw_socket() as SOCKET;
    let data = unsafe {
        setsockopt(
            socket.as_raw_socket() as SOCKET,
            SOL_SOCKET,
            SO_UPDATE_ACCEPT_CONTEXT,
            (&parent as *const SOCKET).cast(),
            size_of::<SOCKET>() as i32,
        )
    };
    if data == SOCKET_ERROR {
        Err(last_error())
    } else {
        Ok(TcpStream::from(socket))
    }
}

/// Schedules a connect operation to the provided address.
///
/// The socket **must not** be bound yet, it is bound to any local address of the same IP version first, as `ConnectEx`
/// requires a bound socket. The packet holds the address until the operation has completed. Once completed,
/// [`connected`](crate::handle::socket::connected) **must** be called *before* the socket can be used.
///
/// # Arguments
///
/// - `stream` — The registered socket, which is not bound yet
/// - `address` — The address to connect to
///
/// # Errors
///
/// Returns:
/// - An error if the socket could not be bound
/// - [`Unsupported`](io::ErrorKind::Unsupported) if `ConnectEx` is not available
/// - An error if the operation could not be started
///
/// # See also
///
/// - [`ChannelManager::connect`](crate::handle::ChannelManager::connect)
pub(crate) fn connect(stream: &TcpStream, address: &SocketAddr) -> io::Result<()> {
    let socket = stream.as_raw_socket() as SOCKET;
    let local = if address.is_ipv4() {
        SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 0)
    } else {
        SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), 0)
    };
    SockRef::from(stream).bind(&SockAddr::from(local))?;

    let mut function: LPFN_CONNECTEX = None;
    let mut returned = 0;
    let data = unsafe {
        WSAIoctl(
            socket,
            SIO_GET_EXTENSION_FUNCTION_POINTER,
            (&WSAID_CONNECTEX as *const GUID).cast(),
            size_of::<GUID>() as u32,
            (&mut function as *mut LPFN_CONNECTEX).cast(),
            size_of::<LPFN_CONNECTEX>() as u32,
            &mut returned,
            ptr::null_mut(),
            None,
        )
    };
    if data == SOCKET_ERROR {
        return Err(last_error());
    }
    let function = function.ok_or(io::ErrorKind::Unsupported)?;

    submit(
        SocketPacket::wrap(vec![socket_address(address)]),
        Type::Connect,
        |buffers, overlapped| {
            let data = unsafe {
                function(
                    socket,
                    buffers[0].buf.cast(),
                    buffers[0].len as i32,
                    ptr::null(),
                    0,
                    ptr::null_mut(),
                    overlapped,
                )
            };
            if data == 0 { SOCKET_ERROR } else { 0 }
        },
    )
}

/// Schedules the operation of the provided packet.
///
/// The packet is held by Windows until the operation has completed, it is returned by
/// [`SocketPacket::handle_success`]. Even if the operation completes at once, it is returned by the
/// [`SocketQueueManager`](crate::handle::SocketQueueManager). The `call` gets the buffers of the packet and its
/// `OVERLAPPED`: a read or accept operation **only** gets the free capacity of the first buffer, a write or connect
/// operation gets the bytes of **all** buffers.
///
/// # Arguments
///
/// - `packet` — The packet of the operation
/// - `method` — The type of the operation
/// - `call` — The function, which starts the operation
///
/// # Errors
///
/// Returns the error of `call`, if it is not `WSA_IO_PENDING`. The packet is released in this case.
fn submit<F>(mut packet: Box<SocketPacket>, method: Type, call: F) -> io::Result<()>
where
    F: FnOnce(&[WSABUF], *mut OVERLAPPED) -> i32,
{
    packet.method = method;
    packet.overlapped = unsafe { mem::zeroed() };
    let packet = Box::into_raw(packet);
    let response = unsafe {
        let packet = &mut *packet;
        match method {
            Type::Read | Type::Accept => {
                let buffer = match packet.buffers.first_mut() {
                    Some(buffer) => {
                        let chunk = buffer.spare_capacity_mut();
                        WSABUF {
                            len: chunk.len() as u32,
                            buf: chunk.as_mut_ptr().cast::<u8>(),
                        }
                    }
                    None => WSABUF {
                        len: 0,
                        buf: ptr::NonNull::dangling().as_ptr(),
                    },
                };
                call(&[buffer], &mut packet.overlapped)
            }
            Type::Write | Type::Connect => {
                let buffers: Vec<WSABUF> = packet
                    .buffers
                    .iter_mut()
                    .map(|buffer| WSABUF {
                        len: buffer.len() as u32,
                        buf: buffer.as_mut_ptr(),
                    })
                    .collect();
                call(&buffers, &mut packet.overlapped)
            }
        }
    };
    if response == SOCKET_ERROR {
        let code = unsafe { WSAGetLastError() };
        if code != WSA_IO_PENDING {
            drop(unsafe { Box::from_raw(packet) });
            return Err(io::Error::from_raw_os_error(code));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handle::SocketQueueManager;
    use crate::handle::socket::{client, connected};
    use std::io::{Read, Write};

    fn connect_node(port: &SocketQueueManager) -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        port.register(&server, 2).unwrap();
        (client, server)
    }

    fn on_success(port: &SocketQueueManager) -> io::Result<Box<SocketPacket>> {
        let mut entries: [OVERLAPPED_ENTRY; 4] = unsafe { mem::zeroed() };
        assert_eq!(port.poll(&mut entries, 1000)?, 1);
        assert_eq!(entries[0].lpCompletionKey, 2);
        unsafe { SocketPacket::handle_success(&entries[0]) }
    }

    #[test]
    fn read() {
        let port = SocketQueueManager::new().unwrap();
        let (mut client, server) = connect_node(&port);

        super::read(&server, SocketPacket::new(16)).unwrap();
        client.write_all(b"ping").unwrap();

        assert_eq!(&on_success(&port).unwrap().buffer()[..], b"ping");
    }

    #[test]
    fn empty() {
        let port = SocketQueueManager::new().unwrap();
        let (mut client, mut server) = connect_node(&port);

        super::read(&server, SocketPacket::new(0)).unwrap();
        client.write_all(b"ping").unwrap();

        assert_eq!(on_success(&port).unwrap().length(), 0);
        let mut received = [0; 4];
        server.read_exact(&mut received).unwrap();
        assert_eq!(&received, b"ping");
    }

    #[test]
    fn error() {
        let port = SocketQueueManager::new().unwrap();
        let (_client, server) = connect_node(&port);

        super::read(&server, SocketPacket::new(16)).unwrap();
        drop(server);

        assert!(on_success(&port).is_err());
    }

    #[test]
    fn write() {
        let port = SocketQueueManager::new().unwrap();
        let (mut client, server) = connect_node(&port);

        super::write(
            &server,
            SocketPacket::wrap(vec![BytesMut::from(&b"po"[..]), BytesMut::from(&b"ng"[..])]),
        )
        .unwrap();
        assert_eq!(on_success(&port).unwrap().length(), 4);

        let mut received = [0; 4];
        client.read_exact(&mut received).unwrap();
        assert_eq!(&received, b"pong");
    }

    #[test]
    fn accept() {
        let port = SocketQueueManager::new().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        port.register(&listener, 2).unwrap();

        super::accept(&listener).unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();

        let server = accepted(&listener, *on_success(&port).unwrap()).unwrap();
        assert_eq!(server.peer_addr().unwrap(), client.local_addr().unwrap());
    }

    #[test]
    fn connect() {
        let port = SocketQueueManager::new().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let client = client(&address).unwrap();
        port.register(&client, 2).unwrap();

        super::connect(&client, &address).unwrap();
        let (server, _) = listener.accept().unwrap();

        assert_eq!(on_success(&port).unwrap().method(), Type::Connect);
        connected(&client).unwrap();
        assert_eq!(client.peer_addr().unwrap(), address);
        assert_eq!(server.peer_addr().unwrap(), client.local_addr().unwrap());
    }

    #[test]
    fn reallocate() {
        let mut packet =
            SocketPacket::wrap(vec![BytesMut::from(&b"pi"[..]), BytesMut::from(&b"ng"[..])]);
        packet.length = 3;
        assert!(packet.reallocate());
        assert_eq!(&packet.buffer()[..], b"g");
        packet.length = 1;
        assert!(!packet.reallocate());
    }
}
