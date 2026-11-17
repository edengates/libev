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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Type {
    Accept,
    Connect,
    Read,
    Write,
}

/// Once submitted, the packet must not be moved or released until Windows has completed the operation.
/// The packet is found again from its `OVERLAPPED`, which must stay the first field.
#[repr(C)]
pub struct SocketPacket {
    overlapped: OVERLAPPED,
    buffers: Vec<BytesMut>,
    length: usize,
    socket: Option<OwnedSocket>,
    method: Type,
}

const ADDRESS_LENGTH: usize = size_of::<SOCKADDR_STORAGE>() + 16;

impl SocketPacket {
    pub fn new(capacity: usize) -> Box<Self> {
        if capacity == 0 {
            Self::wrap(Vec::new())
        } else {
            Self::wrap(vec![BytesMut::with_capacity(capacity)])
        }
    }

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

    pub fn length(&self) -> usize {
        self.length
    }

    pub fn method(&self) -> Type {
        self.method
    }

    #[cfg(test)]
    pub fn into_body(self) -> BytesMut {
        self.buffers.into_iter().next().unwrap_or_default()
    }

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

    /// # Safety
    ///
    /// `entry` must come from [`SocketQueueManager::poll`](crate::handle::SocketQueueManager::poll), hold a packet
    /// submitted by this module, and be handled only once.
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
