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

use crate::handle::Channel;
use crate::handle::event::Event;
use crate::handle::socket::{last_error, socket, socket_address};
use crate::handle::CompletionService;
use crate::utils::concurrent::{ScheduledExecutorService, SHUTDOWN};
use log::error;
use socket2::{SockAddr, SockRef};
use std::io;
use std::mem;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener, TcpStream};
use std::os::windows::io::AsRawSocket;
use std::ptr;
use windows_sys::Win32::Networking::WinSock::{
    AcceptEx, LPFN_CONNECTEX, SIO_GET_EXTENSION_FUNCTION_POINTER, SOCKADDR_STORAGE, SOCKET, SOCKET_ERROR, WSABUF,
    WSAID_CONNECTEX, WSAIoctl, WSARecv, WSASend,
};
use windows_sys::Win32::System::IO::{OVERLAPPED, OVERLAPPED_ENTRY};
use windows_sys::core::GUID;

const ADDRESS_LENGTH: usize = size_of::<SOCKADDR_STORAGE>() + 16;

pub struct EventArray {
    pub(in crate::handle) entries: Vec<OVERLAPPED_ENTRY>,
    pub(in crate::handle) response_total: usize,
}

impl EventArray {
    pub(crate) fn new(_port: &CompletionService, capacity: usize) -> Self {
        Self {
            entries: vec![unsafe { mem::zeroed() }; capacity.max(1)],
            response_total: 0,
        }
    }
}

impl ScheduledExecutorService {
    pub(crate) fn run_worker(&mut self) -> io::Result<()> {
        let mut entries = mem::take(&mut self.array.entries);
        let data = self.port.poll(&mut entries, self.queue.timeout());
        if let Ok(count) = data {
            for entry in &entries[..count] {
                self.handle(entry);
            }
        }
        self.array.entries = entries;
        data.map(|_| ())
    }

    pub fn read<S: AsRawSocket>(&mut self, socket: &S, packet: Box<Event>) -> io::Result<()> {
        self.submit(packet, Event::READ, |buffers, overlapped| {
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

    pub fn write<S: AsRawSocket>(&mut self, socket: &S, packet: Box<Event>) -> io::Result<()> {
        self.submit(packet, Event::WRITE, |buffers, overlapped| unsafe {
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

    pub fn accept(&mut self, listener: &TcpListener) -> io::Result<()> {
        let connect_node = socket(listener.local_addr()?.is_ipv4())?;
        let socket = connect_node.as_raw_socket() as SOCKET;
        let mut packet = Event::new(2 * ADDRESS_LENGTH);
        packet.set_socket(connect_node.into());
        self.submit(packet, Event::ACCEPT, |buffers, overlapped| {
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

    pub fn connect(&mut self, stream: &TcpStream, address: &SocketAddr) -> io::Result<()> {
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

        self.submit(
            Event::wrap(vec![socket_address(address)]),
            Event::CONNECT,
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

    fn submit<F>(&mut self, packet: Box<Event>, opcode: u32, call: F) -> io::Result<()>
    where
        F: FnOnce(&[WSABUF], *mut OVERLAPPED) -> i32,
    {
        Event::submit(packet, opcode, call)?;
        self.array.response_total += 1;
        Ok(())
    }

    fn handle(&mut self, entry: &OVERLAPPED_ENTRY) {
        if entry.lpOverlapped.is_null() {
            self.is_shutdown |= entry.lpCompletionKey == SHUTDOWN;
            return;
        }
        self.array.response_total -= 1;
        let data = unsafe { Event::handle_success(entry) };
        self.handle_event(entry.lpCompletionKey, |handle, manager| match data {
            Ok(mut packet) if packet.opcode() == Event::WRITE => {
                if packet.reallocate() {
                    manager.write(&handle.socket(), packet).is_ok()
                } else {
                    matches!(handle.handle(manager, Ok(*packet)), Ok(true))
                }
            }
            data => matches!(handle.handle(manager, data.map(|packet| *packet)), Ok(true)),
        });
    }

    /// If the port fails here, the packets still in flight are leaked.
    pub(crate) fn close(&mut self) {
        let mut entries = mem::take(&mut self.array.entries);
        let sockets: Vec<(usize, Box<dyn Channel>)> = self.sockets.drain().collect();
        for (key, handle) in sockets {
            self.close_resource(key, handle);
        }
        while self.array.response_total > 0 {
            let count = match self.port.poll(&mut entries, u32::MAX) {
                Ok(count) => count,
                Err(error) => {
                    error!(error:% = error; "Encountered error in gateway worker");
                    return;
                }
            };
            for entry in &entries[..count] {
                if !entry.lpOverlapped.is_null() {
                    self.array.response_total -= 1;
                    drop(unsafe { Event::handle_success(entry) });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::DefaultSocketManagerBuilder;
    use crate::hooks::ListenerAdapter;
    use crate::utils::concurrent::ScheduledThreadPoolExecutor;
    use crate::handle::{ServerSocketChannel, SocketChannel, socket};
    use crate::handle::CompletionService;
    use crate::handle::event::Event;
    use crate::utils::concurrent::ScheduledExecutorService;
    use bytes::BytesMut;
    use std::io::{self, Read, Write};
    use std::mem;
    use std::net::{TcpListener, TcpStream};
    use std::os::windows::io::{FromRawSocket, RawSocket};
    use std::ptr;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use windows_sys::Win32::Networking::WinSock::{
        AF_INET, IPPROTO_TCP, SOCK_STREAM, WSASocketW, WSA_FLAG_OVERLAPPED,
    };
    use windows_sys::Win32::System::IO::OVERLAPPED_ENTRY;

    #[derive(Clone)]
    struct GenericEventAdapter;

    impl ListenerAdapter for GenericEventAdapter {}

    fn listener() -> TcpListener {
        TcpListener::bind("127.0.0.1:0").unwrap()
    }

    fn manager() -> ScheduledExecutorService {
        ScheduledExecutorService::new(Arc::new(CompletionService::new().unwrap()), 64)
    }

    fn connect_node(port: &CompletionService) -> (TcpStream, TcpStream) {
        let listener = listener();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        port.register(&server, 2).unwrap();
        (client, server)
    }

    fn on_success(port: &CompletionService) -> io::Result<Box<Event>> {
        let mut entries: [OVERLAPPED_ENTRY; 4] = unsafe { mem::zeroed() };
        assert_eq!(port.poll(&mut entries, 1000)?, 1);
        assert_eq!(entries[0].lpCompletionKey, 2);
        unsafe { Event::handle_success(&entries[0]) }
    }

    #[test]
    fn read() {
        let mut manager = manager();
        let (mut client, server) = connect_node(&manager.port);

        manager.read(&server, Event::new(16)).unwrap();
        client.write_all(b"ping").unwrap();

        assert_eq!(&on_success(&manager.port).unwrap().buffer()[..], b"ping");
    }

    #[test]
    fn empty() {
        let mut manager = manager();
        let (mut client, mut server) = connect_node(&manager.port);

        manager.read(&server, Event::new(0)).unwrap();
        client.write_all(b"ping").unwrap();

        assert_eq!(on_success(&manager.port).unwrap().length(), 0);
        let mut received = [0; 4];
        server.read_exact(&mut received).unwrap();
        assert_eq!(&received, b"ping");
    }

    #[test]
    fn error() {
        let mut manager = manager();
        let (_client, server) = connect_node(&manager.port);

        manager.read(&server, Event::new(16)).unwrap();
        drop(server);

        assert!(on_success(&manager.port).is_err());
    }

    #[test]
    fn write() {
        let mut manager = manager();
        let (mut client, server) = connect_node(&manager.port);

        manager
            .write(
                &server,
                Event::wrap(vec![BytesMut::from(&b"po"[..]), BytesMut::from(&b"ng"[..])]),
            )
            .unwrap();
        assert_eq!(on_success(&manager.port).unwrap().length(), 4);

        let mut received = [0; 4];
        client.read_exact(&mut received).unwrap();
        assert_eq!(&received, b"pong");
    }

    #[test]
    fn accept() {
        let mut manager = manager();
        let listener = listener();
        manager.port.register(&listener, 2).unwrap();

        manager.accept(&listener).unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();

        let server = socket::accepted(&listener, *on_success(&manager.port).unwrap()).unwrap();
        assert_eq!(server.peer_addr().unwrap(), client.local_addr().unwrap());
    }

    #[test]
    fn connect() {
        let mut manager = manager();
        let listener = listener();
        let address = listener.local_addr().unwrap();
        let client = socket::client(&address).unwrap();
        manager.port.register(&client, 2).unwrap();

        manager.connect(&client, &address).unwrap();
        let (server, _) = listener.accept().unwrap();

        assert_eq!(on_success(&manager.port).unwrap().opcode(), Event::CONNECT);
        socket::connected(&client).unwrap();
        assert_eq!(client.peer_addr().unwrap(), address);
        assert_eq!(server.peer_addr().unwrap(), client.local_addr().unwrap());
    }

    #[test]
    fn reconnect_delay() {
        let socket_manager = DefaultSocketManagerBuilder::new()
            .set_socket_pool(ScheduledThreadPoolExecutor::new(1, 64, crate::handle::DefaultHandler::new_factory()).unwrap())
            .add_event_listener(GenericEventAdapter)
            .set_reconnect_delay(Duration::from_millis(40))
            .set_max_reconnect_delay(Duration::from_millis(100))
            .build()
            .unwrap();
        listener();
        let socket = unsafe { WSASocketW(AF_INET as i32, SOCK_STREAM, IPPROTO_TCP, ptr::null(), 0, WSA_FLAG_OVERLAPPED) };
        let unbound = unsafe { TcpListener::from_raw_socket(socket as RawSocket) };
        let mut manager = ScheduledExecutorService::new(Arc::new(CompletionService::new().unwrap()), 64);
        manager.register(Box::new(ServerSocketChannel::new(unbound, socket_manager))).unwrap();

        for expected in [40, 80, 100, 100] {
            let delay = Duration::from_millis(manager.queue.timeout() as u64);
            assert!(delay <= Duration::from_millis(expected));
            assert!(delay > Duration::from_millis(expected - 25));
            thread::sleep(delay);
            manager.execute();
        }
    }

    #[test]
    fn connect_timeout() {
        let socket_manager = DefaultSocketManagerBuilder::new()
            .set_socket_pool(ScheduledThreadPoolExecutor::new(1, 64, crate::handle::DefaultHandler::new_factory()).unwrap())
            .add_event_listener(GenericEventAdapter)
            .set_connect_timeout(Duration::from_millis(50))
            .build()
            .unwrap();
        let address = listener().local_addr().unwrap();
        let channel = SocketChannel::connect(socket::client(&address).unwrap(), address, socket_manager);
        let mut manager = ScheduledExecutorService::new(Arc::new(CompletionService::new().unwrap()), 64);
        manager.register(Box::new(channel)).unwrap();
        assert_eq!(manager.sockets.len(), 1);

        thread::sleep(Duration::from_millis(60));
        manager.execute();
        assert!(manager.sockets.is_empty());
    }
}
