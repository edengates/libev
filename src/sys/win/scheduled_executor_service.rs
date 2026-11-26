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
use crate::handle::socket::{last_error, socket, socket_address};
use crate::handle::{Completion, CompletionService, Request};
use crate::utils::concurrent::{ScheduledExecutorService, SHUTDOWN};
use log::error;
use rustc_hash::FxHashMap;
use socket2::{SockAddr, SockRef};
use std::io;
use std::mem;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener, TcpStream};
use std::os::windows::io::AsRawSocket;
use std::ptr;
use std::sync::OnceLock;
use windows_sys::Win32::Networking::WinSock::{
    AcceptEx, LPFN_CONNECTEX, SIO_GET_EXTENSION_FUNCTION_POINTER, SOCKADDR_STORAGE, SOCKET, SOCKET_ERROR, WSABUF,
    WSAGetLastError, WSAID_CONNECTEX, WSAIoctl, WSARecv, WSASend, WSA_IO_PENDING,
};
use windows_sys::Win32::System::IO::{OVERLAPPED, OVERLAPPED_ENTRY};
use windows_sys::core::GUID;

const ADDRESS_LENGTH: usize = size_of::<SOCKADDR_STORAGE>() + 16;

pub struct EventArray {
    pub(crate) entries: Vec<OVERLAPPED_ENTRY>,
    pub(crate) requests: FxHashMap<usize, Box<dyn Request>>,
}

impl EventArray {
    pub(crate) fn new(_port: &CompletionService, capacity: usize) -> Self {
        Self {
            entries: vec![unsafe { mem::zeroed() }; capacity.max(1)],
            requests: FxHashMap::default(),
        }
    }
}

impl Drop for EventArray {
    fn drop(&mut self) {
        if !self.requests.is_empty() {
            mem::forget(mem::take(&mut self.requests));
        }
    }
}

impl ScheduledExecutorService {
    /// The number of operations that have been submitted and have not completed yet.
    pub fn pending(&self) -> usize {
        self.array.requests.len()
    }

    /// Waits for operations to complete, at most until the next scheduled task is due, and passes them to their
    /// handlers.
    pub fn run_worker(&mut self) -> io::Result<()> {
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

    pub(crate) fn read<S: AsRawSocket>(&mut self, socket: &S, packet: Box<Event>) -> io::Result<()> {
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

    pub(crate) fn write<S: AsRawSocket>(&mut self, socket: &S, packet: Box<Event>) -> io::Result<()> {
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

    pub(crate) fn accept(&mut self, listener: &TcpListener) -> io::Result<()> {
        let socket = socket(listener.local_addr()?.is_ipv4())?;
        let raw = socket.as_raw_socket() as SOCKET;
        let mut packet = Event::new(2 * ADDRESS_LENGTH);
        packet.set_socket(socket.into());
        self.submit(packet, Event::ACCEPT, |buffers, overlapped| {
            let mut received = 0;
            let data = unsafe {
                AcceptEx(
                    listener.as_raw_socket() as SOCKET,
                    raw,
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

    pub(crate) fn connect(&mut self, stream: &TcpStream, address: &SocketAddr) -> io::Result<()> {
        let socket = stream.as_raw_socket() as SOCKET;
        let local = if address.is_ipv4() {
            SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 0)
        } else {
            SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), 0)
        };
        SockRef::from(stream).bind(&SockAddr::from(local))?;
        let function = connect_ex(socket)?.ok_or(io::ErrorKind::Unsupported)?;

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
        self.submit_request(packet, |packet| packet.start(opcode, call))
    }

    /// Submits an operation for the handler that is being handled.
    ///
    /// The `call` starts the operation with the [`overlapped`](Request::overlapped) of the request and returns the
    /// result of the Win32 function. The request is kept until the operation has completed, then it is passed to the
    /// handler in a [`Completion`].
    ///
    /// # Errors
    ///
    /// Returns the error of the operation if it failed to start.
    pub fn submit_request<R, F>(&mut self, request: Box<R>, call: F) -> io::Result<()>
    where
        R: Request,
        F: FnOnce(&mut R) -> i32,
    {
        let request = Box::into_raw(request);
        let response = call(unsafe { &mut *request });
        if response == SOCKET_ERROR {
            let code = unsafe { WSAGetLastError() };
            if code != WSA_IO_PENDING {
                drop(unsafe { Box::from_raw(request) });
                return Err(io::Error::from_raw_os_error(code));
            }
        }
        let mut request: Box<dyn Request> = unsafe { Box::from_raw(request) };
        self.array.requests.insert(request.overlapped() as usize, request);
        Ok(())
    }

    fn handle(&mut self, entry: &OVERLAPPED_ENTRY) {
        if entry.lpOverlapped.is_null() {
            self.is_shutdown |= entry.lpCompletionKey == SHUTDOWN;
            return;
        }
        let Some(request) = self.array.requests.remove(&(entry.lpOverlapped as usize)) else {
            return;
        };
        let completion = Completion::new(request, entry.dwNumberOfBytesTransferred as usize);
        self.handle_event(entry.lpCompletionKey, |handle, manager| handle.handle(manager, completion).unwrap_or(false));
    }

    /// Closes all handlers and waits for the operations that are still in flight.
    ///
    /// If the port fails here, the packets still in flight are leaked.
    pub fn close(&mut self) {
        let mut entries = mem::take(&mut self.array.entries);
        self.close_sockets();
        while !self.array.requests.is_empty() {
            let count = match self.port.poll(&mut entries, u32::MAX) {
                Ok(count) => count,
                Err(error) => {
                    error!(error:% = error; "Encountered error in socket worker");
                    return;
                }
            };
            for entry in &entries[..count] {
                if !entry.lpOverlapped.is_null() {
                    self.array.requests.remove(&(entry.lpOverlapped as usize));
                }
            }
        }
    }
}

fn connect_ex(socket: SOCKET) -> io::Result<LPFN_CONNECTEX> {
    static CONNECT_EX: OnceLock<LPFN_CONNECTEX> = OnceLock::new();
    if let Some(function) = CONNECT_EX.get() {
        return Ok(*function);
    }
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
    Ok(*CONNECT_EX.get_or_init(|| function))
}

#[cfg(test)]
mod tests {
    use crate::{DefaultSocketManagerBuilder, SocketManager};
    use crate::hooks::ListenerAdapter;
    use crate::utils::concurrent::ScheduledThreadPoolExecutor;
    use crate::handle::{ServerSocketChannel, SocketChannel, socket};
    use crate::handle::CompletionService;
    use crate::handle::event::Event;
    use crate::handle::Completion;
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
        let port = Arc::new(CompletionService::new().unwrap());
        let dispatcher = crate::utils::concurrent::Dispatcher::new(&port).0;
        ScheduledExecutorService::new(port, dispatcher, 64)
    }

    fn socket_manager<F>(configure: F) -> SocketManager
    where
        F: FnOnce(DefaultSocketManagerBuilder) -> DefaultSocketManagerBuilder,
    {
        configure(DefaultSocketManagerBuilder::new())
            .set_socket_pool(ScheduledThreadPoolExecutor::new(1, 64).unwrap())
            .add_event_listener(GenericEventAdapter)
            .build()
            .unwrap()
    }

    fn connect_node(port: &CompletionService) -> (TcpStream, TcpStream) {
        let listener = listener();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        port.register(&server, 2).unwrap();
        (client, server)
    }

    fn on_success(manager: &mut ScheduledExecutorService) -> io::Result<Box<Event>> {
        let mut entries: [OVERLAPPED_ENTRY; 4] = unsafe { mem::zeroed() };
        assert_eq!(manager.port.poll(&mut entries, 1000)?, 1);
        assert_eq!(entries[0].lpCompletionKey, 2);
        let request = manager.array.requests.remove(&(entries[0].lpOverlapped as usize)).unwrap();
        Event::from_completion(Completion::new(request, entries[0].dwNumberOfBytesTransferred as usize))
    }

    #[test]
    fn read() {
        let mut manager = manager();
        let (mut client, server) = connect_node(&manager.port);

        manager.read(&server, Event::new(16)).unwrap();
        client.write_all(b"ping").unwrap();

        assert_eq!(&on_success(&mut manager).unwrap().buffer()[..], b"ping");
    }

    #[test]
    fn empty() {
        let mut manager = manager();
        let (mut client, mut server) = connect_node(&manager.port);

        manager.read(&server, Event::new(0)).unwrap();
        client.write_all(b"ping").unwrap();

        assert_eq!(on_success(&mut manager).unwrap().length(), 0);
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

        assert!(on_success(&mut manager).is_err());
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
        assert_eq!(on_success(&mut manager).unwrap().length(), 4);

        let mut received = [0; 4];
        client.read_exact(&mut received).unwrap();
        assert_eq!(&received, b"pong");
    }

    #[test]
    fn close() {
        let mut manager = manager();
        let (_client, server) = connect_node(&manager.port);

        manager.read(&server, Event::new(16)).unwrap();
        drop(server);
        manager.close();

        assert!(manager.array.requests.is_empty());
    }

    #[test]
    fn accept() {
        let mut manager = manager();
        let listener = listener();
        manager.port.register(&listener, 2).unwrap();

        manager.accept(&listener).unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();

        let server = socket::accepted(&listener, &mut on_success(&mut manager).unwrap()).unwrap();
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

        assert_eq!(on_success(&mut manager).unwrap().opcode(), Event::CONNECT);
        socket::connected(&client).unwrap();
        assert_eq!(client.peer_addr().unwrap(), address);
        assert_eq!(server.peer_addr().unwrap(), client.local_addr().unwrap());
    }

    #[test]
    fn reconnect_delay() {
        let socket_manager = socket_manager(|builder| {
            builder
                .set_reconnect_delay(Duration::from_millis(40))
                .set_max_reconnect_delay(Duration::from_millis(100))
        });
        listener();
        let socket = unsafe {
            WSASocketW(AF_INET as i32, SOCK_STREAM, IPPROTO_TCP, ptr::null(), 0, WSA_FLAG_OVERLAPPED)
        };
        let unbound = unsafe { TcpListener::from_raw_socket(socket as RawSocket) };
        let mut manager = manager();
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
        let socket_manager = socket_manager(|builder| builder.set_connect_timeout(Duration::from_millis(50)));
        let address = listener().local_addr().unwrap();
        let mut manager = manager();
        let channel = SocketChannel::connect(socket::client(&address).unwrap(), address, socket_manager);
        manager.register(Box::new(channel)).unwrap();
        assert_eq!(manager.sockets.len(), 1);

        thread::sleep(Duration::from_millis(60));
        manager.execute();
        assert!(manager.sockets.is_empty());
    }
}
