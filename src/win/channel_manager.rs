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

use crate::handle::SocketHandler;
use crate::handle::socket_packet::{self, SocketPacket, Type};
use crate::handle::{ChannelManager, SHUTDOWN, SocketQueueManager};
use log::error;
use std::io;
use std::mem;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::windows::io::AsRawSocket;
use windows_sys::Win32::System::IO::OVERLAPPED_ENTRY;

pub struct ChannelState {
    pub(in crate::handle) entries: Vec<OVERLAPPED_ENTRY>,
    pub(in crate::handle) response_total: usize,
}

impl ChannelState {
    pub(crate) fn new(_port: &SocketQueueManager, capacity: usize) -> Self {
        Self {
            entries: vec![unsafe { mem::zeroed() }; capacity.max(1)],
            response_total: 0,
        }
    }
}

impl ChannelManager {
    pub(crate) fn run_worker(&mut self) -> io::Result<()> {
        let mut entries = mem::take(&mut self.state.entries);
        let data = self.port.poll(&mut entries, self.worker.timeout());
        if let Ok(count) = data {
            for entry in &entries[..count] {
                self.handle(entry);
            }
        }
        self.state.entries = entries;
        data.map(|_| ())
    }

    pub fn read<S: AsRawSocket>(&mut self, socket: &S, packet: Box<SocketPacket>) -> io::Result<()> {
        socket_packet::read(socket, packet)?;
        self.state.response_total += 1;
        Ok(())
    }

    pub fn write<S: AsRawSocket>(&mut self, socket: &S, packet: Box<SocketPacket>) -> io::Result<()> {
        socket_packet::write(socket, packet)?;
        self.state.response_total += 1;
        Ok(())
    }

    pub fn accept(&mut self, listener: &TcpListener) -> io::Result<()> {
        socket_packet::accept(listener)?;
        self.state.response_total += 1;
        Ok(())
    }

    pub fn connect(&mut self, stream: &TcpStream, address: &SocketAddr) -> io::Result<()> {
        socket_packet::connect(stream, address)?;
        self.state.response_total += 1;
        Ok(())
    }

    fn handle(&mut self, entry: &OVERLAPPED_ENTRY) {
        if entry.lpOverlapped.is_null() {
            self.is_shutdown |= entry.lpCompletionKey == SHUTDOWN;
            return;
        }
        self.state.response_total -= 1;
        let data = unsafe { SocketPacket::handle_success(entry) };
        self.handle_event(entry.lpCompletionKey, |handle, manager| match data {
            Ok(mut packet) if packet.method() == Type::Write => {
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
        let mut entries = mem::take(&mut self.state.entries);
        let sockets: Vec<(usize, Box<dyn SocketHandler>)> = self.sockets.drain().collect();
        for (key, handle) in sockets {
            self.close_resource(key, handle);
        }
        while self.state.response_total > 0 {
            let count = match self.port.poll(&mut entries, u32::MAX) {
                Ok(count) => count,
                Err(error) => {
                    error!(error:% = error; "Encountered error in gateway worker");
                    return;
                }
            };
            for entry in &entries[..count] {
                if !entry.lpOverlapped.is_null() {
                    self.state.response_total -= 1;
                    drop(unsafe { SocketPacket::handle_success(entry) });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::DefaultSocketManagerBuilder;
    use crate::hooks::ListenerAdapter;
    use crate::utils::concurrent::{ScheduledThreadManager, SingleThreadFactory};
    use crate::handle::{SessionController, SocketChannel, socket};
    use crate::handle::{ChannelManager, SocketQueueManager};
    use std::net::TcpListener;
    use std::os::windows::io::{FromRawSocket, RawSocket};
    use std::ptr;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use windows_sys::Win32::Networking::WinSock::{
        AF_INET, IPPROTO_TCP, SOCK_STREAM, WSASocketW, WSA_FLAG_OVERLAPPED,
    };

    #[derive(Clone)]
    struct GenericEventAdapter;

    impl ListenerAdapter for GenericEventAdapter {}

    fn listener() -> TcpListener {
        TcpListener::bind("127.0.0.1:0").unwrap()
    }

    #[test]
    fn reconnect_delay() {
        let socket_manager = DefaultSocketManagerBuilder::new()
            .set_socket_pool(ScheduledThreadManager::new(1, SingleThreadFactory::new(64, crate::handle::SocketQueueFactory::default())).unwrap())
            .add_event_listener(GenericEventAdapter)
            .set_reconnect_delay(Duration::from_millis(40))
            .set_max_reconnect_delay(Duration::from_millis(100))
            .build()
            .unwrap();
        listener();
        let socket = unsafe { WSASocketW(AF_INET as i32, SOCK_STREAM, IPPROTO_TCP, ptr::null(), 0, WSA_FLAG_OVERLAPPED) };
        let unbound = unsafe { TcpListener::from_raw_socket(socket as RawSocket) };
        let mut manager = ChannelManager::new(Arc::new(SocketQueueManager::new().unwrap()), 64);
        manager.register(Box::new(SessionController::new(unbound, socket_manager))).unwrap();

        for expected in [40, 80, 100, 100] {
            let delay = Duration::from_millis(manager.worker.timeout() as u64);
            assert!(delay <= Duration::from_millis(expected));
            assert!(delay > Duration::from_millis(expected - 25));
            thread::sleep(delay);
            manager.execute();
        }
    }

    #[test]
    fn connect_timeout() {
        let socket_manager = DefaultSocketManagerBuilder::new()
            .set_socket_pool(ScheduledThreadManager::new(1, SingleThreadFactory::new(64, crate::handle::SocketQueueFactory::default())).unwrap())
            .add_event_listener(GenericEventAdapter)
            .set_connect_timeout(Duration::from_millis(50))
            .build()
            .unwrap();
        let address = listener().local_addr().unwrap();
        let channel = SocketChannel::connect(socket::client(&address).unwrap(), address, socket_manager);
        let mut manager = ChannelManager::new(Arc::new(SocketQueueManager::new().unwrap()), 64);
        manager.register(Box::new(channel)).unwrap();
        assert_eq!(manager.sockets.len(), 1);

        thread::sleep(Duration::from_millis(60));
        manager.execute();
        assert!(manager.sockets.is_empty());
    }
}
