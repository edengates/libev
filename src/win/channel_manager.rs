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

/// The state of the [`ChannelManager`] on Windows.
///
/// It holds the completed operations of the [`SocketQueueManager`] and the amount of operations, which have not
/// completed yet.
///
/// Each operation, which has been started with one of the following methods, is counted until its [`SocketPacket`]
/// has been returned by the [`SocketQueueManager`]:
/// - [`read`](ChannelManager::read)
/// - [`write`](ChannelManager::write)
/// - [`accept`](ChannelManager::accept)
/// - [`connect`](ChannelManager::connect)
///
/// On [`close`](ChannelManager::close) this amount is used to wait for each pending operation, as the packet of an
/// operation **must not** be released *before* the operation has completed.
pub struct ChannelState {
    pub(in crate::handle) entries: Vec<OVERLAPPED_ENTRY>,
    pub(in crate::handle) response_total: usize,
}

impl ChannelState {
    /// Creates a new instance, which handles up to `capacity` events at once.
    ///
    /// **Note:** At least one event is handled at once, even if `capacity` is `0`.
    ///
    /// # Arguments
    ///
    /// - `_port` — The [`SocketQueueManager`] of the [`ChannelManager`]
    /// - `capacity` — The maximum amount of events, which are handled at once
    pub(crate) fn new(_port: &SocketQueueManager, capacity: usize) -> Self {
        Self {
            entries: vec![unsafe { mem::zeroed() }; capacity.max(1)],
            response_total: 0,
        }
    }
}

impl ChannelManager {
    /// Blocks the current thread until the next operations have completed or the next scheduled task is ready.
    ///
    /// Each completed operation is handled by the [`SocketHandler`] of its identifier, see
    /// [`handle`](Self::handle). If no task has been scheduled, the current thread is blocked until an operation has
    /// completed or an identifier has been sent with [`SocketQueueManager::enqueue`]. Up to the capacity of the
    /// [`ChannelState`] operations are handled at once, the remaining operations are handled with the next call.
    ///
    /// # Errors
    ///
    /// Returns the error of the [`SocketQueueManager`], in this case no operation is handled.
    ///
    /// # See also
    ///
    /// - [`SocketQueueManager::poll`]
    /// - [`execute`](ChannelManager::execute)
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

    /// Schedules a read operation for the provided socket with the provided packet.
    ///
    /// The socket **must** be registered with the [`SocketQueueManager`] of this instance. The packet is held by the
    /// operation until it has completed, then it is given to the [`SocketHandler`] of the socket. A packet without
    /// buffer, see [`SocketPacket::new`], does **not** receive any bytes, the operation **only** completes once bytes
    /// can be received.
    ///
    /// # Arguments
    ///
    /// - `socket` — The registered socket
    /// - `packet` — The [`SocketPacket`], which receives the bytes
    ///
    /// # Errors
    ///
    /// Returns an error if the operation could not be started, the packet is released in this case.
    ///
    /// # See also
    ///
    /// - [`write`](Self::write)
    /// - [`socket_packet::read`]
    pub fn read<S: AsRawSocket>(&mut self, socket: &S, packet: Box<SocketPacket>) -> io::Result<()> {
        socket_packet::read(socket, packet)?;
        self.state.response_total += 1;
        Ok(())
    }

    /// Schedules a write operation for the provided socket with the provided packet.
    ///
    /// The socket **must** be registered with the [`SocketQueueManager`] of this instance. The packet is held by the
    /// operation until it has completed. If only a part of the bytes has been sent, the operation is started again
    /// with the remaining bytes, so the [`SocketHandler`] of the socket gets the packet **only** once **all** bytes
    /// have been sent or the operation has failed.
    ///
    /// # Arguments
    ///
    /// - `socket` — The registered socket
    /// - `packet` — The [`SocketPacket`] with the bytes to send
    ///
    /// # Errors
    ///
    /// Returns an error if the operation could not be started, the packet is released in this case.
    ///
    /// # See also
    ///
    /// - [`read`](Self::read)
    /// - [`socket_packet::write`](fn@socket_packet::write)
    pub fn write<S: AsRawSocket>(&mut self, socket: &S, packet: Box<SocketPacket>) -> io::Result<()> {
        socket_packet::write(socket, packet)?;
        self.state.response_total += 1;
        Ok(())
    }

    /// Schedules an accept operation for the provided socket.
    ///
    /// The socket **must** be registered with the [`SocketQueueManager`] of this instance. A new socket for the
    /// connection is created and held by the packet of the operation. Once the operation has completed, the
    /// connection can be retrieved with [`socket_packet::accepted`].
    ///
    /// # Arguments
    ///
    /// - `listener` — The registered and bound socket
    ///
    /// # Errors
    ///
    /// Returns an error:
    /// - If the new socket could not be created
    /// - If the operation could not be started
    ///
    /// # See also
    ///
    /// - [`SessionController`](crate::handle::SessionController)
    pub fn accept(&mut self, listener: &TcpListener) -> io::Result<()> {
        socket_packet::accept(listener)?;
        self.state.response_total += 1;
        Ok(())
    }

    /// Schedules a connect operation to the provided address.
    ///
    /// The socket **must** be registered with the [`SocketQueueManager`] of this instance and **must not** be bound
    /// yet, it is bound to any local address *before* the operation is started. Once the operation has completed,
    /// [`socket::connected`](crate::handle::socket::connected) **must** be called *before* the socket can be used.
    ///
    /// # Arguments
    ///
    /// - `stream` — The registered socket, which is not bound yet
    /// - `address` — The address to connect to
    ///
    /// # Errors
    ///
    /// Returns an error:
    /// - If the socket could not be bound
    /// - If the operation could not be started
    ///
    /// # See also
    ///
    /// - [`socket::client`](crate::handle::socket::client)
    /// - [`socket_packet::connect`]
    pub fn connect(&mut self, stream: &TcpStream, address: &SocketAddr) -> io::Result<()> {
        socket_packet::connect(stream, address)?;
        self.state.response_total += 1;
        Ok(())
    }

    /// Handles the completed operation.
    ///
    /// An entry without packet holds an identifier, which has been sent with [`SocketQueueManager::enqueue`]. **Only**
    /// [`SHUTDOWN`] is handled here, it shuts down this instance **instantly**. Any other identifier only stops the
    /// blocking of the current thread.
    ///
    /// The packet of any other entry is given to the [`SocketHandler`] of the identifier of the entry, see
    /// [`handle_event`](Self::handle_event). If the [`SocketHandler`] has already been closed, the packet is
    /// released.
    ///
    /// A write operation, which has sent only a part of the messages, is scheduled again with the remaining part. If
    /// it could not be scheduled again, the [`SocketHandler`] is closed.
    ///
    /// # Arguments
    ///
    /// - `entry` — The completed operation of the [`SocketQueueManager`]
    ///
    /// # See also
    ///
    /// - [`run_worker`](Self::run_worker)
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

    /// Closes each socket and blocks the current thread until all operations have completed.
    ///
    /// Once a socket is closed, Windows cancels its pending operations, so each of them completes with an error. The
    /// packets of these operations are released without being handled.
    ///
    /// <div class="warning">
    ///
    /// If the completed operations could not be retrieved, the error is logged and the packets of the remaining
    /// operations are **never** released.
    ///
    /// </div>
    ///
    /// # See also
    ///
    /// - [`close_resource`](ChannelManager::close_resource)
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
