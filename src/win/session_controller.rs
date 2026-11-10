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

use crate::SocketManager;
use crate::handle::SocketHandler;
use crate::handle::socket_packet::{self, SocketPacket};
use crate::handle::ChannelManager;
use crate::handle::SocketChannel;
use log::warn;
use std::io;
use std::net::TcpListener;
use std::os::windows::io::{AsSocket, BorrowedSocket};
use std::time::Duration;

/// Accepts the connections of a bound socket, and registers a new [`SocketChannel`] for each connection with the
/// [worker pool](SocketManager::worker_pool).
///
/// Up to [`concurrency`](SocketManager::concurrency) accept operations are scheduled at the same time with
/// `AcceptEx`, and a new accept operation is scheduled each time one has completed.
///
/// **Note:** If an accept operation has failed, its socket is closed.
///
/// If an accept operation could not be started while no other accept operation is pending, a warning is logged and
/// libev stops accepting new connections for the [`reconnect_delay`](SocketManager::reconnect_delay), to allow the
/// socket to recover. This delay doubles after each further failed attempt, up to the
/// [`max_reconnect_delay`](SocketManager::max_reconnect_delay), and is reset once all accept operations have been
/// scheduled again.
///
/// # See also
///
/// - [`SocketChannel`]
/// - [`ChannelManager::accept`]
pub struct SessionController {
    listener: TcpListener,
    socket_manager: SocketManager,
    queued: usize,
    reconnect_delay: Duration,
}

impl SessionController {
    /// Creates a new instance for the provided bound socket.
    ///
    /// **Note:** No connection is accepted *until* this instance has been registered with the [`ChannelManager`].
    ///
    /// # Arguments
    ///
    /// - `listener` — The bound socket
    /// - `socket_manager` — The [`SocketManager`] to use
    pub fn new(listener: TcpListener, socket_manager: SocketManager) -> Self {
        let reconnect_delay = socket_manager.reconnect_delay();
        Self {
            listener,
            socket_manager,
            queued: 0,
            reconnect_delay,
        }
    }
}

impl SocketHandler for SessionController {
    fn socket(&self) -> BorrowedSocket<'_> {
        self.listener.as_socket()
    }

    fn connect(&mut self, manager: &mut ChannelManager) -> io::Result<bool> {
        while self.queued < self.socket_manager.concurrency() {
            if let Err(error) = manager.accept(&self.listener) {
                if self.queued == 0 {
                    warn!(error:% = error; "Reconnect failed! Next attempt in {}s", self.reconnect_delay.as_secs());
                    manager.retry(self.reconnect_delay);
                    self.reconnect_delay =
                        (self.reconnect_delay * 2).min(self.socket_manager.max_reconnect_delay());
                }
                return Ok(true);
            }
            self.queued += 1;
        }
        self.reconnect_delay = self.socket_manager.reconnect_delay();
        Ok(true)
    }

    fn handle(
        &mut self,
        manager: &mut ChannelManager,
        data: io::Result<SocketPacket>,
    ) -> io::Result<bool> {
        self.queued -= 1;
        if let Ok(stream) =
            data.and_then(|packet| socket_packet::accepted(&self.listener, packet))
        {
            let socket_manager = self.socket_manager.clone();
            self.socket_manager
                .worker_pool()
                .provide()
                .register(move || Box::new(SocketChannel::new(stream, socket_manager)));
        }
        self.connect(manager)
    }
}
