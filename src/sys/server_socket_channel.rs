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
use crate::handle::Channel;
use crate::handle::event::Event;
use crate::handle::socket;
use crate::handle::{self, READABLE};
use crate::utils::concurrent::ScheduledExecutorService;
use log::warn;
use std::io;
use std::net::TcpListener;
use std::os::fd::{AsFd, BorrowedFd};
use std::time::Duration;

pub struct ServerSocketChannel {
    listener: TcpListener,
    socket_manager: SocketManager,
    reconnect_delay: Duration,
}

impl ServerSocketChannel {
    pub(crate) fn new(listener: TcpListener, socket_manager: SocketManager) -> Self {
        let reconnect_delay = socket_manager.reconnect_delay();
        Self {
            listener,
            socket_manager,
            reconnect_delay,
        }
    }

    fn submit(&self, manager: &mut ScheduledExecutorService, is_enabled: bool) -> io::Result<()> {
        handle::submit(manager, &self.listener, if is_enabled { READABLE } else { 0 }, READABLE)
    }
}

impl Channel for ServerSocketChannel {
    fn socket(&self) -> BorrowedFd<'_> {
        self.listener.as_fd()
    }

    fn on_registered(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
        self.submit(manager, true)?;
        Ok(true)
    }

    fn handle(&mut self, manager: &mut ScheduledExecutorService, data: io::Result<Event>) -> io::Result<bool> {
        data?;
        for _ in 0..self.socket_manager.concurrency() {
            match socket::accept(&self.listener) {
                Ok(stream) => {
                    let socket_manager = self.socket_manager.clone();
                    let worker = self.socket_manager.worker_pool().provide();
                    handle::register(worker, stream, socket_manager);
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => {
                    warn!(error:% = error; "Reconnect failed! Next attempt in {}s", self.reconnect_delay.as_secs());
                    self.submit(manager, false)?;
                    manager.retry(self.reconnect_delay);
                    self.reconnect_delay = (self.reconnect_delay * 2).min(self.socket_manager.max_reconnect_delay());
                    return Ok(true);
                }
            }
        }
        self.reconnect_delay = self.socket_manager.reconnect_delay();
        Ok(true)
    }
}
