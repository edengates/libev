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
use crate::utils::concurrent::ScheduledExecutorService;
use crate::handle::SocketChannel;
use log::warn;
use std::io;
use std::net::TcpListener;
use std::os::windows::io::{AsSocket, BorrowedSocket};
use std::time::Duration;

pub struct ServerSocketChannel {
    listener: TcpListener,
    socket_manager: SocketManager,
    queued: usize,
    reconnect_delay: Duration,
}

impl ServerSocketChannel {
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

impl Channel for ServerSocketChannel {
    fn socket(&self) -> BorrowedSocket<'_> {
        self.listener.as_socket()
    }

    fn on_registered(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
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
        manager: &mut ScheduledExecutorService,
        data: io::Result<Event>,
    ) -> io::Result<bool> {
        self.queued -= 1;
        if let Ok(stream) =
            data.and_then(|packet| socket::accepted(&self.listener, packet))
        {
            let socket_manager = self.socket_manager.clone();
            self.socket_manager
                .worker_pool()
                .provide()
                .register(move || Box::new(SocketChannel::new(stream, socket_manager)));
        }
        self.on_registered(manager)
    }
}
