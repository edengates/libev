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
use crate::handle::socket;
use crate::handle::event::Event;
use crate::handle::{SocketChannelMixin, SocketChannelState};
use crate::utils::concurrent::ScheduledExecutorService;
use bytes::BytesMut;
use std::io;
use std::net::{SocketAddr, TcpStream};

pub struct SocketChannel {
    state: SocketChannelState,
}

impl SocketChannel {
    pub(crate) fn new(
        socket: TcpStream,
        socket_manager: SocketManager,
    ) -> Self {
        Self {
            state: SocketChannelState::new(socket, None, socket_manager),
        }
    }

    pub(crate) fn connect(
        socket: TcpStream,
        address: SocketAddr,
        socket_manager: SocketManager,
    ) -> Self {
        Self {
            state: SocketChannelState::new(socket, Some(address), socket_manager),
        }
    }

    fn read(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
        self.state.receiving = false;
        let buffer_size = self.state.channel.socket_manager().buffer_size();
        for _ in 0..self.state.channel.socket_manager().chunk_limit() {
            let data = {
                let buffer = &mut manager.read_buffer;
                buffer.reserve(buffer_size);
                socket::receive(&self.state.socket, buffer).map(|length| (length, buffer.split()))
            };
            match data {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Ok((length, message)) if length > 0 => {
                    self.state.channel.read(manager, message);
                    self.send(manager)?;
                    if length < buffer_size
                        || self.state.channel.is_connected()
                        || !self.state.channel.buffer().borrow().is_open()
                        || self.state.channel.buffer().borrow().is_deafened()
                    {
                        break;
                    }
                }
                Ok((0, _)) => {
                    self.state.channel.buffer().borrow_mut().close();
                    break;
                }
                _ => return Ok(false),
            }
        }
        self.execute(manager)
    }
}

impl SocketChannelMixin for SocketChannel {
    fn state(&self) -> &SocketChannelState {
        &self.state
    }

    fn state_mut(&mut self) -> &mut SocketChannelState {
        &mut self.state
    }

    fn handle_event(&mut self, manager: &mut ScheduledExecutorService, data: io::Result<Event>) -> io::Result<bool> {
        let Ok(content) = data else {
            return Ok(false);
        };
        match content.opcode() {
            Event::CONNECT => {
                socket::connected(&self.state.socket)?;
                self.on_connected(manager)
            }
            Event::READ => self.read(manager),
            Event::WRITE => self.handle_response(manager),
            _ => Ok(false),
        }
    }

    fn do_begin_read(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<()> {
        manager.read(&self.state.socket, Event::new(0))
    }

    fn do_write(&mut self, manager: &mut ScheduledExecutorService, messages: Vec<BytesMut>) -> io::Result<()> {
        manager.write(&self.state.socket, Event::wrap(messages))
    }
}
