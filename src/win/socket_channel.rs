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
use crate::entities::channel::mixin::middleman::MessageChannelMixin;
use crate::handle::SocketHandler;
use crate::handle::socket;
use crate::handle::socket_packet::{SocketPacket, Type};
use crate::handle::{ChannelManager, SocketChannelMixin};
use bytes::BytesMut;
use std::io;
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::rc::Rc;

pub struct SocketChannel {
    socket: TcpStream,
    channel: Rc<MessageChannelMixin>,
    socket_address: Option<SocketAddr>,
    sending: Option<usize>,
    receiving: bool,
}

impl Drop for SocketChannel {
    fn drop(&mut self) {
        self.socket.shutdown(Shutdown::Write).ok();
    }
}

impl SocketChannel {
    pub(crate) fn new(
        socket: TcpStream,
        socket_manager: SocketManager,
    ) -> Self {
        Self {
            socket,
            channel: Rc::new(MessageChannelMixin::new(socket_manager)),
            socket_address: None,
            sending: None,
            receiving: false,
        }
    }

    pub(crate) fn connect(
        socket: TcpStream,
        address: SocketAddr,
        socket_manager: SocketManager,
    ) -> Self {
        let mut channel = Self::new(socket, socket_manager);
        channel.socket_address = Some(address);
        channel
    }

    fn read(&mut self, manager: &mut ChannelManager) -> io::Result<bool> {
        self.receiving = false;
        let buffer_size = self.channel.socket_manager().buffer_size();
        for _ in 0..self.channel.socket_manager().chunk_limit() {
            let data = {
                let buffer = &mut manager.read_buffer;
                buffer.reserve(buffer_size);
                socket::receive(&self.socket, buffer).map(|length| (length, buffer.split()))
            };
            match data {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Ok((length, message)) if length > 0 => {
                    self.channel.read(manager, message);
                    self.send(manager)?;
                    if length < buffer_size
                        || self.channel.is_connected()
                        || self.channel.buffer().borrow().is_shutdown()
                        || self.channel.buffer().borrow().is_deafened()
                    {
                        break;
                    }
                }
                Ok((0, _)) => {
                    self.channel.buffer().borrow_mut().shutdown();
                    break;
                }
                _ => return Ok(false),
            }
        }
        self.execute(manager)
    }
}

impl SocketChannelMixin for SocketChannel {
    fn stream(&self) -> &TcpStream {
        &self.socket
    }

    fn channel(&self) -> &Rc<MessageChannelMixin> {
        &self.channel
    }

    fn socket_address(&mut self) -> &mut Option<SocketAddr> {
        &mut self.socket_address
    }

    fn sending(&mut self) -> &mut Option<usize> {
        &mut self.sending
    }

    fn receiving(&mut self) -> &mut bool {
        &mut self.receiving
    }

    fn complete(&mut self, manager: &mut ChannelManager, data: io::Result<SocketPacket>) -> io::Result<bool> {
        let Ok(content) = data else {
            return Ok(false);
        };
        match content.method() {
            Type::Connect => {
                socket::connected(&self.socket)?;
                self.on_connected(manager)
            }
            Type::Read => self.read(manager),
            Type::Write => self.handle_response(manager),
            Type::Accept => Ok(false),
        }
    }

    fn do_begin_read(&mut self, manager: &mut ChannelManager) -> io::Result<()> {
        manager.read(&self.socket, SocketPacket::new(0))
    }

    fn do_write(&mut self, manager: &mut ChannelManager, messages: Vec<BytesMut>) -> io::Result<()> {
        manager.write(&self.socket, SocketPacket::wrap(messages))
    }
}
