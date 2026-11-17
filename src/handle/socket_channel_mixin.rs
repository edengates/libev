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

use crate::entities::channel::mixin::middleman::MessageChannelMixin;
use crate::handle::SocketHandler;
use crate::handle::socket_packet::SocketPacket;
use crate::handle::{ChannelManager, BorrowedSocket, as_socket, socket};
use bytes::BytesMut;
use log::debug;
use std::io;
use std::net::{SocketAddr, TcpStream};
use std::rc::Rc;

/// The [`SocketHandler`] shared by the platform socket channels.
pub(crate) trait SocketChannelMixin: Sized {
    fn stream(&self) -> &TcpStream;

    fn channel(&self) -> &Rc<MessageChannelMixin>;

    fn socket_address(&mut self) -> &mut Option<SocketAddr>;

    fn sending(&mut self) -> &mut Option<usize>;

    fn receiving(&mut self) -> &mut bool;

    fn complete(&mut self, manager: &mut ChannelManager, data: io::Result<SocketPacket>) -> io::Result<bool>;

    fn do_register(&self) -> io::Result<()> {
        Ok(())
    }

    fn do_begin_read(&mut self, manager: &mut ChannelManager) -> io::Result<()>;

    fn do_write(&mut self, manager: &mut ChannelManager, messages: Vec<BytesMut>) -> io::Result<()>;

    fn do_close(&mut self, _manager: &mut ChannelManager) {}

    fn ready(&mut self, manager: &mut ChannelManager) -> io::Result<bool> {
        let socket_manager = self.channel().socket_manager();
        socket::init(self.stream(), socket_manager.tcp_no_delay(), socket_manager.close_timeout())?;
        self.channel().ready(manager);
        SocketHandler::execute(self, manager)
    }

    fn send(&mut self, manager: &mut ChannelManager) -> io::Result<()> {
        if self.sending().is_none() {
            let limit = self.channel().socket_manager().packet_limit();
            let messages = self.channel().buffer().borrow_mut().next_packet(limit);
            if !messages.is_empty() {
                *self.sending() = Some(messages.iter().map(BytesMut::len).sum());
                self.do_write(manager, messages)?;
            }
        }
        Ok(())
    }

    fn on_connected(&mut self, manager: &mut ChannelManager) -> io::Result<bool> {
        *self.socket_address() = None;
        self.ready(manager)
    }

    fn handle_response(&mut self, manager: &mut ChannelManager) -> io::Result<bool> {
        if let Some(size) = self.sending().take() {
            self.channel().handle_response(size);
        }
        SocketHandler::execute(self, manager)
    }
}

impl<T: SocketChannelMixin> SocketHandler for T {
    fn socket(&self) -> BorrowedSocket<'_> {
        as_socket(self.stream())
    }

    fn connect(&mut self, manager: &mut ChannelManager) -> io::Result<bool> {
        self.do_register()?;
        match *self.socket_address() {
            Some(address) => {
                manager.connect(self.stream(), &address)?;
                manager.setup_keep_alive(self.channel().socket_manager().connect_timeout());
                Ok(true)
            }
            None => self.ready(manager),
        }
    }

    fn handle(&mut self, manager: &mut ChannelManager, data: io::Result<SocketPacket>) -> io::Result<bool> {
        self.complete(manager, data)
    }

    fn execute(&mut self, manager: &mut ChannelManager) -> io::Result<bool> {
        let channel = Rc::clone(self.channel());
        channel.execute(manager);
        self.send(manager)?;
        if !*self.receiving()
            && !channel.is_connected()
            && !channel.buffer().borrow().is_shutdown()
            && !channel.buffer().borrow().is_deafened()
        {
            self.do_begin_read(manager)?;
            *self.receiving() = true;
        }
        Ok(!channel.buffer().borrow().is_shutdown() || self.sending().is_some())
    }

    fn shutdown(&mut self, manager: &mut ChannelManager) -> io::Result<bool> {
        self.channel().buffer().borrow_mut().shutdown();
        self.execute(manager)
    }

    fn check_expired(&mut self, manager: &mut ChannelManager) -> io::Result<bool> {
        if self.socket_address().is_some() {
            debug!("Socket timed out");
            return Ok(false);
        }
        self.channel().check_expired(manager);
        self.execute(manager)
    }

    fn close(&mut self, manager: &mut ChannelManager) {
        self.channel().close(manager);
        self.do_close(manager);
    }
}
