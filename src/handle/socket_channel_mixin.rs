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
use crate::handle::Channel;
use crate::handle::event::Event;
use crate::handle::{BorrowedSocket, as_socket, socket};
use crate::utils::concurrent::ScheduledExecutorService;
use bytes::BytesMut;
use log::debug;
use std::io;
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::rc::Rc;
use std::time::Duration;

pub(crate) struct SocketChannelState {
    pub(crate) socket: TcpStream,
    pub(crate) channel: Rc<MessageChannelMixin>,
    pub(crate) socket_address: Option<SocketAddr>,
    pub(crate) sending: Option<usize>,
    pub(crate) receiving: bool,
    pub(crate) closing: bool,
}

impl SocketChannelState {
    pub(crate) fn new(socket: TcpStream, socket_address: Option<SocketAddr>, socket_manager: SocketManager) -> Self {
        Self {
            socket,
            channel: Rc::new(MessageChannelMixin::new(socket_manager)),
            socket_address,
            sending: None,
            receiving: false,
            closing: false,
        }
    }
}

impl Drop for SocketChannelState {
    fn drop(&mut self) {
        if self.channel.socket_manager().close_timeout() != Some(Duration::ZERO) {
            self.socket.shutdown(Shutdown::Write).ok();
        }
    }
}

/// The [`Channel`] shared by the platform socket channels.
pub(crate) trait SocketChannelMixin: Sized {
    fn state(&self) -> &SocketChannelState;

    fn state_mut(&mut self) -> &mut SocketChannelState;

    fn handle_event(&mut self, manager: &mut ScheduledExecutorService, data: io::Result<Event>) -> io::Result<bool>;

    fn do_register(&self) -> io::Result<()> {
        Ok(())
    }

    fn do_begin_read(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<()>;

    fn do_cancel_read(&mut self, _manager: &mut ScheduledExecutorService) -> io::Result<()> {
        Ok(())
    }

    fn do_write(&mut self, manager: &mut ScheduledExecutorService, messages: Vec<BytesMut>) -> io::Result<()>;

    fn do_close(&mut self, _manager: &mut ScheduledExecutorService) {}

    fn ready(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
        let state = self.state();
        let socket_manager = state.channel.socket_manager();
        socket::init(
            &state.socket,
            socket_manager.tcp_no_delay(),
            socket_manager.is_keep_alive_enabled(),
            socket_manager.keep_alive(),
            socket_manager.close_timeout(),
        )?;
        state.channel.ready(manager);
        Channel::execute(self, manager)
    }

    fn send(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<()> {
        let state = self.state_mut();
        if state.sending.is_none() {
            let limit = state.channel.socket_manager().packet_limit();
            let messages = state.channel.buffer().borrow_mut().read(limit);
            if !messages.is_empty() {
                state.sending = Some(messages.iter().map(BytesMut::len).sum());
                self.do_write(manager, messages)?;
            }
        }
        Ok(())
    }

    fn on_connected(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
        self.state_mut().socket_address = None;
        self.ready(manager)
    }

    fn handle_response(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
        let state = self.state_mut();
        if let Some(size) = state.sending.take() {
            state.channel.handle_response(size);
        }
        Channel::execute(self, manager)
    }
}

impl<T: SocketChannelMixin> Channel for T {
    fn socket(&self) -> BorrowedSocket<'_> {
        as_socket(&self.state().socket)
    }

    fn on_registered(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
        self.do_register()?;
        let state = self.state();
        match state.socket_address {
            Some(address) => {
                manager.connect(&state.socket, &address)?;
                manager.setup_keep_alive(state.channel.socket_manager().connect_timeout());
                Ok(true)
            }
            None => self.ready(manager),
        }
    }

    fn handle(&mut self, manager: &mut ScheduledExecutorService, data: io::Result<Event>) -> io::Result<bool> {
        self.handle_event(manager, data)
    }

    fn execute(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
        let channel = Rc::clone(&self.state().channel);
        channel.execute(manager);
        self.send(manager)?;
        let (is_open, is_deafened) = {
            let buffer = channel.buffer().borrow();
            (buffer.is_open(), buffer.is_deafened())
        };
        if !self.state().receiving && !channel.is_connected() && is_open && !is_deafened {
            self.do_begin_read(manager)?;
            self.state_mut().receiving = true;
        } else if self.state().receiving && is_deafened {
            self.do_cancel_read(manager)?;
        }
        if !is_open && self.state().sending.is_some() && !self.state().closing {
            self.state_mut().closing = true;
            manager.close_after(channel.socket_manager().shutdown_timeout());
        }
        Ok(is_open || self.state().sending.is_some())
    }

    fn shutdown(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
        self.state().channel.buffer().borrow_mut().close();
        self.execute(manager)
    }

    fn check_expired(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
        if self.state().socket_address.is_some() {
            debug!("Socket timed out");
            return Ok(false);
        }
        self.state().channel.check_expired(manager);
        self.execute(manager)
    }

    fn close(&mut self, manager: &mut ScheduledExecutorService) {
        self.state().channel.close(manager);
        self.do_close(manager);
    }
}
