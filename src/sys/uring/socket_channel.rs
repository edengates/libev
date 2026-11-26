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
use crate::handle::event::Event;
use crate::handle::{SocketChannelMixin, SocketChannelState};
use crate::utils::concurrent::ScheduledExecutorService;
use bytes::BytesMut;
use io_uring::opcode;
use rustix::io::Errno;
use std::io;
use std::net::{SocketAddr, TcpStream};

pub struct SocketChannel {
    state: SocketChannelState,
    cancelling: bool,
}

impl SocketChannel {
    pub(crate) fn new(
        socket: TcpStream,
        socket_manager: SocketManager,
    ) -> Self {
        Self {
            state: SocketChannelState::new(socket, None, socket_manager),
            cancelling: false,
        }
    }

    pub(crate) fn connect(
        socket: TcpStream,
        address: SocketAddr,
        socket_manager: SocketManager,
    ) -> Self {
        Self {
            state: SocketChannelState::new(socket, Some(address), socket_manager),
            cancelling: false,
        }
    }

    fn read(&mut self, manager: &mut ScheduledExecutorService, body: Option<BytesMut>, more: bool) -> io::Result<bool> {
        self.state.receiving = more;
        self.cancelling &= more;
        match body {
            Some(message) => self.state.channel.read(manager, message),
            None => self.state.channel.buffer().borrow_mut().close(),
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
        let packet = match data {
            Ok(Event::IoUring(packet)) => packet,
            Err(error)
                if error.raw_os_error() == Some(Errno::NOBUFS.raw_os_error())
                    || error.raw_os_error() == Some(Errno::CANCELED.raw_os_error()) =>
            {
                self.state.receiving = false;
                self.cancelling = false;
                return self.execute(manager);
            }
            _ => return Ok(false),
        };
        match packet.opcode() {
            opcode::Connect::CODE => self.on_connected(manager),
            opcode::Recv::CODE => {
                let more = packet.more();
                self.read(manager, packet.into_body(), more)
            }
            opcode::Writev::CODE => self.handle_response(manager),
            _ => Ok(false),
        }
    }

    fn do_register(&self) -> io::Result<()> {
        self.state.socket.set_nonblocking(false)
    }

    fn do_begin_read(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<()> {
        manager.read(&self.state.socket)
    }

    fn do_cancel_read(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<()> {
        if manager.is_multishot_enabled() && !self.cancelling {
            self.cancelling = true;
            manager.unread()?;
        }
        Ok(())
    }

    fn do_write(&mut self, manager: &mut ScheduledExecutorService, messages: Vec<BytesMut>) -> io::Result<()> {
        manager.write(&self.state.socket, messages)
    }

    fn do_close(&mut self, manager: &mut ScheduledExecutorService) {
        manager.cancel(&self.state.socket).ok();
    }
}
