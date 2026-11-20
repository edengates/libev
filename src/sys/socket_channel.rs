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
use crate::handle::{self, READABLE, WRITABLE};
use crate::utils::concurrent::ScheduledExecutorService;
use crate::handle::event::Event;
use crate::handle::socket;
use crate::utils::buffer;
use bytes::BytesMut;
use log::debug;
use std::io::{self, IoSlice, Write};
use std::mem;
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::os::fd::{AsFd, BorrowedFd};
use std::rc::Rc;
use std::time::Duration;

pub struct SocketChannel {
    socket: TcpStream,
    channel: Rc<MessageChannelMixin>,
    socket_address: Option<SocketAddr>,
    sending: usize,
    packet: Vec<BytesMut>,
    flags: u32,
    closing: bool,
}

impl Drop for SocketChannel {
    fn drop(&mut self) {
        if self.channel.socket_manager().close_timeout() != Some(Duration::ZERO) {
            self.socket.shutdown(Shutdown::Write).ok();
        }
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
            sending: 0,
            packet: Vec::new(),
            flags: 0,
            closing: false,
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

    fn enable_flag(&mut self, manager: &mut ScheduledExecutorService, flag: u32) -> io::Result<()> {
        if self.flags & flag != 0 {
            // we can save a syscall if the flags did not change
            return Ok(());
        }
        self.flags |= flag;
        self.submit(manager, flag)
    }

    fn disable_flag(&mut self, manager: &mut ScheduledExecutorService, flag: u32) -> io::Result<()> {
        if self.flags & flag == 0 {
            // we can save a syscall if the flags did not change
            return Ok(());
        }
        self.flags &= !flag;
        self.submit(manager, flag)
    }

    fn submit(&self, manager: &mut ScheduledExecutorService, flag: u32) -> io::Result<()> {
        handle::submit(manager, &self.socket, self.flags, flag)
    }

    fn ready(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
        socket::init(&self.socket, self.channel.socket_manager().tcp_no_delay(), self.channel.socket_manager().close_timeout())?;
        self.channel.ready(manager);
        self.execute(manager)
    }

    fn next_packet(&mut self) {
        let mut buffer = self.channel.buffer().borrow_mut();
        let packet = buffer.read(self.channel.socket_manager().packet_limit());
        buffer.reuse(mem::replace(&mut self.packet, packet));
        self.sending = self.packet.iter().map(BytesMut::len).sum();
    }

    fn send(&mut self) -> io::Result<bool> {
        if self.packet.is_empty() {
            self.next_packet();
            if self.packet.is_empty() {
                return Ok(false);
            }
        }
        while !self.packet.is_empty() {
            let mut slices = [IoSlice::new(&[]); 64];
            let total = slices.len().min(self.packet.len());
            for (slice, buffer) in slices.iter_mut().zip(&self.packet) {
                *slice = IoSlice::new(buffer);
            }
            let expected: usize = slices[..total].iter().map(|slice| slice.len()).sum();
            let data = if total == 1 {
                (&self.socket).write(&self.packet[0])
            } else {
                (&self.socket).write_vectored(&slices[..total])
            };
            let length = match data {
                Ok(length) => length,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(false),
                Err(error) => return Err(error),
            };
            buffer::advance(&mut self.packet, length);
            if length != expected {
                return Ok(false);
            }
        }
        self.channel.handle_response(self.sending);
        Ok(true)
    }

    fn on_connected(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
        if !socket::connected(&self.socket)? {
            return Ok(true);
        }
        self.socket_address = None;
        self.disable_flag(manager, WRITABLE)?;
        self.ready(manager)
    }

    fn read(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
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
                    if length < buffer_size
                        || self.channel.is_connected()
                        || {
                            let buffer = self.channel.buffer().borrow();
                            !buffer.is_open() || buffer.is_deafened()
                        }
                    {
                        break;
                    }
                }
                Ok((0, _)) => {
                    self.channel.buffer().borrow_mut().close();
                    break;
                }
                _ => return Ok(false),
            }
        }
        self.execute(manager)
    }
}

impl Channel for SocketChannel {
    fn execute(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
        self.channel.execute(manager);
        while self.send()? {
            self.channel.execute(manager);
        }
        if self.packet.is_empty() {
            self.disable_flag(manager, WRITABLE)?;
        } else {
            self.enable_flag(manager, WRITABLE)?;
        }
        let (is_open, is_deafened) = {
            let buffer = self.channel.buffer().borrow();
            (buffer.is_open(), buffer.is_deafened())
        };
        if !self.channel.is_connected() && is_open && !is_deafened {
            self.enable_flag(manager, READABLE)?;
        } else {
            self.disable_flag(manager, READABLE)?;
        }
        if !is_open && !self.packet.is_empty() && !self.closing {
            self.closing = true;
            manager.close_after(self.channel.socket_manager().shutdown_timeout());
        }
        Ok(is_open || !self.packet.is_empty())
    }

    fn socket(&self) -> BorrowedFd<'_> {
        self.socket.as_fd()
    }

    fn on_registered(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
        match self.socket_address {
            Some(address) => {
                if socket::connect(&self.socket, &address)? {
                    return self.on_connected(manager);
                }
                self.enable_flag(manager, WRITABLE)?;
                manager.setup_keep_alive(self.channel.socket_manager().connect_timeout());
                Ok(true)
            }
            None => self.ready(manager),
        }
    }

    fn handle(&mut self, manager: &mut ScheduledExecutorService, data: io::Result<Event>) -> io::Result<bool> {
        let packet = data?;
        if packet.is_writable() && self.socket_address.is_some() {
            return self.on_connected(manager);
        }
        if packet.is_readable() {
            return self.read(manager);
        }
        self.execute(manager)
    }

    fn shutdown(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
        self.channel.buffer().borrow_mut().close();
        self.execute(manager)
    }

    fn check_expired(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
        if self.socket_address.is_some() {
            debug!("Socket timed out");
            return Ok(false);
        }
        self.channel.check_expired(manager);
        self.execute(manager)
    }

    fn close(&mut self, manager: &mut ScheduledExecutorService) {
        self.channel.close(manager);
    }
}
