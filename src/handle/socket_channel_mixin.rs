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

/// Implements the [`SocketHandler`] of a socket channel, which is shared by the platform specific socket channels.
///
/// The platform specific socket channels only provide their state and the operations of the socket. This mixin
/// decides when these operations are called:
/// - [`connect`](SocketHandler::connect) calls [`do_register`](Self::do_register), and then either starts to connect
///   to the [`socket_address`](Self::socket_address), or calls [`ready`](Self::ready) at once for an accepted
///   connection.
/// - Once the channel is ready, each call of [`execute`](SocketHandler::execute) sends the next messages with
///   [`send`](Self::send) and, if needed, schedules the next read operation with
///   [`do_begin_read`](Self::do_begin_read).
/// - Each completed operation is passed to [`complete`](Self::complete).
/// - [`close`](SocketHandler::close) closes the [`channel`](Self::channel), which fires a
///   [`ChannelDisconnectEvent`](crate::events::channel::ChannelDisconnectEvent) if the channel was ready, and then
///   calls [`do_close`](Self::do_close).
///
/// **At most one read operation and one write operation are scheduled at the same time.**
///
/// If the connection has not been established once the [connect timeout](crate::SocketManager::connect_timeout)
/// has expired, [`check_expired`](SocketHandler::check_expired) closes the channel. On
/// [`shutdown`](SocketHandler::shutdown), the channel is **only** closed once the messages, which are ready to be sent,
/// have been sent.
///
/// # See also
///
/// - [`SocketChannel`](crate::handle::SocketChannel)
/// - [`MessageChannelMixin`]
pub(crate) trait SocketChannelMixin: Sized {
    /// The socket of this channel.
    ///
    /// This is the [`socket`](SocketHandler::socket), which is registered with the [`ChannelManager`].
    fn stream(&self) -> &TcpStream;

    /// The state of this channel.
    ///
    /// This holds the queue of pending messages and the listeners of this channel, see [`MessageChannelMixin`].
    fn channel(&self) -> &Rc<MessageChannelMixin>;

    /// The remote address, while the connection has not been established yet.
    ///
    /// This is `None` for an accepted connection, and is set to `None` by [`on_connected`](Self::on_connected) once
    /// the connection is established.
    fn socket_address(&mut self) -> &mut Option<SocketAddr>;

    /// The amount of bytes, which are currently sent.
    ///
    /// This is `None` if no write operation is scheduled. The next messages are **only** sent once
    /// [`handle_response`](Self::handle_response) has been called for the current write operation.
    fn sending(&mut self) -> &mut Option<usize>;

    /// Whether a read operation is currently scheduled.
    ///
    /// This is set by [`execute`](SocketHandler::execute) after [`do_begin_read`](Self::do_begin_read), and **must** be
    /// reset by the platform specific socket channel once the read operation has completed.
    fn receiving(&mut self) -> &mut bool;

    /// Handles the completed operation of the socket.
    ///
    /// The platform specific socket channel:
    /// - calls [`on_connected`](Self::on_connected) for a completed connect operation
    /// - gives the received messages to the [`channel`](Self::channel) for a completed read operation
    /// - calls [`handle_response`](Self::handle_response) for a completed write operation
    ///
    /// # Arguments
    ///
    /// - `manager` — The [`ChannelManager`] of this channel
    /// - `data` — The completed operation, or the error of the operation
    ///
    /// # Errors
    ///
    /// Returns an error if the completed operation could not be handled, for example because the next operation
    /// could not be started. The channel is closed in this case.
    ///
    /// # Returns
    ///
    /// `true` if the channel should stay registered, or `false` if the channel should be closed, for example because
    /// the operation has failed.
    ///
    /// # See also
    ///
    /// - [`SocketHandler::handle`]
    fn complete(&mut self, manager: &mut ChannelManager, data: io::Result<SocketPacket>) -> io::Result<bool>;

    /// Called after the channel has been registered with the [`ChannelManager`].
    ///
    /// This is called at the start of [`connect`](SocketHandler::connect), *before* the channel starts to connect or
    /// is ready. By default, this does nothing.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket could not be set up. The channel is closed in this case.
    fn do_register(&self) -> io::Result<()> {
        Ok(())
    }

    /// Schedules a read operation.
    ///
    /// This is **only** called if:
    /// - no read operation is scheduled
    /// - the listeners are not running
    /// - and the channel is neither shutdown nor deafened
    ///
    /// Once the read operation has completed, it is passed to [`complete`](Self::complete).
    ///
    /// # Arguments
    ///
    /// - `manager` — The [`ChannelManager`] of this channel
    ///
    /// # Errors
    ///
    /// Returns an error if the operation could not be started.
    fn do_begin_read(&mut self, manager: &mut ChannelManager) -> io::Result<()>;

    /// Sends the provided messages over the connection.
    ///
    /// This is **only** called by [`send`](Self::send), if no write operation is scheduled. If only some of the bytes
    /// have been sent, the [`ChannelManager`] sends the remaining bytes. Once **all** bytes have been sent, the
    /// completed write operation is passed to [`complete`](Self::complete).
    ///
    /// # Arguments
    ///
    /// - `manager` — The [`ChannelManager`] of this channel
    /// - `messages` — The messages to send
    ///
    /// # Errors
    ///
    /// Returns an error if the operation could not be started.
    fn do_write(&mut self, manager: &mut ChannelManager, messages: Vec<BytesMut>) -> io::Result<()>;

    /// Closes the channel.
    ///
    /// This is called by [`close`](SocketHandler::close) after the [`channel`](Self::channel) has been closed, for
    /// example to cancel the pending operations of the socket. The socket itself is closed once the channel is
    /// dropped. By default, this does nothing.
    ///
    /// # Arguments
    ///
    /// - `_manager` — The [`ChannelManager`] of this channel
    ///
    /// # See also
    ///
    /// - [`do_register`](Self::do_register)
    fn do_close(&mut self, _manager: &mut ChannelManager) {}

    /// Initializes the socket and fires a [`ChannelReadyEvent`](crate::events::channel::ChannelReadyEvent).
    ///
    /// This enables `TCP_NODELAY` and sets `SO_LINGER`, if configured by the [`SocketManager`](crate::SocketManager).
    /// Then the first messages are sent with [`execute`](SocketHandler::execute). The first read operation is **only**
    /// scheduled once the listeners have handled this event.
    ///
    /// # Arguments
    ///
    /// - `manager` — The [`ChannelManager`] of this channel
    ///
    /// # Errors
    ///
    /// Returns an error:
    /// - If the socket could not be initialized, in which case no event is fired
    /// - If the first messages could not be sent
    ///
    /// The channel is closed in this case.
    ///
    /// # Returns
    ///
    /// `true` if the channel should stay registered, or `false` if the channel should be closed, see
    /// [`complete`](Self::complete).
    ///
    /// # See also
    ///
    /// - [`on_connected`](Self::on_connected)
    fn ready(&mut self, manager: &mut ChannelManager) -> io::Result<bool> {
        let socket_manager = self.channel().socket_manager();
        socket::init(self.stream(), socket_manager.tcp_no_delay(), socket_manager.close_timeout())?;
        self.channel().ready(manager);
        SocketHandler::execute(self, manager)
    }

    /// Sends the next messages, if no messages are currently sent.
    ///
    /// Up to [`packet_limit`](crate::SocketManager::packet_limit) messages, which are ready to be sent, are removed
    /// from the queue of pending messages and passed to [`do_write`](Self::do_write). If no message is ready to be
    /// sent, this does nothing.
    ///
    /// # Arguments
    ///
    /// - `manager` — The [`ChannelManager`] of this channel
    ///
    /// # Errors
    ///
    /// Returns an error if the write operation could not be started.
    ///
    /// # See also
    ///
    /// - [`handle_response`](Self::handle_response)
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

    /// Finishes the connection and fires a [`ChannelReadyEvent`](crate::events::channel::ChannelReadyEvent).
    ///
    /// This is called by the platform specific socket channel once the connect operation has completed. It sets the
    /// [`socket_address`](Self::socket_address) to `None`, so the connect timeout no longer applies, and then calls
    /// [`ready`](Self::ready).
    ///
    /// # Arguments
    ///
    /// - `manager` — The [`ChannelManager`] of this channel
    ///
    /// # Errors
    ///
    /// Returns an error if [`ready`](Self::ready) failed. The channel is closed in this case.
    ///
    /// # Returns
    ///
    /// The result of [`ready`](Self::ready).
    fn on_connected(&mut self, manager: &mut ChannelManager) -> io::Result<bool> {
        *self.socket_address() = None;
        self.ready(manager)
    }

    /// Called once the messages have been sent.
    ///
    /// This removes the sent bytes from the queue of pending messages, and then sends the next messages with
    /// [`execute`](SocketHandler::execute).
    ///
    /// # Arguments
    ///
    /// - `manager` — The [`ChannelManager`] of this channel
    ///
    /// # Errors
    ///
    /// Returns an error if the next operation could not be started. The channel is closed in this case.
    ///
    /// # Returns
    ///
    /// The result of [`execute`](SocketHandler::execute).
    ///
    /// # See also
    ///
    /// - [`send`](Self::send)
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
