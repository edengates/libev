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
use crate::entities::channel::Channel;
use crate::hooks::{EventManager, EventQueue};
use crate::utils::SocketBufferQueue;
use bytes::BytesMut;
use std::cell::RefCell;
use std::io;
use std::rc::Rc;

/// The first [`EventManager`] of every channel, between the socket and the listeners.
///
/// The events of the socket are added to its [`EventQueue`] by the [`MessageChannelMixin`], and are handled by the
/// first listener. If a message goes beyond the first listener, it is added to the queue of pending messages, which
/// are written to the socket by the platform specific socket channel.
///
/// # See also
///
/// - [`InterfacedEventManager`](crate::hooks::InterfacedEventManager)
///
/// [`MessageChannelMixin`]: crate::entities::channel::mixin::middleman::MessageChannelMixin
pub struct EventManagerProxy {
    socket_manager: SocketManager,
    buffer: Rc<RefCell<SocketBufferQueue>>,
    events: EventQueue<BytesMut>,
}

impl EventManagerProxy {
    /// Creates a new instance with the provided queue of pending messages and the [`EventQueue`] of the socket.
    ///
    /// # Arguments
    ///
    /// - `socket_manager` — The [`SocketManager`] instance of this channel
    /// - `buffer` — The queue of pending messages
    /// - `events` — The [`EventQueue`] of the socket
    pub(crate) fn new(socket_manager: &SocketManager, buffer: &Rc<RefCell<SocketBufferQueue>>, events: &EventQueue<BytesMut>) -> Rc<Self> {
        Rc::new(Self {
            socket_manager: socket_manager.clone(),
            buffer: Rc::clone(buffer),
            events: Rc::clone(events),
        })
    }
}

/// The parent channel of the first listener, which adds the messages to the queue of pending messages.
impl Channel<BytesMut> for EventManagerProxy {
    /// Adds the message to the queue of pending messages and updates the slowmode.
    ///
    /// # Arguments
    ///
    /// - `message` — The message to write
    ///
    /// # Errors
    ///
    /// Returns [`NotConnected`] if the queue of pending messages has been shutdown.
    ///
    /// # See also
    ///
    /// - [`execute`](Self::execute)
    ///
    /// [`NotConnected`]: io::ErrorKind::NotConnected
    fn enqueue(&self, message: BytesMut) -> io::Result<()> {
        self.buffer
            .borrow_mut()
            .enqueue(message, self.socket_manager.threshold(), self.socket_manager.large_threshold())
    }

    /// Marks all messages, which have been added since the last call, as ready to be written to the socket.
    ///
    /// # See also
    ///
    /// - [`enqueue`](Self::enqueue)
    fn execute(&self) {
        self.buffer.borrow_mut().execute();
    }

    /// Shuts down the queue of pending messages.
    ///
    /// The connection is closed after all pending messages have been sent.
    fn close(&self) {
        self.buffer.borrow_mut().shutdown();
    }
}

impl EventManager for Rc<EventManagerProxy> {
    type Output = BytesMut;

    fn events(&self) -> &EventQueue<BytesMut> {
        &self.events
    }

    fn channel(&self) -> Rc<dyn Channel<BytesMut>> {
        Rc::clone(self) as Rc<dyn Channel<BytesMut>>
    }

    /// Nothing is handled here: the events of the socket are added to the [`EventQueue`] directly.
    ///
    /// # Returns
    ///
    /// **Always** returns `false`.
    async fn handle(&mut self) -> io::Result<bool> {
        Ok(false)
    }

    fn clear(&mut self) {
        self.events.borrow_mut().clear();
    }
}
