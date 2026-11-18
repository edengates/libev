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
use crate::utils::{EventManager, EventQueue};
use crate::utils::Buffer;
use bytes::BytesMut;
use std::cell::RefCell;
use std::io;
use std::rc::Rc;

/// Abstract channel interface for all channels, to which messages can be sent.
///
/// All operations are asynchronous and return immediately, with no guarantee that the request has been completed at
/// the end of the call.
///
/// [`MessageChannel`]: crate::entities::channel::middleman::MessageChannel
pub trait Sink<T> {
    /// Request to write a message via this channel. This does not request a flush, call
    /// [`flush`](Self::flush) for that.
    ///
    /// # Errors
    ///
    /// Returns [`NotConnected`] if the channel has been closed.
    ///
    /// [`NotConnected`]: io::ErrorKind::NotConnected
    fn write(&self, message: T) -> io::Result<()>;

    /// Request to flush all pending messages via this channel.
    fn flush(&self);

    /// Request to close this channel once all pending messages have been sent.
    fn close(&self);
}

pub struct BufferedSink {
    socket_manager: SocketManager,
    buffer: Rc<RefCell<Buffer>>,
    events: EventQueue,
}

impl BufferedSink {
    pub(crate) fn new(socket_manager: &SocketManager, buffer: &Rc<RefCell<Buffer>>, events: &EventQueue) -> Rc<Self> {
        Rc::new(Self {
            socket_manager: socket_manager.clone(),
            buffer: Rc::clone(buffer),
            events: Rc::clone(events),
        })
    }
}

impl Sink<BytesMut> for BufferedSink {
    fn write(&self, message: BytesMut) -> io::Result<()> {
        self.buffer
            .borrow_mut()
            .write(message, self.socket_manager.threshold(), self.socket_manager.large_threshold())
    }

    fn flush(&self) {
        self.buffer.borrow_mut().emit();
    }

    fn close(&self) {
        self.buffer.borrow_mut().close();
    }
}

impl EventManager for Rc<BufferedSink> {
    type Output = BytesMut;

    fn events(&self) -> &EventQueue {
        &self.events
    }

    fn channel(&self) -> Rc<dyn Sink<BytesMut>> {
        Rc::clone(self) as Rc<dyn Sink<BytesMut>>
    }

    async fn handle(&mut self) -> io::Result<bool> {
        Ok(false)
    }

    fn clear(&mut self) {
        self.events.borrow_mut().clear();
    }
}
