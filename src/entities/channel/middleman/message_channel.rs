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
use crate::events::channel::update::ChannelUpdateSlowmodeEvent;
use crate::events::channel::{ChannelDisconnectEvent, ChannelReadyEvent, ChannelTimeoutEvent};
use crate::events::message::MessageReceivedEvent;
use crate::events::{Event, GenericEvent};
use crate::requests::sockactions::MessageCreateAction;
use crate::requests::Sink;
use crate::utils::EventQueue;
use crate::utils::Buffer;
use bytes::BytesMut;
use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::{Arc, RwLock};

/// Represents a channel that can have messages sent to it.
///
/// Every registered [`ListenerAdapter`] has its own [`MessageChannel`] with the position of the listener.
/// The `on_*` methods fire events to the next listener.
///
/// `N` is the type of messages for the next listener, `P` is the type of messages for the parent channel.
///
/// [`ListenerAdapter`]: crate::hooks::ListenerAdapter
pub struct MessageChannel<N = BytesMut, P = BytesMut> {
    socket_manager: SocketManager,
    buffer: Rc<RefCell<Buffer>>,
    next: EventQueue,
    parent: Rc<dyn Sink<P>>,
    marker: PhantomData<N>,
}

impl<N, P> Clone for MessageChannel<N, P> {
    fn clone(&self) -> Self {
        Self {
            socket_manager: self.socket_manager.clone(),
            buffer: Rc::clone(&self.buffer),
            next: Rc::clone(&self.next),
            parent: Rc::clone(&self.parent),
            marker: PhantomData,
        }
    }
}

impl<N, P> MessageChannel<N, P> {
    pub(crate) fn new(
        socket_manager: SocketManager,
        buffer: Rc<RefCell<Buffer>>,
        next: EventQueue,
        parent: Rc<dyn Sink<P>>,
    ) -> Self {
        Self {
            socket_manager,
            buffer,
            next,
            parent,
            marker: PhantomData,
        }
    }

    /// Returns the [`SocketManager`] instance of this channel.
    pub fn socket_manager(&self) -> &SocketManager {
        &self.socket_manager
    }

    /// The data mappings to use in [`SocketManager`], shared by all of its channels.
    pub fn context_map(&self) -> &RwLock<HashMap<String, Arc<dyn Any + Send + Sync>>> {
        self.socket_manager.context_map()
    }

    /// Fires a [`ChannelReadyEvent`].
    pub fn on_ready(&mut self) {
        self.handle(Box::new(ChannelReadyEvent));
    }

    /// Passes a received message on as a [`MessageReceivedEvent`].
    pub fn on_message_received(&mut self, message: N)
    where
        N: Any + Send,
    {
        self.handle(Box::new(MessageReceivedEvent::new(
            message,
        )));
    }

    /// Tells the next listener that the peer has gone, with a [`ChannelDisconnectEvent`].
    pub fn on_channel_disconnect(&mut self) {
        self.handle(Box::new(ChannelDisconnectEvent));
    }

    /// Fires a [`ChannelUpdateSlowmodeEvent`] when [`is_locked`](Self::is_locked) has changed.
    pub fn on_channel_update_slowmode(&mut self) {
        self.handle(Box::new(
            ChannelUpdateSlowmodeEvent,
        ));
    }

    /// Fires a [`ChannelTimeoutEvent`] once the [AFK timeout] is reached.
    ///
    /// [AFK timeout]: crate::DefaultSocketManagerBuilder::set_afk_timeout
    pub fn on_channel_timeout(&mut self) {
        self.handle(Box::new(ChannelTimeoutEvent));
    }

    /// Fires a user defined event.
    pub fn on_generic_event(&mut self, event: Box<dyn GenericEvent>) {
        self.handle(Box::new(event));
    }

    fn handle(&mut self, event: Box<dyn Event>) {
        self.next.borrow_mut().push_back(event);
    }

    /// Request to write a message via this channel.
    ///
    /// The previous listener gets it in [`ListenerAdapter::write`], and the first one sends it to the socket as
    /// [`BytesMut`]. This does not request a flush, call [`flush`] for that.
    ///
    /// # Errors
    ///
    /// Returns [`NotConnected`] if the channel has been closed.
    ///
    /// [`ListenerAdapter::write`]: crate::hooks::ListenerAdapter::write
    /// [`flush`]: Self::flush
    /// [`NotConnected`]: io::ErrorKind::NotConnected
    pub fn write(&mut self, message: P) -> io::Result<()> {
        Sink::write(self, message)
    }

    /// Whether this channel is in slow mode.
    ///
    /// Slow mode starts when the queue of pending messages is larger than the [`large_threshold`], and ends when it
    /// is smaller than the [`threshold`] again.
    ///
    /// [`large_threshold`]: SocketManager::large_threshold
    /// [`threshold`]: SocketManager::threshold
    pub fn is_locked(&self) -> bool {
        self.buffer.borrow().is_slowmode()
    }

    /// Request to flush all pending messages via this channel.
    ///
    /// [`MessageCreateAction::queue`] calls this method for you.
    ///
    /// [`MessageCreateAction::queue`]: crate::requests::sockactions::SocketAction::queue
    pub fn flush(&mut self) {
        Sink::flush(self);
    }

    /// Send a message to this channel.
    ///
    /// This requires an additional step to complete the execution, for example when you call
    /// [`MessageCreateAction::queue`].
    ///
    /// [`MessageCreateAction::queue`]: crate::requests::sockactions::SocketAction::queue
    pub fn send_message(&mut self, message: P) -> MessageCreateAction<'_, N, P> {
        MessageCreateAction::new(self, message)
    }

    /// Close down the current connection of this channel.
    ///
    /// The connection is closed after all pending messages have been sent. After it is closed the channel can not be
    /// used anymore.
    pub fn close(&mut self) {
        Sink::close(self);
    }

    /// Sets whether connections from this channel should be deafened.
    ///
    /// When deafened, no more messages are read from the socket until it is undeafened.
    pub fn set_self_deafened(&mut self, deafened: bool) {
        self.buffer.borrow_mut().set_deafened(deafened);
    }

    /// Whether connections from this channel are deafened.
    pub fn is_self_deafened(&self) -> bool {
        self.buffer.borrow().is_deafened()
    }
}

impl<N, P> Sink<P> for MessageChannel<N, P> {
    fn write(&self, message: P) -> io::Result<()> {
        // If the buffer is shutdown we know the channel was closed and so
        // need to fail the result right away. If it is not shutdown the handling of the rest
        // will be done in execute()
        if !self.buffer.borrow().is_open() {
            Err(io::ErrorKind::NotConnected.into())
        } else {
            self.parent.write(message)
        }
    }

    fn flush(&self) {
        self.parent.flush();
    }

    fn close(&self) {
        self.parent.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DefaultSocketManagerBuilder;
    use crate::hooks::ListenerAdapter;
    use crate::utils::concurrent::ScheduledThreadPoolExecutor;
    use crate::utils::EventManager;
    use crate::requests::BufferedSink;
    use crate::requests::sockactions::SocketAction;

    struct GenericEventAdapter;

    impl ListenerAdapter for GenericEventAdapter {}

    #[test]
    fn send() {
        let socket_manager = DefaultSocketManagerBuilder::new()
            .set_socket_pool(ScheduledThreadPoolExecutor::new(1, 64).unwrap())
            .add_event_listener_provider(|| GenericEventAdapter)
            .build()
            .unwrap();
        let buffer = Rc::default();
        let head = BufferedSink::new(&socket_manager, &buffer, &EventQueue::default());
        let mut channel: MessageChannel = MessageChannel::new(
            socket_manager,
            Rc::clone(&buffer),
            EventQueue::default(),
            head.channel(),
        );
        channel
            .send_message(BytesMut::from(&b"ping"[..]))
            .complete()
            .unwrap();
        channel.close();
        assert!(channel.write(BytesMut::from(&b"pong"[..])).is_err());
        assert_eq!(buffer.borrow_mut().read(usize::MAX).len(), 1);
    }
}
