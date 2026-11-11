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
use crate::events::GenericEvent;
use crate::events::channel::update::ChannelUpdateSlowmodeEvent;
use crate::events::channel::{ChannelDisconnectEvent, ChannelReadyEvent, ChannelTimeoutEvent};
use crate::events::message::MessageReceivedEvent;
use crate::requests::objactions::MessageCreateAction;
use crate::entities::channel::Channel;
use crate::hooks::EventQueue;
use crate::handle::SocketStatus;
use crate::utils::SocketBufferQueue;
use bytes::BytesMut;
use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::rc::Rc;
use std::sync::{Arc, RwLock};

/// Represents a channel that can have messages sent to it.
///
/// Every registered [`ListenerAdapter`] has its own [`MessageChannel`] with the position of the listener.
/// Events are handled by the next listener, messages are sent through the parent [`Channel`] until they reach
/// the socket.
///
/// `N` is the type of messages for the next listener, `P` is the type of messages for the parent channel.
///
/// # See also
///
/// - [`ListenerAdapter`]
///
/// [`ListenerAdapter`]: crate::hooks::ListenerAdapter
pub struct MessageChannel<N = BytesMut, P = BytesMut> {
    socket_manager: SocketManager,
    buffer: Rc<RefCell<SocketBufferQueue>>,
    next: EventQueue<N>,
    parent: Rc<dyn Channel<P>>,
}

impl<N, P> Clone for MessageChannel<N, P> {
    fn clone(&self) -> Self {
        Self {
            socket_manager: self.socket_manager.clone(),
            buffer: Rc::clone(&self.buffer),
            next: Rc::clone(&self.next),
            parent: Rc::clone(&self.parent),
        }
    }
}

impl<N, P> MessageChannel<N, P> {
    /// Creates a new instance with the provided queue of messages, the queue of events of the next listener and the
    /// parent channel.
    ///
    /// # Arguments
    ///
    /// - `socket_manager` — The [`SocketManager`] instance of this channel
    /// - `buffer` — The queue of messages
    /// - `next` — The [`EventQueue`] of the next listener
    /// - `parent` — The parent [`Channel`]
    pub(crate) fn new(
        socket_manager: SocketManager,
        buffer: Rc<RefCell<SocketBufferQueue>>,
        next: EventQueue<N>,
        parent: Rc<dyn Channel<P>>,
    ) -> Self {
        Self {
            socket_manager,
            buffer,
            next,
            parent,
        }
    }

    /// Returns the [`SocketManager`] instance of this channel.
    pub fn socket_manager(&self) -> &SocketManager {
        &self.socket_manager
    }

    /// The data mappings to use in [`SocketManager`].
    ///
    /// The same map is shared by **all** channels of the [`SocketManager`].
    ///
    /// # See also
    ///
    /// - [`SocketManager::context_map`]
    pub fn context_map(&self) -> &RwLock<HashMap<String, Arc<dyn Any + Send + Sync>>> {
        self.socket_manager.context_map()
    }

    /// Fires a [`ChannelReadyEvent`], indicating that this channel is ready to be used.
    ///
    /// A [`MessageChannel`] is active now, which means it is connected. This will result in having the
    /// [`ListenerAdapter::on_ready`] method called of the next [`ListenerAdapter`].
    ///
    /// [`ListenerAdapter`]: crate::hooks::ListenerAdapter
    /// [`ListenerAdapter::on_ready`]: crate::hooks::ListenerAdapter::on_ready
    pub fn on_ready(&mut self) {
        self.handle(SocketStatus::ChannelReady(ChannelReadyEvent));
    }

    /// Fires a [`MessageReceivedEvent`], indicating that a message was received in this channel.
    ///
    /// A [`MessageChannel`] received a message. This will result in having the
    /// [`ListenerAdapter::on_message_received`] method called of the next [`ListenerAdapter`].
    ///
    /// # Arguments
    ///
    /// - `message` — The received message
    ///
    /// [`ListenerAdapter`]: crate::hooks::ListenerAdapter
    /// [`ListenerAdapter::on_message_received`]: crate::hooks::ListenerAdapter::on_message_received
    pub fn on_message_received(&mut self, message: N) {
        self.handle(SocketStatus::MessageReceived(MessageReceivedEvent::new(
            message,
        )));
    }

    /// Fires a [`ChannelDisconnectEvent`], indicating that this channel has been disconnected from the remote
    /// server.
    ///
    /// A [`MessageChannel`] is inactive now, which means it is closed. This will result in having the
    /// [`ListenerAdapter::on_channel_disconnect`] method called of the next [`ListenerAdapter`].
    ///
    /// [`ListenerAdapter`]: crate::hooks::ListenerAdapter
    /// [`ListenerAdapter::on_channel_disconnect`]: crate::hooks::ListenerAdapter::on_channel_disconnect
    pub fn on_channel_disconnect(&mut self) {
        self.handle(SocketStatus::ChannelDisconnect(ChannelDisconnectEvent));
    }

    /// Fires a [`ChannelUpdateSlowmodeEvent`], indicating that the slow mode of this channel has been
    /// updated.
    ///
    /// Triggers an [`ListenerAdapter::on_channel_update_slowmode`] event to the next [`ListenerAdapter`].
    ///
    /// [`ListenerAdapter`]: crate::hooks::ListenerAdapter
    /// [`ListenerAdapter::on_channel_update_slowmode`]: crate::hooks::ListenerAdapter::on_channel_update_slowmode
    pub fn on_channel_update_slowmode(&mut self) {
        self.handle(SocketStatus::ChannelUpdateSlowmode(
            ChannelUpdateSlowmodeEvent,
        ));
    }

    /// Fires a [`ChannelTimeoutEvent`], indicating that the [AFK-Timeout] of this channel has been reached.
    ///
    /// This will result in having the [`ListenerAdapter::on_channel_timeout`] method called of the next
    /// [`ListenerAdapter`].
    ///
    /// [AFK-Timeout]: SocketManager::afk_timeout
    /// [`ListenerAdapter`]: crate::hooks::ListenerAdapter
    /// [`ListenerAdapter::on_channel_timeout`]: crate::hooks::ListenerAdapter::on_channel_timeout
    pub fn on_channel_timeout(&mut self) {
        self.handle(SocketStatus::ChannelTimeout(ChannelTimeoutEvent));
    }

    /// Fires the provided [`GenericEvent`].
    ///
    /// A [`MessageChannel`] received an user defined event. This will result in having the
    /// [`ListenerAdapter::on_generic_event`] method called of the next [`ListenerAdapter`].
    ///
    /// # Arguments
    ///
    /// - `event` — The event to handle
    ///
    /// [`ListenerAdapter`]: crate::hooks::ListenerAdapter
    /// [`ListenerAdapter::on_generic_event`]: crate::hooks::ListenerAdapter::on_generic_event
    pub fn on_generic_event(&mut self, event: Box<dyn GenericEvent>) {
        self.handle(SocketStatus::GenericEvent(event));
    }

    /// Handles the provided [`SocketStatus`].
    ///
    /// How this is handled is specified by the implementation.
    ///
    /// # Arguments
    ///
    /// - `event` — The event to handle
    fn handle(&mut self, event: SocketStatus<N>) {
        self.next.borrow_mut().push_back(event);
    }

    /// Submits a message for execution.
    ///
    /// The message is handled by [`ListenerAdapter::enqueue`] of the parent channel, for example to encode
    /// it. The last channel sends it to the socket as [`BytesMut`].
    ///
    /// This method will **not** request to actual flush, so be sure to call [`execute`] once you want to request
    /// to flush all pending data to the actual transport.
    ///
    /// # Arguments
    ///
    /// - `message` — The message to write
    ///
    /// # Errors
    ///
    /// Returns [`NotConnected`] if the channel has been shutdown by [`close`].
    ///
    /// # See also
    ///
    /// - [`execute`]
    /// - [`send_message`]
    ///
    /// [`ListenerAdapter::enqueue`]: crate::hooks::ListenerAdapter::enqueue
    /// [`execute`]: Self::execute
    /// [`send_message`]: Self::send_message
    /// [`close`]: Self::close
    /// [`NotConnected`]: io::ErrorKind::NotConnected
    pub fn enqueue(&mut self, message: P) -> io::Result<()> {
        Channel::enqueue(self, message)
    }

    /// Whether this channel is locked or not.
    ///
    /// A channel is locked in slow mode: the queue of pending messages is larger than the
    /// [`large_threshold`], until it is smaller than the [`threshold`] again. **While the channel is locked you
    /// should stop sending messages**, for example with [`set_self_deafened`].
    ///
    /// # See also
    ///
    /// - [`ListenerAdapter::on_channel_update_slowmode`]
    ///
    /// [`ListenerAdapter::on_channel_update_slowmode`]: crate::hooks::ListenerAdapter::on_channel_update_slowmode
    /// [`large_threshold`]: SocketManager::large_threshold
    /// [`threshold`]: SocketManager::threshold
    /// [`set_self_deafened`]: Self::set_self_deafened
    pub fn is_locked(&self) -> bool {
        self.buffer.borrow().is_slowmode()
    }

    /// Request to flush all pending messages via this [`MessageChannel`].
    ///
    /// Messages from [`enqueue`] are **only** sent after this call. [`MessageCreateAction::queue`] calls this
    /// method for you.
    ///
    /// # See also
    ///
    /// - [`enqueue`]
    /// - [`send_message`]
    ///
    /// [`enqueue`]: Self::enqueue
    /// [`send_message`]: Self::send_message
    pub fn execute(&mut self) {
        Channel::execute(self);
    }

    /// Send a message to this channel.
    ///
    /// **This requires an additional step to complete the execution**, for example when you call
    /// [`MessageCreateAction::queue`].
    ///
    /// # Arguments
    ///
    /// - `message` — The message to write
    ///
    /// # Returns
    ///
    /// [`MessageCreateAction`]
    ///
    /// # See also
    ///
    /// - [`enqueue`]
    /// - [`execute`]
    ///
    /// [`enqueue`]: Self::enqueue
    /// [`execute`]: Self::execute
    pub fn send_message(&mut self, message: P) -> MessageCreateAction<'_, N, P> {
        MessageCreateAction::new(self, message)
    }

    /// Close down the current connection of this [`MessageChannel`].
    ///
    /// This is handled by [`ListenerAdapter::close`] of the parent channel. The last channel closes the
    /// connection after all pending messages have been sent.
    ///
    /// <div class="warning">
    ///
    /// After it is closed the [`MessageChannel`] can **not** be used anymore.
    ///
    /// </div>
    ///
    /// [`ListenerAdapter::close`]: crate::hooks::ListenerAdapter::close
    pub fn close(&mut self) {
        Channel::close(self);
    }

    /// Sets whether connections from this [`MessageChannel`] should be deafened.
    ///
    /// While deafened you will no longer receive [`MessageReceivedEvent`]s from the remote server. This can be
    /// used to wait until [`is_locked`] returns `false` again.
    ///
    /// # Arguments
    ///
    /// - `deafened` — Whether connections from this [`MessageChannel`] should be deafened
    ///
    /// # See also
    ///
    /// - [`is_self_deafened`]
    ///
    /// [`is_locked`]: Self::is_locked
    /// [`is_self_deafened`]: Self::is_self_deafened
    pub fn set_self_deafened(&mut self, deafened: bool) {
        self.buffer.borrow_mut().set_deafened(deafened);
    }

    /// Whether connections from this [`MessageChannel`] are deafened.
    ///
    /// # See also
    ///
    /// - [`set_self_deafened`](Self::set_self_deafened)
    pub fn is_self_deafened(&self) -> bool {
        self.buffer.borrow().is_deafened()
    }
}

impl<N, P> Channel<P> for MessageChannel<N, P> {
    fn enqueue(&self, message: P) -> io::Result<()> {
        // If the buffer is shutdown we know the channel was closed and so
        // need to fail the result right away. If it is not shutdown the handling of the rest
        // will be done in execute()
        if self.buffer.borrow().is_shutdown() {
            Err(io::ErrorKind::NotConnected.into())
        } else {
            self.parent.enqueue(message)
        }
    }

    fn execute(&self) {
        self.parent.execute();
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
    use crate::utils::concurrent::{ScheduledThreadManager, SingleThreadFactory};
    use crate::hooks::{EventManager, EventManagerProxy};

    struct GenericEventAdapter;

    impl ListenerAdapter for GenericEventAdapter {}

    #[test]
    fn send() {
        let socket_manager = DefaultSocketManagerBuilder::new()
            .set_socket_pool(
                ScheduledThreadManager::new(
                    1,
                    SingleThreadFactory::new(64, crate::handle::SocketQueueFactory::default()),
                )
                .unwrap(),
            )
            .add_event_listener_provider(|| GenericEventAdapter)
            .build()
            .unwrap();
        let buffer = Rc::default();
        let head = EventManagerProxy::new(&socket_manager, &buffer, &EventQueue::default());
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
        assert!(channel.enqueue(BytesMut::from(&b"pong"[..])).is_err());
        assert_eq!(buffer.borrow_mut().next_packet(usize::MAX).len(), 1);
    }
}
