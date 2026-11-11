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
use crate::hooks::{EventQueue, EventTask};
use crate::handle::ChannelManager;
use crate::handle::SocketStatus;
use crate::utils::SocketBufferQueue;
use bytes::BytesMut;
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::Instant;

/// The state of a socket channel, used by the platform specific socket channels.
///
/// A socket channel provides:
/// - the current state of the channel (is it ready? are the listeners running?),
/// - the queue of pending messages, which have to be sent,
/// - the pipeline of [`ListenerAdapter`]s, which handles all events of the channel.
///
/// <div class="warning">
///
/// All methods **must** be called from the thread of the [`ChannelManager`] on which the channel is registered.
///
/// </div>
///
/// All operations are **asynchronous**: messages are **only** added to the queue and are sent by the platform
/// specific socket channel once the socket is ready, events are **only** added to the event queue and are handled
/// by the listeners later.
///
/// [`ListenerAdapter`]: crate::hooks::ListenerAdapter
pub struct MessageChannelMixin {
    socket_manager: SocketManager,
    buffer: Rc<RefCell<SocketBufferQueue>>,
    events: EventQueue<BytesMut>,
    task: Rc<dyn EventTask>,
    is_connected: Cell<bool>,
    is_ready: Cell<bool>,
    heartbeat_start_time: Cell<Instant>,
}

impl MessageChannelMixin {
    /// Creates a new instance with the [`EventTask`] of the provided [`SocketManager`].
    ///
    /// The [`EventTask`] is created with the listeners which have been added to the
    /// [`DefaultSocketManagerBuilder`](crate::DefaultSocketManagerBuilder).
    ///
    /// # Arguments
    ///
    /// - `socket_manager` — The [`SocketManager`] instance of this channel
    pub(crate) fn new(socket_manager: SocketManager) -> Self {
        let buffer = Rc::default();
        let events = EventQueue::default();
        let task = socket_manager.task(&buffer, &events);
        Self {
            socket_manager,
            buffer,
            events,
            task,
            is_connected: Cell::new(false),
            is_ready: Cell::new(false),
            heartbeat_start_time: Cell::new(Instant::now()),
        }
    }

    /// Returns the [`SocketManager`] instance of this channel.
    pub(crate) fn socket_manager(&self) -> &SocketManager {
        &self.socket_manager
    }

    /// The queue of pending messages, which have to be sent by the platform specific socket channel.
    ///
    /// This also holds whether the channel is:
    /// - shutdown
    /// - in slowmode
    /// - deafened
    pub(crate) fn buffer(&self) -> &RefCell<SocketBufferQueue> {
        &self.buffer
    }

    /// Whether the listeners are currently running, which means the [`EventTask`] is enqueued to the
    /// [`ChannelManager`].
    ///
    /// # See also
    ///
    /// - [`connect`](Self::connect)
    pub(crate) fn is_connected(&self) -> bool {
        self.is_connected.get()
    }

    /// Sets whether the listeners are currently running.
    ///
    /// # Arguments
    ///
    /// - `is_connected` — Whether the listeners are currently running
    ///
    /// # See also
    ///
    /// - [`is_connected`](Self::is_connected)
    pub(crate) fn connect(&self, is_connected: bool) {
        self.is_connected.set(is_connected);
    }

    /// Whether this channel is ready to be used, which means it is connected.
    ///
    /// This stays `true` after [`ready`](Self::ready), even if the channel has been closed since.
    pub(crate) fn is_ready(&self) -> bool {
        self.is_ready.get()
    }

    /// Fires a [`ChannelReadyEvent`], indicating that this channel is ready to be used.
    ///
    /// This is called by the platform specific socket channel once the connection is established or has been
    /// accepted. This also starts the check of the [AFK-Timeout], if enabled.
    ///
    /// # Arguments
    ///
    /// - `manager` — The [`ChannelManager`] on which the channel is registered
    ///
    /// # See also
    ///
    /// - [`check_expired`](Self::check_expired)
    ///
    /// [AFK-Timeout]: SocketManager::afk_timeout
    pub(crate) fn ready(self: &Rc<Self>, manager: &mut ChannelManager) {
        self.is_ready.set(true);
        self.handle_event(manager, SocketStatus::ChannelReady(ChannelReadyEvent));
        if let Some(timeout) = self.socket_manager.afk_timeout() {
            manager.setup_keep_alive(timeout);
        }
    }

    /// Fires a [`MessageReceivedEvent`], invoked when this channel has read a message from the remote peer.
    ///
    /// This also resets the [AFK-Timeout].
    ///
    /// # Arguments
    ///
    /// - `manager` — The [`ChannelManager`] on which the channel is registered
    /// - `message` — The received message
    ///
    /// [AFK-Timeout]: SocketManager::afk_timeout
    pub(crate) fn read(self: &Rc<Self>, manager: &mut ChannelManager, message: BytesMut) {
        if self.socket_manager.afk_timeout().is_some() {
            self.heartbeat_start_time.set(Instant::now());
        }
        self.handle_event(manager, SocketStatus::MessageReceived(MessageReceivedEvent::new(message)));
    }

    /// Called once a write has completed, with the amount of bytes written.
    ///
    /// This removes the written bytes from the queue of pending messages and updates the slowmode, see
    /// [`SocketManager::large_threshold`]. This also resets the [AFK-Timeout].
    ///
    /// # Arguments
    ///
    /// - `size` — The amount of bytes written
    ///
    /// [AFK-Timeout]: SocketManager::afk_timeout
    pub(crate) fn handle_response(&self, size: usize) {
        if self.socket_manager.afk_timeout().is_some() {
            self.heartbeat_start_time.set(Instant::now());
        }
        self.buffer
            .borrow_mut()
            .handle_response(size, self.socket_manager.threshold(), self.socket_manager.large_threshold());
    }

    /// Fires a [`ChannelUpdateSlowmodeEvent`] each time the slowmode of this channel changed.
    ///
    /// You can check the slowmode with [`MessageChannel::is_locked`].
    ///
    /// # Arguments
    ///
    /// - `manager` — The [`ChannelManager`] on which the channel is registered
    ///
    /// [`MessageChannel::is_locked`]: crate::entities::channel::middleman::MessageChannel::is_locked
    pub(crate) fn execute(self: &Rc<Self>, manager: &mut ChannelManager) {
        while self.buffer.borrow_mut().take_slowmode_change() {
            self.handle_event(manager, SocketStatus::ChannelUpdateSlowmode(ChannelUpdateSlowmodeEvent));
        }
    }

    /// Fires a [`ChannelTimeoutEvent`] if neither read nor write was performed for the [AFK-Timeout], and
    /// schedules the next check.
    ///
    /// The event is fired again after each further [AFK-Timeout] without read or write. Nothing is done if the
    /// [AFK-Timeout] is disabled.
    ///
    /// # Arguments
    ///
    /// - `manager` — The [`ChannelManager`] on which the channel is registered
    ///
    /// # See also
    ///
    /// - [`SocketHandler::check_expired`](crate::handle::SocketHandler::check_expired)
    ///
    /// [AFK-Timeout]: SocketManager::afk_timeout
    pub(crate) fn check_expired(self: &Rc<Self>, manager: &mut ChannelManager) {
        let Some(timeout) = self.socket_manager.afk_timeout() else {
            return;
        };
        let elapsed = self.heartbeat_start_time.get().elapsed();
        if elapsed >= timeout {
            self.heartbeat_start_time.set(Instant::now());
            self.handle_event(manager, SocketStatus::ChannelTimeout(ChannelTimeoutEvent));
            manager.setup_keep_alive(timeout);
        } else {
            manager.setup_keep_alive(timeout - elapsed);
        }
    }

    /// Fires a [`ChannelDisconnectEvent`], indicating that this channel is now inactive and can not be used anymore.
    ///
    /// This marks the queue of pending messages as shutdown, so [`MessageChannel::enqueue`] returns an error from now
    /// on. Nothing is fired if the channel was **never** ready. If the listeners are still running, they are
    /// cancelled after the [`timeout`](SocketManager::timeout).
    ///
    /// # Arguments
    ///
    /// - `manager` — The [`ChannelManager`] on which the channel is registered
    ///
    /// [`MessageChannel::enqueue`]: crate::entities::channel::middleman::MessageChannel::enqueue
    pub(crate) fn close(self: &Rc<Self>, manager: &mut ChannelManager) {
        if !self.is_ready() {
            return;
        }
        self.buffer.borrow_mut().shutdown();
        self.handle_event(manager, SocketStatus::ChannelDisconnect(ChannelDisconnectEvent));
        if self.is_connected() {
            manager.deadline(self.socket_manager.timeout());
        }
    }

    /// Adds the provided event to the event queue of the listeners.
    ///
    /// If the listeners are not running yet, the [`EventTask`] is enqueued to the [`ChannelManager`] to handle
    /// **all** events in the event queue.
    ///
    /// # Arguments
    ///
    /// - `manager` — The [`ChannelManager`] on which the channel is registered
    /// - `event` — The event to handle
    fn handle_event(self: &Rc<Self>, manager: &mut ChannelManager, event: SocketStatus<BytesMut>) {
        self.events.borrow_mut().push_back(event);
        if !self.is_connected() {
            self.connect(true);
            manager.enqueue(Rc::clone(&self.task).handle(Rc::clone(self)));
        }
    }
}
