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

/// The state shared by the platform specific socket channels, such as the queue of pending messages.
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

    pub(crate) fn socket_manager(&self) -> &SocketManager {
        &self.socket_manager
    }

    pub(crate) fn buffer(&self) -> &RefCell<SocketBufferQueue> {
        &self.buffer
    }

    pub(crate) fn is_connected(&self) -> bool {
        self.is_connected.get()
    }

    pub(crate) fn connect(&self, is_connected: bool) {
        self.is_connected.set(is_connected);
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.is_ready.get()
    }

    pub(crate) fn ready(self: &Rc<Self>, manager: &mut ChannelManager) {
        self.is_ready.set(true);
        self.handle_event(manager, SocketStatus::ChannelReady(ChannelReadyEvent));
        if let Some(timeout) = self.socket_manager.afk_timeout() {
            manager.setup_keep_alive(timeout);
        }
    }

    pub(crate) fn read(self: &Rc<Self>, manager: &mut ChannelManager, message: BytesMut) {
        if self.socket_manager.afk_timeout().is_some() {
            self.heartbeat_start_time.set(Instant::now());
        }
        self.handle_event(manager, SocketStatus::MessageReceived(MessageReceivedEvent::new(message)));
    }

    pub(crate) fn handle_response(&self, size: usize) {
        if self.socket_manager.afk_timeout().is_some() {
            self.heartbeat_start_time.set(Instant::now());
        }
        self.buffer
            .borrow_mut()
            .handle_response(size, self.socket_manager.threshold(), self.socket_manager.large_threshold());
    }

    pub(crate) fn execute(self: &Rc<Self>, manager: &mut ChannelManager) {
        while self.buffer.borrow_mut().take_slowmode_change() {
            self.handle_event(manager, SocketStatus::ChannelUpdateSlowmode(ChannelUpdateSlowmodeEvent));
        }
    }

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

    fn handle_event(self: &Rc<Self>, manager: &mut ChannelManager, event: SocketStatus<BytesMut>) {
        self.events.borrow_mut().push_back(event);
        if !self.is_connected() {
            self.connect(true);
            manager.enqueue(Rc::clone(&self.task).handle(Rc::clone(self)));
        }
    }
}
