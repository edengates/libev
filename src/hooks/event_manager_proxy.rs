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

pub struct EventManagerProxy {
    socket_manager: SocketManager,
    buffer: Rc<RefCell<SocketBufferQueue>>,
    events: EventQueue<BytesMut>,
}

impl EventManagerProxy {
    pub(crate) fn new(socket_manager: &SocketManager, buffer: &Rc<RefCell<SocketBufferQueue>>, events: &EventQueue<BytesMut>) -> Rc<Self> {
        Rc::new(Self {
            socket_manager: socket_manager.clone(),
            buffer: Rc::clone(buffer),
            events: Rc::clone(events),
        })
    }
}

impl Channel<BytesMut> for EventManagerProxy {
    fn enqueue(&self, message: BytesMut) -> io::Result<()> {
        self.buffer
            .borrow_mut()
            .enqueue(message, self.socket_manager.threshold(), self.socket_manager.large_threshold())
    }

    fn execute(&self) {
        self.buffer.borrow_mut().execute();
    }

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

    async fn handle(&mut self) -> io::Result<bool> {
        Ok(false)
    }

    fn clear(&mut self) {
        self.events.borrow_mut().clear();
    }
}
