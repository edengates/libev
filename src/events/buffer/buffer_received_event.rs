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

use crate::events::{Event, GenericEvent};
use bytes::BytesMut;
use std::any::Any;

/// Indicates that a message was received in a [`BufferedChannel`].
///
/// [`BufferedChannel`]: crate::entities::channel::middleman::BufferedChannel
pub struct BufferReceivedEvent<T = BytesMut> {
    buffer: T,
}

impl BufferReceivedEvent {
    pub const IDENTIFIER: usize = 1 << 1;
}

impl<T> BufferReceivedEvent<T> {
    pub(crate) fn new(buffer: T) -> Self {
        Self { buffer }
    }

    /// The received message.
    pub fn buffer(self) -> T {
        self.buffer
    }
}

impl<T: Any + Send> GenericEvent for BufferReceivedEvent<T> {}

impl<T: Any + Send> Event for BufferReceivedEvent<T> {
    fn identifier(&self) -> usize {
        BufferReceivedEvent::IDENTIFIER
    }
}
