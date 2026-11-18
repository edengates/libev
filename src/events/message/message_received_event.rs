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

/// Indicates that a message was received in a [`MessageChannel`].
///
/// [`MessageChannel`]: crate::entities::channel::middleman::MessageChannel
pub struct MessageReceivedEvent<T = BytesMut> {
    message: T,
}

impl MessageReceivedEvent {
    pub const IDENTIFIER: usize = 1 << 1;
}

impl<T> MessageReceivedEvent<T> {
    pub(crate) fn new(message: T) -> Self {
        Self { message }
    }

    /// The received message.
    pub fn message(self) -> T {
        self.message
    }
}

impl<T: Any + Send> GenericEvent for MessageReceivedEvent<T> {}

impl<T: Any + Send> Event for MessageReceivedEvent<T> {
    fn identifier(&self) -> usize {
        MessageReceivedEvent::IDENTIFIER
    }
}
