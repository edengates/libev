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

use crate::entities::channel::Channel;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::io;

/// A request of a [`Channel`], which is queued until the [`InterfacedEventManager`] handles it.
///
/// The commands are handled in the order they were queued.
///
/// [`InterfacedEventManager`]: crate::hooks::InterfacedEventManager
pub(crate) enum Command<T> {
    /// Request to write the message, see [`Channel::enqueue`].
    Enqueue(T),
    /// Request to send all pending messages, see [`Channel::execute`].
    Execute,
    /// Request to close the channel, see [`Channel::close`].
    Close,
}

/// Queues the requests as `Command`s, which are handled later by the `InterfacedEventManager`.
impl<T> Channel<T> for RefCell<VecDeque<Command<T>>> {
    /// Queues a `Command::Enqueue` with the provided message.
    ///
    /// # Arguments
    ///
    /// - `message` — The message to write
    ///
    /// # Errors
    ///
    /// Never returns an error: the error of the listener is returned when the command is handled.
    fn enqueue(&self, message: T) -> io::Result<()> {
        self.borrow_mut().push_back(Command::Enqueue(message));
        Ok(())
    }

    /// Queues a `Command::Execute`.
    fn execute(&self) {
        self.borrow_mut().push_back(Command::Execute);
    }

    /// Queues a `Command::Close`.
    fn close(&self) {
        self.borrow_mut().push_back(Command::Close);
    }
}
