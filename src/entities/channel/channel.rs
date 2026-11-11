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

use std::io;

/// Abstract channel interface for all channels, to which messages can be sent.
///
/// Every [`MessageChannel`] is a [`Channel`], and so is its parent channel: the messages are sent through every
/// parent channel until they reach the socket.
///
/// All operations are **asynchronous**: they will return **immediately**, with no guarantee that the request has
/// been completed at the end of the call.
///
/// [`MessageChannel`]: crate::entities::channel::middleman::MessageChannel
pub trait Channel<T> {
    /// Request to write a message via this [`Channel`].
    ///
    /// This method will **not** request to actual flush, so be sure to call [`execute`](Self::execute) once you want
    /// to request to flush all pending messages to the socket.
    ///
    /// # Arguments
    ///
    /// - `message` — The message to write
    ///
    /// # Errors
    ///
    /// Returns [`NotConnected`] if the channel has been shutdown by [`close`](Self::close).
    ///
    /// # See also
    ///
    /// - [`execute`](Self::execute)
    ///
    /// [`NotConnected`]: io::ErrorKind::NotConnected
    fn enqueue(&self, message: T) -> io::Result<()>;

    /// Request to flush all pending messages via this [`Channel`].
    ///
    /// # See also
    ///
    /// - [`enqueue`](Self::enqueue)
    fn execute(&self);

    /// Request to close this [`Channel`].
    ///
    /// The connection is closed after all pending messages have been sent.
    ///
    /// <div class="warning">
    ///
    /// After it is closed the channel can **not** be used anymore.
    ///
    /// </div>
    fn close(&self);
}
