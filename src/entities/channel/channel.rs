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
/// All operations are asynchronous and return immediately, with no guarantee that the request has been completed at
/// the end of the call.
///
/// [`MessageChannel`]: crate::entities::channel::middleman::MessageChannel
pub trait Channel<T> {
    /// Request to write a message via this channel. This does not request a flush, call
    /// [`execute`](Self::execute) for that.
    ///
    /// # Errors
    ///
    /// Returns [`NotConnected`] if the channel has been closed.
    ///
    /// [`NotConnected`]: io::ErrorKind::NotConnected
    fn enqueue(&self, message: T) -> io::Result<()>;

    /// Request to flush all pending messages via this channel.
    fn execute(&self);

    /// Request to close this channel once all pending messages have been sent.
    fn close(&self);
}
