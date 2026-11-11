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

use crate::entities::channel::middleman::MessageChannel;
use std::future::{self, IntoFuture, Ready};
use std::io;

//todo: rename
#[must_use]
pub struct MessageCreateAction<'a, N, P> {
    channel: &'a mut MessageChannel<N, P>,
    message: P,
}

impl<'a, N, P> MessageCreateAction<'a, N, P> {
    /// Creates a new action, which will send the provided message to the provided channel.
    ///
    /// # Arguments
    ///
    /// - `channel` — The [`MessageChannel`] to send the message to
    /// - `message` — The message to send
    ///
    /// # See also
    ///
    /// - [`MessageChannel::send_message`]
    pub(crate) fn new(channel: &'a mut MessageChannel<N, P>, message: P) -> Self {
        Self { channel, message }
    }

    pub fn queue<S, F>(self, success: S, failure: F) -> io::Result<()>
    where
        S: FnOnce(()),
        F: FnOnce(io::Error),
    {
        match self.complete() {
            Ok(()) => success(()),
            Err(error) => failure(error),
        }
        Ok(())
    }

    pub fn complete(self) -> io::Result<()> {
        let data = self.channel.enqueue(self.message);
        self.channel.execute();
        data
    }
}

impl<N, P> IntoFuture for MessageCreateAction<'_, N, P> {
    type Output = io::Result<()>;
    type IntoFuture = Ready<io::Result<()>>;

    fn into_future(self) -> Self::IntoFuture {
        future::ready(self.complete())
    }
}
