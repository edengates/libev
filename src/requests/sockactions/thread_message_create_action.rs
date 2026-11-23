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

use crate::entities::channel::concrete::ThreadChannel;
use crate::requests::sockactions::SocketAction;
use std::future::{self, IntoFuture, Ready};
use std::io;

#[must_use]
pub struct ThreadMessageCreateAction<'a, N, P> {
    channel: &'a ThreadChannel<N, P>,
    message: P,
}

impl<'a, N, P> ThreadMessageCreateAction<'a, N, P> {
    pub(crate) fn new(channel: &'a ThreadChannel<N, P>, message: P) -> Self {
        Self { channel, message }
    }
}

impl<N: 'static, P: Send + 'static> SocketAction<()> for ThreadMessageCreateAction<'_, N, P> {
    fn complete(self) -> io::Result<()> {
        let message = self.message;
        self.channel.submit(move |channel| channel.send_message(message).complete())
    }
}

impl<N: 'static, P: Send + 'static> IntoFuture for ThreadMessageCreateAction<'_, N, P> {
    type Output = io::Result<()>;
    type IntoFuture = Ready<io::Result<()>>;

    fn into_future(self) -> Self::IntoFuture {
        future::ready(self.complete())
    }
}
