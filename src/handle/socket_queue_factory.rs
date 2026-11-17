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

use crate::handle::{DefaultHandler, SocketQueueManager};
use std::io;

/// Factory for the socket queues, which handle the IO of a [`SingleThreadManager`].
///
/// A [`SocketQueueFactory`] is created by the `new_factory` method of an IO handler, such as `EpollHandler`.
/// [`Default`] uses the default IO handler of the platform:
/// - epoll on Linux
/// - kqueue on macOS and FreeBSD
/// - IOCP on Windows
///
/// [`SingleThreadManager`]: crate::utils::concurrent::SingleThreadManager
#[derive(Clone, Copy)]
pub struct SocketQueueFactory {
    manager: fn(u32) -> io::Result<SocketQueueManager>,
}

impl SocketQueueFactory {
    /// Creates a new instance, which uses the provided function to create each [`SocketQueueManager`].
    ///
    /// # Arguments
    ///
    /// - `function` — The function used to create each [`SocketQueueManager`]
    pub(crate) fn new(function: fn(u32) -> io::Result<SocketQueueManager>) -> Self {
        Self { manager: function }
    }

    /// Called when a new [`SocketQueueManager`] instance is needed.
    ///
    /// # Arguments
    ///
    /// - `capacity` — The maximum number of events handled at once
    ///
    /// # Errors
    ///
    /// Returns an error if the [`SocketQueueManager`] could not be created.
    ///
    /// # Returns
    ///
    /// The newly constructed [`SocketQueueManager`].
    pub(crate) fn new_manager(&self, capacity: u32) -> io::Result<SocketQueueManager> {
        (self.manager)(capacity)
    }
}

impl Default for SocketQueueFactory {
    fn default() -> Self {
        DefaultHandler::new_factory()
    }
}
