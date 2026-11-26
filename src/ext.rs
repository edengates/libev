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

//! What a library needs to run sockets of its own protocol on the event loops of libev.
//!
//! A [`SocketHandler`] is registered to a [`ScheduledExecutorService`], which runs on the thread of an
//! [`ExecutorCompletionService`] and passes it the [`Completion`] of every operation that the handler has submitted.
pub use crate::entities::channel::mixin::ChannelMixin;
pub use crate::handle::event::Event;
pub use crate::handle::{BorrowedSocket, Completion, CompletionService, EventArray, Request, SocketHandler};
#[cfg(windows)]
pub use crate::handle::set_power_suppressed;
pub use crate::utils::concurrent::{
    DISPATCH, Dispatcher, ExecutorCompletionService, SHUTDOWN, ScheduledExecutorService, ScheduledThreadPoolExecutor,
    Task,
};
