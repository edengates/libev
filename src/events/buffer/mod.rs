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

//! Events that are fired for messages in a [`BufferedChannel`], such as [receiving].
//!
//! [`BufferedChannel`]: crate::entities::channel::middleman::BufferedChannel
//! [receiving]: BufferReceivedEvent
mod buffer_received_event;
pub use buffer_received_event::BufferReceivedEvent;
