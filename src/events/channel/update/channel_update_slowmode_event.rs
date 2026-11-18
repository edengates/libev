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

/// Indicates that a [`MessageChannel`]'s slow mode has been updated.
///
/// The new slow mode is returned by [`MessageChannel::is_locked`].
///
/// [`MessageChannel`]: crate::entities::channel::middleman::MessageChannel
/// [`MessageChannel::is_locked`]: crate::entities::channel::middleman::MessageChannel::is_locked
pub struct ChannelUpdateSlowmodeEvent;

impl ChannelUpdateSlowmodeEvent {
    pub const IDENTIFIER: usize = 1 << 2;
}

impl GenericEvent for ChannelUpdateSlowmodeEvent {}

impl Event for ChannelUpdateSlowmodeEvent {
    fn identifier(&self) -> usize {
        Self::IDENTIFIER
    }
}
