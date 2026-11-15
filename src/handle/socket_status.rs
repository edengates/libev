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

use crate::events::GenericEvent;
use crate::events::channel::update::ChannelUpdateSlowmodeEvent;
use crate::events::channel::{ChannelDisconnectEvent, ChannelReadyEvent, ChannelTimeoutEvent};
use crate::events::message::MessageReceivedEvent;

/// An event of a channel, which is handled by the next listener.
///
/// The events are added to the queue of the next listener and handled in the order they have been added. Each event
/// is handled by the matching method of the [`ListenerAdapter`](crate::hooks::ListenerAdapter).
pub enum SocketStatus<T> {
    /// See [`ChannelReadyEvent`].
    ///
    /// Handled by [`ListenerAdapter::on_ready`](crate::hooks::ListenerAdapter::on_ready).
    ChannelReady(ChannelReadyEvent),
    /// See [`MessageReceivedEvent`].
    ///
    /// Handled by [`ListenerAdapter::on_message_received`](crate::hooks::ListenerAdapter::on_message_received).
    MessageReceived(MessageReceivedEvent<T>),
    /// See [`ChannelDisconnectEvent`].
    ///
    /// Handled by [`ListenerAdapter::on_channel_disconnect`](crate::hooks::ListenerAdapter::on_channel_disconnect).
    ChannelDisconnect(ChannelDisconnectEvent),
    /// See [`ChannelUpdateSlowmodeEvent`].
    ///
    /// Handled by [`ListenerAdapter::on_channel_update_slowmode`].
    ///
    /// [`ListenerAdapter::on_channel_update_slowmode`]: crate::hooks::ListenerAdapter::on_channel_update_slowmode
    ChannelUpdateSlowmode(ChannelUpdateSlowmodeEvent),
    /// See [`ChannelTimeoutEvent`].
    ///
    /// Handled by [`ListenerAdapter::on_channel_timeout`](crate::hooks::ListenerAdapter::on_channel_timeout).
    ChannelTimeout(ChannelTimeoutEvent),
    /// A user defined event, see [`GenericEvent`].
    ///
    /// Handled by [`ListenerAdapter::on_generic_event`](crate::hooks::ListenerAdapter::on_generic_event).
    GenericEvent(Box<dyn GenericEvent>),
}
