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

/// Indicates that a [`MessageChannel`]'s slow mode has been updated.
///
/// Can be used to retrieve the new slow mode with [`MessageChannel::is_locked`].
///
/// # See also
///
/// - [`ListenerAdapter::on_channel_update_slowmode`]
/// - [`DefaultSocketManagerBuilder::set_large_threshold`](crate::DefaultSocketManagerBuilder::set_large_threshold)
///
/// [`MessageChannel`]: crate::entities::channel::middleman::MessageChannel
/// [`MessageChannel::is_locked`]: crate::entities::channel::middleman::MessageChannel::is_locked
/// [`ListenerAdapter::on_channel_update_slowmode`]: crate::hooks::ListenerAdapter::on_channel_update_slowmode
pub struct ChannelUpdateSlowmodeEvent;

impl GenericEvent for ChannelUpdateSlowmodeEvent {}
