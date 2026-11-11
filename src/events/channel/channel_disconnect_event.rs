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

/// Indicates that a [`MessageChannel`] has been disconnected from the remote server.
///
/// # See also
///
/// - [`ListenerAdapter::on_channel_disconnect`]
/// - [`DefaultSocketManagerBuilder::set_timeout`](crate::DefaultSocketManagerBuilder::set_timeout)
///
/// [`MessageChannel`]: crate::entities::channel::middleman::MessageChannel
/// [`ListenerAdapter::on_channel_disconnect`]: crate::hooks::ListenerAdapter::on_channel_disconnect
pub struct ChannelDisconnectEvent;

impl GenericEvent for ChannelDisconnectEvent {}
