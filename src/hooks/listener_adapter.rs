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
use crate::events::GenericEvent;
use crate::hooks::cast;
use bytes::BytesMut;
use std::any::Any;
use std::io;

/// A trait which divides [`GenericEvent`]s for you. You should override the methods provided by this trait for your
/// event listener implementation.
///
/// Register a [`ListenerAdapter`] with the [`DefaultSocketManagerBuilder`]. Listeners are handled in the order they
/// are registered: events from the socket are handled by the first listener, then by the next one.
///
/// `I` is the type of the messages from the socket side, `O` is the type of the messages for the next listener.
/// **If a method returns an error, the [`MessageChannel`] is closed.**
///
/// # See also
///
/// - [`add_event_listener_provider`](crate::DefaultSocketManagerBuilder::add_event_listener_provider)
///
/// [`DefaultSocketManagerBuilder`]: crate::DefaultSocketManagerBuilder
#[allow(async_fn_in_trait)]
pub trait ListenerAdapter<I: Any + Send = BytesMut, O: Any + Send = BytesMut>: Send {
    /// Handles a [`ChannelReadyEvent`].
    ///
    /// By default it is fired to the next listener with [`MessageChannel::on_ready`].
    ///
    /// # Arguments
    ///
    /// - `channel` — The [`MessageChannel`] of this listener
    ///
    /// [`ChannelReadyEvent`]: crate::events::channel::ChannelReadyEvent
    async fn on_ready(&mut self, channel: &mut MessageChannel<O, I>) -> io::Result<()> {
        channel.on_ready();
        Ok(())
    }

    /// Handles a [`MessageReceivedEvent`] with the received message.
    ///
    /// By default the message is fired to the next listener with [`MessageChannel::on_message_received`].
    ///
    /// # Arguments
    ///
    /// - `channel` — The [`MessageChannel`] of this listener
    /// - `message` — The received message
    ///
    /// # Errors
    ///
    /// By default returns [`InvalidInput`] if `I` and `O` are different types.
    ///
    /// [`MessageReceivedEvent`]: crate::events::message::MessageReceivedEvent
    /// [`InvalidInput`]: io::ErrorKind::InvalidInput
    async fn on_message_received(&mut self, channel: &mut MessageChannel<O, I>, message: I) -> io::Result<()> {
        let message = cast(message).ok_or(io::ErrorKind::InvalidInput)?;
        channel.on_message_received(message);
        Ok(())
    }

    /// Handles a [`ChannelDisconnectEvent`].
    ///
    /// By default it is fired to the next listener with [`MessageChannel::on_channel_disconnect`].
    ///
    /// # Arguments
    ///
    /// - `channel` — The [`MessageChannel`] of this listener
    ///
    /// [`ChannelDisconnectEvent`]: crate::events::channel::ChannelDisconnectEvent
    async fn on_channel_disconnect(&mut self, channel: &mut MessageChannel<O, I>) -> io::Result<()> {
        channel.on_channel_disconnect();
        Ok(())
    }

    /// Handles a [`ChannelUpdateSlowmodeEvent`].
    ///
    /// By default it is fired to the next listener with [`MessageChannel::on_channel_update_slowmode`].
    ///
    /// # Arguments
    ///
    /// - `channel` — The [`MessageChannel`] of this listener
    ///
    /// # See also
    ///
    /// - [`MessageChannel::is_locked`]
    ///
    /// [`ChannelUpdateSlowmodeEvent`]: crate::events::channel::update::ChannelUpdateSlowmodeEvent
    async fn on_channel_update_slowmode(&mut self, channel: &mut MessageChannel<O, I>) -> io::Result<()> {
        channel.on_channel_update_slowmode();
        Ok(())
    }

    /// Handles a [`ChannelTimeoutEvent`].
    ///
    /// By default it is fired to the next listener with [`MessageChannel::on_channel_timeout`].
    ///
    /// # Arguments
    ///
    /// - `channel` — The [`MessageChannel`] of this listener
    ///
    /// # See also
    ///
    /// - [`SocketManager::afk_timeout`](crate::SocketManager::afk_timeout)
    ///
    /// [`ChannelTimeoutEvent`]: crate::events::channel::ChannelTimeoutEvent
    async fn on_channel_timeout(&mut self, channel: &mut MessageChannel<O, I>) -> io::Result<()> {
        channel.on_channel_timeout();
        Ok(())
    }

    /// Handles any [`GenericEvent`].
    ///
    /// By default it is fired to the next listener with [`MessageChannel::on_generic_event`].
    ///
    /// # Arguments
    ///
    /// - `channel` — The [`MessageChannel`] of this listener
    /// - `event` — The event to handle
    async fn on_generic_event(&mut self, channel: &mut MessageChannel<O, I>, event: Box<dyn GenericEvent>) -> io::Result<()> {
        channel.on_generic_event(event);
        Ok(())
    }

    /// Handles a message from the next listener, for example to encode it.
    ///
    /// By default the message is sent to the parent channel with [`MessageChannel::enqueue`].
    ///
    /// # Arguments
    ///
    /// - `channel` — The [`MessageChannel`] of this listener
    /// - `message` — The message from the next listener
    ///
    /// # Errors
    ///
    /// By default returns an error:
    /// - [`InvalidInput`] if `I` and `O` are different types
    /// - [`NotConnected`] if the channel has been shutdown by [`MessageChannel::close`]
    ///
    /// # See also
    ///
    /// - [`MessageChannel::send_message`]
    ///
    /// [`InvalidInput`]: io::ErrorKind::InvalidInput
    /// [`NotConnected`]: io::ErrorKind::NotConnected
    async fn enqueue(&mut self, channel: &mut MessageChannel<O, I>, message: O) -> io::Result<()> {
        let message = cast(message).ok_or(io::ErrorKind::InvalidInput)?;
        channel.enqueue(message)
    }

    /// Handles [`MessageChannel::close`] of the next listener.
    ///
    /// By default it is forwarded to the parent channel with [`MessageChannel::close`].
    ///
    /// # Arguments
    ///
    /// - `channel` — The [`MessageChannel`] of this listener
    async fn close(&mut self, channel: &mut MessageChannel<O, I>) -> io::Result<()> {
        channel.close();
        Ok(())
    }
}
