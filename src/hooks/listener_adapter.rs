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
/// Register a [`ListenerAdapter`] with [`add_event_listener_provider`]. Events from the socket reach the listeners in
/// the order they were registered, and a message written by a listener passes the listeners before it on the way to
/// the socket. By default every method passes the event or message on through the [`MessageChannel`].
///
/// `I` is the type of the messages from the socket side, `O` is the type of the messages for the next listener.
/// If a method returns an error, the [`MessageChannel`] is closed.
///
/// [`add_event_listener_provider`]: crate::DefaultSocketManagerBuilder::add_event_listener_provider
#[allow(async_fn_in_trait)]
pub trait ListenerAdapter<I: Any + Send = BytesMut, O: Any + Send = BytesMut>: Send {
    /// The channel is connected now.
    async fn on_ready(&mut self, channel: &mut MessageChannel<O, I>) -> io::Result<()> {
        channel.on_ready();
        Ok(())
    }

    /// Gets called once the channel received a message.
    ///
    /// # Errors
    ///
    /// By default returns [`InvalidInput`] if `I` and `O` are different types.
    ///
    /// [`InvalidInput`]: io::ErrorKind::InvalidInput
    async fn on_message_received(&mut self, channel: &mut MessageChannel<O, I>, message: I) -> io::Result<()> {
        let message = cast(message).ok_or(io::ErrorKind::InvalidInput)?;
        channel.on_message_received(message);
        Ok(())
    }

    /// The channel is closed now.
    async fn on_channel_disconnect(&mut self, channel: &mut MessageChannel<O, I>) -> io::Result<()> {
        channel.on_channel_disconnect();
        Ok(())
    }

    /// Gets called once the slow mode of the channel changed. You can check it with [`MessageChannel::is_locked`].
    async fn on_channel_update_slowmode(&mut self, channel: &mut MessageChannel<O, I>) -> io::Result<()> {
        channel.on_channel_update_slowmode();
        Ok(())
    }

    /// Nothing was read or written for the [AFK timeout].
    ///
    /// [AFK timeout]: crate::DefaultSocketManagerBuilder::set_afk_timeout
    async fn on_channel_timeout(&mut self, channel: &mut MessageChannel<O, I>) -> io::Result<()> {
        channel.on_channel_timeout();
        Ok(())
    }

    /// Handles any other [`GenericEvent`].
    async fn on_generic_event(&mut self, channel: &mut MessageChannel<O, I>, event: Box<dyn GenericEvent>) -> io::Result<()> {
        channel.on_generic_event(event);
        Ok(())
    }

    /// Handles a message from the next listener, for example to encode it.
    ///
    /// # Errors
    ///
    /// By default returns [`InvalidInput`] if `I` and `O` are different types, or [`NotConnected`] if the channel has
    /// been closed.
    ///
    /// [`InvalidInput`]: io::ErrorKind::InvalidInput
    /// [`NotConnected`]: io::ErrorKind::NotConnected
    async fn enqueue(&mut self, channel: &mut MessageChannel<O, I>, message: O) -> io::Result<()> {
        let message = cast(message).ok_or(io::ErrorKind::InvalidInput)?;
        channel.enqueue(message)
    }

    /// Handles [`MessageChannel::close`] of the next listener.
    async fn close(&mut self, channel: &mut MessageChannel<O, I>) -> io::Result<()> {
        channel.close();
        Ok(())
    }
}
