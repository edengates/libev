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

use crate::requests::sockactions::operator::{
    CombineObjectAction, FlatMapErrorObjectAction, FlatMapObjectAction, MapErrorObjectAction, MapObjectAction,
    ZipObjectAction,
};
use std::future::{self, Future};
use std::io;

/// A trait representing a terminal between the user and the socket.
///
/// Methods that return an instance of [`SocketAction`] require an additional step to complete the execution. Thus the
/// user needs to append a follow-up method.
///
/// A default [`SocketAction`] is issued with the following operations:
///
/// - [`queue`](Self::queue): the fastest and most simplistic way to execute a [`SocketAction`] is to queue it. This
///   method has two callback functions, one with the generic type and another with a failure error.
/// - [`submit`](Self::submit): provides a [`Future`] representing the pending request.
/// - [`complete`](Self::complete): executes the request and returns its result, or an error.
///
/// `T` is the generic response type for this [`SocketAction`].
///
/// # Examples
///
/// The most common way to use a [`SocketAction`] is not using the returned value. For instance sending messages
/// usually means you will not require to view the message once it was sent. Thus, we can simply use the
/// [`queue`](Self::queue) operation:
///
/// ```
/// # use bytes::BytesMut;
/// # use libev::entities::channel::middleman::BufferedChannel;
/// # use libev::requests::sockactions::SocketAction;
/// # use std::io;
/// # fn send(channel: &mut BufferedChannel) -> io::Result<()> {
/// let action = channel.send_buffer(BytesMut::from(&b"Hello World"[..]));
/// action.queue(|_| (), |_| ())?; // Execute the socket action
/// # Ok(())
/// # }
/// ```
pub trait SocketAction<T>: Sized {
    /// Executes the request and returns its result. Used for synchronous logic.
    ///
    /// # Errors
    ///
    /// Returns an error if the request encounters an error at its execution point.
    fn complete(self) -> io::Result<T>;

    /// Submits a request for execution.
    ///
    /// The `success` callback will be called at a convenient time, the `failure` callback will be called if the
    /// request encounters an error at its execution point.
    ///
    /// # Examples
    ///
    /// ```
    /// # use bytes::BytesMut;
    /// # use libev::entities::channel::middleman::BufferedChannel;
    /// # use libev::requests::sockactions::SocketAction;
    /// # use std::io;
    /// fn send_buffer(channel: &mut BufferedChannel, message: BytesMut) -> io::Result<()> {
    ///     channel.send_buffer(message).queue(
    ///         // Handle success once the message is sent
    ///         |_| println!("The message was sent"),
    ///         // Handle failure if the channel is closed (or another issue appeared)
    ///         |error| eprintln!("The message could not be sent: {error}"),
    ///     )
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the request could not be submitted.
    fn queue<S, F>(self, success: S, failure: F) -> io::Result<()>
    where
        S: FnOnce(T),
        F: FnOnce(io::Error),
    {
        match self.complete() {
            Ok(value) => success(value),
            Err(error) => failure(error),
        }
        Ok(())
    }

    /// Submits a request for execution and provides a [`Future`] representing its completion task.
    ///
    /// # Examples
    ///
    /// ```
    /// # use bytes::BytesMut;
    /// # use libev::entities::channel::middleman::BufferedChannel;
    /// # use libev::requests::sockactions::SocketAction;
    /// # use std::io;
    /// async fn send_buffer_raw(channel: &mut BufferedChannel, message: BytesMut) -> io::Result<String> {
    ///     let content = String::from_utf8_lossy(&message).into_owned();
    ///     channel.send_buffer(message) // send the message
    ///         .map(move |_| content) // return its content once it is sent
    ///         .submit()
    ///         .await
    /// }
    /// ```
    fn submit(self) -> impl Future<Output = io::Result<T>> {
        future::ready(self.complete())
    }

    /// Intermediate operator that returns a modified [`SocketAction`].
    ///
    /// This does not modify this instance but returns a new [`SocketAction`] which will apply the `map` function on
    /// successful execution. If this action fails, the error is passed on unchanged.
    ///
    /// The `map` function is applied to the result of this action, `O` is the target output type. Returns a
    /// [`SocketAction`] for the mapped type.
    ///
    /// # Examples
    ///
    /// ```
    /// # use bytes::BytesMut;
    /// # use libev::entities::channel::middleman::BufferedChannel;
    /// # use libev::requests::sockactions::SocketAction;
    /// fn send_buffer_raw(channel: &mut BufferedChannel, message: BytesMut) -> impl SocketAction<String> + '_ {
    ///     let content = String::from_utf8_lossy(&message).into_owned();
    ///     channel.send_buffer(message).map(move |_| content)
    /// }
    /// ```
    fn map<O, M>(self, map: M) -> MapObjectAction<Self, M, T>
    where
        M: FnOnce(T) -> O,
    {
        MapObjectAction::new(self, map)
    }

    /// Intermediate operator that returns a modified [`SocketAction`].
    ///
    /// This does not modify this instance but returns a new [`SocketAction`] which will apply the `flat_map` function
    /// on successful execution. This will compute the result of both [`SocketAction`]s. If one of them fails, the
    /// error is passed on unchanged.
    ///
    /// The `flat_map` function is applied to the result of this action and must return a [`SocketAction`], `O` is the
    /// target output type. Returns a [`SocketAction`] for the mapped type.
    ///
    /// # Examples
    ///
    /// ```
    /// # use bytes::BytesMut;
    /// # use libev::entities::channel::middleman::BufferedChannel;
    /// # use libev::requests::sockactions::SocketAction;
    /// fn send_buffer<'a>(
    ///     channel: &'a mut BufferedChannel,
    ///     target: &'a mut BufferedChannel,
    ///     message: BytesMut,
    /// ) -> impl SocketAction<()> + 'a {
    ///     channel.send_buffer(message.clone()) // send the message
    ///         .flat_map(move |_| target.send_buffer(message)) // then forward it to the target
    /// }
    /// ```
    fn flat_map<O, A, M>(self, flat_map: M) -> FlatMapObjectAction<Self, M, T>
    where
        M: FnOnce(T) -> A,
        A: SocketAction<O>,
    {
        FlatMapObjectAction::new(self, flat_map)
    }

    /// Supply a fallback value when the [`SocketAction`] fails for any reason.
    ///
    /// This does not modify this instance but returns a new [`SocketAction`] which will apply the `map` function on
    /// failed execution.
    ///
    /// The `map` function provides the fallback value to use. Returns a [`SocketAction`] with fallback handling.
    ///
    /// # Examples
    ///
    /// ```
    /// # use bytes::BytesMut;
    /// # use libev::entities::channel::middleman::BufferedChannel;
    /// # use libev::requests::sockactions::SocketAction;
    /// fn send_buffer(channel: &mut BufferedChannel, message: BytesMut) -> impl SocketAction<String> + '_ {
    ///     let content = String::from_utf8_lossy(&message).into_owned();
    ///     channel.send_buffer(message) // SocketAction<()>
    ///         .map(move |_| content) // SocketAction<String>
    ///         .on_error_map(|error| error.to_string()) // SocketAction<String> (must be the same as above)
    /// }
    /// ```
    fn on_error_map<M>(self, map: M) -> MapErrorObjectAction<Self, M>
    where
        M: FnOnce(io::Error) -> T,
    {
        MapErrorObjectAction::new(self, map)
    }

    /// Supply a fallback value when the [`SocketAction`] fails for any reason.
    ///
    /// This does not modify this instance but returns a new [`SocketAction`] which will apply the `map` function on
    /// failed execution.
    ///
    /// The `map` function provides the fallback action to use. Returns a [`SocketAction`] with fallback handling.
    ///
    /// # Examples
    ///
    /// ```
    /// # use bytes::BytesMut;
    /// # use libev::entities::channel::middleman::BufferedChannel;
    /// # use libev::requests::sockactions::SocketAction;
    /// fn send_buffer<'a>(
    ///     channel: &'a mut BufferedChannel,
    ///     context: &'a mut BufferedChannel,
    ///     message: BytesMut,
    /// ) -> impl SocketAction<()> + 'a {
    ///     channel.send_buffer(message) // SocketAction<()>
    ///         .on_error_flat_map(move |error| {
    ///             let content = format!("Failed to send message. Reason: {error}");
    ///             context.send_buffer(BytesMut::from(content.as_bytes()))
    ///         }) // SocketAction<()> (must be the same as above)
    /// }
    /// ```
    fn on_error_flat_map<A, M>(self, map: M) -> FlatMapErrorObjectAction<Self, M>
    where
        M: FnOnce(io::Error) -> A,
        A: SocketAction<T>,
    {
        FlatMapErrorObjectAction::new(self, map)
    }

    /// Combines this [`SocketAction`] with the provided action.
    ///
    /// If one of the actions fails, the combined action fails with its error.
    ///
    /// The `other` action is the action to combine, `U` is the type of the other action. Returns a combined
    /// [`SocketAction`] with empty result.
    ///
    /// # Examples
    ///
    /// ```
    /// # use bytes::BytesMut;
    /// # use libev::entities::channel::middleman::BufferedChannel;
    /// # use libev::requests::sockactions::SocketAction;
    /// fn send_buffer<'a>(
    ///     channel: &'a mut BufferedChannel,
    ///     target: &'a mut BufferedChannel,
    ///     message: BytesMut,
    /// ) -> impl SocketAction<()> + 'a {
    ///     channel.send_buffer(message.clone()) // SocketAction<()>
    ///         .and(target.send_buffer(message)) // SocketAction<()> once both messages are sent
    /// }
    /// ```
    fn and<U, B>(self, other: B) -> CombineObjectAction<Self, B, T, U>
    where
        B: SocketAction<U>,
    {
        CombineObjectAction::new(self, other)
    }

    /// Accumulates this [`SocketAction`] with the provided actions into a [`Vec`].
    ///
    /// If one of the actions fails, the accumulated action fails with its error.
    ///
    /// The `other` actions are the actions to accumulate into the list, they must have the same result type `T`.
    /// Returns a combined [`SocketAction`] with the results in the order of the actions.
    ///
    /// # Examples
    ///
    /// ```
    /// # use bytes::BytesMut;
    /// # use libev::entities::channel::middleman::BufferedChannel;
    /// # use libev::requests::sockactions::SocketAction;
    /// fn send_buffer<'a>(
    ///     channel: &'a mut BufferedChannel,
    ///     first: &'a mut BufferedChannel,
    ///     second: &'a mut BufferedChannel,
    ///     message: BytesMut,
    /// ) -> impl SocketAction<Vec<()>> + 'a {
    ///     channel.send_buffer(message.clone()) // SocketAction<()>
    ///         .zip([first.send_buffer(message.clone()), second.send_buffer(message)]) // SocketAction<Vec<()>>
    /// }
    /// ```
    fn zip<I>(self, other: I) -> ZipObjectAction<Self, I::IntoIter>
    where
        I: IntoIterator,
        I::Item: SocketAction<T>,
    {
        ZipObjectAction::new(self, other.into_iter())
    }
}
