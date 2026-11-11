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

use crate::SocketManager;
use crate::entities::channel::mixin::middleman::MessageChannelMixin;
use crate::entities::channel::Channel;
use crate::handle::SocketStatus;
use crate::utils::SocketBufferQueue;
use crate::utils::concurrent::WorkTask;
use bytes::BytesMut;
use std::any::Any;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::io;
use std::rc::Rc;
use std::sync::Arc;

/// A queue of events, which are handled by the next listener.
///
/// The events are handled in the order they were added.
pub type EventQueue<T> = Rc<RefCell<VecDeque<SocketStatus<T>>>>;

/// Used to create the [`EventTask`] of each new channel.
///
/// It is called with:
/// - the [`SocketManager`],
/// - the queue of pending messages,
/// - the [`EventQueue`] of the socket,
///
/// and creates the event managers of **all** listeners which have been added to the
/// [`DefaultSocketManagerBuilder`](crate::DefaultSocketManagerBuilder).
pub type EventManagerProvider =
    Arc<dyn Fn(&SocketManager, &Rc<RefCell<SocketBufferQueue>>, &EventQueue<BytesMut>) -> Rc<dyn EventTask> + Send + Sync>;

/// An interface for libev's event manager system.
///
/// Every listener added to the [`DefaultSocketManagerBuilder`] has its own event manager: an
/// [`InterfacedEventManager`], which wraps the event manager of the previous listener. The first event manager is
/// the [`EventManagerProxy`], which receives the events of the socket.
///
/// An event is handled in the bottom-up direction: by the [`EventManagerProxy`] first, then by the event manager of
/// each listener in the order they were added. **If an event goes beyond the last listener, it is discarded
/// silently.**
///
/// A message is handled in the top-down direction: each listener sends it to its parent [`Channel`], until it
/// reaches the [`EventManagerProxy`], which adds it to the queue of pending messages.
///
/// # See also
///
/// - [`ListenerAdapter`]
/// - [`EventTask`]
///
/// [`ListenerAdapter`]: crate::hooks::ListenerAdapter
/// [`DefaultSocketManagerBuilder`]: crate::DefaultSocketManagerBuilder
/// [`InterfacedEventManager`]: crate::hooks::InterfacedEventManager
/// [`EventManagerProxy`]: crate::hooks::EventManagerProxy
#[allow(async_fn_in_trait)]
pub trait EventManager: 'static {
    /// The type of the messages, which are sent to and received from the next listener.
    type Output: Any + Send;

    /// The [`EventQueue`] of this event manager, from which the next listener takes its events.
    fn events(&self) -> &EventQueue<Self::Output>;

    /// The [`Channel`] to which the next listener sends its messages.
    ///
    /// This is the parent channel of the [`MessageChannel`] of the next listener.
    ///
    /// [`MessageChannel`]: crate::entities::channel::middleman::MessageChannel
    fn channel(&self) -> Rc<dyn Channel<Self::Output>>;

    /// Handles all events, which the previous event manager has added to its [`EventQueue`], and all requests of the
    /// next listener.
    ///
    /// How this is handled is specified by the implementation.
    ///
    /// # Errors
    ///
    /// Returns an error if one of the listeners has returned an error, the channel is then shutdown.
    ///
    /// # Returns
    ///
    /// Returns `true` if something has been handled, `false` if there was nothing left to handle.
    ///
    /// # See also
    ///
    /// - [`clear`](Self::clear)
    async fn handle(&mut self) -> io::Result<bool>;

    /// Removes all pending events and requests of this and all previous event managers.
    ///
    /// This is called *after* one of the listeners has returned an error.
    ///
    /// # See also
    ///
    /// - [`handle`](Self::handle)
    fn clear(&mut self);
}

/// An [`EventManager`], which can be stored as `dyn` and so does not need to know the types of the listeners.
///
/// Every [`MessageChannelMixin`] has its own [`EventTask`], which is created by the [`EventManagerProvider`].
pub trait EventTask {
    /// Creates the task, which handles all events of the provided channel until there is nothing left to handle.
    ///
    /// The task is enqueued to the [`ChannelManager`] by the [`MessageChannelMixin`]. If one of the listeners has
    /// returned an error, the queue of pending messages is shutdown and **all** pending events and requests are
    /// removed.
    ///
    /// # Arguments
    ///
    /// - `channel` — The [`MessageChannelMixin`] of the channel
    ///
    /// # Returns
    ///
    /// The new [`WorkTask`]
    ///
    /// [`ChannelManager`]: crate::handle::ChannelManager
    fn handle(self: Rc<Self>, channel: Rc<MessageChannelMixin>) -> WorkTask;
}

impl<P: EventManager> EventTask for RefCell<P> {
    fn handle(self: Rc<Self>, channel: Rc<MessageChannelMixin>) -> WorkTask {
        Box::pin(async move {
            let mut data = Ok(true);
            while let Ok(true) = data {
                data = self.borrow_mut().handle().await;
                self.borrow().events().borrow_mut().clear();
            }
            if data.is_err() {
                channel.buffer().borrow_mut().shutdown();
                self.borrow_mut().clear();
            }
            channel.connect(false);
        })
    }
}

/// Converts the provided value to the type `B`, if `A` and `B` are the same type.
///
/// # Arguments
///
/// - `value` — The value to convert
///
/// # Returns
///
/// The value as `B`, if `A` and `B` are the same type. Returns `None` otherwise.
pub(crate) fn cast<A: Any, B: Any>(value: A) -> Option<B> {
    (&mut Some(value) as &mut dyn Any).downcast_mut::<Option<B>>().and_then(Option::take)
}

#[cfg(test)]
mod tests {
    #[test]
    fn cast() {
        assert_eq!(super::cast::<u32, u32>(7), Some(7));
        assert_eq!(super::cast::<u32, String>(7), None);
    }
}
