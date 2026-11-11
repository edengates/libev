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
use crate::entities::channel::middleman::MessageChannel;
use crate::hooks::ListenerAdapter;
use crate::entities::channel::Channel;
use crate::hooks::{EventManager, EventQueue};
use crate::requests::Command;
use crate::handle::SocketStatus;
use crate::utils::SocketBufferQueue;
use std::any::Any;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::io;
use std::rc::Rc;

/// An [`EventManager`] implementation that uses the [`ListenerAdapter`] interface for event listeners.
///
/// Every listener has its own [`InterfacedEventManager`], which wraps the event manager of the previous listener
/// (`P`). It provides methods for each individual event, and gives the listener `L` a [`MessageChannel`] to fire
/// events to the next listener and to send messages to the parent channel.
///
/// `O` is the type of the messages, which are sent to and received from the next listener.
///
/// # See also
///
/// - [`EventManagerProxy`](crate::hooks::EventManagerProxy)
pub struct InterfacedEventManager<P: EventManager, L, O> {
    lower: P,
    listener: L,
    channel: MessageChannel<O, P::Output>,
    events: EventQueue<O>,
    outbound: Rc<RefCell<VecDeque<Command<O>>>>,
}

impl<P: EventManager, L, O: Any + Send> InterfacedEventManager<P, L, O> {
    /// Creates a new instance with the provided listener, which wraps the event manager of the previous listener.
    ///
    /// The [`MessageChannel`] of the listener uses the [`Channel`] of the previous event manager as its parent channel.
    ///
    /// # Arguments
    ///
    /// - `lower` — The event manager of the previous listener
    /// - `listener` — The listener
    /// - `socket_manager` — The [`SocketManager`] instance of this channel
    /// - `buffer` — The queue of pending messages
    pub(crate) fn new(lower: P, listener: L, socket_manager: &SocketManager, buffer: &Rc<RefCell<SocketBufferQueue>>) -> Self {
        let events = EventQueue::default();
        let channel = MessageChannel::new(socket_manager.clone(), Rc::clone(buffer), Rc::clone(&events), lower.channel());
        Self {
            lower,
            listener,
            channel,
            events,
            outbound: Rc::default(),
        }
    }
}

impl<P, L, O> InterfacedEventManager<P, L, O>
where
    P: EventManager,
    L: ListenerAdapter<P::Output, O> + 'static,
    O: Any + Send,
{
    /// Handles the provided event by calling the method of the [`ListenerAdapter`] for this event.
    ///
    /// # Arguments
    ///
    /// - `event` — The event to handle
    ///
    /// # Errors
    ///
    /// Returns the error of the listener.
    async fn handle_event(&mut self, event: SocketStatus<P::Output>) -> io::Result<()> {
        match event {
            SocketStatus::ChannelReady(_) => self.listener.on_ready(&mut self.channel).await,
            SocketStatus::MessageReceived(event) => {
                self.listener.on_message_received(&mut self.channel, event.message()).await
            }
            SocketStatus::ChannelDisconnect(_) => self.listener.on_channel_disconnect(&mut self.channel).await,
            SocketStatus::ChannelUpdateSlowmode(_) => self.listener.on_channel_update_slowmode(&mut self.channel).await,
            SocketStatus::ChannelTimeout(_) => self.listener.on_channel_timeout(&mut self.channel).await,
            SocketStatus::GenericEvent(event) => self.listener.on_generic_event(&mut self.channel, event).await,
        }
    }

    /// Handles the provided request of the next listener.
    ///
    /// The requests are handled as follows:
    /// - [`Command::Enqueue`] is handled by [`ListenerAdapter::enqueue`]
    /// - [`Command::Execute`] calls [`MessageChannel::execute`] of the listener
    /// - [`Command::Close`] is handled by [`ListenerAdapter::close`]
    ///
    /// # Arguments
    ///
    /// - `command` — The request to handle
    ///
    /// # Errors
    ///
    /// Returns the error of the listener.
    async fn handle_command(&mut self, command: Command<O>) -> io::Result<()> {
        match command {
            Command::Enqueue(message) => self.listener.enqueue(&mut self.channel, message).await,
            Command::Execute => {
                self.channel.execute();
                Ok(())
            }
            Command::Close => self.listener.close(&mut self.channel).await,
        }
    }
}

impl<P, L, O> EventManager for InterfacedEventManager<P, L, O>
where
    P: EventManager,
    L: ListenerAdapter<P::Output, O> + 'static,
    O: Any + Send,
{
    type Output = O;

    fn events(&self) -> &EventQueue<O> {
        &self.events
    }

    fn channel(&self) -> Rc<dyn Channel<O>> {
        Rc::clone(&self.outbound) as Rc<dyn Channel<O>>
    }

    /// Handles the events of the previous event manager first, then the requests of the next listener.
    async fn handle(&mut self) -> io::Result<bool> {
        let mut data = self.lower.handle().await?;
        while let Some(event) = pop_front(self.lower.events()) {
            data = true;
            self.handle_event(event).await?;
        }
        while let Some(command) = pop_front(&self.outbound) {
            data = true;
            self.handle_command(command).await?;
        }
        Ok(data)
    }

    fn clear(&mut self) {
        self.lower.clear();
        self.events.borrow_mut().clear();
        self.outbound.borrow_mut().clear();
    }
}

/// Removes the first element of the provided queue and returns it, or `None` if the queue is empty.
///
/// The queue is **only** borrowed while the element is removed, so it is **not** borrowed while the listener is
/// running.
///
/// # Arguments
///
/// - `queue` — The queue
///
/// # Returns
///
/// The removed element, or `None` if the queue is empty
fn pop_front<T>(queue: &RefCell<VecDeque<T>>) -> Option<T> {
    queue.borrow_mut().pop_front()
}
