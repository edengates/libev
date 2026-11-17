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

pub struct InterfacedEventManager<P: EventManager, L, O> {
    lower: P,
    listener: L,
    channel: MessageChannel<O, P::Output>,
    events: EventQueue<O>,
    outbound: Rc<RefCell<VecDeque<Command<O>>>>,
}

impl<P: EventManager, L, O: Any + Send> InterfacedEventManager<P, L, O> {
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

fn pop_front<T>(queue: &RefCell<VecDeque<T>>) -> Option<T> {
    queue.borrow_mut().pop_front()
}
