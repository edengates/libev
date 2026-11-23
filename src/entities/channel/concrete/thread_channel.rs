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

use crate::entities::ISnowflake;
use crate::entities::channel::middleman::MessageChannel;
use crate::handle::CompletionService;
use crate::requests::sockactions::ThreadMessageCreateAction;
use crate::utils::concurrent::{DISPATCH, ScheduledExecutorService, Task};
use bytes::BytesMut;
use std::io;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::mpsc::{self, Sender};

/// A [`MessageChannel`] that can be shared with other threads.
///
/// Every method runs the same method of the [`MessageChannel`] on the thread of the channel.
///
/// `N` is the type of messages for the next listener, `P` is the type of messages for the parent channel.
pub struct ThreadChannel<N = BytesMut, P = BytesMut> {
    id: u64,
    position: usize,
    sender: Sender<Task>,
    port: Arc<CompletionService>,
    marker: PhantomData<fn() -> (N, P)>,
}

impl<N, P> Clone for ThreadChannel<N, P> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            position: self.position,
            sender: self.sender.clone(),
            port: Arc::clone(&self.port),
            marker: PhantomData,
        }
    }
}

impl<N, P> ThreadChannel<N, P> {
    pub(crate) fn new(id: u64, position: usize, sender: Sender<Task>, port: Arc<CompletionService>) -> Self {
        Self {
            id,
            position,
            sender,
            port,
            marker: PhantomData,
        }
    }

    pub(crate) fn at_position<A, B>(&self, position: usize) -> ThreadChannel<A, B> {
        ThreadChannel::new(self.id, position, self.sender.clone(), Arc::clone(&self.port))
    }
}

impl<N: 'static, P: Send + 'static> ThreadChannel<N, P> {
    /// Request to write a message via this channel from any thread.
    ///
    /// This does not request a flush, call [`flush`] for that. The message is written on the thread of the channel,
    /// the calling thread waits for the result.
    ///
    /// # Errors
    ///
    /// Returns [`NotConnected`] if the channel has been closed, [`BrokenPipe`] if the thread of the channel has been
    /// shut down and [`Deadlock`] if called on the thread of the channel.
    ///
    /// [`flush`]: Self::flush
    /// [`NotConnected`]: io::ErrorKind::NotConnected
    /// [`BrokenPipe`]: io::ErrorKind::BrokenPipe
    /// [`Deadlock`]: io::ErrorKind::Deadlock
    pub fn write(&self, message: P) -> io::Result<()> {
        self.submit(move |channel| channel.write(message))
    }

    /// Request to flush all pending messages via this channel from any thread.
    ///
    /// # Errors
    ///
    /// Returns [`BrokenPipe`] if the thread of the channel has been shut down.
    ///
    /// [`BrokenPipe`]: io::ErrorKind::BrokenPipe
    pub fn flush(&self) -> io::Result<()> {
        self.execute(|channel| channel.flush())
    }

    /// Send a message to this channel from any thread.
    ///
    /// This requires an additional step to complete the execution, for example when you call
    /// [`ThreadMessageCreateAction::queue`]. The message is written on the thread of the channel, the calling thread
    /// waits for the result. Called on the thread of the channel, the execution fails with [`Deadlock`].
    ///
    /// [`ThreadMessageCreateAction::queue`]: crate::requests::sockactions::SocketAction::queue
    /// [`Deadlock`]: io::ErrorKind::Deadlock
    pub fn send_message(&self, message: P) -> ThreadMessageCreateAction<'_, N, P> {
        ThreadMessageCreateAction::new(self, message)
    }

    /// Closes this channel from any thread, once all pending messages have been sent.
    ///
    /// # Errors
    ///
    /// Returns [`BrokenPipe`] if the thread of the channel has been shut down.
    ///
    /// [`BrokenPipe`]: io::ErrorKind::BrokenPipe
    pub fn close(&self) -> io::Result<()> {
        self.execute(|channel| channel.close())
    }

    /// Sets whether connections from this channel should be deafened, from any thread.
    ///
    /// When deafened, no more messages are read from the socket until it is undeafened.
    ///
    /// # Errors
    ///
    /// Returns [`BrokenPipe`] if the thread of the channel has been shut down.
    ///
    /// [`BrokenPipe`]: io::ErrorKind::BrokenPipe
    pub fn set_self_deafened(&self, deafened: bool) -> io::Result<()> {
        self.execute(move |channel| channel.set_self_deafened(deafened))
    }

    pub(crate) fn submit<T, F>(&self, action: F) -> io::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut MessageChannel<N, P>) -> io::Result<T> + Send + 'static,
    {
        if ScheduledExecutorService::is_current(&self.port) {
            return Err(io::ErrorKind::Deadlock.into());
        }
        let (sender, receiver) = mpsc::sync_channel(1);
        self.execute(move |channel| {
            sender.send(action(channel)).ok();
        })?;
        receiver.recv().map_err(|_| io::Error::from(io::ErrorKind::NotConnected))?
    }

    fn execute<F: FnOnce(&mut MessageChannel<N, P>) + Send + 'static>(&self, action: F) -> io::Result<()> {
        let (id, position) = (self.id, self.position);
        self.sender
            .send(Box::new(move |manager| manager.execute_channel(id, position, action)))
            .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;
        self.port.wakeup(DISPATCH)
    }
}

impl<N, P> ISnowflake for ThreadChannel<N, P> {
    fn id(&self) -> u64 {
        self.id
    }
}
