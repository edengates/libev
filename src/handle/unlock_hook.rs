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

use crate::handle::{DISPATCH, SocketQueueManager};
use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::task::Wake;

/// The [`Waker`](std::task::Waker) of the pending task of a listener.
///
/// Each time the task can continue, the identifier of the task is sent to the
/// [`QueueWorker`](crate::utils::concurrent::QueueWorker) and [`DISPATCH`] to the [`SocketQueueManager`], so the task
/// is handled again on the thread of the [`ChannelManager`](crate::handle::ChannelManager).
///
/// The [`Waker`](std::task::Waker) can be used from every thread. Nothing is sent, if the
/// [`ChannelManager`](crate::handle::ChannelManager) no longer exists, and an error of the
/// [`SocketQueueManager`] is ignored.
///
/// **Note:** If the task has finished or has been canceled already, the identifier is ignored by the
/// [`QueueWorker`](crate::utils::concurrent::QueueWorker).
///
/// # See also
///
/// - [`ChannelManager::enqueue`](crate::handle::ChannelManager::enqueue)
/// - [`DISPATCH`]
pub struct UnlockHook {
    response_number: usize,
    sender: Sender<usize>,
    port: Arc<SocketQueueManager>,
}

impl UnlockHook {
    /// Creates a new instance with the provided identifier of the task.
    ///
    /// The identifier is sent with the provided [`Sender`], and [`DISPATCH`] to the provided [`SocketQueueManager`].
    ///
    /// # Arguments
    ///
    /// - `response_number` — The identifier of the task
    /// - `sender` — The [`Sender`], which is used to send the identifier
    /// - `port` — The [`SocketQueueManager`], which is used to send [`DISPATCH`]
    pub(crate) fn new(response_number: usize, sender: Sender<usize>, port: Arc<SocketQueueManager>) -> Self {
        Self {
            response_number,
            sender,
            port,
        }
    }

    /// Sets the identifier of the task, so the same instance can be used for the next task.
    ///
    /// This is **only** done if no other [`Waker`](std::task::Waker) of this instance exists, as it would otherwise
    /// send the identifier of the next task.
    ///
    /// # Arguments
    ///
    /// - `response_number` — The new identifier of the task
    pub(crate) fn set_response_number(&mut self, response_number: usize) {
        self.response_number = response_number;
    }
}

impl Wake for UnlockHook {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.sender.send(self.response_number).is_ok() {
            self.port.enqueue(DISPATCH).ok();
        }
    }
}
