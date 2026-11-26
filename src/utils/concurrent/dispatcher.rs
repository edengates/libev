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

use crate::handle::CompletionService;
use crate::utils::concurrent::{DISPATCH, Task};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;

/// Sends tasks to the thread of a [`ScheduledExecutorService`](crate::utils::concurrent::ScheduledExecutorService) and
/// wakes it up.
#[derive(Clone)]
pub struct Dispatcher {
    sender: Sender<Task>,
    port: Arc<CompletionService>,
    woken: Arc<AtomicBool>,
}

impl Dispatcher {
    /// Creates the dispatcher of the executor that uses `port`, together with the receiver of its tasks.
    pub fn new(port: &Arc<CompletionService>) -> (Self, Receiver<Task>) {
        let (sender, receiver) = mpsc::channel();
        let dispatcher = Self {
            sender,
            port: Arc::clone(port),
            woken: Arc::default(),
        };
        (dispatcher, receiver)
    }

    /// The completion service of the executor.
    pub fn port(&self) -> &Arc<CompletionService> {
        &self.port
    }

    /// Sends the `task` to the executor and wakes it up.
    ///
    /// # Errors
    ///
    /// Returns [`BrokenPipe`](io::ErrorKind::BrokenPipe) if the executor has been stopped.
    pub fn dispatch(&self, task: Task) -> io::Result<()> {
        self.sender
            .send(task)
            .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;
        if self.woken.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        self.port.wakeup(DISPATCH).inspect_err(|_| self.woken.store(false, Ordering::SeqCst))
    }

    /// Lets the next dispatched task wake up the executor again.
    pub fn reset(&self) {
        self.woken.swap(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch() {
        let port = Arc::new(crate::handle::CompletionService::new_factory()(8).unwrap());
        let (dispatcher, receiver) = Dispatcher::new(&port);
        for _ in 0..3 {
            dispatcher.dispatch(Box::new(|_| ())).unwrap();
        }
        assert_eq!(receiver.try_iter().count(), 3);
        assert!(dispatcher.woken.load(Ordering::SeqCst));
        dispatcher.reset();
        assert!(!dispatcher.woken.load(Ordering::SeqCst));
        drop(receiver);
        assert_eq!(dispatcher.dispatch(Box::new(|_| ())).unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        assert!(!dispatcher.woken.load(Ordering::SeqCst));
    }
}
