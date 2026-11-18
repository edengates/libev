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

use crate::utils::concurrent::CountingThreadFactory;
use std::io;

/// Executor which creates a new thread for each task, used for blocking execution.
pub struct ThreadPerTaskExecutor {
    thread_factory: CountingThreadFactory,
}

impl ThreadPerTaskExecutor {
    /// Creates a new [`ThreadPerTaskExecutor`].
    pub fn new(thread_factory: CountingThreadFactory) -> Self {
        Self { thread_factory }
    }

    /// Executes the command on a new thread.
    ///
    /// # Errors
    ///
    /// Returns the error of [`CountingThreadFactory::new_thread`].
    pub fn execute<F>(&self, command: F) -> io::Result<()>
    where
        F: FnOnce() + Send + 'static,
    {
        self.thread_factory.new_thread(command)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::thread;

    #[test]
    fn execute() {
        let factory = CountingThreadFactory::new(|| "Test", "Executor");
        let manager = ThreadPerTaskExecutor::new(factory);
        let (sender, receiver) = mpsc::channel();
        manager
            .execute(move || sender.send(thread::current().name().unwrap().to_string()).unwrap())
            .unwrap();
        assert_eq!(receiver.recv().unwrap(), "Test Executor-Worker 1");
    }
}
