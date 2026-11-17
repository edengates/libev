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

use crate::utils::concurrent::{CountingThreadFactory, ElasticThreadManager, ThreadManagerFactory};
use crate::utils::concurrent::{ThreadPoolProvider, lazy};
use std::sync::Arc;
use std::{io, thread};

/// Acts as a manager for multiple shards. Each shard is created by the [`ThreadManagerFactory`].
pub struct ScheduledThreadManager<T> {
    provider: Arc<dyn ThreadPoolProvider<T>>,
    shards_total: usize,
}

impl<T: Send + Sync + 'static> ScheduledThreadManager<T> {
    /// Creates `shards_total` shards, each one with the `factory`.
    ///
    /// If `shards_total` is 0, libev will select the number of shards automatically: `2 *` [`available_parallelism`].
    ///
    /// # Errors
    ///
    /// Returns an error if one of the shards could not be created.
    ///
    /// [`available_parallelism`]: thread::available_parallelism
    pub fn new<C>(shards_total: usize, factory: C) -> io::Result<Self>
    where
        C: ThreadManagerFactory<ThreadManager = T>,
    {
        Self::create(factory, {
            if shards_total == 0 {
                retrieve_shard_total()
            } else {
                shards_total
            }
        })
    }

    fn create<C>(factory: C, shards_total: usize) -> io::Result<Self>
    where
        C: ThreadManagerFactory<ThreadManager = T>,
    {
        let manager = ElasticThreadManager::new(CountingThreadFactory::of::<Self>("Scheduler"));
        let mut shards = Vec::with_capacity(shards_total);
        for _ in 0..shards_total {
            shards.push(factory.new_manager(&manager)?);
        }
        
        Ok(Self {
            provider: Arc::from(lazy(shards)),
            shards_total,
        })
    }

    /// Returns one of the shards managed by this [`ScheduledThreadManager`], in round-robin order.
    pub fn provide(&self) -> &T {
        self.provider.provide()
    }

    /// Gets all shards bound to this [`ScheduledThreadManager`].
    pub fn shards(&self) -> &[T] {
        self.provider.shards()
    }

    /// Returns the number of shards managed by this [`ScheduledThreadManager`].
    pub fn shards_total(&self) -> usize {
        self.shards_total
    }
}

impl<T> Clone for ScheduledThreadManager<T> {
    fn clone(&self) -> Self {
        Self {
            provider: Arc::clone(&self.provider),
            shards_total: self.shards_total,
        }
    }
}

fn retrieve_shard_total() -> usize {
    thread::available_parallelism().map_or(1, |n| n.get()) * 2
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(windows)]
    use crate::utils::concurrent::SingleThreadFactory;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[cfg(windows)]
    use std::sync::mpsc;

    struct CountingThreadManagerFactory {
        index: AtomicUsize,
    }

    impl ThreadManagerFactory for CountingThreadManagerFactory {
        type ThreadManager = usize;

        fn new_manager(&self, _manager: &ElasticThreadManager) -> io::Result<usize> {
            Ok(self.index.fetch_add(1, Ordering::Relaxed) + 1)
        }
    }

    struct ErrorThreadManagerFactory;

    impl ThreadManagerFactory for ErrorThreadManagerFactory {
        type ThreadManager = usize;

        fn new_manager(&self, _manager: &ElasticThreadManager) -> io::Result<usize> {
            Err(io::Error::other("test"))
        }
    }

    fn factory() -> CountingThreadManagerFactory {
        CountingThreadManagerFactory {
            index: AtomicUsize::new(0),
        }
    }

    #[test]
    fn provide() {
        let manager = ScheduledThreadManager::new(3, factory()).unwrap();
        assert_eq!(manager.shards_total(), 3);
        assert_eq!(*manager.provide(), 1);
        assert_eq!(*manager.provide(), 2);
        assert_eq!(*manager.provide(), 3);
        assert_eq!(*manager.provide(), 1);
    }

    #[test]
    fn count() {
        let manager = ScheduledThreadManager::new(0, factory()).unwrap();
        assert_eq!(manager.shards_total(), retrieve_shard_total());
    }

    #[test]
    fn error() {
        assert!(ScheduledThreadManager::new(3, ErrorThreadManagerFactory).is_err());
    }

    #[cfg(windows)]
    #[test]
    fn shard_manager() {
        let shard_manager = ScheduledThreadManager::new(2, SingleThreadFactory::new(64, crate::handle::SocketQueueFactory::default())).unwrap();
        let (sender, receiver) = mpsc::channel();
        
        for _ in 0..2 {
            let sender = sender.clone();
            shard_manager
                .provide()
                .execute(move || {
                    sender
                        .send(thread::current().name().unwrap().to_string())
                        .unwrap();
                })
                .unwrap();
        }

        let mut names = vec![receiver.recv().unwrap(), receiver.recv().unwrap()];
        names.sort();
        assert_eq!(
            names,
            [
                "scheduledThreadManager Scheduler-Worker 1",
                "scheduledThreadManager Scheduler-Worker 2"
            ]
        );
    }
}
