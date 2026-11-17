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

use crate::handle::SocketQueueFactory;
use crate::utils::concurrent::{ElasticThreadManager, SingleThreadManager, ThreadManagerFactory};
use std::io;

/// Used to create new [`SingleThreadManager`] instances.
pub struct SingleThreadFactory {
    capacity: usize,
    factory: SocketQueueFactory,
}

impl SingleThreadFactory {
    /// Creates a new [`SingleThreadFactory`] instance.
    ///
    /// The `capacity` is the maximum number of events handled per poll, see [`SingleThreadManager::new`].
    pub fn new(capacity: usize, factory: SocketQueueFactory) -> Self {
        Self { capacity, factory }
    }
}

impl ThreadManagerFactory for SingleThreadFactory {
    type ThreadManager = SingleThreadManager;

    fn new_manager(&self, manager: &ElasticThreadManager) -> io::Result<SingleThreadManager> {
        SingleThreadManager::new(manager, self.capacity, self.factory)
    }
}
