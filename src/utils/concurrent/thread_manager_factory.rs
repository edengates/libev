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

use crate::utils::concurrent::ElasticThreadManager;
use std::io;

/// Factory interface for the creation of new shards.
///
/// Implementations are passed to [`ScheduledThreadManager::new`], by default it is a [`SingleThreadFactory`].
///
/// [`SingleThreadFactory`]: crate::utils::concurrent::SingleThreadFactory
/// [`ScheduledThreadManager::new`]: crate::utils::concurrent::ScheduledThreadManager::new
pub trait ThreadManagerFactory {
    type ThreadManager;

    /// Called by the [`ScheduledThreadManager`] once for each shard. The `manager` is used for dispatching the work.
    ///
    /// # Errors
    ///
    /// An error here makes [`ScheduledThreadManager::new`](crate::utils::concurrent::ScheduledThreadManager::new) fail.
    ///
    /// [`ScheduledThreadManager`]: crate::utils::concurrent::ScheduledThreadManager
    fn new_manager(&self, manager: &ElasticThreadManager) -> io::Result<Self::ThreadManager>;
}
