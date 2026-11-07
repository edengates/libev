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

/// Factory interface for the creation of new thread manager objects.
///
/// By default, uses [`SingleThreadFactory`] for the creation of its thread managers.
///
/// Implementations of this interface are provided to [`ScheduledThreadManager::new`].
///
/// # See also
///
/// - [`SingleThreadFactory`]
/// - [`ScheduledThreadManager::new`]
///
/// [`SingleThreadFactory`]: crate::utils::concurrent::SingleThreadFactory
/// [`ScheduledThreadManager::new`]: crate::utils::concurrent::ScheduledThreadManager::new
pub trait ThreadManagerFactory {
    type ThreadManager;

    /// Creates a new [`ThreadManager`](Self::ThreadManager) instance with the specified [`ElasticThreadManager`].
    ///
    /// The [`ScheduledThreadManager`] will call this for each shard.
    ///
    /// # Arguments
    ///
    /// - `manager` — The [`ElasticThreadManager`] that is used for dispatching the work
    ///
    /// # Errors
    ///
    /// Returns an error if the [`ThreadManager`](Self::ThreadManager) could not be created.
    ///
    /// # Returns
    ///
    /// The newly constructed [`ThreadManager`](Self::ThreadManager).
    ///
    /// [`ScheduledThreadManager`]: crate::utils::concurrent::ScheduledThreadManager
    fn new_manager(&self, manager: &ElasticThreadManager) -> io::Result<Self::ThreadManager>;
}
