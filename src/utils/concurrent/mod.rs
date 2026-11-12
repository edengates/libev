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

//! Implementations for threading related functionality such as Thread-Factories.
mod delayed_completable_future;
pub use delayed_completable_future::DelayedCompletableFuture;
mod direct_thread_manager;
pub use direct_thread_manager::DirectThreadManager;
mod elastic_thread_manager;
pub use elastic_thread_manager::ElasticThreadManager;
mod scheduled_thread_manager;
pub use scheduled_thread_manager::ScheduledThreadManager;
mod single_thread_manager;
pub use single_thread_manager::SingleThreadManager;
mod single_thread_factory;
pub use single_thread_factory::SingleThreadFactory;
mod counting_thread_factory;
pub use counting_thread_factory::CountingThreadFactory;
mod thread_manager_factory;
pub use thread_manager_factory::ThreadManagerFactory;
mod queue_worker;
pub(crate) use queue_worker::{QueueWorker, WorkTask};
mod thread_pool_provider;
pub(crate) use thread_pool_provider::{ThreadPoolProvider, lazy};
