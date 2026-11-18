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
mod thread_per_task_executor;
pub use thread_per_task_executor::ThreadPerTaskExecutor;
mod scheduled_thread_pool_executor;
pub use scheduled_thread_pool_executor::ScheduledThreadPoolExecutor;
mod thread_waker;
pub(crate) use thread_waker::ThreadWaker;
mod executor_completion_service;
pub use executor_completion_service::ExecutorCompletionService;
mod scheduled_executor_service;
pub(crate) use scheduled_executor_service::{ScheduledExecutorService, DISPATCH, SHUTDOWN, Task};
mod counting_thread_factory;
pub use counting_thread_factory::CountingThreadFactory;
