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

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Waker};

/// Specialized [`Future`] used in combination with a scheduler.
///
/// See [`SingleThreadManager::delay`](crate::utils::concurrent::SingleThreadManager::delay).
#[derive(Clone)]
pub struct DelayedCompletableFuture {
    done: Arc<AtomicBool>,
    future: Arc<Mutex<Option<Waker>>>,
}

impl DelayedCompletableFuture {
    pub(crate) fn new() -> Self {
        Self {
            done: Arc::new(AtomicBool::new(false)),
            future: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn complete(&self) {
        self.done.store(true, Ordering::Release);
        if let Some(waker) = self.future.lock().unwrap_or_else(PoisonError::into_inner).take() {
            waker.wake();
        }
    }

    fn is_done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }
}

impl Future for DelayedCompletableFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        *self.future.lock().unwrap_or_else(PoisonError::into_inner) = Some(context.waker().clone());
        if self.is_done() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
