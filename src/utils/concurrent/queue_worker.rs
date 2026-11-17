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

use rustc_hash::FxHashMap;
use log::error;
use std::collections::BTreeMap;
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::mpsc::{self, Receiver, Sender};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

pub type WorkTask = Pin<Box<dyn Future<Output = ()>>>;

pub struct QueueWorker<M> {
    scheduled_tasks: BTreeMap<(Instant, usize), Box<dyn FnOnce(&mut M) + Send>>,
    sequence: usize,
    keep_alive_handles: FxHashMap<usize, (Instant, usize)>,
    pending_tasks: FxHashMap<usize, (WorkTask, Waker, usize)>,
    task_id: usize,
    sender: Sender<usize>,
    receiver: Receiver<usize>,
}

impl<M> QueueWorker<M> {
    pub(crate) fn new() -> Self {
        let (sender, receiver) = mpsc::channel();
        Self {
            scheduled_tasks: BTreeMap::new(),
            sequence: 0,
            keep_alive_handles: FxHashMap::default(),
            pending_tasks: FxHashMap::default(),
            task_id: 0,
            sender,
            receiver,
        }
    }

    pub(crate) fn schedule(&mut self, delay: Duration, task: Box<dyn FnOnce(&mut M) + Send>) -> (Instant, usize) {
        let worker_handle = (Instant::now() + delay, self.sequence);
        self.scheduled_tasks.insert(worker_handle, task);
        self.sequence += 1;
        worker_handle
    }

    pub(crate) fn timeout(&self) -> u32 {
        match self.scheduled_tasks.first_key_value() {
            Some(((deadline, _), _)) => {
                let delay = deadline.saturating_duration_since(Instant::now());
                delay.as_nanos().div_ceil(1_000_000).min(u32::MAX as u128 - 1) as u32
            }
            None => u32::MAX,
        }
    }

    pub(crate) fn next(&mut self, now: Instant) -> Option<Box<dyn FnOnce(&mut M) + Send>> {
        let entry = self.scheduled_tasks.first_entry()?;
        if entry.key().0 > now {
            return None;
        }
        Some(entry.remove())
    }

    pub(crate) fn setup_keep_alive(&mut self, key: usize, delay: Duration, task: Box<dyn FnOnce(&mut M) + Send>) {
        let worker_handle = self.schedule(delay, task);
        if let Some(old_keep_alive) = self.keep_alive_handles.insert(key, worker_handle) {
            self.scheduled_tasks.remove(&old_keep_alive);
        }
    }

    pub(crate) fn stop_keep_alive(&mut self, key: usize) {
        if let Some(worker_handle) = self.keep_alive_handles.remove(&key) {
            self.scheduled_tasks.remove(&worker_handle);
        }
    }

    pub(crate) fn enqueue<W>(&mut self, mut task: WorkTask, socket_id: usize, waker: W)
    where
        W: FnOnce(usize, &Sender<usize>) -> Waker,
    {
        let task_id = self.task_id;
        self.task_id += 1;
        let waker = waker(task_id, &self.sender);
        if task.as_mut().poll(&mut Context::from_waker(&waker)).is_pending() {
            self.pending_tasks.insert(task_id, (task, waker, socket_id));
        }
    }

    pub(crate) fn run_pending_tasks(&mut self) -> Vec<(usize, bool)> {
        let mut sockets = Vec::new();
        let queued: Vec<usize> = self.receiver.try_iter().collect();
        for task_id in queued {
            let Some((mut task, waker, socket_id)) = self.pending_tasks.remove(&task_id) else {
                continue;
            };
            let mut context = Context::from_waker(&waker);
            match panic::catch_unwind(AssertUnwindSafe(|| task.as_mut().poll(&mut context))) {
                Ok(Poll::Pending) => {
                    self.pending_tasks.insert(task_id, (task, waker, socket_id));
                    sockets.push((socket_id, true));
                }
                Ok(Poll::Ready(())) => sockets.push((socket_id, true)),
                Err(_) => {
                    error!("Caught an exception in queue processing thread");
                    sockets.push((socket_id, false));
                }
            }
        }
        sockets
    }

    pub(crate) fn cancel(&mut self, key: usize) {
        self.pending_tasks.retain(|_, (_, _, socket_id)| *socket_id != key);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.pending_tasks.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.scheduled_tasks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::task::Wake;

    struct UnlockHook {
        task_id: usize,
        sender: Sender<usize>,
    }

    impl Wake for UnlockHook {
        fn wake(self: Arc<Self>) {
            self.sender.send(self.task_id).ok();
        }
    }

    struct YieldNow {
        yielded: bool,
    }

    impl Future for YieldNow {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
            if self.yielded {
                return Poll::Ready(());
            }
            self.yielded = true;
            context.waker().wake_by_ref();
            Poll::Pending
        }
    }

    fn hook(task_id: usize, sender: &Sender<usize>) -> Waker {
        Waker::from(Arc::new(UnlockHook {
            task_id,
            sender: sender.clone(),
        }))
    }

    #[test]
    fn schedule() {
        let mut worker = QueueWorker::<Vec<u32>>::new();
        assert_eq!(worker.timeout(), u32::MAX);
        worker.schedule(Duration::from_millis(20), Box::new(|order| order.push(2)));
        worker.schedule(Duration::ZERO, Box::new(|order| order.push(1)));
        assert!(worker.timeout() <= 20);

        let mut order = Vec::new();
        while let Some(task) = worker.next(Instant::now() + Duration::from_millis(20)) {
            task(&mut order);
        }
        assert_eq!(order, [1, 2]);
        assert!(worker.next(Instant::now()).is_none());
    }

    #[test]
    fn keep_alive() {
        let mut worker = QueueWorker::<()>::new();
        worker.setup_keep_alive(2, Duration::from_secs(60), Box::new(|_| ()));
        worker.setup_keep_alive(2, Duration::from_secs(60), Box::new(|_| ()));
        assert_eq!(worker.len(), 1);
        worker.stop_keep_alive(2);
        assert_eq!(worker.len(), 0);
    }

    #[test]
    fn run_pending_tasks() {
        let mut worker = QueueWorker::<()>::new();
        worker.enqueue(Box::pin(async {}), 2, hook);
        assert!(worker.is_empty());

        worker.enqueue(Box::pin(YieldNow { yielded: false }), 3, hook);
        worker.enqueue(
            Box::pin(async {
                YieldNow { yielded: false }.await;
                panic!();
            }),
            4,
            hook,
        );
        assert!(!worker.is_empty());
        let mut sockets = worker.run_pending_tasks();
        sockets.sort();
        assert_eq!(sockets, [(3, true), (4, false)]);
        assert!(worker.is_empty());
    }

    #[test]
    fn cancel() {
        let mut worker = QueueWorker::<()>::new();
        worker.enqueue(Box::pin(std::future::pending()), 2, hook);
        worker.enqueue(Box::pin(std::future::pending()), 3, hook);
        worker.cancel(2);
        assert!(!worker.is_empty());
        worker.cancel(3);
        assert!(worker.is_empty());
    }
}
