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
use crate::utils::concurrent::{CountingThreadFactory, ThreadPerTaskExecutor, ExecutorCompletionService};
use futures_core::future::LocalBoxFuture;
use log::error;
use rustc_hash::FxHashMap;
use std::collections::BTreeMap;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};
use std::{io, thread};

/// Acts as a manager for multiple shards.
#[derive(Clone)]
pub struct ScheduledThreadPoolExecutor {
    count: Arc<AtomicUsize>,
    shards: Arc<[ExecutorCompletionService]>,
}

impl ScheduledThreadPoolExecutor {
    /// Creates `shards_total` shards, each one with the `factory`.
    ///
    /// If `shards_total` is 0, libev will select the number of shards automatically: `2 *` [`available_parallelism`].
    /// The `capacity` is the maximum number of events handled per poll.
    ///
    /// # Errors
    ///
    /// Returns an error if one of the shards could not be created.
    ///
    /// [`available_parallelism`]: thread::available_parallelism
    pub fn new(shards_total: usize, capacity: usize, factory: fn(u32) -> io::Result<CompletionService>) -> io::Result<Self> {
        let shards_total = if shards_total == 0 { default_shard_total() } else { shards_total };
        let manager = ThreadPerTaskExecutor::new(CountingThreadFactory::of::<Self>("Scheduler"));
        let mut shards = Vec::with_capacity(shards_total);
        for _ in 0..shards_total {
            shards.push(ExecutorCompletionService::new(&manager, capacity, factory)?);
        }

        Ok(Self {
            count: Arc::new(AtomicUsize::new(0)),
            shards: Arc::from(shards),
        })
    }

    /// Returns one of the shards managed by this [`ScheduledThreadPoolExecutor`], in round-robin order.
    pub fn provide(&self) -> &ExecutorCompletionService {
        let count = self.count.fetch_add(1, Ordering::Relaxed);
        let length = self.shards.len();
        &self.shards[if length.is_power_of_two() { count & (length - 1) } else { count % length }]
    }

    /// Gets all shards bound to this [`ScheduledThreadPoolExecutor`].
    pub fn shards(&self) -> &[ExecutorCompletionService] {
        &self.shards
    }

    /// Returns the number of shards managed by this [`ScheduledThreadPoolExecutor`].
    pub fn shards_total(&self) -> usize {
        self.shards.len()
    }
}

fn default_shard_total() -> usize {
    thread::available_parallelism().map_or(1, |n| n.get()) * 2
}


pub struct DelayedWorkQueue<M> {
    scheduled_tasks: BTreeMap<(Instant, usize), Box<dyn FnOnce(&mut M) + Send>>,
    sequence: usize,
    keep_alive_handles: FxHashMap<usize, (Instant, usize)>,
    pending_tasks: FxHashMap<usize, (LocalBoxFuture<'static, ()>, Waker, usize)>,
    task_id: usize,
    sender: Sender<usize>,
    receiver: Receiver<usize>,
}

impl<M> DelayedWorkQueue<M> {
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

    pub(crate) fn enqueue<W>(&mut self, mut task: LocalBoxFuture<'static, ()>, socket_id: usize, waker: W)
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
    use crate::handle::DefaultHandler;
    use std::future::Future;
    use std::pin::Pin;
    use std::ptr;
    use std::task::Wake;

    fn manager(shards_total: usize) -> ScheduledThreadPoolExecutor {
        ScheduledThreadPoolExecutor::new(shards_total, 64, DefaultHandler::new_factory()).unwrap()
    }

    #[test]
    fn provide() {
        for (shards_total, order) in [(3, [0, 1, 2, 0]), (2, [0, 1, 0, 1])] {
            let manager = manager(shards_total);
            assert_eq!(manager.shards_total(), shards_total);
            for index in order {
                assert!(ptr::eq(manager.provide(), &manager.shards()[index]));
            }
        }
    }

    #[test]
    fn count() {
        assert_eq!(manager(0).shards_total(), default_shard_total());
    }

    #[cfg(windows)]
    #[test]
    fn shard_manager() {
        let shard_manager = manager(2);
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
                "scheduledThreadPoolExecutor Scheduler-Worker 1",
                "scheduledThreadPoolExecutor Scheduler-Worker 2"
            ]
        );
    }

    struct ThreadWaker {
        task_id: usize,
        sender: Sender<usize>,
    }

    impl Wake for ThreadWaker {
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
        Waker::from(Arc::new(ThreadWaker {
            task_id,
            sender: sender.clone(),
        }))
    }

    #[test]
    fn schedule() {
        let mut queue = DelayedWorkQueue::<Vec<u32>>::new();
        assert_eq!(queue.timeout(), u32::MAX);
        queue.schedule(Duration::from_millis(20), Box::new(|order| order.push(2)));
        queue.schedule(Duration::ZERO, Box::new(|order| order.push(1)));
        assert!(queue.timeout() <= 20);

        let mut order = Vec::new();
        while let Some(task) = queue.next(Instant::now() + Duration::from_millis(20)) {
            task(&mut order);
        }
        assert_eq!(order, [1, 2]);
        assert!(queue.next(Instant::now()).is_none());
    }

    #[test]
    fn keep_alive() {
        let mut queue = DelayedWorkQueue::<()>::new();
        queue.setup_keep_alive(2, Duration::from_secs(60), Box::new(|_| ()));
        queue.setup_keep_alive(2, Duration::from_secs(60), Box::new(|_| ()));
        assert_eq!(queue.len(), 1);
        queue.stop_keep_alive(2);
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn run_pending_tasks() {
        let mut queue = DelayedWorkQueue::<()>::new();
        queue.enqueue(Box::pin(async {}), 2, hook);
        assert!(queue.is_empty());

        queue.enqueue(Box::pin(YieldNow { yielded: false }), 3, hook);
        queue.enqueue(
            Box::pin(async {
                YieldNow { yielded: false }.await;
                panic!();
            }),
            4,
            hook,
        );
        assert!(!queue.is_empty());
        let mut sockets = queue.run_pending_tasks();
        sockets.sort();
        assert_eq!(sockets, [(3, true), (4, false)]);
        assert!(queue.is_empty());
    }

    #[test]
    fn cancel() {
        let mut queue = DelayedWorkQueue::<()>::new();
        queue.enqueue(Box::pin(std::future::pending()), 2, hook);
        queue.enqueue(Box::pin(std::future::pending()), 3, hook);
        queue.cancel(2);
        assert!(!queue.is_empty());
        queue.cancel(3);
        assert!(queue.is_empty());
    }
}
