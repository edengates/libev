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

use crate::utils::concurrent::{ScheduledThreadManager, SingleThreadManager};
use std::io;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct ThreadingConfig {
    worker_pool: ScheduledThreadManager<SingleThreadManager>,
    socket_pool: ScheduledThreadManager<SingleThreadManager>,
    backlog: i32,
    concurrency: usize,
    buffer_size: usize,
    reconnect_delay: Duration,
    max_reconnect_delay: Duration,
    threshold: usize,
    large_threshold: usize,
    shutdown_timeout: Duration,
    chunk_limit: usize,
    packet_limit: usize,
}

impl ThreadingConfig {
    pub fn new(
        socket_pool: ScheduledThreadManager<SingleThreadManager>,
        worker_pool: ScheduledThreadManager<SingleThreadManager>,
        backlog: i32,
        concurrency: usize,
        buffer_size: usize,
        reconnect_delay: Duration,
        max_reconnect_delay: Duration,
        threshold: usize,
        large_threshold: usize,
        shutdown_timeout: Duration,
        chunk_limit: usize,
        packet_limit: usize,
    ) -> Self {
        Self {
            socket_pool,
            worker_pool,
            backlog,
            concurrency,
            buffer_size,
            reconnect_delay,
            max_reconnect_delay,
            threshold,
            large_threshold,
            shutdown_timeout,
            chunk_limit,
            packet_limit,
        }
    }

    pub fn socket_pool(&self) -> &ScheduledThreadManager<SingleThreadManager> {
        &self.socket_pool
    }

    pub fn worker_pool(&self) -> &ScheduledThreadManager<SingleThreadManager> {
        &self.worker_pool
    }

    pub fn backlog(&self) -> i32 {
        self.backlog
    }

    pub fn concurrency(&self) -> usize {
        self.concurrency
    }

    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    pub fn reconnect_delay(&self) -> Duration {
        self.reconnect_delay
    }

    pub fn max_reconnect_delay(&self) -> Duration {
        self.max_reconnect_delay
    }

    pub fn threshold(&self) -> usize {
        self.threshold
    }

    pub fn large_threshold(&self) -> usize {
        self.large_threshold
    }

    pub fn shutdown_timeout(&self) -> Duration {
        self.shutdown_timeout
    }

    pub fn chunk_limit(&self) -> usize {
        self.chunk_limit
    }

    pub fn packet_limit(&self) -> usize {
        self.packet_limit
    }

    pub fn shutdown(&self) {
        for shard in self.socket_pool.shards().iter().chain(self.worker_pool.shards()) {
            shard.shutdown(self.shutdown_timeout);
        }
    }

    pub fn await_shutdown(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        self.socket_pool
            .shards()
            .iter()
            .chain(self.worker_pool.shards())
            .all(|shard| shard.await_shutdown(deadline.saturating_duration_since(Instant::now())))
    }

    pub fn shutdown_now(&self) -> io::Result<()> {
        let mut data = Ok(());
        for shard in self.socket_pool.shards().iter().chain(self.worker_pool.shards()) {
            data = data.and(shard.shutdown_now());
        }
        data
    }
}
