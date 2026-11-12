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

/// Holds the thread managers and the threading configuration of a [`SocketManager`](crate::SocketManager), which
/// are set with the [`DefaultSocketManagerBuilder`](crate::DefaultSocketManagerBuilder).
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
    /// Creates a new instance with the provided thread managers and configuration.
    ///
    /// # Arguments
    ///
    /// - `socket_pool` — The thread-pool used for each bound socket
    /// - `worker_pool` — The thread-pool used for the channels
    /// - `backlog` — The `backlog` value used when a socket is bound
    /// - `concurrency` — The maximum amount of connections which are accepted at once
    /// - `buffer_size` — The size of the buffer used to receive messages
    /// - `reconnect_delay` — The amount of time that libev will wait before it accepts connections again
    /// - `max_reconnect_delay` — The maximum amount of time that libev will wait between reconnect attempts
    /// - `threshold` — The threshold at which the slow mode of a channel ends
    /// - `large_threshold` — The threshold at which the slow mode of a channel starts
    /// - `shutdown_timeout` — The maximum amount of time to wait until the shutdown has completed
    /// - `chunk_limit` — The maximum amount of chunks which are received at once
    /// - `packet_limit` — The maximum amount of messages which are sent at once
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

    /// The thread-pool used for each bound socket.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_socket_pool`](crate::DefaultSocketManagerBuilder::set_socket_pool)
    /// - [`SocketManager::socket_pool`](crate::SocketManager::socket_pool)
    pub fn socket_pool(&self) -> &ScheduledThreadManager<SingleThreadManager> {
        &self.socket_pool
    }

    /// The thread-pool used for the channels.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_worker_pool`](crate::DefaultSocketManagerBuilder::set_worker_pool)
    /// - [`SocketManager::worker_pool`](crate::SocketManager::worker_pool)
    pub fn worker_pool(&self) -> &ScheduledThreadManager<SingleThreadManager> {
        &self.worker_pool
    }

    /// The `backlog` value used when a socket is bound.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_backlog`](crate::DefaultSocketManagerBuilder::set_backlog)
    /// - [`SocketManager::backlog`](crate::SocketManager::backlog)
    pub fn backlog(&self) -> i32 {
        self.backlog
    }

    /// The maximum amount of connections which are accepted at once.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_concurrency`](crate::DefaultSocketManagerBuilder::set_concurrency)
    /// - [`SocketManager::concurrency`](crate::SocketManager::concurrency)
    pub fn concurrency(&self) -> usize {
        self.concurrency
    }

    /// The size of the buffer used to receive messages.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_buffer_size`](crate::DefaultSocketManagerBuilder::set_buffer_size)
    /// - [`SocketManager::buffer_size`](crate::SocketManager::buffer_size)
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// The amount of time that libev will wait before it accepts connections again after an error.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_reconnect_delay`](crate::DefaultSocketManagerBuilder::set_reconnect_delay)
    /// - [`SocketManager::reconnect_delay`](crate::SocketManager::reconnect_delay)
    pub fn reconnect_delay(&self) -> Duration {
        self.reconnect_delay
    }

    /// The maximum amount of time that libev will wait between reconnect attempts.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_max_reconnect_delay`][set_max_reconnect_delay]
    /// - [`SocketManager::max_reconnect_delay`](crate::SocketManager::max_reconnect_delay)
    ///
    /// [set_max_reconnect_delay]: crate::DefaultSocketManagerBuilder::set_max_reconnect_delay
    pub fn max_reconnect_delay(&self) -> Duration {
        self.max_reconnect_delay
    }

    /// The threshold at which the slow mode of a channel ends.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_threshold`](crate::DefaultSocketManagerBuilder::set_threshold)
    /// - [`SocketManager::threshold`](crate::SocketManager::threshold)
    pub fn threshold(&self) -> usize {
        self.threshold
    }

    /// The threshold at which the slow mode of a channel starts.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_large_threshold`](crate::DefaultSocketManagerBuilder::set_large_threshold)
    /// - [`SocketManager::large_threshold`](crate::SocketManager::large_threshold)
    pub fn large_threshold(&self) -> usize {
        self.large_threshold
    }

    /// The maximum amount of time to wait until the shutdown has completed.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_shutdown_timeout`]
    /// - [`SocketManager::shutdown_timeout`](crate::SocketManager::shutdown_timeout)
    ///
    /// [`DefaultSocketManagerBuilder::set_shutdown_timeout`]: crate::DefaultSocketManagerBuilder::set_shutdown_timeout
    pub fn shutdown_timeout(&self) -> Duration {
        self.shutdown_timeout
    }

    /// The maximum amount of chunks which are received at once.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_chunk_limit`](crate::DefaultSocketManagerBuilder::set_chunk_limit)
    /// - [`SocketManager::chunk_limit`](crate::SocketManager::chunk_limit)
    pub fn chunk_limit(&self) -> usize {
        self.chunk_limit
    }

    /// The maximum amount of messages which are sent at once.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_packet_limit`](crate::DefaultSocketManagerBuilder::set_packet_limit)
    /// - [`SocketManager::packet_limit`](crate::SocketManager::packet_limit)
    pub fn packet_limit(&self) -> usize {
        self.packet_limit
    }

    /// Shuts down all shards of the socket pool and the worker pool, closing all their connections.
    ///
    /// # See also
    ///
    /// - [`SocketManager::shutdown`](crate::SocketManager::shutdown)
    /// - [`shutdown_now`](Self::shutdown_now)
    /// - [`await_shutdown`](Self::await_shutdown)
    pub fn shutdown(&self) {
        for shard in self.socket_pool.shards().iter().chain(self.worker_pool.shards()) {
            shard.shutdown(self.shutdown_timeout);
        }
    }

    /// Blocks the current thread until all shards have been shutdown.
    ///
    /// # Arguments
    ///
    /// - `timeout` — The maximum time to wait
    ///
    /// # Returns
    ///
    /// `false`, if the timeout has elapsed before the shutdown has completed, `true` otherwise.
    ///
    /// # See also
    ///
    /// - [`SocketManager::await_shutdown`](crate::SocketManager::await_shutdown)
    /// - [`shutdown`](Self::shutdown)
    pub fn await_shutdown(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        self.socket_pool
            .shards()
            .iter()
            .chain(self.worker_pool.shards())
            .all(|shard| shard.await_shutdown(deadline.saturating_duration_since(Instant::now())))
    }

    /// Shuts down all shards instantly, closing all their connections.
    ///
    /// # Errors
    ///
    /// Returns the first error, the other shards are still shut down.
    ///
    /// # See also
    ///
    /// - [`SocketManager::shutdown_now`](crate::SocketManager::shutdown_now)
    /// - [`shutdown`](Self::shutdown)
    pub fn shutdown_now(&self) -> io::Result<()> {
        let mut data = Ok(());
        for shard in self.socket_pool.shards().iter().chain(self.worker_pool.shards()) {
            data = data.and(shard.shutdown_now());
        }
        data
    }
}
