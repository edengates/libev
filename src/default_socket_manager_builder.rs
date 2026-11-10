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

use crate::SocketManager;
use crate::handle::SocketQueueFactory;
use crate::hooks::ListenerAdapter;
use crate::utils::concurrent::{ScheduledThreadManager, SingleThreadManager, SingleThreadFactory};
use crate::hooks::{InterfacedEventManager, EventManager, EventManagerProxy, EventQueue, EventManagerProvider, EventTask};
use crate::utils::SocketBufferQueue;
use crate::utils::config::{SocketConfig, ThreadingConfig};
use bytes::BytesMut;
use std::any::Any;
use std::cell::RefCell;
use std::io;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

/// Used to create the event managers of a new channel.
type PipelineFactory<P> = Arc<dyn Fn(&SocketManager, &Rc<RefCell<SocketBufferQueue>>, &EventQueue<BytesMut>) -> P + Send + Sync>;

/// Used to create new instances of [`SocketManager`].
pub struct DefaultSocketManagerBuilder<P = Rc<EventManagerProxy>> {
    socket_pool: Option<ScheduledThreadManager<SingleThreadManager>>,
    worker_pool: Option<ScheduledThreadManager<SingleThreadManager>>,
    factory: PipelineFactory<P>,
    listeners: usize,
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
    tcp_no_delay: bool,
    close_timeout: Option<Duration>,
    connect_timeout: Duration,
    afk_timeout: Option<Duration>,
    is_address_shared: bool,
    is_fallback_enabled: bool,
    timeout: Duration,
}

impl DefaultSocketManagerBuilder {
    /// Creates a [`DefaultSocketManagerBuilder`] with recommended default settings.
    ///
    /// **Note:** these defaults can potentially change in the future.
    pub fn new() -> Self {
        Self {
            socket_pool: None,
            worker_pool: None,
            factory: Arc::new(EventManagerProxy::new),
            listeners: 0,
            backlog: 200,
            concurrency: 32,
            buffer_size: 4096,
            reconnect_delay: Duration::from_secs(2),
            max_reconnect_delay: Duration::from_secs(900),
            threshold: 32 * 1024,
            large_threshold: 64 * 1024,
            shutdown_timeout: Duration::from_secs(15),
            chunk_limit: 16,
            packet_limit: 1024,
            tcp_no_delay: true,
            close_timeout: None,
            connect_timeout: Duration::from_secs(30),
            afk_timeout: None,
            is_address_shared: false,
            is_fallback_enabled: true,
            timeout: Duration::from_secs(30),
        }
    }
}

impl<P: EventManager> DefaultSocketManagerBuilder<P> {
    /// Sets the backlog value to specify when [`SocketManager::bind`] binds to a local address.
    ///
    /// This is the maximum length of the queue of incoming connections, which have **not** been accepted yet.
    ///
    /// Default: `200`
    ///
    /// # Arguments
    ///
    /// - `backlog` — The backlog value to use
    ///
    /// # Returns
    ///
    /// The [`DefaultSocketManagerBuilder`] instance. Useful for chaining.
    ///
    /// # See also
    ///
    /// - [`SocketManager::backlog`]
    pub fn set_backlog(mut self, backlog: i32) -> Self {
        self.backlog = backlog;
        self
    }

    /// Sets the maximum number of connections to accept per read loop.
    ///
    /// If this value is greater than 1, the thread manager might attempt to accept multiple times to retrieve multiple
    /// connections.
    ///
    /// **Note:** With IOCP and io_uring this is the number of accept operations queued at once.
    ///
    /// Default: `32`
    ///
    /// # Arguments
    ///
    /// - `concurrency` — The maximum number of connections to accept per read loop
    ///
    /// # Returns
    ///
    /// The [`DefaultSocketManagerBuilder`] instance. Useful for chaining.
    ///
    /// # See also
    ///
    /// - [`SocketManager::concurrency`]
    pub fn set_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// The size, in bytes, of the buffer used for reading messages.
    ///
    /// Every read reserves **at least** this size in the buffer.
    ///
    /// **Note:** With io_uring this is the size of each buffer in the buffer ring.
    ///
    /// Default: `4096`
    ///
    /// # Arguments
    ///
    /// - `buffer_size` — The size of the buffer in bytes
    ///
    /// # Returns
    ///
    /// The [`DefaultSocketManagerBuilder`] instance. Useful for chaining.
    ///
    /// # See also
    ///
    /// - [`SocketManager::buffer_size`]
    pub fn set_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// Sets the amount of time that libev will stop to accept new connections after an error, to allow the socket to
    /// recover (for example when there are too many open files).
    ///
    /// This will use an incremental reconnect (timeouts are increased each time an attempt fails), up to the
    /// [`max_reconnect_delay`](Self::set_max_reconnect_delay).
    ///
    /// Default: `2` seconds
    ///
    /// # Arguments
    ///
    /// - `reconnect_delay` — The amount of time that libev will wait before it accepts connections again
    ///
    /// # Returns
    ///
    /// The [`DefaultSocketManagerBuilder`] instance. Useful for chaining.
    ///
    /// # See also
    ///
    /// - [`SocketManager::reconnect_delay`]
    /// - [`set_max_reconnect_delay`](Self::set_max_reconnect_delay)
    pub fn set_reconnect_delay(mut self, reconnect_delay: Duration) -> Self {
        self.reconnect_delay = reconnect_delay;
        self
    }

    /// Sets the maximum amount of time that libev will back off to wait when attempting to accept new connections
    /// again.
    ///
    /// Default: `900` seconds
    ///
    /// # Arguments
    ///
    /// - `max_reconnect_delay` — The maximum amount of time that libev will wait between reconnect attempts
    ///
    /// # Returns
    ///
    /// The [`DefaultSocketManagerBuilder`] instance. Useful for chaining.
    ///
    /// # See also
    ///
    /// - [`SocketManager::max_reconnect_delay`]
    /// - [`set_reconnect_delay`](Self::set_reconnect_delay)
    pub fn set_max_reconnect_delay(mut self, max_reconnect_delay: Duration) -> Self {
        self.max_reconnect_delay = max_reconnect_delay;
        self
    }

    /// Sets the low water mark for the write buffer of each channel.
    ///
    /// If the number of bytes queued in the write buffer exceeds the [`large_threshold`](Self::set_large_threshold) and
    /// then dropped down below the low water mark, the channel will stop to use slowmode and a
    /// [`ChannelUpdateSlowmodeEvent`] is fired again.
    ///
    /// Default: `32768`
    ///
    /// # Arguments
    ///
    /// - `threshold` — The low water mark in bytes
    ///
    /// # Returns
    ///
    /// The [`DefaultSocketManagerBuilder`] instance. Useful for chaining.
    ///
    /// # See also
    ///
    /// - [`SocketManager::threshold`]
    /// - [`set_large_threshold`](Self::set_large_threshold)
    ///
    /// [`ChannelUpdateSlowmodeEvent`]: crate::events::channel::update::ChannelUpdateSlowmodeEvent
    pub fn set_threshold(mut self, threshold: usize) -> Self {
        self.threshold = threshold;
        self
    }

    /// Sets the high water mark for the write buffer of each channel.
    ///
    /// If the number of bytes queued in the write buffer exceeds the high water mark, the channel will start to use
    /// slowmode and a [`ChannelUpdateSlowmodeEvent`] is fired.
    ///
    /// Default: `65536`
    ///
    /// # Arguments
    ///
    /// - `large_threshold` — The high water mark in bytes
    ///
    /// # Returns
    ///
    /// The [`DefaultSocketManagerBuilder`] instance. Useful for chaining.
    ///
    /// # See also
    ///
    /// - [`SocketManager::large_threshold`]
    /// - [`set_threshold`](Self::set_threshold)
    ///
    /// [`ChannelUpdateSlowmodeEvent`]: crate::events::channel::update::ChannelUpdateSlowmodeEvent
    pub fn set_large_threshold(mut self, large_threshold: usize) -> Self {
        self.large_threshold = large_threshold;
        self
    }

    /// Sets the maximum amount of time to wait until the thread managers are shut down on [`SocketManager::shutdown`],
    /// regardless of open connections.
    ///
    /// After this time **all** remaining connections are closed.
    ///
    /// Default: `15` seconds
    ///
    /// # Arguments
    ///
    /// - `shutdown_timeout` — The maximum time to wait
    ///
    /// # Returns
    ///
    /// The [`DefaultSocketManagerBuilder`] instance. Useful for chaining.
    ///
    /// # See also
    ///
    /// - [`SocketManager::shutdown_timeout`]
    /// - [`SocketManager::shutdown`]
    pub fn set_shutdown_timeout(mut self, shutdown_timeout: Duration) -> Self {
        self.shutdown_timeout = shutdown_timeout;
        self
    }

    /// Sets the maximum number of chunks to read per read loop.
    ///
    /// If this value is greater than 1, the thread manager might attempt to read multiple times to retrieve multiple
    /// chunks of [`buffer_size`](Self::set_buffer_size) bytes. This is **not** used with io_uring.
    ///
    /// Default: `16`
    ///
    /// # Arguments
    ///
    /// - `chunk_limit` — The maximum number of chunks to read per read loop
    ///
    /// # Returns
    ///
    /// The [`DefaultSocketManagerBuilder`] instance. Useful for chaining.
    ///
    /// # See also
    ///
    /// - [`SocketManager::chunk_limit`]
    pub fn set_chunk_limit(mut self, chunk_limit: usize) -> Self {
        self.chunk_limit = chunk_limit;
        self
    }

    /// Sets the maximum number of messages to write at once.
    ///
    /// The messages are written with one gathering write (`writev` or `WSASend`), so this should **not** be greater
    /// than `IOV_MAX`, the maximum number of `iovec` structs that can be passed to `writev`.
    ///
    /// Default: `1024`
    ///
    /// # Arguments
    ///
    /// - `packet_limit` — The maximum number of messages to write at once
    ///
    /// # Returns
    ///
    /// The [`DefaultSocketManagerBuilder`] instance. Useful for chaining.
    ///
    /// # See also
    ///
    /// - [`SocketManager::packet_limit`]
    pub fn set_packet_limit(mut self, packet_limit: usize) -> Self {
        self.packet_limit = packet_limit;
        self
    }

    /// Enables/Disables the use of `TCP_NODELAY` (disables/enables Nagle's algorithm).
    ///
    /// **Note:** the default value of this option is `true` unlike the operating system default (`false`).
    ///
    /// Default: **true (enabled)**
    ///
    /// # Arguments
    ///
    /// - `tcp_no_delay` — Whether `TCP_NODELAY` should be enabled
    ///
    /// # Returns
    ///
    /// The [`DefaultSocketManagerBuilder`] instance. Useful for chaining.
    ///
    /// # See also
    ///
    /// - [`SocketManager::tcp_no_delay`]
    pub fn set_tcp_no_delay(mut self, tcp_no_delay: bool) -> Self {
        self.tcp_no_delay = tcp_no_delay;
        self
    }

    /// Enables the use of `SO_LINGER` with the specified linger time in seconds.
    ///
    /// The linger time will be truncated to seconds.
    ///
    /// **Note:** With a linger time of `0` the connection is reset on close instead of a normal shutdown.
    ///
    /// Default: `None` (disabled)
    ///
    /// # Arguments
    ///
    /// - `close_timeout` — The linger time to use
    ///
    /// # Returns
    ///
    /// The [`DefaultSocketManagerBuilder`] instance. Useful for chaining.
    ///
    /// # See also
    ///
    /// - [`SocketManager::close_timeout`]
    pub fn set_close_timeout(mut self, close_timeout: Duration) -> Self {
        self.close_timeout = Some(close_timeout);
        self
    }

    /// Sets the connect timeout of the channels created by [`SocketManager::connect`].
    ///
    /// If the connection is **not** established in this time, the channel is closed.
    ///
    /// Default: `30` seconds
    ///
    /// # Arguments
    ///
    /// - `connect_timeout` — The maximum amount of time to wait until a connection is established
    ///
    /// # Returns
    ///
    /// The [`DefaultSocketManagerBuilder`] instance. Useful for chaining.
    ///
    /// # See also
    ///
    /// - [`SocketManager::connect_timeout`]
    /// - [`SocketManager::connect`]
    pub fn set_connect_timeout(mut self, connect_timeout: Duration) -> Self {
        self.connect_timeout = connect_timeout;
        self
    }

    /// Fires a [`ChannelTimeoutEvent`] when a channel has not performed read or write operation for the specified
    /// amount of time.
    ///
    /// The event is fired again after each further period without read or write. The channel is **not** closed, a
    /// listener can close it on the event.
    ///
    /// Default: `None` (disabled)
    ///
    /// # Arguments
    ///
    /// - `afk_timeout` — The amount of time without read or write
    ///
    /// # Returns
    ///
    /// The [`DefaultSocketManagerBuilder`] instance. Useful for chaining.
    ///
    /// # See also
    ///
    /// - [`SocketManager::afk_timeout`]
    /// - [`ChannelTimeoutEvent`]
    ///
    /// [`ChannelTimeoutEvent`]: crate::events::channel::ChannelTimeoutEvent
    pub fn set_afk_timeout(mut self, afk_timeout: Duration) -> Self {
        self.afk_timeout = Some(afk_timeout);
        self
    }

    /// Enables/Disables the use of `SO_REUSEPORT` for the sockets bound by [`SocketManager::bind`].
    ///
    /// This will allow to bind multiple sockets to the same port and so accept connections with multiple threads.
    ///
    /// **Note:** On Windows this uses `SO_REUSEADDR` instead of `SO_EXCLUSIVEADDRUSE`.
    ///
    /// Default: **false (disabled)**
    ///
    /// # Arguments
    ///
    /// - `is_address_shared` — Whether `SO_REUSEPORT` should be enabled
    ///
    /// # Returns
    ///
    /// The [`DefaultSocketManagerBuilder`] instance. Useful for chaining.
    ///
    /// # See also
    ///
    /// - [`SocketManager::is_address_shared`]
    pub fn set_address_shared(mut self, is_address_shared: bool) -> Self {
        self.is_address_shared = is_address_shared;
        self
    }

    /// Enables/Disables IPv4 connections on IPv6 sockets bound by [`SocketManager::bind`].
    ///
    /// If enabled, `IPV6_V6ONLY` is disabled, so a socket bound to `[::]` also accepts IPv4 connections.
    ///
    /// Default: **true (enabled)**
    ///
    /// # Arguments
    ///
    /// - `is_fallback_enabled` — Whether IPv4 connections should be enabled on IPv6 sockets
    ///
    /// # Returns
    ///
    /// The [`DefaultSocketManagerBuilder`] instance. Useful for chaining.
    ///
    /// # See also
    ///
    /// - [`SocketManager::is_fallback_enabled`]
    pub fn set_fallback_enabled(mut self, is_fallback_enabled: bool) -> Self {
        self.is_fallback_enabled = is_fallback_enabled;
        self
    }

    /// Sets the maximum amount of time that the listeners of a closed channel may still run.
    ///
    /// After a [`ChannelDisconnectEvent`] the listeners can still handle the remaining events, after this time the
    /// processing is cancelled.
    ///
    /// Default: `30` seconds
    ///
    /// # Arguments
    ///
    /// - `timeout` — The maximum amount of time that the listeners of a closed channel may still run
    ///
    /// # Returns
    ///
    /// The [`DefaultSocketManagerBuilder`] instance. Useful for chaining.
    ///
    /// # See also
    ///
    /// - [`SocketManager::timeout`]
    /// - [`ChannelDisconnectEvent`]
    ///
    /// [`ChannelDisconnectEvent`]: crate::events::channel::ChannelDisconnectEvent
    pub fn set_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets the [`ScheduledThreadManager`] that should be used for the channels created by [`SocketManager::connect`]
    /// and the accepted connections.
    ///
    /// **Only change this pool if you know what you're doing.**
    ///
    /// Default: the [`socket_pool`](Self::set_socket_pool)
    ///
    /// # Arguments
    ///
    /// - `worker_pool` — The thread-pool to use for the channels
    ///
    /// # Returns
    ///
    /// The [`DefaultSocketManagerBuilder`] instance. Useful for chaining.
    ///
    /// # See also
    ///
    /// - [`SocketManager::worker_pool`]
    /// - [`set_socket_pool`](Self::set_socket_pool)
    pub fn set_worker_pool(
        mut self,
        worker_pool: ScheduledThreadManager<SingleThreadManager>,
    ) -> Self {
        self.worker_pool = Some(worker_pool);
        self
    }

    /// Sets the [`ScheduledThreadManager`] that should be used for the sockets bound by [`SocketManager::bind`].
    ///
    /// **Only change this pool if you know what you're doing.**
    ///
    /// Default: Shared [`ScheduledThreadManager`] with (`2 *` [`available_parallelism`]) shards.
    ///
    /// # Arguments
    ///
    /// - `socket_pool` — The thread-pool to use for the bound sockets
    ///
    /// # Returns
    ///
    /// The [`DefaultSocketManagerBuilder`] instance. Useful for chaining.
    ///
    /// # See also
    ///
    /// - [`SocketManager::socket_pool`]
    /// - [`set_worker_pool`](Self::set_worker_pool)
    ///
    /// [`available_parallelism`]: std::thread::available_parallelism
    pub fn set_socket_pool(
        mut self,
        socket_pool: ScheduledThreadManager<SingleThreadManager>,
    ) -> Self {
        self.socket_pool = Some(socket_pool);
        self
    }

    /// Adds the provided listener to the list of listeners that will be used to populate the [`SocketManager`] object.
    ///
    /// **Note:** Every channel uses its own clone of the listener.
    ///
    /// Listeners are handled in the order they are added: events from the socket are handled by the first listener,
    /// then by the next one.
    ///
    /// # Arguments
    ///
    /// - `listener` — The [`ListenerAdapter`] to add
    ///
    /// # Returns
    ///
    /// The [`DefaultSocketManagerBuilder`] instance. Useful for chaining.
    ///
    /// # See also
    ///
    /// - [`add_event_listener_provider`](Self::add_event_listener_provider)
    /// - [`ListenerAdapter`]
    pub fn add_event_listener<L, O>(self, listener: L) -> DefaultSocketManagerBuilder<InterfacedEventManager<P, L, O>>
    where
        L: ListenerAdapter<P::Output, O> + Clone + Sync + 'static,
        O: Any + Send,
    {
        self.add_event_listener_provider(move || listener.clone())
    }

    /// Adds the provided listener provider to the list of listener providers that will be used to create listeners.
    ///
    /// # Arguments
    ///
    /// - `provider` — The listener provider to add to the list of listener providers
    ///
    /// # Returns
    ///
    /// The [`DefaultSocketManagerBuilder`] instance. Useful for chaining.
    ///
    /// # See also
    ///
    /// - [`add_event_listener`](Self::add_event_listener)
    pub fn add_event_listener_provider<F, L, O>(self, provider: F) -> DefaultSocketManagerBuilder<InterfacedEventManager<P, L, O>>
    where
        F: Fn() -> L + Send + Sync + 'static,
        L: ListenerAdapter<P::Output, O> + 'static,
        O: Any + Send,
    {
        let factory = Arc::clone(&self.factory);
        self.set_factory(Arc::new(move |socket_manager, buffer, inbound| {
            InterfacedEventManager::new(factory(socket_manager, buffer, inbound), provider(), socket_manager, buffer)
        }))
    }

    /// Returns a new builder with the provided factory, which keeps all settings of this builder.
    ///
    /// # Arguments
    ///
    /// - `factory` — The new factory to use
    fn set_factory<Q>(self, factory: PipelineFactory<Q>) -> DefaultSocketManagerBuilder<Q> {
        DefaultSocketManagerBuilder {
            socket_pool: self.socket_pool,
            worker_pool: self.worker_pool,
            factory,
            listeners: self.listeners + 1,
            backlog: self.backlog,
            concurrency: self.concurrency,
            buffer_size: self.buffer_size,
            reconnect_delay: self.reconnect_delay,
            max_reconnect_delay: self.max_reconnect_delay,
            threshold: self.threshold,
            large_threshold: self.large_threshold,
            shutdown_timeout: self.shutdown_timeout,
            chunk_limit: self.chunk_limit,
            packet_limit: self.packet_limit,
            tcp_no_delay: self.tcp_no_delay,
            close_timeout: self.close_timeout,
            connect_timeout: self.connect_timeout,
            afk_timeout: self.afk_timeout,
            is_address_shared: self.is_address_shared,
            is_fallback_enabled: self.is_fallback_enabled,
            timeout: self.timeout,
        }
    }

    /// Builds a new [`SocketManager`] instance.
    ///
    /// # Errors
    ///
    /// Returns an error:
    /// - [`InvalidInput`] if no listener has been added
    /// - If the default [`socket_pool`](Self::set_socket_pool) could not be created
    ///
    /// # Returns
    ///
    /// A new [`SocketManager`] instance.
    ///
    /// # See also
    ///
    /// - [`add_event_listener`](Self::add_event_listener)
    ///
    /// [`InvalidInput`]: io::ErrorKind::InvalidInput
    pub fn build(self) -> io::Result<SocketManager> {
        if self.listeners == 0 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let factory = self.factory;
        let provider: EventManagerProvider = Arc::new(move |socket_manager, buffer, inbound| {
            Rc::new(RefCell::new(factory(socket_manager, buffer, inbound))) as Rc<dyn EventTask>
        });
        let socket_pool = match self.socket_pool {
            Some(socket_pool) => socket_pool,
            None => ScheduledThreadManager::new(0, SingleThreadFactory::new(64, SocketQueueFactory::default()))?,
        };
        let worker_pool = self.worker_pool.unwrap_or_else(|| socket_pool.clone());
        let threading_config = ThreadingConfig::new(
            socket_pool,
            worker_pool,
            self.backlog,
            self.concurrency,
            self.buffer_size,
            self.reconnect_delay,
            self.max_reconnect_delay,
            self.threshold,
            self.large_threshold,
            self.shutdown_timeout,
            self.chunk_limit,
            self.packet_limit,
        );
        let socket_config = SocketConfig::new(
            self.tcp_no_delay,
            self.close_timeout,
            self.connect_timeout,
            self.afk_timeout,
            self.is_address_shared,
            self.is_fallback_enabled,
            self.timeout,
        );
        Ok(SocketManager::new(threading_config, socket_config, provider))
    }
}
