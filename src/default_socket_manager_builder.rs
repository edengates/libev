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
use crate::hooks::ListenerAdapter;
use crate::utils::concurrent::ScheduledThreadPoolExecutor;
use crate::utils::{InterfacedEventManager, EventManager, EventQueue, EventManagerProvider, EventManagerProxy};
use crate::requests::BufferedSink;
use crate::utils::Buffer;
use crate::utils::config::{SocketConfig, ThreadingConfig};
use std::any::Any;
use std::cell::RefCell;
use std::io;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

type PipelineFactory<P> = Arc<dyn Fn(&SocketManager, &Rc<RefCell<Buffer>>, &EventQueue) -> P + Send + Sync>;

/// Used to create new instances of [`SocketManager`].
pub struct DefaultSocketManagerBuilder<P = Rc<BufferedSink>> {
    socket_pool: Option<ScheduledThreadPoolExecutor>,
    worker_pool: Option<ScheduledThreadPoolExecutor>,
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
    /// Note that these defaults can potentially change in the future.
    pub fn new() -> Self {
        Self {
            socket_pool: None,
            worker_pool: None,
            factory: Arc::new(BufferedSink::new),
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
    /// Sets the backlog passed to `listen(2)` for the sockets bound by [`SocketManager::bind`].
    ///
    /// This is the maximum length of the queue of incoming connections that are not accepted yet.
    ///
    /// Default: `200`
    pub fn set_backlog(mut self, backlog: i32) -> Self {
        self.backlog = backlog;
        self
    }

    /// Sets the maximum number of connections to accept per read loop.
    ///
    /// With IOCP and io_uring this is instead the number of pending accept operations.
    ///
    /// Default: `32`
    pub fn set_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Sets the size, in bytes, of the buffer used to read messages.
    ///
    /// Every read reserves at least this size. With io_uring this is the size of each buffer in the buffer ring.
    ///
    /// Default: `4096`
    pub fn set_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// Sets how long libev stops accepting new connections after an error.
    ///
    /// The delay grows with each failed attempt, up to the [`max_reconnect_delay`](Self::set_max_reconnect_delay).
    ///
    /// Default: `2` seconds
    pub fn set_reconnect_delay(mut self, reconnect_delay: Duration) -> Self {
        self.reconnect_delay = reconnect_delay;
        self
    }

    /// Sets the upper limit for the [`reconnect_delay`](Self::set_reconnect_delay).
    ///
    /// Default: `900` seconds
    pub fn set_max_reconnect_delay(mut self, max_reconnect_delay: Duration) -> Self {
        self.max_reconnect_delay = max_reconnect_delay;
        self
    }

    /// Sets the low water mark for the write buffer of each channel.
    ///
    /// Once a channel in slow mode drops below this many queued bytes, it leaves slow mode and a
    /// [`ChannelUpdateSlowmodeEvent`] is fired again. See [`set_large_threshold`](Self::set_large_threshold).
    ///
    /// Default: `32768`
    ///
    /// [`ChannelUpdateSlowmodeEvent`]: crate::events::channel::update::ChannelUpdateSlowmodeEvent
    pub fn set_threshold(mut self, threshold: usize) -> Self {
        self.threshold = threshold;
        self
    }

    /// Sets the high water mark for the write buffer of each channel.
    ///
    /// Above this many queued bytes the channel goes into slow mode and a [`ChannelUpdateSlowmodeEvent`] is fired.
    ///
    /// Default: `65536`
    ///
    /// [`ChannelUpdateSlowmodeEvent`]: crate::events::channel::update::ChannelUpdateSlowmodeEvent
    pub fn set_large_threshold(mut self, large_threshold: usize) -> Self {
        self.large_threshold = large_threshold;
        self
    }

    /// Sets how long [`SocketManager::shutdown`] and a closed channel wait for pending messages before the connections
    /// are closed and the pools are shut down.
    ///
    /// Default: `15` seconds
    pub fn set_shutdown_timeout(mut self, shutdown_timeout: Duration) -> Self {
        self.shutdown_timeout = shutdown_timeout;
        self
    }

    /// Sets the maximum number of [`buffer_size`](Self::set_buffer_size) chunks to read per read loop.
    ///
    /// Not used with io_uring.
    ///
    /// Default: `16`
    pub fn set_chunk_limit(mut self, chunk_limit: usize) -> Self {
        self.chunk_limit = chunk_limit;
        self
    }

    /// Sets the maximum number of messages written with one gathering write (`writev` or `WSASend`).
    ///
    /// Should not be greater than `IOV_MAX`.
    ///
    /// Default: `1024`
    pub fn set_packet_limit(mut self, packet_limit: usize) -> Self {
        self.packet_limit = packet_limit;
        self
    }

    /// Sets the value of the `TCP_NODELAY` option on each connection.
    ///
    /// If set, this option disables the Nagle algorithm. This means that segments are always sent as soon as possible,
    /// even if there is only a small amount of data. The operating system default is `false`.
    ///
    /// Default: `true`
    pub fn set_tcp_no_delay(mut self, tcp_no_delay: bool) -> Self {
        self.tcp_no_delay = tcp_no_delay;
        self
    }

    /// Sets value for the `SO_LINGER` option on each connection.
    ///
    /// The duration only has a precision of seconds and will be silently truncated. With `0` the connection is reset on
    /// close instead of a normal shutdown.
    ///
    /// Default: `None` (disabled)
    pub fn set_close_timeout(mut self, close_timeout: Duration) -> Self {
        self.close_timeout = Some(close_timeout);
        self
    }

    /// Sets the connect timeout for [`SocketManager::connect`], after which the channel is closed.
    ///
    /// Default: `30` seconds
    pub fn set_connect_timeout(mut self, connect_timeout: Duration) -> Self {
        self.connect_timeout = connect_timeout;
        self
    }

    /// Sets the AFK timeout of each channel.
    ///
    /// A [`ChannelTimeoutEvent`] is fired after each period of this length without read or write.
    ///
    /// Default: `None` (disabled)
    ///
    /// [`ChannelTimeoutEvent`]: crate::events::channel::ChannelTimeoutEvent
    pub fn set_afk_timeout(mut self, afk_timeout: Duration) -> Self {
        self.afk_timeout = Some(afk_timeout);
        self
    }

    /// Sets value for the `SO_REUSEPORT` option on the sockets bound by [`SocketManager::bind`].
    ///
    /// This allows multiple threads to bind to the same port. On Windows this uses `SO_REUSEADDR` instead of
    /// `SO_EXCLUSIVEADDRUSE`.
    ///
    /// Default: `false`
    pub fn set_address_shared(mut self, is_address_shared: bool) -> Self {
        self.is_address_shared = is_address_shared;
        self
    }

    /// Allows IPv4 connections on the IPv6 sockets bound by [`SocketManager::bind`].
    ///
    /// If `true`, `IPV6_V6ONLY` is cleared, so a socket bound to `[::]` can be used with IPv4-mapped addresses.
    ///
    /// Default: `true`
    pub fn set_fallback_enabled(mut self, is_fallback_enabled: bool) -> Self {
        self.is_fallback_enabled = is_fallback_enabled;
        self
    }

    /// Sets how long the listeners of a closed channel may still handle the remaining events.
    ///
    /// The time starts with the [`ChannelDisconnectEvent`], after it the processing is canceled.
    ///
    /// Default: `30` seconds
    ///
    /// [`ChannelDisconnectEvent`]: crate::events::channel::ChannelDisconnectEvent
    pub fn set_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets the pool used for the channels created by [`SocketManager::connect`] and for the accepted connections.
    ///
    /// Default: the [`socket_pool`](Self::set_socket_pool)
    pub fn set_worker_pool(
        mut self,
        worker_pool: ScheduledThreadPoolExecutor,
    ) -> Self {
        self.worker_pool = Some(worker_pool);
        self
    }

    /// Sets the pool used for the sockets bound by [`SocketManager::bind`].
    ///
    /// Default: Shared [`ScheduledThreadPoolExecutor`] with (`2 *` [`available_parallelism`]) shards.
    ///
    /// [`available_parallelism`]: std::thread::available_parallelism
    pub fn set_socket_pool(
        mut self,
        socket_pool: ScheduledThreadPoolExecutor,
    ) -> Self {
        self.socket_pool = Some(socket_pool);
        self
    }

    /// Adds the listener to the list of listeners that will be used to populate the [`SocketManager`] object.
    ///
    /// Every channel uses its own clone of the listener. Listeners are called in the order they are added.
    pub fn add_event_listener<L, O>(self, listener: L) -> DefaultSocketManagerBuilder<InterfacedEventManager<P, L, O>>
    where
        L: ListenerAdapter<P::Output, O> + Clone + Sync + 'static,
        O: Any + Send,
    {
        self.add_event_listener_provider(move || listener.clone())
    }

    /// Like [`add_event_listener`](Self::add_event_listener), but the provider is called once for each new channel.
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
    /// Fails with [`InvalidInput`] if no listener was added, or if the default socket pool cannot be created.
    ///
    /// [`InvalidInput`]: io::ErrorKind::InvalidInput
    pub fn build(self) -> io::Result<SocketManager> {
        if self.listeners == 0 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let factory = self.factory;
        let provider: EventManagerProvider = Arc::new(move |socket_manager, buffer, inbound| {
            Rc::new(RefCell::new(factory(socket_manager, buffer, inbound))) as Rc<dyn EventManagerProxy>
        });
        let socket_pool = match self.socket_pool {
            Some(socket_pool) => socket_pool,
            None => ScheduledThreadPoolExecutor::new(0, 64)?,
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
