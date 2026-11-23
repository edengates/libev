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

use crate::entities::channel::concrete::ThreadChannel;
use crate::utils::concurrent::ScheduledThreadPoolExecutor;
use crate::utils::{EventQueue, EventManagerProvider, EventManagerProxy};
use crate::handle::{self, socket};
use crate::utils::Buffer;
use crate::utils::cache::ChannelCacheView;
use crate::utils::config::{SocketConfig, ThreadingConfig};
use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::rc::Rc;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// The core of libev. Acts as a manager for all channels. It binds sockets with [`bind`](Self::bind) and connects to
/// peers with [`connect`](Self::connect).
///
/// To create new instances use the [`DefaultSocketManagerBuilder`](crate::DefaultSocketManagerBuilder).
#[derive(Clone)]
pub struct SocketManager {
    inner: Arc<SocketManagerImpl>,
}

struct SocketManagerImpl {
    threading_config: ThreadingConfig,
    socket_config: SocketConfig,
    provider: EventManagerProvider,
    context_map: RwLock<HashMap<String, Arc<dyn Any + Send + Sync>>>,
    channel_cache: ChannelCacheView,
}

impl SocketManager {
    pub(crate) fn new(
        threading_config: ThreadingConfig,
        socket_config: SocketConfig,
        provider: EventManagerProvider,
    ) -> Self {
        Self {
            inner: Arc::new(SocketManagerImpl {
                threading_config,
                socket_config,
                provider,
                context_map: RwLock::default(),
                channel_cache: ChannelCacheView::default(),
            }),
        }
    }

    /// The pool used for the channels created by [`connect`](Self::connect) and for the accepted connections.
    pub fn worker_pool(&self) -> &ScheduledThreadPoolExecutor {
        self.inner.threading_config.worker_pool()
    }

    /// The pool used for the sockets bound by [`bind`](Self::bind).
    pub fn socket_pool(&self) -> &ScheduledThreadPoolExecutor {
        self.inner.threading_config.socket_pool()
    }

    /// The `listen(2)` backlog of the sockets bound by [`bind`](Self::bind).
    pub fn backlog(&self) -> i32 {
        self.inner.threading_config.backlog()
    }

    /// The maximum number of connections to accept per read loop.
    pub fn concurrency(&self) -> usize {
        self.inner.threading_config.concurrency()
    }

    /// The read buffer size in bytes.
    pub fn buffer_size(&self) -> usize {
        self.inner.threading_config.buffer_size()
    }

    /// How long libev stops accepting after the first error.
    pub fn reconnect_delay(&self) -> Duration {
        self.inner.threading_config.reconnect_delay()
    }

    /// The upper limit of the [`reconnect_delay`](Self::reconnect_delay).
    pub fn max_reconnect_delay(&self) -> Duration {
        self.inner.threading_config.max_reconnect_delay()
    }

    /// The low water mark of the write buffer, where slow mode ends.
    pub fn threshold(&self) -> usize {
        self.inner.threading_config.threshold()
    }

    /// The high water mark of the write buffer, where slow mode starts.
    pub fn large_threshold(&self) -> usize {
        self.inner.threading_config.large_threshold()
    }

    /// How long [`shutdown`](Self::shutdown) and a closed channel wait for pending messages.
    pub fn shutdown_timeout(&self) -> Duration {
        self.inner.threading_config.shutdown_timeout()
    }

    /// The maximum number of chunks to read per read loop.
    pub fn chunk_limit(&self) -> usize {
        self.inner.threading_config.chunk_limit()
    }

    /// The maximum number of messages per gathering write.
    pub fn packet_limit(&self) -> usize {
        self.inner.threading_config.packet_limit()
    }

    /// Whether the system may suppress the power of the pool threads to save energy.
    pub fn is_power_suppressed_enabled(&self) -> bool {
        self.inner.threading_config.is_power_suppressed_enabled()
    }

    /// Gets the value of the `TCP_NODELAY` option used for each connection.
    pub fn tcp_no_delay(&self) -> bool {
        self.inner.socket_config.tcp_no_delay()
    }

    /// The `SO_LINGER` time, or `None` if disabled.
    pub fn close_timeout(&self) -> Option<Duration> {
        self.inner.socket_config.close_timeout()
    }

    /// The connect timeout of [`connect`](Self::connect).
    pub fn connect_timeout(&self) -> Duration {
        self.inner.socket_config.connect_timeout()
    }

    /// The AFK timeout of each channel, or `None` if disabled.
    pub fn afk_timeout(&self) -> Option<Duration> {
        self.inner.socket_config.afk_timeout()
    }

    /// Whether `SO_REUSEPORT` is set on the sockets bound by [`bind`](Self::bind).
    pub fn is_address_shared(&self) -> bool {
        self.inner.socket_config.is_address_shared()
    }

    /// Whether the IPv6 sockets bound by [`bind`](Self::bind) also accept IPv4.
    pub fn is_fallback_enabled(&self) -> bool {
        self.inner.socket_config.is_fallback_enabled()
    }

    /// How long the listeners of a closed channel may still run.
    pub fn timeout(&self) -> Duration {
        self.inner.socket_config.timeout()
    }

    /// The data mappings to use in this [`SocketManager`], shared by all its channels.
    pub fn context_map(&self) -> &RwLock<HashMap<String, Arc<dyn Any + Send + Sync>>> {
        &self.inner.context_map
    }

    /// [`ChannelCacheView`] of all cached channels of this [`SocketManager`].
    pub fn channel_cache(&self) -> &ChannelCacheView {
        &self.inner.channel_cache
    }

    /// Gets all channels of this [`SocketManager`].
    ///
    /// This copies the backing store into a list. This means every call creates a new list with O(n) complexity. It is
    /// recommended to store this into a local variable or use [`channel_cache`](Self::channel_cache) and use its more
    /// efficient versions of handling these values.
    pub fn channels<N: 'static, P: 'static>(&self) -> Vec<ThreadChannel<N, P>> {
        self.inner.channel_cache.to_vec()
    }

    /// Gets a channel that has the same id as the one provided.
    ///
    /// If there is no channel with an id that matches the provided one, then this returns `None`.
    pub fn get_channel_by_id<N: 'static, P: 'static>(&self, id: u64) -> Option<ThreadChannel<N, P>> {
        self.inner.channel_cache.get_element_by_id(id)
    }

    pub(crate) fn proxy(
        &self,
        buffer: &Rc<RefCell<Buffer>>,
        thread_channel: &ThreadChannel,
        inbound: &EventQueue,
    ) -> Rc<dyn EventManagerProxy> {
        (self.inner.provider)(self, buffer, thread_channel, inbound)
    }

    /// Creates a new socket, which will be bound to the specified address, and returns its local address.
    ///
    /// Binding with a port number of 0 will request that the OS assigns a port to this socket, the returned address
    /// has it. Only the first address yielded by `address` is used. The socket is handled by the
    /// [`socket_pool`](Self::socket_pool), the accepted connections by the [`worker_pool`](Self::worker_pool).
    ///
    /// # Errors
    ///
    /// Returns an error if the address could not be resolved or the socket could not be bound.
    pub fn bind<A: ToSocketAddrs>(&self, address: A) -> io::Result<SocketAddr> {
        let address = address
            .to_socket_addrs()?
            .next()
            .ok_or(io::ErrorKind::InvalidInput)?;
        let listener = socket::listener(
            &address,
            self.backlog(),
            self.is_address_shared(),
            self.is_fallback_enabled(),
        )?;
        let address = listener.local_addr()?;
        let socket_manager = self.clone();
        let pool = self.socket_pool().provide();
        sync(handle::bind(pool, listener, socket_manager))?;
        Ok(address)
    }

    /// Connects a new channel to the peer.
    ///
    /// Returns once the connection process has started. When the connection is established a [`ChannelReadyEvent`]
    /// is fired, if not within the [`connect_timeout`](Self::connect_timeout) the channel is closed.
    ///
    /// # Errors
    ///
    /// Returns an error if the address could not be resolved or the connection process could not be started.
    ///
    /// [`ChannelReadyEvent`]: crate::events::channel::ChannelReadyEvent
    pub fn connect<A: ToSocketAddrs>(&self, address: A) -> io::Result<()> {
        let address = address
            .to_socket_addrs()?
            .next()
            .ok_or(io::ErrorKind::InvalidInput)?;
        let socket = socket::client(&address)?;
        let socket_manager = self.clone();
        let pool = self.worker_pool().provide();
        sync(handle::connect(pool, socket, address, socket_manager))
    }

    /// Shuts down this [`SocketManager`], closing all its connections.
    ///
    /// After this command is issued the [`SocketManager`] can not be used anymore. Already enqueued messages are still
    /// going to be sent, for at most the [`shutdown_timeout`](Self::shutdown_timeout). If you want this instance to
    /// shut down without sending, use [`shutdown_now`](Self::shutdown_now).
    pub fn shutdown(&self) {
        self.inner.threading_config.shutdown()
    }

    /// Shuts down this [`SocketManager`] instantly, closing all its connections.
    ///
    /// This will also cancel all queued messages. If you want this instance to shut down without canceling enqueued
    /// messages, use [`shutdown`](Self::shutdown).
    ///
    /// # Errors
    ///
    /// Returns an error if one of the shards could not be shut down.
    pub fn shutdown_now(&self) -> io::Result<()> {
        self.inner.threading_config.shutdown_now()
    }

    /// Blocks the current thread until the shutdown has completed.
    ///
    /// This will not implicitly call [`shutdown`](Self::shutdown), you are responsible to ensure that the shutdown
    /// process has started. Returns `false` if the `timeout` elapsed first.
    pub fn await_shutdown(&self, timeout: Duration) -> bool {
        self.inner.threading_config.await_shutdown(timeout)
    }
}

fn sync(data: Receiver<io::Result<()>>) -> io::Result<()> {
    data.recv().map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?
}
