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
use crate::hooks::{EventQueue, EventManagerProvider, EventTask};
use crate::handle::{self, socket};
use crate::utils::SocketBufferQueue;
use crate::utils::config::{SocketConfig, ThreadingConfig};
use bytes::BytesMut;
use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::rc::Rc;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// The core of libev. Acts as a manager for all channels: it binds sockets with [`bind`](Self::bind) and connects to
/// remote peers with [`connect`](Self::connect).
///
/// **Note:** Cloning is cheap, **all** clones share the same channels and thread managers.
///
/// To create new instances use the [`DefaultSocketManagerBuilder`](crate::DefaultSocketManagerBuilder).
#[derive(Clone)]
pub struct SocketManager {
    inner: Arc<SocketManagerImpl>,
}

/// Holds the configuration of a [`SocketManager`], all copies of it use the same configuration.
struct SocketManagerImpl {
    threading_config: ThreadingConfig,
    socket_config: SocketConfig,
    provider: EventManagerProvider,
    context_map: RwLock<HashMap<String, Arc<dyn Any + Send + Sync>>>,
}

impl SocketManager {
    /// Creates a new instance with the provided configuration.
    ///
    /// # Arguments
    ///
    /// - `threading_config` — The [`ThreadingConfig`] to use
    /// - `socket_config` — The [`SocketConfig`] to use
    /// - `provider` — The [`EventManagerProvider`] used to create the [`EventTask`] of each channel
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
            }),
        }
    }

    /// [`ScheduledThreadManager`] used for the channels created by [`connect`](Self::connect) and the accepted
    /// connections.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_worker_pool`](crate::DefaultSocketManagerBuilder::set_worker_pool)
    pub fn worker_pool(&self) -> &ScheduledThreadManager<SingleThreadManager> {
        self.inner.threading_config.worker_pool()
    }

    /// [`ScheduledThreadManager`] used for the sockets bound by [`bind`](Self::bind).
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_socket_pool`](crate::DefaultSocketManagerBuilder::set_socket_pool)
    pub fn socket_pool(&self) -> &ScheduledThreadManager<SingleThreadManager> {
        self.inner.threading_config.socket_pool()
    }

    /// The backlog value used when [`bind`](Self::bind) binds to a local address.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_backlog`](crate::DefaultSocketManagerBuilder::set_backlog)
    pub fn backlog(&self) -> i32 {
        self.inner.threading_config.backlog()
    }

    /// The maximum number of connections to accept per read loop.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_concurrency`](crate::DefaultSocketManagerBuilder::set_concurrency)
    pub fn concurrency(&self) -> usize {
        self.inner.threading_config.concurrency()
    }

    /// The size, in bytes, of the buffer used for reading messages.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_buffer_size`](crate::DefaultSocketManagerBuilder::set_buffer_size)
    pub fn buffer_size(&self) -> usize {
        self.inner.threading_config.buffer_size()
    }

    /// The amount of time that libev will stop to accept new connections after an error.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_reconnect_delay`](crate::DefaultSocketManagerBuilder::set_reconnect_delay)
    pub fn reconnect_delay(&self) -> Duration {
        self.inner.threading_config.reconnect_delay()
    }

    /// The maximum amount of time that libev will back off to wait when attempting to accept new connections again.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_max_reconnect_delay`][set_max_reconnect_delay]
    ///
    /// [set_max_reconnect_delay]: crate::DefaultSocketManagerBuilder::set_max_reconnect_delay
    pub fn max_reconnect_delay(&self) -> Duration {
        self.inner.threading_config.max_reconnect_delay()
    }

    /// The low water mark for the write buffer of each channel.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_threshold`](crate::DefaultSocketManagerBuilder::set_threshold)
    pub fn threshold(&self) -> usize {
        self.inner.threading_config.threshold()
    }

    /// The high water mark for the write buffer of each channel.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_large_threshold`](crate::DefaultSocketManagerBuilder::set_large_threshold)
    pub fn large_threshold(&self) -> usize {
        self.inner.threading_config.large_threshold()
    }

    /// The maximum amount of time to wait until the thread managers are shut down on [`shutdown`](Self::shutdown).
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_shutdown_timeout`]
    ///
    /// [`DefaultSocketManagerBuilder::set_shutdown_timeout`]: crate::DefaultSocketManagerBuilder::set_shutdown_timeout
    pub fn shutdown_timeout(&self) -> Duration {
        self.inner.threading_config.shutdown_timeout()
    }

    /// The maximum number of chunks to read per read loop.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_chunk_limit`](crate::DefaultSocketManagerBuilder::set_chunk_limit)
    pub fn chunk_limit(&self) -> usize {
        self.inner.threading_config.chunk_limit()
    }

    /// The maximum number of messages to write at once.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_packet_limit`](crate::DefaultSocketManagerBuilder::set_packet_limit)
    pub fn packet_limit(&self) -> usize {
        self.inner.threading_config.packet_limit()
    }

    /// Whether `TCP_NODELAY` is enabled (Nagle's algorithm is disabled).
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_tcp_no_delay`](crate::DefaultSocketManagerBuilder::set_tcp_no_delay)
    pub fn tcp_no_delay(&self) -> bool {
        self.inner.socket_config.tcp_no_delay()
    }

    /// The linger time used with `SO_LINGER`, or `None` if disabled.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_close_timeout`](crate::DefaultSocketManagerBuilder::set_close_timeout)
    pub fn close_timeout(&self) -> Option<Duration> {
        self.inner.socket_config.close_timeout()
    }

    /// The connect timeout of the channels created by [`connect`](Self::connect).
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_connect_timeout`](crate::DefaultSocketManagerBuilder::set_connect_timeout)
    pub fn connect_timeout(&self) -> Duration {
        self.inner.socket_config.connect_timeout()
    }

    /// The amount of time without read or write after which a [`ChannelTimeoutEvent`] is fired, or `None` if disabled.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_afk_timeout`](crate::DefaultSocketManagerBuilder::set_afk_timeout)
    ///
    /// [`ChannelTimeoutEvent`]: crate::events::channel::ChannelTimeoutEvent
    pub fn afk_timeout(&self) -> Option<Duration> {
        self.inner.socket_config.afk_timeout()
    }

    /// Whether `SO_REUSEPORT` is enabled for the sockets bound by [`bind`](Self::bind).
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_address_shared`](crate::DefaultSocketManagerBuilder::set_address_shared)
    pub fn is_address_shared(&self) -> bool {
        self.inner.socket_config.is_address_shared()
    }

    /// Whether IPv4 connections are enabled on IPv6 sockets bound by [`bind`](Self::bind).
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_fallback_enabled`]
    ///
    /// [`DefaultSocketManagerBuilder::set_fallback_enabled`]: crate::DefaultSocketManagerBuilder::set_fallback_enabled
    pub fn is_fallback_enabled(&self) -> bool {
        self.inner.socket_config.is_fallback_enabled()
    }

    /// The maximum amount of time that the listeners of a closed channel may still run.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_timeout`](crate::DefaultSocketManagerBuilder::set_timeout)
    pub fn timeout(&self) -> Duration {
        self.inner.socket_config.timeout()
    }

    /// The data mappings to use in this [`SocketManager`].
    ///
    /// The same map is shared by **all** channels of this [`SocketManager`].
    pub fn context_map(&self) -> &RwLock<HashMap<String, Arc<dyn Any + Send + Sync>>> {
        &self.inner.context_map
    }

    /// Creates the [`EventTask`] of a new channel with the provided queue of messages and queue of events.
    ///
    /// # Arguments
    ///
    /// - `buffer` — The queue of messages, which have to be sent to the socket
    /// - `inbound` — The queue of events
    pub(crate) fn task(
        &self,
        buffer: &Rc<RefCell<SocketBufferQueue>>,
        inbound: &EventQueue<BytesMut>,
    ) -> Rc<dyn EventTask> {
        (self.inner.provider)(self, buffer, inbound)
    }

    /// Creates a new socket and binds it to the provided address.
    ///
    /// The socket is handled by the [`socket_pool`](Self::socket_pool), the accepted connections by the
    /// [`worker_pool`](Self::worker_pool).
    ///
    /// # Arguments
    ///
    /// - `address` — The local address to bind to
    ///
    /// # Errors
    ///
    /// Returns an error:
    /// - If the address could not be resolved
    /// - If the socket could not be bound
    ///
    /// # Returns
    ///
    /// The local address of the socket, which is useful when binding to port `0`.
    ///
    /// # See also
    ///
    /// - [`connect`](Self::connect)
    /// - [`DefaultSocketManagerBuilder::set_socket_pool`](crate::DefaultSocketManagerBuilder::set_socket_pool)
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

    /// Connects a new channel to the remote peer.
    ///
    /// **This returns once the connection process has started.** When the connection is established a
    /// [`ChannelReadyEvent`] is fired, if it is **not** established in the [`connect_timeout`](Self::connect_timeout)
    /// the channel is closed.
    ///
    /// # Arguments
    ///
    /// - `address` — The address of the remote peer
    ///
    /// # Errors
    ///
    /// Returns an error:
    /// - If the address could not be resolved
    /// - If the connection process could not be started
    ///
    /// # See also
    ///
    /// - [`bind`](Self::bind)
    /// - [`ChannelReadyEvent`]
    /// - [`DefaultSocketManagerBuilder::set_connect_timeout`](crate::DefaultSocketManagerBuilder::set_connect_timeout)
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
    /// <div class="warning">
    ///
    /// After this command is issued the [`SocketManager`] can **not** be used anymore.
    ///
    /// </div>
    ///
    /// Already enqueued messages are still going to be sent, until the [`shutdown_timeout`](Self::shutdown_timeout) has
    /// elapsed.
    ///
    /// If you want this instance to shutdown without sending, use [`shutdown_now`](Self::shutdown_now).
    ///
    /// # See also
    ///
    /// - [`shutdown_now`](Self::shutdown_now)
    /// - [`await_shutdown`](Self::await_shutdown)
    pub fn shutdown(&self) {
        self.inner.threading_config.shutdown()
    }

    /// Shuts down this [`SocketManager`] instantly, closing all its connections.
    ///
    /// <div class="warning">
    ///
    /// After this command is issued the [`SocketManager`] can **not** be used anymore.
    ///
    /// </div>
    ///
    /// This will also cancel **all** queued messages.
    ///
    /// If you want this instance to shutdown without cancelling enqueued messages, use [`shutdown`](Self::shutdown).
    ///
    /// # Errors
    ///
    /// Returns an error if one of the shards could not be shut down.
    ///
    /// # See also
    ///
    /// - [`shutdown`](Self::shutdown)
    /// - [`await_shutdown`](Self::await_shutdown)
    pub fn shutdown_now(&self) -> io::Result<()> {
        self.inner.threading_config.shutdown_now()
    }

    /// Blocks the current thread until the shutdown has completed.
    ///
    /// **This will not implicitly call [`shutdown`](Self::shutdown), you are responsible to ensure that the
    /// shutdown process has started.**
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
    /// - [`shutdown`](Self::shutdown)
    /// - [`shutdown_now`](Self::shutdown_now)
    pub fn await_shutdown(&self, timeout: Duration) -> bool {
        self.inner.threading_config.await_shutdown(timeout)
    }
}

/// Blocks the current thread until the provided operation has completed.
///
/// # Arguments
///
/// - `data` — The [`Receiver`], which receives the result once the operation completes
///
/// # Errors
///
/// Returns an error:
/// - The error of the operation
/// - [`BrokenPipe`](io::ErrorKind::BrokenPipe) if the thread manager has been shutdown
fn sync(data: Receiver<io::Result<()>>) -> io::Result<()> {
    data.recv().map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?
}
