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

use std::time::Duration;

/// Holds the socket configuration of a [`SocketManager`](crate::SocketManager), which is set with the
/// [`DefaultSocketManagerBuilder`](crate::DefaultSocketManagerBuilder).
#[derive(Clone)]
pub struct SocketConfig {
    tcp_no_delay: bool,
    close_timeout: Option<Duration>,
    connect_timeout: Duration,
    afk_timeout: Option<Duration>,
    is_address_shared: bool,
    is_fallback_enabled: bool,
    timeout: Duration,
}

impl SocketConfig {
    /// Creates a new instance with the provided configuration.
    ///
    /// # Arguments
    ///
    /// - `tcp_no_delay` — Whether `TCP_NODELAY` is enabled
    /// - `close_timeout` — The timeout of `SO_LINGER`, or `None` if disabled
    /// - `connect_timeout` — The maximum amount of time to wait until a connection is established
    /// - `afk_timeout` — The AFK-Timeout of the channels, or `None` if disabled
    /// - `is_address_shared` — Whether `SO_REUSEPORT` is enabled for each bound socket
    /// - `is_fallback_enabled` — Whether IPv4 connections are enabled on each IPv6 socket
    /// - `timeout` — The maximum amount of time that the listeners of a closed channel may still run
    pub fn new(
        tcp_no_delay: bool,
        close_timeout: Option<Duration>,
        connect_timeout: Duration,
        afk_timeout: Option<Duration>,
        is_address_shared: bool,
        is_fallback_enabled: bool,
        timeout: Duration,
    ) -> Self {
        Self {
            tcp_no_delay,
            close_timeout,
            connect_timeout,
            afk_timeout,
            is_address_shared,
            is_fallback_enabled,
            timeout,
        }
    }

    /// Whether `TCP_NODELAY` is enabled.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_tcp_no_delay`](crate::DefaultSocketManagerBuilder::set_tcp_no_delay)
    /// - [`SocketManager::tcp_no_delay`](crate::SocketManager::tcp_no_delay)
    pub fn tcp_no_delay(&self) -> bool {
        self.tcp_no_delay
    }

    /// The timeout of `SO_LINGER`, or `None` if disabled.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_close_timeout`](crate::DefaultSocketManagerBuilder::set_close_timeout)
    /// - [`SocketManager::close_timeout`](crate::SocketManager::close_timeout)
    pub fn close_timeout(&self) -> Option<Duration> {
        self.close_timeout
    }

    /// The maximum amount of time to wait until a connection is established.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_connect_timeout`](crate::DefaultSocketManagerBuilder::set_connect_timeout)
    /// - [`SocketManager::connect_timeout`](crate::SocketManager::connect_timeout)
    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// The AFK-Timeout of the channels, or `None` if disabled.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_afk_timeout`](crate::DefaultSocketManagerBuilder::set_afk_timeout)
    /// - [`SocketManager::afk_timeout`](crate::SocketManager::afk_timeout)
    pub fn afk_timeout(&self) -> Option<Duration> {
        self.afk_timeout
    }

    /// Whether `SO_REUSEPORT` is enabled for each bound socket.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_address_shared`](crate::DefaultSocketManagerBuilder::set_address_shared)
    /// - [`SocketManager::is_address_shared`](crate::SocketManager::is_address_shared)
    pub fn is_address_shared(&self) -> bool {
        self.is_address_shared
    }

    /// Whether IPv4 connections are enabled on each IPv6 socket.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_fallback_enabled`]
    /// - [`SocketManager::is_fallback_enabled`](crate::SocketManager::is_fallback_enabled)
    ///
    /// [`DefaultSocketManagerBuilder::set_fallback_enabled`]: crate::DefaultSocketManagerBuilder::set_fallback_enabled
    pub fn is_fallback_enabled(&self) -> bool {
        self.is_fallback_enabled
    }

    /// The maximum amount of time that the listeners of a closed channel may still run.
    ///
    /// # See also
    ///
    /// - [`DefaultSocketManagerBuilder::set_timeout`](crate::DefaultSocketManagerBuilder::set_timeout)
    /// - [`SocketManager::timeout`](crate::SocketManager::timeout)
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}
