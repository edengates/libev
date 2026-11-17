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

    pub fn tcp_no_delay(&self) -> bool {
        self.tcp_no_delay
    }

    pub fn close_timeout(&self) -> Option<Duration> {
        self.close_timeout
    }

    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    pub fn afk_timeout(&self) -> Option<Duration> {
        self.afk_timeout
    }

    pub fn is_address_shared(&self) -> bool {
        self.is_address_shared
    }

    pub fn is_fallback_enabled(&self) -> bool {
        self.is_fallback_enabled
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}
