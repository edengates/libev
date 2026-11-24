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
    pub(crate) tcp_no_delay: bool,
    pub(crate) is_keep_alive_enabled: bool,
    pub(crate) keep_alive: Option<Duration>,
    pub(crate) close_timeout: Option<Duration>,
    pub(crate) connect_timeout: Duration,
    pub(crate) afk_timeout: Option<Duration>,
    pub(crate) is_address_shared: bool,
    pub(crate) is_fallback_enabled: bool,
    pub(crate) timeout: Duration,
}

impl SocketConfig {
    pub fn tcp_no_delay(&self) -> bool {
        self.tcp_no_delay
    }

    pub fn is_keep_alive_enabled(&self) -> bool {
        self.is_keep_alive_enabled
    }

    pub fn keep_alive(&self) -> Option<Duration> {
        self.keep_alive
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
