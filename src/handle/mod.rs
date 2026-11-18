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

//! The IO handlers of each platform.
#[cfg_attr(target_os = "linux", path = "../unix/linux.rs")]
#[cfg_attr(target_os = "macos", path = "../unix/darwin.rs")]
#[cfg_attr(target_os = "freebsd", path = "../unix/freebsd.rs")]
#[cfg_attr(windows, path = "../win/mod.rs")]
mod platform;
pub(crate) use platform::*;
pub use platform::transport::*;
pub use platform::CompletionService;

#[cfg(any(windows, target_os = "linux"))]
mod socket_channel_mixin;
#[cfg(any(windows, target_os = "linux"))]
pub(crate) use socket_channel_mixin::SocketChannelMixin;
mod channel;
pub(crate) use channel::Channel;
