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

use crate::handle::{ChannelManager, BorrowedSocket};
use crate::handle::socket_packet::SocketPacket;
use std::io;

/// A handle that can be registered to an [`ChannelManager`].
///
/// **All** methods are called from the thread of the [`ChannelManager`] on which this handle was registered with
/// [`ChannelManager::register`].
///
/// Once a handle is registered, it is possible to submit operations related to the [`socket`](Self::socket), such
/// as:
/// - read
/// - write
/// - accept
/// - connect
///
/// These operations are the source of the [`SocketPacket`]s, which are dispatched to the handle via
/// [`handle`](Self::handle).
///
/// The methods which return [`io::Result<bool>`](io::Result) return `true` if the handle should stay registered,
/// `false` or an error if the handle should be closed. **A panic in [`handle`](Self::handle),
/// [`execute`](Self::execute), [`shutdown`](Self::shutdown) or [`check_expired`](Self::check_expired) also closes
/// the handle.**
pub trait SocketHandler {
    /// The socket of this handle, which is registered to the [`ChannelManager`].
    fn socket(&self) -> BorrowedSocket<'_>;

    /// Called once this handle was registered and so will start to receive events via [`handle`](Self::handle).
    ///
    /// This is the place to submit the first operations, for example to start to read or to connect to the remote
    /// peer.
    ///
    /// # Arguments
    ///
    /// - `manager` — The [`ChannelManager`] on which this handle is registered
    ///
    /// # Returns
    ///
    /// `true` if the handle should stay registered, `false` if the handle should be closed
    fn connect(&mut self, manager: &mut ChannelManager) -> io::Result<bool>;

    /// Called once there is something to handle.
    ///
    /// The given [`SocketPacket`] belongs to an operation which has completed for this handle, or is an error if the
    /// operation has failed.
    ///
    /// # Arguments
    ///
    /// - `manager` — The [`ChannelManager`] on which this handle is registered
    /// - `data` — The [`SocketPacket`] of the operation, or an error if the operation has failed
    ///
    /// # Returns
    ///
    /// `true` if the handle should stay registered, `false` if the handle should be closed
    fn handle(&mut self, manager: &mut ChannelManager, data: io::Result<SocketPacket>) -> io::Result<bool>;

    /// Called once a task of this handle, enqueued to the [`ChannelManager`], has completed.
    ///
    /// By default this does nothing.
    ///
    /// # Arguments
    ///
    /// - `_manager` — The [`ChannelManager`] on which this handle is registered
    ///
    /// # Returns
    ///
    /// `true` if the handle should stay registered, `false` if the handle should be closed
    fn execute(&mut self, _manager: &mut ChannelManager) -> io::Result<bool> {
        Ok(true)
    }

    /// Called by the [`ChannelManager`] when a shutdown has been requested with [`SocketManager::shutdown`].
    ///
    /// By default the handle is closed at once. A channel can return `true` to stay registered until its pending
    /// messages have been sent.
    ///
    /// # Arguments
    ///
    /// - `_manager` — The [`ChannelManager`] on which this handle is registered
    ///
    /// # Returns
    ///
    /// `true` if the handle should stay registered, `false` if the handle should be closed
    ///
    /// [`SocketManager::shutdown`]: crate::SocketManager::shutdown
    fn shutdown(&mut self, _manager: &mut ChannelManager) -> io::Result<bool> {
        Ok(false)
    }

    /// Called once the keep alive, set up with [`ChannelManager::setup_keep_alive`], has expired.
    ///
    /// This is used for the [connect timeout] and the [AFK-Timeout]. By default this does nothing.
    ///
    /// # Arguments
    ///
    /// - `_manager` — The [`ChannelManager`] on which this handle is registered
    ///
    /// # Returns
    ///
    /// `true` if the handle should stay registered, `false` if the handle should be closed
    ///
    /// [connect timeout]: crate::SocketManager::connect_timeout
    /// [AFK-Timeout]: crate::SocketManager::afk_timeout
    fn check_expired(&mut self, _manager: &mut ChannelManager) -> io::Result<bool> {
        Ok(true)
    }

    /// Called when this handle is no longer registered and will not receive any more events via
    /// [`handle`](Self::handle).
    ///
    /// All related resources should be cleaned up here, for example pending operations should be cancelled.
    ///
    /// # Arguments
    ///
    /// - `_manager` — The [`ChannelManager`] on which this handle is registered
    fn close(&mut self, _manager: &mut ChannelManager) {}
}
