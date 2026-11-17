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
use crate::utils::concurrent::SingleThreadManager;
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
pub(crate) use std::os::windows::io::BorrowedSocket;
use std::os::windows::io::AsSocket;
use std::sync::mpsc::Receiver;

pub mod socket;
pub mod socket_packet;
mod socket_queue_manager;
pub use socket_queue_manager::SocketQueueManager;
mod channel_manager;
pub use channel_manager::ChannelState;
mod socket_channel;
pub use socket_channel::SocketChannel;
mod session_controller;
pub use session_controller::SessionController;
mod iocp_handler;

pub mod transport {
    pub use super::iocp_handler::IocpHandler;
}

/// The default IO handler on Windows.
///
/// This handler uses IOCP, see [`IocpHandler`](iocp_handler::IocpHandler).
pub(crate) type DefaultHandler = iocp_handler::IocpHandler;

/// Returns the socket of the provided connection.
///
/// This socket is registered with the [`ChannelManager`](crate::handle::ChannelManager) as the
/// [`socket`](crate::handle::SocketHandler::socket) of a channel.
///
/// # Arguments
///
/// - `stream` — The connection
pub(crate) fn as_socket(stream: &TcpStream) -> BorrowedSocket<'_> {
    stream.as_socket()
}

/// Registers the [`SessionController`] of the provided socket with the provided thread manager.
///
/// The returned [`Receiver`] gets the result once the [`SessionController`] has been registered, which also starts to
/// accept connections. The accepted connections are registered with the [worker pool](SocketManager::worker_pool).
///
/// # Arguments
///
/// - `pool` — The thread manager to use
/// - `listener` — The bound socket
/// - `socket_manager` — The [`SocketManager`] to use
///
/// # Returns
///
/// The [`Receiver`] of the result.
///
/// # See also
///
/// - [`connect`]
/// - [`SessionController`]
pub(crate) fn bind(pool: &SingleThreadManager, listener: TcpListener, socket_manager: SocketManager) -> Receiver<io::Result<()>> {
    pool.register(move || Box::new(SessionController::new(listener, socket_manager)))
}

/// Registers a new [`SocketChannel`] with the provided thread manager, which connects to the provided address.
///
/// The returned [`Receiver`] gets the result once the [`SocketChannel`] has been registered and has started to
/// connect, or the error if it could not start to connect. The connection is established later, or the channel is
/// closed once the [connect timeout](SocketManager::connect_timeout) has expired.
///
/// # Arguments
///
/// - `pool` — The thread manager to use
/// - `socket` — The new socket, see [`socket::client`]
/// - `address` — The address to connect to
/// - `socket_manager` — The [`SocketManager`] to use
///
/// # Returns
///
/// The [`Receiver`] of the result.
///
/// # See also
///
/// - [`bind`]
/// - [`SocketChannel::connect`]
pub(crate) fn connect(
    pool: &SingleThreadManager,
    socket: TcpStream,
    address: SocketAddr,
    socket_manager: SocketManager,
) -> Receiver<io::Result<()>> {
    pool.register(move || Box::new(SocketChannel::connect(socket, address, socket_manager)))
}
