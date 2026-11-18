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
use crate::utils::concurrent::ExecutorCompletionService;
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
pub(crate) use std::os::windows::io::BorrowedSocket;
use std::os::windows::io::AsSocket;
use std::sync::mpsc::Receiver;

pub mod socket;
pub mod event;
mod completion_service;
pub use completion_service::CompletionService;
mod scheduled_executor_service;
pub use scheduled_executor_service::EventArray;
mod socket_channel;
pub use socket_channel::SocketChannel;
mod server_socket_channel;
pub use server_socket_channel::ServerSocketChannel;
mod iocp_handler;

pub mod transport {
    pub use super::iocp_handler::IocpHandler;

    pub type DefaultHandler = IocpHandler;
}

pub(crate) fn as_socket(stream: &TcpStream) -> BorrowedSocket<'_> {
    stream.as_socket()
}

pub(crate) fn bind(pool: &ExecutorCompletionService, listener: TcpListener, socket_manager: SocketManager) -> Receiver<io::Result<()>> {
    pool.register(move || Box::new(ServerSocketChannel::new(listener, socket_manager)))
}

pub(crate) fn connect(
    pool: &ExecutorCompletionService,
    socket: TcpStream,
    address: SocketAddr,
    socket_manager: SocketManager,
) -> Receiver<io::Result<()>> {
    pool.register(move || Box::new(SocketChannel::connect(socket, address, socket_manager)))
}
