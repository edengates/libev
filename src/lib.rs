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

//! An asynchronous event-driven network application framework for rapid development of maintainable high performance
//! protocol servers & clients.
//!
//! You can use [`DefaultSocketManagerBuilder`] to create a [`SocketManager`] instance.
//! Each [`SocketManager`] instance manages the sockets and connections to receive [events].
pub mod entities;
pub mod events;
pub mod handle;
pub mod hooks;
pub mod requests;
pub mod utils;

mod default_socket_manager_builder;
pub use default_socket_manager_builder::DefaultSocketManagerBuilder;
mod socket_manager;
pub use socket_manager::SocketManager;
