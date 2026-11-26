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

use crate::handle::{BorrowedSocket, Completion};
use crate::utils::concurrent::ScheduledExecutorService;
use std::io;

/// A handle that can be registered to a [`ScheduledExecutorService`].
///
/// The executor calls the methods of a handler one at a time, on its own thread. A method that returns
/// `io::Result<bool>` tells the executor whether the handler stays registered: `Ok(true)` keeps it, `Ok(false)` or an
/// error closes it.
pub trait SocketHandler {
    /// The socket of this handler.
    fn socket(&self) -> BorrowedSocket<'_>;

    /// Called once, after the socket has been registered.
    fn on_registered(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool>;

    /// Called when an operation of this handler has completed.
    fn handle(&mut self, manager: &mut ScheduledExecutorService, completion: Completion) -> io::Result<bool>;

    /// Called when this handler may have work to do, for example after an action has been run on its channel.
    ///
    /// By default the handler stays registered.
    fn execute(&mut self, _manager: &mut ScheduledExecutorService) -> io::Result<bool> {
        Ok(true)
    }

    /// Called when the executor shuts down.
    ///
    /// By default the handler is closed.
    fn shutdown(&mut self, _manager: &mut ScheduledExecutorService) -> io::Result<bool> {
        Ok(false)
    }

    /// Called when the keep-alive that was set up with
    /// [`setup_keep_alive`](ScheduledExecutorService::setup_keep_alive) has elapsed.
    ///
    /// By default the handler stays registered.
    fn check_expired(&mut self, _manager: &mut ScheduledExecutorService) -> io::Result<bool> {
        Ok(true)
    }

    /// Called once, right before the handler is dropped.
    fn close(&mut self, _manager: &mut ScheduledExecutorService) {}
}
