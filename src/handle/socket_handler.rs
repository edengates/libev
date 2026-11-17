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

/// A handle that can be registered to a [`ChannelManager`].
pub trait SocketHandler {
    fn socket(&self) -> BorrowedSocket<'_>;

    fn connect(&mut self, manager: &mut ChannelManager) -> io::Result<bool>;

    fn handle(&mut self, manager: &mut ChannelManager, data: io::Result<SocketPacket>) -> io::Result<bool>;

    fn execute(&mut self, _manager: &mut ChannelManager) -> io::Result<bool> {
        Ok(true)
    }

    fn shutdown(&mut self, _manager: &mut ChannelManager) -> io::Result<bool> {
        Ok(false)
    }

    fn check_expired(&mut self, _manager: &mut ChannelManager) -> io::Result<bool> {
        Ok(true)
    }

    fn close(&mut self, _manager: &mut ChannelManager) {}
}
