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

use crate::handle::SocketQueueFactory;
use crate::handle::SocketQueueManager;

/// IO handler which uses I/O completion ports (IOCP) under the covers. Only works on Windows!
pub struct IocpHandler;

impl IocpHandler {
    /// Returns a new [`SocketQueueFactory`] that creates IOCP instances.
    pub fn new_factory() -> SocketQueueFactory {
        SocketQueueFactory::new(|_| SocketQueueManager::new())
    }
}
