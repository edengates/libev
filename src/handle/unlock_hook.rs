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

use crate::handle::{DISPATCH, SocketQueueManager};
use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::task::Wake;

pub struct UnlockHook {
    response_number: usize,
    sender: Sender<usize>,
    port: Arc<SocketQueueManager>,
}

impl UnlockHook {
    pub(crate) fn new(response_number: usize, sender: Sender<usize>, port: Arc<SocketQueueManager>) -> Self {
        Self {
            response_number,
            sender,
            port,
        }
    }

    pub(crate) fn set_response_number(&mut self, response_number: usize) {
        self.response_number = response_number;
    }
}

impl Wake for UnlockHook {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.sender.send(self.response_number).is_ok() {
            self.port.enqueue(DISPATCH).ok();
        }
    }
}
