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

use crate::handle::kqueue::event::Event;
use crate::handle::CompletionService;
use crate::utils::concurrent::{ScheduledExecutorService, DISPATCH, SHUTDOWN};
use rustix::event::kqueue::{self, EventFilter};
use std::io;
use std::mem;
use std::os::fd::{AsRawFd, RawFd};

pub struct EventArray {
    pub(in crate::handle) entries: Vec<kqueue::Event>,
}

impl EventArray {
    pub(crate) fn new(_port: &CompletionService, capacity: usize) -> Self {
        Self {
            entries: vec![unsafe { mem::zeroed() }; capacity.max(1)],
        }
    }
}

impl ScheduledExecutorService {
    /// The number of operations that have been submitted and have not completed yet, always `0` as kqueue only
    /// reports the readiness of sockets.
    pub fn pending(&self) -> usize {
        0
    }

    /// Waits for events, at most until the next scheduled task is due, and passes them to their handlers.
    pub fn run_worker(&mut self) -> io::Result<()> {
        let mut entries = mem::take(&mut self.array.entries);
        let data = self.port.poll(&mut entries, self.queue.timeout());
        if let Ok(count) = data {
            for entry in &entries[..count] {
                self.handle(entry);
            }
        }
        self.array.entries = entries;
        data.map(|_| ())
    }

    /// Enables or disables the `filter` of `socket` for the handler that is being handled.
    pub fn submit<S: AsRawFd>(&mut self, socket: &S, filter: fn(RawFd) -> EventFilter, is_enabled: bool) -> io::Result<()> {
        self.port.modify(socket, self.socket_id, filter, is_enabled)
    }

    fn handle(&mut self, entry: &kqueue::Event) {
        match entry.udata() as usize {
            DISPATCH => {}
            SHUTDOWN => self.is_shutdown = true,
            key => {
                let packet = Event::new(entry.filter(), entry.flags());
                self.handle_event(key, |handle, manager| matches!(handle.handle(manager, Ok(packet)), Ok(true)));
            }
        }
    }

    /// Closes all handlers.
    pub fn close(&mut self) {
        self.close_sockets();
    }
}
