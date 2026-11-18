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

use rustix::event::kqueue::{EventFilter, EventFlags};

pub struct Event {
    filter: EventFilter,
    flags: EventFlags,
}

impl Event {
    pub(crate) fn new(filter: EventFilter, flags: EventFlags) -> Self {
        Self { filter, flags }
    }

    pub(crate) fn is_readable(&self) -> bool {
        matches!(self.filter, EventFilter::Read(_)) || self.flags.contains(EventFlags::EOF)
    }

    pub(crate) fn is_writable(&self) -> bool {
        matches!(self.filter, EventFilter::Write(_))
    }
}
