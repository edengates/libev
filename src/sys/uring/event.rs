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

use bytes::BytesMut;

pub struct Event {
    opcode: u8,
    res: i32,
    buffer: Option<BytesMut>,
    more: bool,
}

impl Event {
    pub(crate) fn new(opcode: u8, res: i32, buffer: Option<BytesMut>, more: bool) -> Self {
        Self {
            opcode,
            res,
            buffer,
            more,
        }
    }

    pub(crate) fn more(&self) -> bool {
        self.more
    }

    pub(crate) fn opcode(&self) -> u8 {
        self.opcode
    }

    pub(crate) fn result(&self) -> i32 {
        self.res
    }

    pub(crate) fn into_body(self) -> Option<BytesMut> {
        self.buffer
    }
}
