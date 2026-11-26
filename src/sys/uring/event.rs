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

use crate::handle::Request;
use bytes::BytesMut;

pub struct Event {
    opcode: u8,
    res: i32,
    buffer: Option<BytesMut>,
    more: bool,
    request: Option<Box<dyn Request>>,
}

impl Event {
    pub(crate) fn new(opcode: u8, res: i32, buffer: Option<BytesMut>, more: bool) -> Self {
        Self {
            opcode,
            res,
            buffer,
            more,
            request: None,
        }
    }

    pub(crate) fn with_request(mut self, request: Option<Box<dyn Request>>) -> Self {
        self.request = request;
        self
    }

    /// Returns the request that was submitted for the operation, if it is an `R`.
    pub fn into_request<R: Request>(self) -> Option<Box<R>> {
        self.request?.into_any().downcast::<R>().ok()
    }

    /// Whether more completions follow for the same operation.
    pub fn more(&self) -> bool {
        self.more
    }

    /// The code of the operation.
    pub fn opcode(&self) -> u8 {
        self.opcode
    }

    /// The result of the operation.
    pub fn result(&self) -> i32 {
        self.res
    }

    /// Returns the buffer that the operation has filled, if it had one.
    pub fn into_body(self) -> Option<BytesMut> {
        self.buffer
    }
}
