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
use std::{io, mem};

#[derive(Default)]
pub struct SocketBufferQueue {
    queued: Vec<BytesMut>,
    ready: Vec<BytesMut>,
    shutdown: bool,
    queue_size: usize,
    slowmode: bool,
    slowmode_changed: bool,
    deafened: bool,
}

impl SocketBufferQueue {
    fn update(&mut self, threshold: usize, large_threshold: usize) {
        let slowmode = if self.slowmode {
            self.queue_size >= threshold
        } else {
            self.queue_size > large_threshold
        };
        self.slowmode_changed |= slowmode != self.slowmode;
        self.slowmode = slowmode;
    }

    pub fn enqueue(&mut self, message: BytesMut, threshold: usize, large_threshold: usize) -> io::Result<()> {
        if self.shutdown {
            return Err(io::ErrorKind::NotConnected.into());
        }
        self.queue_size += message.len();
        self.update(threshold, large_threshold);
        self.queued.push(message);
        Ok(())
    }

    pub fn handle_response(&mut self, size: usize, threshold: usize, large_threshold: usize) {
        self.queue_size -= size;
        self.update(threshold, large_threshold);
    }

    pub fn execute(&mut self) {
        if self.ready.is_empty() {
            mem::swap(&mut self.ready, &mut self.queued);
        } else {
            self.ready.append(&mut self.queued);
        }
    }

    pub fn next_packet(&mut self, limit: usize) -> Vec<BytesMut> {
        if self.ready.len() <= limit {
            mem::take(&mut self.ready)
        } else {
            let rest = self.ready.split_off(limit);
            mem::replace(&mut self.ready, rest)
        }
    }

    pub fn recycle(&mut self, mut packet: Vec<BytesMut>) {
        if self.queued.capacity() == 0 {
            packet.clear();
            self.queued = packet;
        }
    }

    pub fn take_slowmode_change(&mut self) -> bool {
        mem::take(&mut self.slowmode_changed)
    }

    pub fn is_slowmode(&self) -> bool {
        self.slowmode
    }

    pub fn shutdown(&mut self) {
        self.shutdown = true;
    }

    pub fn is_shutdown(&self) -> bool {
        self.shutdown
    }

    pub fn set_deafened(&mut self, deafened: bool) {
        self.deafened = deafened;
    }

    pub fn is_deafened(&self) -> bool {
        self.deafened
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slowmode() {
        let mut buffer = SocketBufferQueue::default();
        buffer.enqueue(BytesMut::zeroed(40 * 1024), 32 * 1024, 64 * 1024).unwrap();
        assert!(!buffer.is_slowmode());
        buffer.enqueue(BytesMut::zeroed(40 * 1024), 32 * 1024, 64 * 1024).unwrap();
        assert!(buffer.is_slowmode());
        assert!(buffer.take_slowmode_change());

        buffer.handle_response(40 * 1024, 32 * 1024, 64 * 1024);
        assert!(buffer.is_slowmode());
        assert!(!buffer.take_slowmode_change());

        buffer.handle_response(10 * 1024, 32 * 1024, 64 * 1024);
        assert!(!buffer.is_slowmode());
        assert!(buffer.take_slowmode_change());
    }
}
