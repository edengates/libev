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

/// The queue of messages, which have to be sent to the socket.
///
/// This also holds whether the channel has been shutdown, is in slow mode or is deafened.
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
    /// Updates the slow mode with the provided thresholds, see
    /// [`SocketManager::large_threshold`](crate::SocketManager::large_threshold).
    ///
    /// # Arguments
    ///
    /// - `threshold` — The threshold at which the slow mode ends
    /// - `large_threshold` — The threshold at which the slow mode starts
    fn update(&mut self, threshold: usize, large_threshold: usize) {
        let slowmode = if self.slowmode {
            self.queue_size >= threshold
        } else {
            self.queue_size > large_threshold
        };
        self.slowmode_changed |= slowmode != self.slowmode;
        self.slowmode = slowmode;
    }

    /// Adds the provided message to the queue.
    ///
    /// # Arguments
    ///
    /// - `message` — The message to add
    /// - `threshold` — The threshold at which the slow mode ends
    /// - `large_threshold` — The threshold at which the slow mode starts
    ///
    /// # Errors
    ///
    /// Returns [`NotConnected`](io::ErrorKind::NotConnected) if the queue has been shutdown.
    ///
    /// # See also
    ///
    /// - [`execute`](Self::execute)
    pub fn enqueue(&mut self, message: BytesMut, threshold: usize, large_threshold: usize) -> io::Result<()> {
        if self.shutdown {
            return Err(io::ErrorKind::NotConnected.into());
        }
        self.queue_size += message.len();
        self.update(threshold, large_threshold);
        self.queued.push(message);
        Ok(())
    }

    /// Removes the written bytes from the queue and updates the slow mode.
    ///
    /// # Arguments
    ///
    /// - `size` — The number of written bytes
    /// - `threshold` — The threshold at which the slow mode ends
    /// - `large_threshold` — The threshold at which the slow mode starts
    pub fn handle_response(&mut self, size: usize, threshold: usize, large_threshold: usize) {
        self.queue_size -= size;
        self.update(threshold, large_threshold);
    }

    /// Marks all messages, which have been added since the last time, as ready to be sent.
    ///
    /// # See also
    ///
    /// - [`enqueue`](Self::enqueue)
    /// - [`next_packet`](Self::next_packet)
    pub fn execute(&mut self) {
        if self.ready.is_empty() {
            mem::swap(&mut self.ready, &mut self.queued);
        } else {
            self.ready.append(&mut self.queued);
        }
    }

    /// Removes the next messages, which are ready to be sent, and returns up to `limit` of them.
    ///
    /// # Arguments
    ///
    /// - `limit` — The maximum number of messages to return
    ///
    /// # See also
    ///
    /// - [`recycle`](Self::recycle)
    pub fn next_packet(&mut self, limit: usize) -> Vec<BytesMut> {
        if self.ready.len() <= limit {
            mem::take(&mut self.ready)
        } else {
            let rest = self.ready.split_off(limit);
            mem::replace(&mut self.ready, rest)
        }
    }

    /// The memory of the provided packet is reused for the next messages, if the queue has no memory yet.
    ///
    /// # Arguments
    ///
    /// - `packet` — The packet returned by [`next_packet`](Self::next_packet)
    pub fn recycle(&mut self, mut packet: Vec<BytesMut>) {
        if self.queued.capacity() == 0 {
            packet.clear();
            self.queued = packet;
        }
    }

    /// Whether the slow mode has changed since the last time.
    ///
    /// # See also
    ///
    /// - [`is_slowmode`](Self::is_slowmode)
    pub fn take_slowmode_change(&mut self) -> bool {
        mem::take(&mut self.slowmode_changed)
    }

    /// Whether the channel is in slow mode.
    pub fn is_slowmode(&self) -> bool {
        self.slowmode
    }

    /// Shuts down the queue, no more messages can be added.
    ///
    /// # See also
    ///
    /// - [`is_shutdown`](Self::is_shutdown)
    pub fn shutdown(&mut self) {
        self.shutdown = true;
    }

    /// Whether the queue has been shutdown.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown
    }

    /// Sets whether the channel should be deafened, which means no more messages are received from the socket.
    ///
    /// # Arguments
    ///
    /// - `deafened` — Whether the channel should be deafened
    ///
    /// # See also
    ///
    /// - [`is_deafened`](Self::is_deafened)
    pub fn set_deafened(&mut self, deafened: bool) {
        self.deafened = deafened;
    }

    /// Whether the channel is deafened.
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
