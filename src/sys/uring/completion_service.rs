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

use crate::handle;
use crate::utils::concurrent::{DISPATCH, SHUTDOWN};
use bytes::BytesMut;
use io_uring::types::BufRingEntry;
use io_uring::{IoUring, Submitter, opcode, squeue, types};
use rustix::event::{EventfdFlags, eventfd};
use rustix::io::Errno;
use std::alloc::{self, Layout};
use std::cell::UnsafeCell;
use std::io;
use std::mem;
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::time::Duration;

const EVENTFD_TOKEN: u64 = u64::MAX;
const RINGFD_TOKEN: u64 = u64::MAX - 1;

pub struct CompletionService {
    ring: IoUring,
    event_fd: OwnedFd,
    event_value: Box<UnsafeCell<u64>>,
    is_reading: AtomicBool,
    is_shutdown: AtomicBool,
    is_deferred: bool,
    is_disabled: bool,
}

unsafe impl Send for CompletionService {}
unsafe impl Sync for CompletionService {}

impl CompletionService {
    /// Returns a new factory that creates `io_uring` instances.
    pub(crate) fn new_factory() -> fn(u32) -> io::Result<crate::handle::CompletionService> {
        |entries| Self::build(entries, true).map(crate::handle::CompletionService::IoUring)
    }

    #[cfg(test)]
    pub(crate) fn new(entries: u32) -> io::Result<Self> {
        Self::build(entries, false)
    }

    fn build(entries: u32, is_disabled: bool) -> io::Result<Self> {
        let mut builder = IoUring::builder();
        builder.setup_single_issuer().setup_defer_taskrun();
        if is_disabled {
            builder.setup_r_disabled();
        }
        let (ring, is_deferred) = match builder.build(entries) {
            Ok(ring) => (ring, true),
            Err(error) if error.kind() == io::ErrorKind::InvalidInput => (IoUring::new(entries)?, false),
            Err(error) => return Err(error),
        };
        probe(&ring.submitter())?;
        Ok(Self {
            ring,
            event_fd: eventfd(0, EventfdFlags::CLOEXEC)?,
            event_value: Box::new(UnsafeCell::new(0)),
            is_reading: AtomicBool::new(false),
            is_shutdown: AtomicBool::new(false),
            is_deferred,
            is_disabled: is_disabled && is_deferred,
        })
    }

    pub(crate) fn enable(&self) -> io::Result<()> {
        if self.is_disabled {
            return self.ring.submitter().register_enable_rings();
        }
        Ok(())
    }

    pub(crate) fn is_multishot_enabled(&self) -> bool {
        self.is_deferred
    }

    pub(crate) fn wakeup(&self, id: usize) -> io::Result<()> {
        if id == SHUTDOWN {
            self.is_shutdown.store(true, Ordering::Release);
        }
        loop {
            match rustix::io::write(&self.event_fd, &1u64.to_ne_bytes()) {
                Ok(_) => return Ok(()),
                Err(Errno::INTR) => {}
                Err(error) => return Err(error.into()),
            }
        }
    }

    pub(crate) fn poll<F: FnMut(i32, u32, u64)>(&self, timeout: u32, mut callback: F) -> io::Result<usize> {
        if unsafe { self.ring.completion_shared() }.is_empty() {
            if !self.is_reading.swap(true, Ordering::Relaxed) {
                let event_value = self.event_value.get().cast::<u8>();
                let read = opcode::Read::new(types::Fd(self.event_fd.as_raw_fd()), event_value, mem::size_of::<u64>() as u32);
                self.enqueue_sqe(read.build().user_data(EVENTFD_TOKEN))?;
            }
            let delay = Duration::from_millis(timeout as u64);
            let timespec = types::Timespec::new().sec(delay.as_secs()).nsec(delay.subsec_nanos());
            if timeout != u32::MAX {
                self.enqueue_sqe(opcode::Timeout::new(&timespec).count(1).build().user_data(RINGFD_TOKEN))?;
            }
            self.submit_and_wait(1)?;
        } else {
            self.submit()?;
        }
        let mut count = 0;
        self.process(|res, flags, user_data| match user_data {
            EVENTFD_TOKEN => {
                self.is_reading.store(false, Ordering::Relaxed);
                let id = if self.is_shutdown.load(Ordering::Acquire) { SHUTDOWN } else { DISPATCH };
                callback(res, flags, id as u64);
                count += 1;
            }
            RINGFD_TOKEN => {}
            user_data => {
                callback(res, flags, user_data);
                count += 1;
            }
        });
        Ok(count)
    }

    pub(crate) fn enqueue_sqe(&self, sqe: squeue::Entry) -> io::Result<()> {
        if unsafe { self.ring.submission_shared() }.is_full() && self.submit()? == 0 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        unsafe { self.ring.submission_shared().push(&sqe) }.map_err(|_| io::ErrorKind::WouldBlock.into())
    }

    pub(crate) fn submit(&self) -> io::Result<usize> {
        self.submit_and_wait(0)
    }

    pub(crate) fn submit_and_wait(&self, want: usize) -> io::Result<usize> {
        loop {
            match self.ring.submit_and_wait(want) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                data => return data,
            }
        }
    }

    pub(crate) fn process<F: FnMut(i32, u32, u64)>(&self, mut callback: F) -> usize {
        let mut queue = unsafe { self.ring.completion_shared() };
        let mut total = 0;
        while let Some(cqe) = queue.next() {
            queue.sync();
            callback(cqe.result(), cqe.flags(), cqe.user_data());
            total += 1;
        }
        total
    }

    pub(crate) fn submitter(&self) -> Submitter<'_> {
        self.ring.submitter()
    }
}

impl Drop for CompletionService {
    fn drop(&mut self) {
        if self.is_reading.load(Ordering::Relaxed) && self.wakeup(DISPATCH).is_ok() {
            while self.is_reading.load(Ordering::Relaxed) {
                if self.submit_and_wait(1).is_err() {
                    mem::forget(mem::replace(&mut self.event_value, Box::new(UnsafeCell::new(0))));
                    break;
                }
                self.process(|_, _, user_data| {
                    if user_data == EVENTFD_TOKEN {
                        self.is_reading.store(false, Ordering::Relaxed);
                    }
                });
            }
        }
    }
}

fn probe(submitter: &Submitter<'_>) -> io::Result<()> {
    let layout = Layout::from_size_align(mem::size_of::<BufRingEntry>(), 4096).map_err(|_| io::ErrorKind::InvalidInput)?;
    let ring = unsafe { alloc::alloc_zeroed(layout) };
    if ring.is_null() {
        return Err(io::ErrorKind::OutOfMemory.into());
    }
    if let Err(error) = unsafe { submitter.register_buf_ring_with_flags(ring as u64, 1, 0, 0) } {
        unsafe { alloc::dealloc(ring, layout) };
        return Err(match error.kind() {
            io::ErrorKind::InvalidInput => io::ErrorKind::Unsupported.into(),
            _ => error,
        });
    }
    if submitter.unregister_buf_ring(0).is_ok() {
        unsafe { alloc::dealloc(ring, layout) };
    }
    Ok(())
}

pub struct BufferRing {
    ring: *mut BufRingEntry,
    layout: Layout,
    entries: u16,
    buffers: Vec<Option<BytesMut>>,
    buffer_size: usize,
    port: Arc<handle::CompletionService>,
}

impl BufferRing {
    pub(crate) fn new(port: &Arc<handle::CompletionService>, entries: u16, buffer_size: usize) -> io::Result<Self> {
        let handle::CompletionService::IoUring(uring) = &**port else {
            return Err(io::ErrorKind::Unsupported.into());
        };
        let layout = Layout::from_size_align(entries as usize * mem::size_of::<BufRingEntry>(), 4096)
            .map_err(|_| io::ErrorKind::InvalidInput)?;
        let ring = unsafe { alloc::alloc_zeroed(layout) }.cast::<BufRingEntry>();
        if ring.is_null() {
            return Err(io::ErrorKind::OutOfMemory.into());
        }
        if let Err(error) = unsafe { uring.submitter().register_buf_ring_with_flags(ring as u64, entries, 0, 0) } {
            unsafe { alloc::dealloc(ring.cast(), layout) };
            return Err(error);
        }
        let mut buffer_ring = Self {
            ring,
            layout,
            entries,
            buffers: (0..entries).map(|_| None).collect(),
            buffer_size,
            port: Arc::clone(port),
        };
        for bid in 0..entries {
            buffer_ring.fill(bid);
        }
        Ok(buffer_ring)
    }

    pub(crate) fn group(&self) -> u16 {
        0
    }

    pub(crate) fn use_buffer(&mut self, bid: u16, read: usize) -> Option<BytesMut> {
        let mut buffer = self.buffers.get_mut(bid as usize)?.take()?;
        unsafe { buffer.set_len(read.min(buffer.capacity())) };
        self.fill(bid);
        Some(buffer)
    }

    fn fill(&mut self, bid: u16) {
        let mut buffer = BytesMut::with_capacity(self.buffer_size);
        let tail = self.tail().load(Ordering::Relaxed);
        let entry = unsafe { &mut *self.ring.add((tail & (self.entries - 1)) as usize) };
        entry.set_addr(buffer.spare_capacity_mut().as_mut_ptr() as u64);
        entry.set_len(buffer.capacity() as u32);
        entry.set_bid(bid);
        self.buffers[bid as usize] = Some(buffer);
        self.tail().store(tail.wrapping_add(1), Ordering::Release);
    }

    fn tail(&self) -> &AtomicU16 {
        unsafe { &*BufRingEntry::tail(self.ring).cast::<AtomicU16>() }
    }
}

impl Drop for BufferRing {
    fn drop(&mut self) {
        let handle::CompletionService::IoUring(uring) = &*self.port else {
            return;
        };
        if uring.submitter().unregister_buf_ring(self.group()).is_ok() {
            unsafe { alloc::dealloc(self.ring.cast(), self.layout) };
        } else {
            mem::forget(mem::take(&mut self.buffers));
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use io_uring::cqueue;
    use std::io::Write;
    use std::net::{TcpListener, TcpStream};
    use std::ptr;
    use std::thread;
    use std::time::Instant;

    fn nop(manager: &CompletionService, user_data: u64) {
        manager.enqueue_sqe(opcode::Nop::new().build().user_data(user_data)).unwrap();
    }

    #[test]
    fn process() {
        let manager = CompletionService::new(4).unwrap();
        nop(&manager, 42);
        assert_eq!(manager.submit_and_wait(1).unwrap(), 1);
        let mut entries = Vec::new();
        manager.process(|res, _, user_data| entries.push((res, user_data)));
        assert_eq!(entries, [(0, 42)]);
        assert_eq!(manager.process(|_, _, _| ()), 0);
    }

    #[test]
    fn submit() {
        let manager = CompletionService::new(4).unwrap();
        for user_data in 0..6 {
            nop(&manager, user_data);
        }
        assert_eq!(manager.submit_and_wait(2).unwrap(), 2);
        let mut entries = Vec::new();
        while entries.len() < 6 {
            manager.process(|_, _, user_data| entries.push(user_data));
        }
        assert_eq!(entries, [0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn wakeup() {
        let manager = CompletionService::new(4).unwrap();
        thread::scope(|scope| {
            scope.spawn(|| manager.wakeup(DISPATCH).unwrap());
        });
        let mut entries = Vec::new();
        manager.poll(1000, |_, _, user_data| entries.push(user_data)).unwrap();
        assert_eq!(entries, [DISPATCH as u64]);

        manager.wakeup(SHUTDOWN).unwrap();
        manager.poll(1000, |_, _, user_data| entries.push(user_data)).unwrap();
        assert_eq!(entries, [DISPATCH as u64, SHUTDOWN as u64]);
    }

    #[test]
    fn timeout() {
        let manager = CompletionService::new(4).unwrap();
        let start = Instant::now();
        assert_eq!(manager.poll(10, |_, _, _| ()).unwrap(), 0);
        assert!(start.elapsed() >= Duration::from_millis(10));

        nop(&manager, 7);
        let mut entries = Vec::new();
        manager.poll(1000, |res, _, user_data| entries.push((user_data, res))).unwrap();
        assert_eq!(entries, [(7, 0)]);
    }

    fn receive(manager: &CompletionService, buffer_ring: &BufferRing, stream: &TcpStream) {
        let recv = opcode::Recv::new(types::Fd(stream.as_raw_fd()), ptr::null_mut(), 0).buf_group(buffer_ring.group());
        manager.enqueue_sqe(recv.build().flags(squeue::Flags::BUFFER_SELECT).user_data(5)).unwrap();
    }

    #[test]
    fn use_buffer() {
        let port = Arc::new(CompletionService::new_factory()(8).unwrap());
        let handle::CompletionService::IoUring(manager) = &*port else {
            unreachable!()
        };
        manager.enable().unwrap();
        let mut buffer_ring = BufferRing::new(&port, 2, 16).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();

        for message in [&b"ping"[..], b"pong", b"0123456789abcdefXYZ"] {
            client.write_all(message).unwrap();
            receive(manager, &buffer_ring, &server);
            let mut entries = Vec::new();
            while entries.is_empty() {
                manager.poll(1000, |res, flags, user_data| entries.push((res, flags, user_data))).unwrap();
            }
            let (res, flags, user_data) = entries[0];
            assert_eq!(user_data, 5);
            let buffer = buffer_ring.use_buffer(cqueue::buffer_select(flags).unwrap(), res as usize).unwrap();
            assert_eq!(&buffer[..], &message[..message.len().min(16)]);
            if message.len() > 16 {
                let mut rest = [0; 3];
                std::io::Read::read_exact(&mut &server, &mut rest).unwrap();
                assert_eq!(&rest, b"XYZ");
            }
        }
    }
}
