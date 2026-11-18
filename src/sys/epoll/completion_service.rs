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

use crate::utils::concurrent::{DISPATCH, SHUTDOWN};
use rustix::event::epoll::{self, CreateFlags, Event, EventData, EventFlags};
use rustix::event::{EventfdFlags, Timespec, eventfd};
use rustix::io::Errno;
use std::io;
use std::os::fd::{AsFd, OwnedFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

pub struct CompletionService {
    epoll_fd: OwnedFd,
    event_fd: OwnedFd,
    is_shutdown: AtomicBool,
}

impl CompletionService {
    /// Returns a new factory that creates epoll instances.
    pub(crate) fn new_factory() -> fn(u32) -> io::Result<crate::handle::CompletionService> {
        |_| Self::new().map(crate::handle::CompletionService::Epoll)
    }

    pub(crate) fn new() -> io::Result<Self> {
        let manager = Self {
            epoll_fd: epoll::create(CreateFlags::CLOEXEC)?,
            event_fd: eventfd(0, EventfdFlags::CLOEXEC | EventfdFlags::NONBLOCK)?,
            is_shutdown: AtomicBool::new(false),
        };
        epoll::add(&manager.epoll_fd, &manager.event_fd, EventData::new_u64(DISPATCH as u64), EventFlags::IN | EventFlags::ET)?;
        Ok(manager)
    }

    pub(crate) fn wakeup(&self, id: usize) -> io::Result<()> {
        if id == SHUTDOWN {
            self.is_shutdown.store(true, Ordering::Release);
        }
        loop {
            match rustix::io::write(&self.event_fd, &1u64.to_ne_bytes()) {
                Ok(_) => return Ok(()),
                Err(Errno::AGAIN) => {}
                Err(error) => return Err(error.into()),
            }
            match rustix::io::read(&self.event_fd, &mut [0; 8]) {
                Ok(_) | Err(Errno::AGAIN) => {}
                Err(error) => return Err(error.into()),
            }
        }
    }

    pub(crate) fn poll(&self, entries: &mut [Event], timeout: u32) -> io::Result<usize> {
        let count = self.await_status(entries, timeout)?;
        if self.is_shutdown.load(Ordering::Acquire) {
            for entry in &mut entries[..count] {
                if { entry.data }.u64() == DISPATCH as u64 {
                    entry.data = EventData::new_u64(SHUTDOWN as u64);
                }
            }
        }
        Ok(count)
    }

    fn await_status(&self, entries: &mut [Event], timeout: u32) -> io::Result<usize> {
        let deadline = (timeout != u32::MAX).then(|| Instant::now() + Duration::from_millis(timeout as u64));
        loop {
            let delay = deadline.map(|deadline| {
                let delay = deadline.saturating_duration_since(Instant::now());
                Timespec {
                    tv_sec: delay.as_secs() as _,
                    tv_nsec: delay.subsec_nanos() as _,
                }
            });
            match epoll::wait(&self.epoll_fd, &mut *entries, delay.as_ref()) {
                Ok(count) => return Ok(count),
                Err(Errno::INTR) => {}
                Err(error) => return Err(error.into()),
            }
        }
    }

    pub(crate) fn register(&self, socket: &impl AsFd, id: usize, flags: EventFlags) -> io::Result<()> {
        Ok(epoll::add(&self.epoll_fd, socket, EventData::new_u64(id as u64), flags)?)
    }

    pub(crate) fn modify(&self, socket: &impl AsFd, id: usize, flags: EventFlags) -> io::Result<()> {
        Ok(epoll::modify(&self.epoll_fd, socket, EventData::new_u64(id as u64), flags)?)
    }

    pub(crate) fn unregister(&self, socket: &impl AsFd) -> io::Result<()> {
        Ok(epoll::delete(&self.epoll_fd, socket)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{TcpListener, TcpStream};

    fn entries() -> [Event; 4] {
        [Event { flags: EventFlags::empty(), data: EventData::new_u64(0) }; 4]
    }

    #[test]
    fn wakeup() {
        let manager = CompletionService::new().unwrap();
        let mut entries = entries();
        manager.wakeup(DISPATCH).unwrap();
        assert_eq!(manager.poll(&mut entries, 1000).unwrap(), 1);
        assert_eq!({ entries[0].data }.u64(), DISPATCH as u64);
        manager.wakeup(SHUTDOWN).unwrap();
        assert_eq!(manager.poll(&mut entries, 1000).unwrap(), 1);
        assert_eq!({ entries[0].data }.u64(), SHUTDOWN as u64);
    }

    #[test]
    fn timeout() {
        let manager = CompletionService::new().unwrap();
        let start = Instant::now();
        assert_eq!(manager.poll(&mut entries(), 10).unwrap(), 0);
        assert!(start.elapsed() >= Duration::from_millis(10));
    }

    #[test]
    fn register() {
        let manager = CompletionService::new().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        manager.register(&listener, 2, EventFlags::empty()).unwrap();
        manager.modify(&listener, 2, EventFlags::IN).unwrap();
        let _client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let mut entries = entries();
        assert_eq!(manager.poll(&mut entries, 1000).unwrap(), 1);
        assert_eq!({ entries[0].data }.u64(), 2);
        assert!({ entries[0].flags }.contains(EventFlags::IN));
        manager.modify(&listener, 3, EventFlags::IN | EventFlags::RDHUP | EventFlags::ET).unwrap();
        manager.unregister(&listener).unwrap();
        assert!(manager.unregister(&listener).is_err());
    }
}
