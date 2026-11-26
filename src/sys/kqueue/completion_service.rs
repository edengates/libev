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
use rustix::event::kqueue::{self, Event, EventFilter, EventFlags, UserDefinedFlags, UserFlags};
use rustix::io::Errno;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

pub struct CompletionService {
    kqueue_fd: OwnedFd,
    is_shutdown: AtomicBool,
}

impl CompletionService {
    /// Returns a new factory that creates kqueue instances.
    pub fn new_factory() -> fn(u32) -> io::Result<Self> {
        |_| Self::new()
    }

    /// Creates a new kqueue instance.
    pub fn new() -> io::Result<Self> {
        let manager = Self {
            kqueue_fd: kqueue::kqueue()?,
            is_shutdown: AtomicBool::new(false),
        };
        manager.execute(user(UserFlags::NOINPUT, EventFlags::ADD | EventFlags::ENABLE | EventFlags::CLEAR, DISPATCH))?;
        Ok(manager)
    }

    pub(crate) fn wakeup(&self, id: usize) -> io::Result<()> {
        if id == SHUTDOWN {
            self.is_shutdown.store(true, Ordering::Release);
        }
        self.execute(user(UserFlags::TRIGGER | UserFlags::NOINPUT, EventFlags::empty(), DISPATCH))
    }

    pub(crate) fn poll(&self, entries: &mut [Event], timeout: u32) -> io::Result<usize> {
        let count = self.await_status(entries, timeout)?;
        if self.is_shutdown.load(Ordering::Acquire) {
            for entry in &mut entries[..count] {
                if matches!(entry.filter(), EventFilter::User { .. }) {
                    *entry = Event::new(entry.filter(), entry.flags(), SHUTDOWN as *mut _);
                }
            }
        }
        Ok(count)
    }

    fn await_status(&self, entries: &mut [Event], timeout: u32) -> io::Result<usize> {
        let deadline = (timeout != u32::MAX).then(|| Instant::now() + Duration::from_millis(timeout as u64));
        loop {
            let delay = deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()));
            match unsafe { kqueue::kevent(&self.kqueue_fd, &[], &mut *entries, delay) } {
                Ok(count) => return Ok(count),
                Err(Errno::INTR) => {}
                Err(error) => return Err(error.into()),
            }
        }
    }

    pub(crate) fn register<S: AsRawFd>(&self, _socket: &S, _id: usize) -> io::Result<()> {
        Ok(())
    }

    pub(crate) fn modify<S: AsRawFd>(
        &self,
        socket: &S,
        id: usize,
        filter: fn(RawFd) -> EventFilter,
        is_enabled: bool,
    ) -> io::Result<()> {
        let flags = if is_enabled {
            EventFlags::ADD | EventFlags::ENABLE
        } else {
            EventFlags::DELETE | EventFlags::DISABLE
        };
        self.execute(Event::new(filter(socket.as_raw_fd()), flags, id as *mut _))
    }

    fn execute(&self, change: Event) -> io::Result<()> {
        match unsafe { kqueue::kevent(&self.kqueue_fd, &[change], &mut [] as &mut [Event], None) } {
            // When kevent() call fails with EINTR error, all changes in the changelist have been applied.
            Ok(_) | Err(Errno::INTR) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

fn user(flags: UserFlags, event_flags: EventFlags, id: usize) -> Event {
    let filter = EventFilter::User {
        ident: 0,
        flags,
        user_flags: UserDefinedFlags::new(0),
    };
    Event::new(filter, event_flags, id as *mut _)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;
    use std::net::{TcpListener, TcpStream};

    fn entries() -> [Event; 4] {
        unsafe { mem::zeroed() }
    }

    #[test]
    fn wakeup() {
        let manager = CompletionService::new().unwrap();
        let mut entries = entries();
        manager.wakeup(DISPATCH).unwrap();
        assert_eq!(manager.poll(&mut entries, 1000).unwrap(), 1);
        assert_eq!(entries[0].udata() as usize, DISPATCH);
        manager.wakeup(SHUTDOWN).unwrap();
        assert_eq!(manager.poll(&mut entries, 1000).unwrap(), 1);
        assert_eq!(entries[0].udata() as usize, SHUTDOWN);
    }

    #[test]
    fn timeout() {
        let manager = CompletionService::new().unwrap();
        let start = Instant::now();
        assert_eq!(manager.poll(&mut entries(), 10).unwrap(), 0);
        assert!(start.elapsed() >= Duration::from_millis(10));
    }

    #[test]
    fn modify() {
        let manager = CompletionService::new().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        manager.register(&listener, 2).unwrap();
        manager.modify(&listener, 2, EventFilter::Read, true).unwrap();
        let _client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let mut entries = entries();
        assert_eq!(manager.poll(&mut entries, 1000).unwrap(), 1);
        assert_eq!(entries[0].udata() as usize, 2);
        assert!(matches!(entries[0].filter(), EventFilter::Read(_)));
        manager.modify(&listener, 2, EventFilter::Read, false).unwrap();
        assert_eq!(manager.poll(&mut entries, 0).unwrap(), 0);
        assert!(manager.modify(&listener, 2, EventFilter::Read, false).is_err());
    }
}
