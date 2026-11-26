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

use std::io;
use std::os::windows::io::{AsRawHandle, AsRawSocket, FromRawHandle, OwnedHandle};
use std::ptr;
use windows_sys::Win32::Foundation::{HANDLE, INVALID_HANDLE_VALUE, WAIT_TIMEOUT};
use windows_sys::Win32::System::IO::{
    CreateIoCompletionPort, GetQueuedCompletionStatusEx, OVERLAPPED_ENTRY,
    PostQueuedCompletionStatus,
};

pub struct CompletionService {
    handle: OwnedHandle,
}

impl CompletionService {
    /// Returns a new factory that creates IOCP instances.
    pub fn new_factory() -> fn(u32) -> io::Result<Self> {
        |_| Self::new()
    }

    /// Creates a new IOCP instance.
    pub fn new() -> io::Result<Self> {
        let handle = unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, ptr::null_mut(), 0, 1) };
        if handle.is_null() {
            Err(io::Error::last_os_error())
        } else {
            Ok(Self { handle: unsafe { OwnedHandle::from_raw_handle(handle) } })
        }
    }

    pub(crate) fn wakeup(&self, id: usize) -> io::Result<()> {
        let status = unsafe { PostQueuedCompletionStatus(self.handle.as_raw_handle(), 0, id, ptr::null_mut()) };
        if status == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub(crate) fn poll(&self, entries: &mut [OVERLAPPED_ENTRY], timeout: u32) -> io::Result<usize> {
        let mut count = 0;
        let data: i32 = unsafe {
            GetQueuedCompletionStatusEx(
                self.handle.as_raw_handle(),
                entries.as_mut_ptr(),
                entries.len() as u32,
                &mut count,
                timeout,
                0,
            )
        };
        if data != 0 {
            return Ok(count as usize);
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(WAIT_TIMEOUT as i32) {
            Ok(0)
        } else {
            Err(error)
        }
    }

    pub(crate) fn register<S: AsRawSocket>(&self, socket: &S, id: usize) -> io::Result<()> {
        let handle = unsafe {
            CreateIoCompletionPort(socket.as_raw_socket() as HANDLE, self.handle.as_raw_handle(), id, 0)
        };
        if handle.is_null() {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;
    use std::net::TcpListener;

    fn entries() -> [OVERLAPPED_ENTRY; 4] {
        unsafe { mem::zeroed() }
    }

    #[test]
    fn wakeup() {
        let port = CompletionService::new().unwrap();
        let mut entries = entries();
        port.wakeup(7).unwrap();
        port.wakeup(9).unwrap();
        assert_eq!(port.poll(&mut entries, 1000).unwrap(), 2);
        assert_eq!(entries[0].lpCompletionKey, 7);
        assert_eq!(entries[1].lpCompletionKey, 9);
    }

    #[test]
    fn timeout() {
        let port = CompletionService::new().unwrap();
        assert_eq!(port.poll(&mut entries(), 0).unwrap(), 0);
    }

    #[test]
    fn register() {
        let port = CompletionService::new().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        port.register(&listener, 2).unwrap();
        assert!(
            CompletionService::new()
                .unwrap()
                .register(&listener, 2)
                .is_err()
        );
    }
}
