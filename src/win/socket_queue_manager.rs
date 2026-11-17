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
use std::os::windows::io::AsRawSocket;
use std::ptr;
use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE, WAIT_TIMEOUT};
use windows_sys::Win32::System::IO::{
    CreateIoCompletionPort, GetQueuedCompletionStatusEx, OVERLAPPED_ENTRY,
    PostQueuedCompletionStatus,
};

pub struct SocketQueueManager {
    handle: HANDLE,
}

impl SocketQueueManager {
    pub fn new() -> io::Result<Self> {
        let handle = unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, ptr::null_mut(), 0, 1) };
        if handle.is_null() {
            Err(io::Error::last_os_error())
        } else {
            Ok(Self { handle })
        }
    }

    pub fn enqueue(&self, id: usize) -> io::Result<()> {
        let status = unsafe { PostQueuedCompletionStatus(self.handle, 0, id, ptr::null_mut()) };
        if status == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub fn poll(&self, entries: &mut [OVERLAPPED_ENTRY], timeout: u32) -> io::Result<usize> {
        let mut count = 0;
        let data: i32 = unsafe {
            GetQueuedCompletionStatusEx(
                self.handle,
                entries.as_mut_ptr(),
                entries.len() as u32,
                &mut count,
                timeout,
                0,
            )
        };
        if data == 0 {
            self.handle_error()
        } else {
            Ok(count as usize)
        }
    }

    fn handle_error(&self) -> io::Result<usize> {
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(WAIT_TIMEOUT as i32) {
            Ok(0)
        } else {
            Err(error)
        }
    }

    pub fn register<S: AsRawSocket>(&self, socket: &S, id: usize) -> io::Result<()> {
        let handle = unsafe {
            CreateIoCompletionPort(socket.as_raw_socket() as HANDLE, self.handle, id, 0)
        };
        if handle.is_null() {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

unsafe impl Send for SocketQueueManager {}

unsafe impl Sync for SocketQueueManager {}

impl Drop for SocketQueueManager {
    fn drop(&mut self) {
        unsafe {
            CloseHandle(self.handle);
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
    fn enqueue() {
        let manager = SocketQueueManager::new().unwrap();
        let mut entries = entries();
        manager.enqueue(7).unwrap();
        manager.enqueue(9).unwrap();
        assert_eq!(manager.poll(&mut entries, 1000).unwrap(), 2);
        assert_eq!(entries[0].lpCompletionKey, 7);
        assert_eq!(entries[1].lpCompletionKey, 9);
    }

    #[test]
    fn timeout() {
        let manager = SocketQueueManager::new().unwrap();
        assert_eq!(manager.poll(&mut entries(), 0).unwrap(), 0);
    }

    #[test]
    fn register() {
        let manager = SocketQueueManager::new().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        manager.register(&listener, 2).unwrap();
        assert!(
            SocketQueueManager::new()
                .unwrap()
                .register(&listener, 2)
                .is_err()
        );
    }
}
