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

/// Holds an I/O completion port, which provides the completed operations of each registered socket.
///
/// Each socket is registered with an identifier, which is returned with each completed operation of the socket, see
/// [`register`](Self::register). The completed operations are retrieved with [`poll`](Self::poll), while
/// [`enqueue`](Self::enqueue) can be called from **any** thread. The I/O completion port is closed once this instance
/// is dropped.
///
/// # See also
///
/// - [`IocpHandler`](crate::handle::IocpHandler)
/// - [`ChannelManager`](crate::handle::ChannelManager)
pub struct SocketQueueManager {
    handle: HANDLE,
}

impl SocketQueueManager {
    /// Creates a new instance.
    ///
    /// The I/O completion port is created for **one** thread, which retrieves the completed operations with
    /// [`poll`](Self::poll).
    ///
    /// # Errors
    ///
    /// Returns an error if the I/O completion port could not be created.
    pub fn new() -> io::Result<Self> {
        let handle = unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, ptr::null_mut(), 0, 1) };
        if handle.is_null() {
            Err(io::Error::last_os_error())
        } else {
            Ok(Self { handle })
        }
    }

    /// Sends the provided identifier to the thread, which is blocked in [`poll`](Self::poll).
    ///
    /// The identifier is returned by [`poll`](Self::poll) as an entry without packet, like the identifier of a
    /// completed operation. If no thread is blocked in [`poll`](Self::poll), it is returned with the next call. This
    /// can be called from **any** thread, for example with [`DISPATCH`](crate::handle::DISPATCH) to handle the queued
    /// tasks or with [`SHUTDOWN`](crate::handle::SHUTDOWN).
    ///
    /// # Arguments
    ///
    /// - `id` — The identifier to send
    ///
    /// # Errors
    ///
    /// Returns an error if the identifier could not be sent.
    ///
    /// # See also
    ///
    /// - [`poll`](Self::poll)
    pub fn enqueue(&self, id: usize) -> io::Result<()> {
        let status = unsafe { PostQueuedCompletionStatus(self.handle, 0, id, ptr::null_mut()) };
        if status == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    /// Blocks the current thread until the next operations have completed or the timeout has elapsed.
    ///
    /// Up to `entries.len()` completed operations are written to `entries`, the remaining operations are returned
    /// with the next call. Each entry holds the identifier of the socket and the [`SocketPacket`] of the operation,
    /// which **must** be released with [`SocketPacket::handle_success`]. The `timeout` is in milliseconds, `u32::MAX`
    /// blocks the current thread until the next operation has completed.
    ///
    /// # Arguments
    ///
    /// - `entries` — The entries to fill
    /// - `timeout` — The timeout in milliseconds
    ///
    /// # Errors
    ///
    /// Returns an error if the completed operations could not be retrieved.
    ///
    /// # Returns
    ///
    /// The amount of completed operations, `0` if the timeout has elapsed.
    ///
    /// # See also
    ///
    /// - [`enqueue`](Self::enqueue)
    ///
    /// [`SocketPacket`]: crate::handle::socket_packet::SocketPacket
    /// [`SocketPacket::handle_success`]: crate::handle::socket_packet::SocketPacket::handle_success
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

    /// Returns `0` if the timeout has elapsed, the error otherwise.
    fn handle_error(&self) -> io::Result<usize> {
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(WAIT_TIMEOUT as i32) {
            Ok(0)
        } else {
            Err(error)
        }
    }

    /// Registers the provided socket with the provided identifier.
    ///
    /// The identifier is returned with each completed operation of the socket. A socket can **only** be registered with
    /// one I/O completion port, it can **not** be removed until the socket is closed.
    ///
    /// # Arguments
    ///
    /// - `socket` — The socket to register
    /// - `id` — The identifier of the socket
    ///
    /// # Errors
    ///
    /// Returns an error if the socket could not be registered, for example if it is already registered with another
    /// I/O completion port.
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
