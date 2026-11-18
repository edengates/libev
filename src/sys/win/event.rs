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

use bytes::{Buf, BufMut, BytesMut};
use std::io;
use std::mem;
use std::os::windows::io::OwnedSocket;
use std::ptr;
use windows_sys::Win32::Foundation::{NTSTATUS, RtlNtStatusToDosError};
use windows_sys::Win32::Networking::WinSock::{SOCKET_ERROR, WSABUF, WSAGetLastError, WSA_IO_PENDING};
use windows_sys::Win32::System::IO::{OVERLAPPED, OVERLAPPED_ENTRY};

/// Once submitted, the packet must not be moved or released until Windows has completed the operation.
/// The packet is found again from its `OVERLAPPED`, which must stay the first field.
#[repr(C)]
pub struct Event {
    overlapped: OVERLAPPED,
    buffers: Vec<BytesMut>,
    length: usize,
    socket: Option<OwnedSocket>,
    opcode: u32,
}

impl Event {
    pub(crate) const READ: u32 = 1 << 0;
    pub(crate) const WRITE: u32 = 1 << 2;
    pub(crate) const CONNECT: u32 = 1 << 3;
    pub(crate) const ACCEPT: u32 = 1 << 4;

    pub(crate) fn new(capacity: usize) -> Box<Self> {
        if capacity == 0 {
            Self::wrap(Vec::new())
        } else {
            Self::wrap(vec![BytesMut::with_capacity(capacity)])
        }
    }

    pub(crate) fn wrap(buffers: Vec<BytesMut>) -> Box<Self> {
        Box::new(Self {
            overlapped: unsafe { mem::zeroed() },
            buffers,
            length: 0,
            socket: None,
            opcode: Event::READ,
        })
    }

    #[cfg(test)]
    pub(crate) fn buffer(&self) -> &BytesMut {
        &self.buffers[0]
    }

    pub(crate) fn length(&self) -> usize {
        self.length
    }

    pub(crate) fn opcode(&self) -> u32 {
        self.opcode
    }

    pub(crate) fn set_socket(&mut self, socket: OwnedSocket) {
        self.socket = Some(socket);
    }

    pub(crate) fn take_socket(&mut self) -> Option<OwnedSocket> {
        self.socket.take()
    }

    #[cfg(test)]
    pub(crate) fn into_body(self) -> BytesMut {
        self.buffers.into_iter().next().unwrap_or_default()
    }

    pub(crate) fn advance(&mut self) -> bool {
        let mut length = mem::take(&mut self.length);
        for buffer in &mut self.buffers {
            let step = length.min(buffer.len());
            buffer.advance(step);
            length -= step;
        }
        self.buffers.retain(|buffer| buffer.has_remaining());
        !self.buffers.is_empty()
    }

    pub(crate) fn submit<F>(mut packet: Box<Event>, opcode: u32, call: F) -> io::Result<()>
    where
        F: FnOnce(&[WSABUF], *mut OVERLAPPED) -> i32,
    {
        packet.opcode = opcode;
        packet.overlapped = unsafe { mem::zeroed() };
        let packet = Box::into_raw(packet);
        let response = unsafe {
            let packet = &mut *packet;
            match opcode {
                Event::READ | Event::ACCEPT => {
                    let buffer = match packet.buffers.first_mut() {
                        Some(buffer) => {
                            let chunk = buffer.spare_capacity_mut();
                            WSABUF {
                                len: chunk.len() as u32,
                                buf: chunk.as_mut_ptr().cast::<u8>(),
                            }
                        }
                        None => WSABUF {
                            len: 0,
                            buf: ptr::NonNull::dangling().as_ptr(),
                        },
                    };
                    call(&[buffer], &mut packet.overlapped)
                }
                _ => {
                    let buffers: Vec<WSABUF> = packet
                        .buffers
                        .iter_mut()
                        .map(|buffer| WSABUF {
                            len: buffer.len() as u32,
                            buf: buffer.as_mut_ptr(),
                        })
                        .collect();
                    call(&buffers, &mut packet.overlapped)
                }
            }
        };
        if response == SOCKET_ERROR {
            let code = unsafe { WSAGetLastError() };
            if code != WSA_IO_PENDING {
                drop(unsafe { Box::from_raw(packet) });
                return Err(io::Error::from_raw_os_error(code));
            }
        }
        Ok(())
    }

    /// # Safety
    ///
    /// `entry` must come from [`CompletionService::poll`](crate::handle::CompletionService::poll), hold a packet
    /// submitted by this module, and be handled only once.
    pub(crate) unsafe fn from_entry(entry: &OVERLAPPED_ENTRY) -> io::Result<Box<Self>> {
        let mut packet = unsafe { Box::from_raw(entry.lpOverlapped as *mut Self) };
        let status = packet.overlapped.Internal as NTSTATUS;
        if status != 0 {
            return Err(io::Error::from_raw_os_error(
                unsafe { RtlNtStatusToDosError(status) } as i32,
            ));
        }
        packet.length = entry.dwNumberOfBytesTransferred as usize;
        if packet.opcode == Event::READ {
            let length = packet.length();
            if let Some(buffer) = packet.buffers.first_mut() {
                unsafe { buffer.advance_mut(length) }
            }
        }
        Ok(packet)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advance() {
        let mut packet =
            Event::wrap(vec![BytesMut::from(&b"pi"[..]), BytesMut::from(&b"ng"[..])]);
        packet.length = 3;
        assert!(packet.advance());
        assert_eq!(&packet.buffer()[..], b"g");
        packet.length = 1;
        assert!(!packet.advance());
    }
}
