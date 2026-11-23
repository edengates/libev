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

use crate::handle::uring::CompletionService;
use crate::handle::uring::event;
use io_uring::{cqueue, opcode, squeue, types};
use crate::handle;
use socket2::SockAddr;
use crate::handle::event::Event;
use crate::utils::concurrent::{ScheduledExecutorService, DISPATCH, SHUTDOWN};
use crate::utils::buffer;
use bytes::BytesMut;
use rustix::net::SocketFlags;
use std::ptr;
use std::slice;
use std::io::{self, IoSlice};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::fd::AsRawFd;

fn user_data(key: usize, opcode: u8) -> u64 {
    ((key as u64) << 32) | ((opcode as u64) << 16)
}

fn ring(port: &handle::CompletionService) -> io::Result<&CompletionService> {
    match port {
        handle::CompletionService::IoUring(port) => Ok(port),
        handle::CompletionService::Epoll(_) => Err(io::ErrorKind::Unsupported.into()),
    }
}

pub(crate) fn run_worker(manager: &mut ScheduledExecutorService, port: &CompletionService) -> io::Result<()> {
    let mut is_shutdown = false;
    let data = port.poll(manager.queue.timeout(), |res, flags, user_data| match user_data as usize {
        DISPATCH => {}
        SHUTDOWN => is_shutdown = true,
        _ => complete(manager, res, flags, user_data),
    });
    manager.is_shutdown |= is_shutdown;
    data.map(|_| ())
}

pub(crate) fn close(manager: &mut ScheduledExecutorService, port: &CompletionService) {
    if manager.array.response_total > 0 {
        let sqe = opcode::AsyncCancel2::new(types::CancelBuilder::any().all())
            .build()
            .user_data(user_data(DISPATCH, opcode::AsyncCancel2::CODE));
        if port.enqueue_sqe(sqe).is_err() {
            return;
        }
    }
    while manager.array.response_total > 0 {
        let data = port.poll(u32::MAX, |_, _, user_data| {
            let key = (user_data >> 32) as usize;
            if key > SHUTDOWN {
                manager.array.response_total -= 1;
                manager.array.writes.remove(&key);
                manager.array.addresses.remove(&key);
            }
        });
        if data.is_err() {
            return;
        }
    }
}

fn complete(manager: &mut ScheduledExecutorService, res: i32, flags: u32, user_data: u64) {
    let key = (user_data >> 32) as usize;
    let opcode = (user_data >> 16) as u8;
    if key <= SHUTDOWN {
        return;
    }
    manager.array.response_total -= 1;
    let buffer = match opcode {
        opcode::Recv::CODE if res > 0 => cqueue::buffer_select(flags).and_then(|bid| {
            manager.array.buffer_ring.as_mut().and_then(|buffer_ring| buffer_ring.use_buffer(bid, res as usize))
        }),
        opcode::Writev::CODE => {
            if let Some((mut buffers, _)) = manager.array.writes.remove(&key) {
                buffer::advance(&mut buffers, res.max(0) as usize);
                if res > 0 && !buffers.is_empty() {
                    manager.handle_event(key, |handle, manager| manager.write(&handle.socket(), buffers).is_ok());
                    return;
                }
            }
            None
        }
        opcode::Connect::CODE => {
            manager.array.addresses.remove(&key);
            None
        }
        _ => None,
    };
    let data = if res < 0 {
        Err(io::Error::from_raw_os_error(-res))
    } else {
        Ok(Event::IoUring(event::Event::new(opcode, res, buffer)))
    };
    manager.handle_event(key, |handle, manager| matches!(handle.handle(manager, data), Ok(true)));
}

impl ScheduledExecutorService {
    pub(crate) fn read<S: AsRawFd>(&mut self, socket: &S) -> io::Result<()> {
        let port = ring(&self.port)?;
        let buffer_ring = self.array.buffer_ring.as_ref().ok_or(io::ErrorKind::Unsupported)?;
        let recv = opcode::Recv::new(types::Fd(socket.as_raw_fd()), ptr::null_mut(), 0).buf_group(buffer_ring.group());
        port.enqueue_sqe(
            recv.build()
                .flags(squeue::Flags::BUFFER_SELECT)
                .user_data(user_data(self.socket_id, opcode::Recv::CODE)),
        )?;
        self.array.response_total += 1;
        Ok(())
    }

    pub(crate) fn write<S: AsRawFd>(&mut self, socket: &S, buffers: Vec<BytesMut>) -> io::Result<()> {
        let port = ring(&self.port)?;
        let iovecs: Vec<IoSlice<'static>> = buffers
            .iter()
            .take(1024)
            .map(|buffer| IoSlice::new(unsafe { slice::from_raw_parts(buffer.as_ptr(), buffer.len()) }))
            .collect();
        let writev = opcode::Writev::new(types::Fd(socket.as_raw_fd()), iovecs.as_ptr().cast(), iovecs.len() as u32);
        port.enqueue_sqe(writev.build().user_data(user_data(self.socket_id, opcode::Writev::CODE)))?;
        self.array.writes.insert(self.socket_id, (buffers, iovecs));
        self.array.response_total += 1;
        Ok(())
    }

    pub(crate) fn accept(&mut self, listener: &TcpListener) -> io::Result<()> {
        let port = ring(&self.port)?;
        let accept = opcode::Accept::new(types::Fd(listener.as_raw_fd()), ptr::null_mut(), ptr::null_mut())
            .flags(SocketFlags::CLOEXEC.bits() as i32);
        port.enqueue_sqe(accept.build().user_data(user_data(self.socket_id, opcode::Accept::CODE)))?;
        self.array.response_total += 1;
        Ok(())
    }

    pub(crate) fn connect(&mut self, stream: &TcpStream, address: &SocketAddr) -> io::Result<()> {
        let port = ring(&self.port)?;
        let address = Box::new(SockAddr::from(*address));
        let connect = opcode::Connect::new(types::Fd(stream.as_raw_fd()), address.as_ptr().cast(), address.len());
        port.enqueue_sqe(connect.build().user_data(user_data(self.socket_id, opcode::Connect::CODE)))?;
        self.array.addresses.insert(self.socket_id, address);
        self.array.response_total += 1;
        Ok(())
    }

    pub(crate) fn cancel<S: AsRawFd>(&mut self, socket: &S) -> io::Result<()> {
        let port = ring(&self.port)?;
        let cancel = opcode::AsyncCancel2::new(types::CancelBuilder::fd(types::Fd(socket.as_raw_fd())).all());
        port.enqueue_sqe(cancel.build().user_data(user_data(DISPATCH, opcode::AsyncCancel2::CODE)))?;
        port.submit().map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handle::Channel;
    use std::io::{Read, Write};
    use std::os::fd::{AsFd, BorrowedFd, FromRawFd};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    struct AcceptChannel {
        listener: TcpListener,
    }

    impl Channel for AcceptChannel {
        fn socket(&self) -> BorrowedFd<'_> {
            self.listener.as_fd()
        }

        fn on_registered(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
            manager.accept(&self.listener).map(|_| true)
        }

        fn handle(&mut self, manager: &mut ScheduledExecutorService, data: io::Result<Event>) -> io::Result<bool> {
            let Event::IoUring(packet) = data? else {
                return Ok(false);
            };
            let stream = unsafe { TcpStream::from_raw_fd(packet.result()) };
            manager.register(Box::new(EchoChannel { stream }))?;
            manager.accept(&self.listener).map(|_| true)
        }

        fn close(&mut self, manager: &mut ScheduledExecutorService) {
            manager.cancel(&self.listener).ok();
        }
    }

    struct EchoChannel {
        stream: TcpStream,
    }

    impl Channel for EchoChannel {
        fn socket(&self) -> BorrowedFd<'_> {
            self.stream.as_fd()
        }

        fn on_registered(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
            manager.read(&self.stream).map(|_| true)
        }

        fn handle(&mut self, manager: &mut ScheduledExecutorService, data: io::Result<Event>) -> io::Result<bool> {
            let Event::IoUring(packet) = data? else {
                return Ok(false);
            };
            match packet.opcode() {
                opcode::Recv::CODE => match packet.into_body() {
                    Some(body) => manager.write(&self.stream, vec![body]).map(|_| true),
                    None => Ok(false),
                },
                _ => manager.read(&self.stream).map(|_| true),
            }
        }

        fn close(&mut self, manager: &mut ScheduledExecutorService) {
            manager.cancel(&self.stream).ok();
        }
    }

    #[test]
    fn echo() {
        let port = crate::handle::uring::CompletionService::new_factory()(64).unwrap();
        let mut manager = ScheduledExecutorService::new(Arc::new(port), std::sync::mpsc::channel().0, 64);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        manager.register(Box::new(AcceptChannel { listener })).unwrap();

        let client = thread::spawn(move || {
            let mut client = TcpStream::connect(address).unwrap();
            let mut received = [0; 4];
            for message in [b"ping", b"pong"] {
                client.write_all(message).unwrap();
                client.read_exact(&mut received).unwrap();
                assert_eq!(&received, message);
            }
            client
        });
        while !client.is_finished() {
            manager.schedule(Duration::from_millis(10), |_| ());
            manager.run_worker().unwrap();
            manager.execute();
        }
        let mut client = client.join().unwrap();
        assert_eq!(manager.sockets.len(), 2);

        manager.close();
        assert_eq!(manager.array.response_total, 0);
        assert_eq!(client.read(&mut [0; 4]).unwrap_or(0), 0);
    }
}
