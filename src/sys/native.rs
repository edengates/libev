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

use crate::SocketManager;
use crate::utils::concurrent::{ExecutorCompletionService, ScheduledExecutorService};
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
#[cfg(target_os = "linux")]
use std::os::fd::AsFd;
#[cfg(any(target_os = "macos", target_os = "freebsd"))]
use std::os::fd::AsRawFd;
pub(crate) use std::os::fd::BorrowedFd as BorrowedSocket;
use std::sync::mpsc::Receiver;

#[cfg(target_os = "linux")]
#[path = "epoll/mod.rs"]
pub(crate) mod epoll;
#[cfg(target_os = "linux")]
#[path = "uring/mod.rs"]
pub(crate) mod uring;
#[cfg(target_os = "linux")]
pub use linux::{CompletionService, EventArray};
#[cfg(target_os = "linux")]
pub(crate) use linux::event;
#[cfg(any(target_os = "macos", target_os = "freebsd"))]
#[path = "kqueue/mod.rs"]
pub(crate) mod kqueue;
#[cfg(any(target_os = "macos", target_os = "freebsd"))]
pub use kqueue::{CompletionService, EventArray};
#[cfg(any(target_os = "macos", target_os = "freebsd"))]
pub(crate) use kqueue::event;
#[path = "socket.rs"]
pub(crate) mod socket;
#[path = "socket_channel.rs"]
mod socket_channel;
pub use socket_channel::SocketChannel;
#[path = "server_socket_channel.rs"]
mod server_socket_channel;
pub use server_socket_channel::ServerSocketChannel;

#[cfg(target_os = "linux")]
pub(crate) const READABLE: u32 = rustix::event::epoll::EventFlags::IN.bits();
#[cfg(target_os = "linux")]
pub(crate) const WRITABLE: u32 = rustix::event::epoll::EventFlags::OUT.bits();
#[cfg(any(target_os = "macos", target_os = "freebsd"))]
pub(crate) const READABLE: u32 = 1;
#[cfg(any(target_os = "macos", target_os = "freebsd"))]
pub(crate) const WRITABLE: u32 = 2;

#[cfg(target_os = "linux")]
mod linux {
    use super::{epoll, uring};
    use crate::utils::concurrent::ScheduledExecutorService;
    use bytes::BytesMut;
    use rustc_hash::FxHashMap;
    use rustix::event::epoll::{self as poll, EventData, EventFlags};
    use socket2::SockAddr;
    use std::io::{self, IoSlice};
    use std::os::fd::AsFd;
    use std::sync::Arc;
    use uring::BufferRing;

    pub(crate) mod event {
        use super::super::{epoll, uring};

        pub enum Event {
            Epoll(epoll::event::Event),
            IoUring(uring::event::Event),
        }

        impl Event {
            pub(crate) fn is_readable(&self) -> bool {
                matches!(self, Self::Epoll(packet) if packet.is_readable())
            }

            pub(crate) fn is_writable(&self) -> bool {
                matches!(self, Self::Epoll(packet) if packet.is_writable())
            }
        }
    }

    pub enum CompletionService {
        Epoll(epoll::CompletionService),
        IoUring(uring::CompletionService),
    }

    impl CompletionService {
        pub(crate) fn new_factory() -> fn(u32) -> io::Result<Self> {
            |entries| match uring::CompletionService::new_factory()(entries) {
                Err(error) if matches!(error.kind(), io::ErrorKind::Unsupported | io::ErrorKind::PermissionDenied) => {
                    epoll::CompletionService::new_factory()(entries)
                }
                result => result,
            }
        }

        pub(crate) fn is_io_uring(&self) -> bool {
            matches!(self, Self::IoUring(_))
        }

        pub(crate) fn wakeup(&self, id: usize) -> io::Result<()> {
            match self {
                Self::Epoll(port) => port.wakeup(id),
                Self::IoUring(port) => port.wakeup(id),
            }
        }

        pub(crate) fn register<S: AsFd>(&self, socket: &S, id: usize) -> io::Result<()> {
            match self {
                Self::Epoll(port) => port.register(socket, id, EventFlags::empty()),
                Self::IoUring(_) => Ok(()),
            }
        }
    }

    pub struct EventArray {
        pub(in crate::handle) entries: Vec<poll::Event>,
        pub(in crate::handle) response_total: usize,
        pub(in crate::handle) buffer_ring: Option<BufferRing>,
        pub(in crate::handle) writes: FxHashMap<usize, (Vec<BytesMut>, Vec<IoSlice<'static>>)>,
        pub(in crate::handle) addresses: FxHashMap<usize, Box<SockAddr>>,
    }

    impl EventArray {
        pub(crate) fn new(port: &Arc<CompletionService>, capacity: usize) -> Self {
            let buffer_ring = match &**port {
                CompletionService::IoUring(_) => {
                    BufferRing::new(port, capacity.max(2).next_power_of_two() as u16, 16384).ok()
                }
                CompletionService::Epoll(_) => None,
            };
            Self {
                entries: vec![poll::Event { flags: EventFlags::empty(), data: EventData::new_u64(0) }; capacity.max(1)],
                response_total: 0,
                buffer_ring,
                writes: FxHashMap::default(),
                addresses: FxHashMap::default(),
            }
        }
    }

    impl ScheduledExecutorService {
        pub(crate) fn run_worker(&mut self) -> io::Result<()> {
            let port = Arc::clone(&self.port);
            match &*port {
                CompletionService::Epoll(port) => epoll::run_worker(self, port),
                CompletionService::IoUring(port) => uring::run_worker(self, port),
            }
        }

        pub(crate) fn submit<S: AsFd>(&mut self, socket: &S, flags: u32) -> io::Result<()> {
            match &*self.port {
                CompletionService::Epoll(port) => epoll::submit(port, self.socket_id, socket, flags),
                CompletionService::IoUring(_) => Err(io::ErrorKind::Unsupported.into()),
            }
        }

        pub(crate) fn close(&mut self) {
            self.close_sockets();
            if let CompletionService::IoUring(port) = &*Arc::clone(&self.port) {
                uring::close(self, port);
            }
        }
    }

}

#[cfg(target_os = "linux")]
pub(crate) fn submit<S: AsFd>(manager: &mut ScheduledExecutorService, socket: &S, flags: u32, _flag: u32) -> io::Result<()> {
    manager.submit(socket, flags)
}

#[cfg(any(target_os = "macos", target_os = "freebsd"))]
pub(crate) fn submit<S: AsRawFd>(manager: &mut ScheduledExecutorService, socket: &S, flags: u32, flag: u32) -> io::Result<()> {
    let filter = if flag == WRITABLE {
        rustix::event::kqueue::EventFilter::Write
    } else {
        rustix::event::kqueue::EventFilter::Read
    };
    manager.submit(socket, filter, flags & flag != 0)
}

#[cfg(target_os = "linux")]
pub(crate) fn as_socket(stream: &TcpStream) -> BorrowedSocket<'_> {
    stream.as_fd()
}

pub(crate) fn bind(pool: &ExecutorCompletionService, listener: TcpListener, socket_manager: SocketManager) -> Receiver<io::Result<()>> {
    #[cfg(target_os = "linux")]
    if pool.port().is_io_uring() {
        return pool.register(move || Box::new(uring::ServerSocketChannel::new(listener, socket_manager)));
    }
    pool.register(move || Box::new(ServerSocketChannel::new(listener, socket_manager)))
}

pub(crate) fn connect(
    pool: &ExecutorCompletionService,
    socket: TcpStream,
    address: SocketAddr,
    socket_manager: SocketManager,
) -> Receiver<io::Result<()>> {
    #[cfg(target_os = "linux")]
    if pool.port().is_io_uring() {
        return pool.register(move || Box::new(uring::SocketChannel::connect(socket, address, socket_manager)));
    }
    pool.register(move || Box::new(SocketChannel::connect(socket, address, socket_manager)))
}

pub(crate) fn register(pool: &ExecutorCompletionService, stream: TcpStream, socket_manager: SocketManager) -> Receiver<io::Result<()>> {
    #[cfg(target_os = "linux")]
    if pool.port().is_io_uring() {
        return pool.register(move || Box::new(uring::SocketChannel::new(stream, socket_manager)));
    }
    pool.register(move || Box::new(SocketChannel::new(stream, socket_manager)))
}
