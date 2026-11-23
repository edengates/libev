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

use crate::handle::epoll::CompletionService;
use crate::handle::epoll::event;
use crate::handle::event::Event;
use crate::utils::concurrent::{ScheduledExecutorService, DISPATCH, SHUTDOWN};
use rustix::event::epoll::{self, EventFlags};
use rustix::io::Errno;
use std::io;
use std::mem;
use std::os::fd::AsFd;

pub(crate) fn run_worker(manager: &mut ScheduledExecutorService, port: &CompletionService) -> io::Result<()> {
    let mut entries = mem::take(&mut manager.array.entries);
    let data = port.poll(&mut entries, manager.queue.timeout());
    if let Ok(count) = data {
        for entry in &entries[..count] {
            handle(manager, entry);
        }
    }
    manager.array.entries = entries;
    data.map(|_| ())
}

pub(crate) fn submit<S: AsFd>(port: &CompletionService, id: usize, socket: &S, flags: u32) -> io::Result<()> {
    if flags == 0 {
        // 0 means there is nothing to handle anymore, unregister the fd as otherwise
        // we might get notified forever because of EPOLLHUP / EPOLLERR.
        return port.unregister(socket);
    }
    let flags = EventFlags::from_bits_retain(flags);
    match port.modify(socket, id, flags) {
        Err(error) if error.raw_os_error() == Some(Errno::NOENT.raw_os_error()) => port.register(socket, id, flags),
        data => data,
    }
}

fn handle(manager: &mut ScheduledExecutorService, entry: &epoll::Event) {
    match { entry.data }.u64() as usize {
        DISPATCH => {}
        SHUTDOWN => manager.is_shutdown = true,
        key => {
            let packet = Event::Epoll(event::Event::new(entry.flags));
            manager.handle_event(key, |handle, manager| matches!(handle.handle(manager, Ok(packet)), Ok(true)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handle::Channel;
    use crate::DefaultSocketManagerBuilder;
    use crate::hooks::ListenerAdapter;
    use crate::utils::concurrent::ScheduledThreadPoolExecutor;
    use crate::handle::socket;
    use crate::handle::{ServerSocketChannel, SocketChannel};
    use std::net::{TcpListener, TcpStream};
    use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
    use std::sync::Arc;
    use std::sync::mpsc::{self, Sender};
    use std::thread;
    use std::time::Duration;

    #[derive(Clone)]
    struct GenericEventAdapter;

    impl ListenerAdapter for GenericEventAdapter {}

    struct AcceptChannel {
        listener: TcpListener,
        sender: Sender<bool>,
    }

    impl Channel for AcceptChannel {
        fn socket(&self) -> BorrowedFd<'_> {
            self.listener.as_fd()
        }

        fn on_registered(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
            manager.submit(&self.listener, 0)?;
            manager.submit(&self.listener, (EventFlags::IN | EventFlags::ET).bits()).map(|_| true)
        }

        fn handle(&mut self, _manager: &mut ScheduledExecutorService, data: io::Result<Event>) -> io::Result<bool> {
            if let Event::Epoll(packet) = data? {
                self.sender.send(packet.is_readable()).ok();
            }
            Ok(true)
        }
    }

    fn manager() -> ScheduledExecutorService {
        let port = Arc::new(crate::handle::epoll::CompletionService::new_factory()(64).unwrap());
        let dispatcher = crate::utils::concurrent::Dispatcher::new(&port).0;
        ScheduledExecutorService::new(port, dispatcher, 64)
    }

    #[test]
    fn submit() {
        let mut manager = manager();
        let (sender, receiver) = mpsc::channel();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        manager.register(Box::new(AcceptChannel { listener, sender })).unwrap();

        let _client = TcpStream::connect(address).unwrap();
        manager.schedule(Duration::from_secs(5), |_| ());
        manager.run_worker().unwrap();
        assert!(receiver.try_recv().unwrap());
    }

    #[test]
    fn reconnect_delay() {
        let socket_manager = DefaultSocketManagerBuilder::new()
            .set_socket_pool(ScheduledThreadPoolExecutor::new(1, 64).unwrap())
            .add_event_listener(GenericEventAdapter)
            .set_reconnect_delay(Duration::from_millis(40))
            .set_max_reconnect_delay(Duration::from_millis(100))
            .build()
            .unwrap();
        let socket = socket::client(&"127.0.0.1:0".parse().unwrap()).unwrap();
        let unbound = TcpListener::from(OwnedFd::from(socket));
        let mut manager = manager();
        manager.register(Box::new(ServerSocketChannel::new(unbound, socket_manager))).unwrap();
        manager.schedule(Duration::from_secs(5), |_| ());

        for expected in [40, 80, 100, 100] {
            manager.run_worker().unwrap();
            let delay = Duration::from_millis(manager.queue.timeout() as u64);
            assert!(delay <= Duration::from_millis(expected));
            assert!(delay > Duration::from_millis(expected - 25));
            thread::sleep(delay);
            manager.execute();
        }
    }

    #[test]
    fn connect_timeout() {
        let socket_manager = DefaultSocketManagerBuilder::new()
            .set_socket_pool(ScheduledThreadPoolExecutor::new(1, 64).unwrap())
            .add_event_listener(GenericEventAdapter)
            .set_connect_timeout(Duration::from_millis(50))
            .build()
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let mut manager = manager();
        let channel = SocketChannel::connect(socket::client(&address).unwrap(), address, socket_manager);
        manager.register(Box::new(channel)).unwrap();
        assert_eq!(manager.sockets.len(), 1);

        thread::sleep(Duration::from_millis(60));
        manager.execute();
        assert!(manager.sockets.is_empty());
    }
}
