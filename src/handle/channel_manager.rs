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

use crate::handle::SocketHandler;
use crate::handle::{ChannelState, SocketQueueManager, UnlockHook};
use rustc_hash::FxHashMap;
use crate::utils::concurrent::{QueueWorker, WorkTask};
use bytes::BytesMut;
use log::{debug, error, warn};
use std::io;
use std::mem;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::task::Waker;
use std::time::{Duration, Instant};

pub type Task = Box<dyn FnOnce(&mut ChannelManager) + Send>;

pub const DISPATCH: usize = 0;
pub const SHUTDOWN: usize = 1;

/// Runs the sockets of one [`SingleThreadManager`](crate::utils::concurrent::SingleThreadManager) on its thread.
pub struct ChannelManager {
    pub(in crate::handle) port: Arc<SocketQueueManager>,
    pub(in crate::handle) sockets: FxHashMap<usize, Box<dyn SocketHandler>>,
    sequence: usize,
    unlock_hook: Option<Arc<UnlockHook>>,
    pub(in crate::handle) socket_id: usize,
    pub(in crate::handle) read_buffer: BytesMut,
    pub(in crate::handle) state: ChannelState,
    is_stopped: bool,
    pub(in crate::handle) is_shutdown: bool,
    pub(in crate::handle) worker: QueueWorker<ChannelManager>,
}

impl ChannelManager {
    pub(crate) fn new(port: Arc<SocketQueueManager>, capacity: usize) -> Self {
        Self {
            state: ChannelState::new(&port, capacity),
            port,
            sockets: FxHashMap::default(),
            sequence: 2,
            unlock_hook: None,
            socket_id: 0,
            read_buffer: BytesMut::new(),
            is_stopped: false,
            is_shutdown: false,
            worker: QueueWorker::new(),
        }
    }

    pub(crate) fn shutdown(&mut self, timeout: Duration) {
        if self.is_stopped {
            return;
        }
        self.is_stopped = true;
        self.schedule(timeout, |manager| {
            warn!("Shutdown timeout! Closing {} remaining connections now...", manager.sockets.len());
            manager.is_shutdown = true;
        });
        let keys: Vec<usize> = self.sockets.keys().copied().collect();
        for key in keys {
            self.handle_event(key, |handle, manager| matches!(handle.shutdown(manager), Ok(true)));
        }
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.is_shutdown || (self.is_stopped && self.sockets.is_empty() && self.worker.is_empty())
    }

    pub(in crate::handle) fn handle_event<F>(&mut self, key: usize, call: F)
    where
        F: FnOnce(&mut dyn SocketHandler, &mut ChannelManager) -> bool,
    {
        if let Some(mut handle) = self.sockets.remove(&key) {
            let socket_id = mem::replace(&mut self.socket_id, key);
            match panic::catch_unwind(AssertUnwindSafe(|| call(handle.as_mut(), self))) {
                Ok(true) => {
                    self.sockets.insert(key, handle);
                }
                Ok(false) => {
                    self.stop_keep_alive(key);
                    self.close_resource(key, handle);
                }
                Err(_) => {
                    error!("One of the EventListeners had an uncaught exception");
                    self.stop_keep_alive(key);
                    self.close_resource(key, handle);
                }
            }
            self.socket_id = socket_id;
        }
    }

    pub fn schedule<F>(&mut self, delay: Duration, task: F) -> (Instant, usize)
    where
        F: FnOnce(&mut ChannelManager) + Send + 'static,
    {
        self.worker.schedule(delay, Box::new(task))
    }

    pub(crate) fn execute(&mut self) {
        let now = Instant::now();
        while let Some(task) = self.worker.next(now) {
            if panic::catch_unwind(AssertUnwindSafe(|| task(self))).is_err() {
                error!("An error occurred while running the timeout callback");
            }
        }
        for (socket_id, is_done) in self.worker.run_pending_tasks() {
            if is_done {
                self.handle_event(socket_id, |handle, manager| matches!(handle.execute(manager), Ok(true)));
            } else {
                self.handle_event(socket_id, |_, _| false);
            }
        }
    }

    /// # Panics
    ///
    /// Panics if the first poll of `task` panics.
    pub(crate) fn enqueue(&mut self, task: WorkTask) {
        let port = &self.port;
        let unlock_hook = &mut self.unlock_hook;
        self.worker.enqueue(task, self.socket_id, |response_number, sender| {
            match unlock_hook.as_mut().and_then(Arc::get_mut) {
                Some(hook) => hook.set_response_number(response_number),
                None => {
                    *unlock_hook = Some(Arc::new(UnlockHook::new(response_number, sender.clone(), Arc::clone(port))));
                }
            }
            Waker::from(unlock_hook.clone().unwrap())
        });
    }

    /// # Panics
    ///
    /// Panics if [`SocketHandler::connect`] panics.
    pub fn register(&mut self, mut handle: Box<dyn SocketHandler>) -> io::Result<()> {
        if self.is_stopped {
            debug!("Rejected task after shutdown");
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        let key = self.sequence;
        self.sequence += 1;
        self.port.register(&handle.socket(), key)?;
        let socket_id = mem::replace(&mut self.socket_id, key);
        let data = handle.connect(self);
        self.socket_id = socket_id;
        if let Ok(true) = data {
            self.sockets.insert(key, handle);
            return Ok(());
        }
        self.stop_keep_alive(key);
        self.close_resource(key, handle);
        data.map(|_| ())
    }

    pub fn retry(&mut self, delay: Duration) {
        let key = self.socket_id;
        self.schedule(delay, move |manager| manager.reconnect(key));
    }

    pub fn setup_keep_alive(&mut self, delay: Duration) {
        let key = self.socket_id;
        self.worker.setup_keep_alive(
            key,
            delay,
            Box::new(move |manager: &mut ChannelManager| {
                manager.handle_event(key, |handle, manager| matches!(handle.check_expired(manager), Ok(true)));
            }),
        );
    }

    fn stop_keep_alive(&mut self, key: usize) {
        self.worker.stop_keep_alive(key);
    }

    pub(crate) fn deadline(&mut self, delay: Duration) {
        let key = self.socket_id;
        self.schedule(delay, move |manager| manager.worker.cancel(key));
    }

    pub(in crate::handle) fn close_resource(&mut self, key: usize, mut handle: Box<dyn SocketHandler>) {
        let socket_id = mem::replace(&mut self.socket_id, key);
        panic::catch_unwind(AssertUnwindSafe(|| {
            handle.close(self);
            drop(handle);
        }))
        .ok();
        self.socket_id = socket_id;
    }

    fn reconnect(&mut self, key: usize) {
        self.handle_event(key, |handle, manager| matches!(handle.connect(manager), Ok(true)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handle::socket_packet::SocketPacket;
    use std::net::TcpListener;
    #[cfg(unix)]
    use std::os::fd::{AsFd, BorrowedFd};
    #[cfg(windows)]
    use std::os::windows::io::{AsSocket, BorrowedSocket};

    struct HeartbeatChannel {
        listener: TcpListener,
        active: bool,
    }

    impl SocketHandler for HeartbeatChannel {
        #[cfg(unix)]
        fn socket(&self) -> BorrowedFd<'_> {
            self.listener.as_fd()
        }

        #[cfg(windows)]
        fn socket(&self) -> BorrowedSocket<'_> {
            self.listener.as_socket()
        }

        fn connect(&mut self, manager: &mut ChannelManager) -> io::Result<bool> {
            manager.setup_keep_alive(Duration::from_secs(60));
            manager.setup_keep_alive(Duration::from_secs(60));
            Ok(self.active)
        }

        fn handle(&mut self, _manager: &mut ChannelManager, _data: io::Result<SocketPacket>) -> io::Result<bool> {
            Ok(true)
        }
    }

    #[cfg(target_os = "linux")]
    fn manager() -> ChannelManager {
        ChannelManager::new(Arc::new(crate::handle::SocketQueueFactory::default().new_manager(64).unwrap()), 64)
    }

    #[cfg(any(windows, target_os = "macos", target_os = "freebsd"))]
    fn manager() -> ChannelManager {
        ChannelManager::new(Arc::new(SocketQueueManager::new().unwrap()), 64)
    }

    fn channel(active: bool) -> Box<HeartbeatChannel> {
        Box::new(HeartbeatChannel {
            listener: TcpListener::bind("127.0.0.1:0").unwrap(),
            active,
        })
    }

    #[test]
    fn keep_alive() {
        let mut manager = manager();
        manager.register(channel(true)).unwrap();
        assert_eq!(manager.worker.len(), 1);
        manager.register(channel(false)).unwrap();
        assert_eq!(manager.worker.len(), 1);
        assert_eq!(manager.sockets.len(), 1);
    }

    #[test]
    fn register() {
        let mut manager = manager();
        manager.shutdown(Duration::from_secs(5));
        assert!(manager.is_shutdown());
        assert!(manager.register(channel(true)).is_err());
    }

    #[test]
    fn enqueue() {
        let mut manager = manager();
        manager.enqueue(Box::pin(async {}));
        let hook = Arc::as_ptr(manager.unlock_hook.as_ref().unwrap());
        manager.enqueue(Box::pin(async {}));
        assert_eq!(Arc::as_ptr(manager.unlock_hook.as_ref().unwrap()), hook);
        manager.enqueue(Box::pin(std::future::pending()));
        manager.enqueue(Box::pin(async {}));
        assert_ne!(Arc::as_ptr(manager.unlock_hook.as_ref().unwrap()), hook);
    }

    #[test]
    fn shutdown() {
        let mut manager = manager();
        manager.port.enqueue(SHUTDOWN).unwrap();
        manager.run_worker().unwrap();
        assert!(manager.is_shutdown());
    }
}
