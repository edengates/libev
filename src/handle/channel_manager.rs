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

/// A task, which is executed on the thread of the [`ChannelManager`].
///
/// The tasks of a [`SingleThreadManager`](crate::utils::concurrent::SingleThreadManager) are executed one after
/// another in the order they have been added, before the scheduled tasks and the pending tasks of the listeners. If a
/// task calls [`panic!`], an error is logged and the next task is executed.
pub type Task = Box<dyn FnOnce(&mut ChannelManager) + Send>;

/// The identifier, which is sent to the [`SocketQueueManager`] to handle the queued tasks.
///
/// If the [`ChannelManager`] is waiting for events, it stops waiting, so the queued tasks are executed as soon as
/// possible. This can be sent from every thread.
///
/// **Note:** No registered socket uses this identifier.
///
/// # See also
///
/// - [`SHUTDOWN`]
/// - [`SocketQueueManager::enqueue`]
pub const DISPATCH: usize = 0;
/// The identifier, which is sent to the [`SocketQueueManager`] to shut down the [`ChannelManager`] instantly.
///
/// Unlike [`ChannelManager::shutdown`], this does **not** wait for the handlers: once it has been received, **all**
/// remaining handlers are closed with [`SocketHandler::close`] and no more events are handled. This can be sent from
/// every thread.
///
/// **Note:** No registered socket uses this identifier.
///
/// # See also
///
/// - [`DISPATCH`]
/// - [`ChannelManager::shutdown`]
/// - [`ChannelManager::is_shutdown`]
pub const SHUTDOWN: usize = 1;

/// Handles each socket of a [`SingleThreadManager`](crate::utils::concurrent::SingleThreadManager) on its thread.
///
/// Every registered [`SocketHandler`] has its own identifier, which is used for its socket in the
/// [`SocketQueueManager`]. Each event of the [`SocketQueueManager`] is handled by the [`SocketHandler`] of its
/// identifier.
///
/// In a loop, the thread:
/// - is waiting for the events of the [`SocketQueueManager`] until the next scheduled task is ready, and handles them
/// - then executes the queued [`Task`]s
/// - and at last the scheduled tasks and the pending tasks of the listeners with [`execute`](Self::execute)
///
/// The loop continues until [`is_shutdown`](Self::is_shutdown) returns `true`.
///
/// **An [`ChannelManager`] cannot be moved to another thread, so all of its methods are called on its own thread.**
/// The current handler is the [`SocketHandler`], whose method is called at the moment. It is used by methods like
/// [`retry`](Self::retry), [`setup_keep_alive`](Self::setup_keep_alive) and [`deadline`](Self::deadline).
///
/// # See also
///
/// - [`SocketHandler`]
/// - [`SocketQueueManager`]
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
    /// Creates a new instance with the provided [`SocketQueueManager`], which handles up to `capacity` events at once.
    ///
    /// # Arguments
    ///
    /// - `port` — The [`SocketQueueManager`] to use
    /// - `capacity` — The maximum amount of events, which are handled at once
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

    /// Shuts down each socket, the remaining connections are closed after the specified timeout.
    ///
    /// [`SocketHandler::shutdown`] is called for each registered handler. A handler is closed at once, if it:
    /// - returns `false`
    /// - returns an error
    /// - calls [`panic!`]
    ///
    /// **From now on, [`register`](Self::register) returns an error.**
    ///
    /// The shutdown has completed once **all** handlers have been closed and **all** pending tasks of the listeners
    /// have finished, see [`is_shutdown`](Self::is_shutdown). If this has not happened *before* the timeout, the
    /// remaining connections are closed with [`SocketHandler::close`]. If [`shutdown`](Self::shutdown) has already been
    /// invoked, this does nothing.
    ///
    /// # Arguments
    ///
    /// - `timeout` — The timeout, after which the remaining connections are closed with [`SocketHandler::close`]
    ///
    /// # See also
    ///
    /// - [`SocketManager::shutdown`](crate::SocketManager::shutdown)
    /// - [`is_shutdown`](Self::is_shutdown)
    /// - [`SHUTDOWN`]
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

    /// Whether the shutdown has completed.
    ///
    /// This is `true`:
    /// - once [`SHUTDOWN`] has been received
    /// - once the timeout of [`shutdown`](Self::shutdown) has elapsed
    /// - or once **all** handlers have been closed and **all** pending tasks of the listeners have finished after
    ///   [`shutdown`](Self::shutdown)
    ///
    /// Scheduled tasks, which are not ready yet, are **not** taken into account.
    ///
    /// # See also
    ///
    /// - [`shutdown`](Self::shutdown)
    /// - [`SHUTDOWN`]
    pub(crate) fn is_shutdown(&self) -> bool {
        self.is_shutdown || (self.is_stopped && self.sockets.is_empty() && self.worker.is_empty())
    }

    /// Calls the provided function with the [`SocketHandler`] of the provided identifier.
    ///
    /// While the function is called, the handler is the current handler. If there is no handler with this identifier,
    /// for example because it has been closed already, this does nothing.
    ///
    /// The handler is closed, if the function returns `false` or calls [`panic!`]. In this case, the keep alive of the
    /// handler is canceled and [`close_resource`](Self::close_resource) is called. If the function calls [`panic!`], an
    /// error is logged.
    ///
    /// # Arguments
    ///
    /// - `key` — The identifier of the [`SocketHandler`]
    /// - `call` — The function, which is called with the [`SocketHandler`] and this [`ChannelManager`]
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

    /// Schedules the task to be executed after the specified delay.
    ///
    /// The task is executed on the thread of this [`ChannelManager`] by [`execute`](Self::execute), once the
    /// delay has elapsed. Tasks with the same deadline are executed in the order they have been scheduled. The thread
    /// does **not** wait for events longer than until the next scheduled task is ready. If the task calls [`panic!`],
    /// an error is logged and the next task is executed.
    ///
    /// # Arguments
    ///
    /// - `delay` — The delay after which the task should be executed
    /// - `task` — The task to schedule
    ///
    /// # Returns
    ///
    /// The identifier of the scheduled task.
    ///
    /// # See also
    ///
    /// - [`execute`](Self::execute)
    /// - [`retry`](Self::retry)
    pub fn schedule<F>(&mut self, delay: Duration, task: F) -> (Instant, usize)
    where
        F: FnOnce(&mut ChannelManager) + Send + 'static,
    {
        self.worker.schedule(delay, Box::new(task))
    }

    /// Executes all scheduled tasks, which are ready, and handles all pending tasks of the listeners.
    ///
    /// The scheduled tasks are executed first, in the order of their deadline. Then each pending task, which can
    /// continue (see [`UnlockHook`]), is handled again, and [`SocketHandler::execute`] of its handler is called. The
    /// handler is closed:
    /// - If the task calls [`panic!`]
    /// - If [`SocketHandler::execute`] returns `false` or an error
    /// - If [`SocketHandler::execute`] calls [`panic!`]
    ///
    /// # See also
    ///
    /// - [`schedule`](Self::schedule)
    /// - [`enqueue`](Self::enqueue)
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

    /// Handles the provided task of a listener, until it has finished.
    ///
    /// The task is handled immediately for the first time. If the task has not finished yet, it belongs to the
    /// current handler from now on.
    ///
    /// <div class="warning">
    ///
    /// A [`panic!`] during this first call is **not** handled here and is passed to the caller.
    ///
    /// </div>
    ///
    /// Each time the task can continue, it is handled again on the thread of this [`ChannelManager`], see
    /// [`UnlockHook`]. The same [`UnlockHook`] is used for the next task, as long as no other [`Waker`] of it exists.
    /// The pending tasks of a handler can be canceled with [`deadline`](Self::deadline).
    ///
    /// # Arguments
    ///
    /// - `task` — The task of the listener
    ///
    /// # See also
    ///
    /// - [`execute`](Self::execute)
    /// - [`deadline`](Self::deadline)
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

    /// Registers the provided [`SocketHandler`] and calls [`SocketHandler::connect`].
    ///
    /// The handler gets a new identifier, which is used to register its socket to the [`SocketQueueManager`]. While
    /// [`SocketHandler::connect`] is called, the handler is the current handler. If it returns `false` or an error, the
    /// handler is closed with [`SocketHandler::close`], but **only** an error is returned.
    ///
    /// <div class="warning">
    ///
    /// Unlike [`handle_event`](Self::handle_event), a [`panic!`] of [`SocketHandler::connect`] is **not** handled here.
    ///
    /// </div>
    ///
    /// # Arguments
    ///
    /// - `handle` — The [`SocketHandler`] to register
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`BrokenPipe`](io::ErrorKind::BrokenPipe) if [`shutdown`](Self::shutdown) has already been invoked
    /// - The error of the [`SocketQueueManager`] if the socket could not be registered
    /// - The error of [`SocketHandler::connect`]
    ///
    /// In the first two cases, neither [`SocketHandler::connect`] nor [`SocketHandler::close`] is called.
    ///
    /// # See also
    ///
    /// - [`shutdown`](Self::shutdown)
    /// - [`close_resource`](Self::close_resource)
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

    /// Schedules another call to [`SocketHandler::connect`] of the current handler after the specified delay.
    ///
    /// Unlike [`setup_keep_alive`](Self::setup_keep_alive), **every** call schedules another call. If the handler has
    /// been closed *before*, the call does nothing. The handler is closed, if [`SocketHandler::connect`] returns
    /// `false`, an error or calls [`panic!`].
    ///
    /// # Arguments
    ///
    /// - `delay` — The delay after which [`SocketHandler::connect`] should be called
    ///
    /// # See also
    ///
    /// - [`setup_keep_alive`](Self::setup_keep_alive)
    /// - [`schedule`](Self::schedule)
    pub fn retry(&mut self, delay: Duration) {
        let key = self.socket_id;
        self.schedule(delay, move |manager| manager.reconnect(key));
    }

    /// Schedules a call to [`SocketHandler::check_expired`] of the current handler after the specified delay.
    ///
    /// This replaces the previous call, which has been scheduled with this method. The call is done **only** once, and
    /// canceled once the handler is closed. The handler is closed, if [`SocketHandler::check_expired`] returns
    /// `false`, an error or calls [`panic!`].
    ///
    /// # Arguments
    ///
    /// - `delay` — The delay after which [`SocketHandler::check_expired`] should be called
    ///
    /// # See also
    ///
    /// - [`retry`](Self::retry)
    /// - [`deadline`](Self::deadline)
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

    /// Cancels the call of [`SocketHandler::check_expired`] for the provided handler.
    ///
    /// If no call has been scheduled with [`setup_keep_alive`](Self::setup_keep_alive), this does nothing.
    ///
    /// # Arguments
    ///
    /// - `key` — The identifier of the [`SocketHandler`]
    fn stop_keep_alive(&mut self, key: usize) {
        self.worker.stop_keep_alive(key);
    }

    /// Cancels all pending tasks of the current handler after the specified delay.
    ///
    /// **Only** the tasks, which are still pending once the delay has elapsed, are canceled. They are **not** handled
    /// again, and the handler stays registered.
    ///
    /// # Arguments
    ///
    /// - `delay` — The delay after which the pending tasks are canceled
    ///
    /// # See also
    ///
    /// - [`enqueue`](Self::enqueue)
    /// - [`setup_keep_alive`](Self::setup_keep_alive)
    pub(crate) fn deadline(&mut self, delay: Duration) {
        let key = self.socket_id;
        self.schedule(delay, move |manager| manager.worker.cancel(key));
    }

    /// Closes the provided [`SocketHandler`] with [`SocketHandler::close`].
    ///
    /// While [`SocketHandler::close`] is called, the handler is the current handler. A [`panic!`] of it is ignored.
    /// The keep alive of the handler is **not** canceled here.
    ///
    /// # Arguments
    ///
    /// - `key` — The identifier of the [`SocketHandler`]
    /// - `handle` — The [`SocketHandler`] to close
    ///
    /// # See also
    ///
    /// - [`stop_keep_alive`](Self::stop_keep_alive)
    pub(in crate::handle) fn close_resource(&mut self, key: usize, mut handle: Box<dyn SocketHandler>) {
        let socket_id = mem::replace(&mut self.socket_id, key);
        panic::catch_unwind(AssertUnwindSafe(|| {
            handle.close(self);
            drop(handle);
        }))
        .ok();
        self.socket_id = socket_id;
    }

    /// Calls [`SocketHandler::connect`] of the provided handler again.
    ///
    /// If the handler has been closed *before*, this does nothing.
    ///
    /// # Arguments
    ///
    /// - `key` — The identifier of the [`SocketHandler`]
    ///
    /// # See also
    ///
    /// - [`retry`](Self::retry)
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
