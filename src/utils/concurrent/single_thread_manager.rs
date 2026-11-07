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

use crate::handle::SocketQueueFactory;
use crate::utils::concurrent::{DelayedCompletableFuture, ElasticThreadManager};
use crate::handle::SocketHandler;
use crate::handle::{ChannelManager, DISPATCH, SHUTDOWN, SocketQueueManager, Task};
use log::error;
use std::future::Future;
use std::io;
use std::panic::{self, AssertUnwindSafe};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

/// Executor that executes all its submitted tasks in a single thread using the provided [`SocketQueueFactory`].
pub struct SingleThreadManager {
    port: Arc<SocketQueueManager>,
    sender: Sender<Task>,
    shutdown_handle: Mutex<Receiver<()>>,
}

impl SingleThreadManager {
    /// Creates a new instance.
    ///
    /// # Arguments
    ///
    /// - `executor` — The [`ElasticThreadManager`] that is used for dispatching the work
    /// - `capacity` — The maximum number of events handled at once
    /// - `factory` — The [`SocketQueueFactory`] that should be used to handle IO
    pub fn new(executor: &ElasticThreadManager, capacity: usize, factory: SocketQueueFactory) -> io::Result<Self> {
        let port = Arc::new(factory.new_manager(capacity as u32)?);
        let (sender, receiver) = mpsc::channel::<Task>();
        let (signal, shutdown_handle) = mpsc::channel::<()>();
        executor.execute({
            let port = Arc::clone(&port);
            move || {
                let _signal = signal;
                let mut manager = ChannelManager::new(port, capacity);
                while !manager.is_shutdown() {
                    if let Err(error) = manager.run_worker() {
                        error!(error:% = error; "Encountered error in gateway worker");
                        break;
                    }
                    // Now run all tasks before trying to run IO again.
                    while let Ok(task) = receiver.try_recv() {
                        if panic::catch_unwind(AssertUnwindSafe(|| task(&mut manager))).is_err() {
                            error!("Caught an exception in queue processing thread");
                        }
                    }
                    manager.execute();

                    // We should continue with our loop until we confirmed a shutdown.
                }
                manager.close();
            }
        })?;
        Ok(Self {
            port,
            sender,
            shutdown_handle: Mutex::new(shutdown_handle),
        })
    }

    /// The [`SocketQueueManager`] used to handle IO for this [`SingleThreadManager`].
    pub(crate) fn port(&self) -> &SocketQueueManager {
        &self.port
    }

    /// Submits the provided task for execution.
    ///
    /// # Arguments
    ///
    /// - `task` — The task to execute
    ///
    /// # Errors
    ///
    /// Returns an error if this task cannot be accepted for execution.
    ///
    /// # See also
    ///
    /// - [`schedule`](Self::schedule)
    /// - [`enqueue`](Self::enqueue)
    pub fn execute<F: FnOnce() + Send + 'static>(&self, task: F) -> io::Result<()> {
        self.submit(Box::new(move |_: &mut ChannelManager| task()))
    }

    /// Schedules the provided task to be executed after the specified delay.
    ///
    /// # Arguments
    ///
    /// - `delay` — The delay after which the task is executed
    /// - `task` — The task to execute
    ///
    /// # Errors
    ///
    /// Returns an error if this task cannot be accepted for execution.
    ///
    /// # See also
    ///
    /// - [`execute`](Self::execute)
    /// - [`delay`](Self::delay)
    pub fn schedule<F: FnOnce() + Send + 'static>(&self, delay: Duration, task: F) -> io::Result<()> {
        self.submit(Box::new(move |manager: &mut ChannelManager| {
            manager.schedule(delay, move |_| task());
        }))
    }

    /// Schedules a call to complete the returned [`DelayedCompletableFuture`] after the specified delay.
    ///
    /// # Arguments
    ///
    /// - `delay` — The delay after which the [`DelayedCompletableFuture`] is completed
    ///
    /// # Errors
    ///
    /// Returns an error if this task cannot be accepted for execution.
    ///
    /// # Returns
    ///
    /// The [`DelayedCompletableFuture`] representing the delayed operation.
    ///
    /// # See also
    ///
    /// - [`schedule`](Self::schedule)
    pub fn delay(&self, delay: Duration) -> io::Result<DelayedCompletableFuture> {
        let future = DelayedCompletableFuture::new();
        self.schedule(delay, {
            let future = future.clone();
            move || future.complete()
        })?;
        Ok(future)
    }

    /// Submits the provided [`Future`] for execution.
    ///
    /// # Arguments
    ///
    /// - `future` — The [`Future`] to execute
    ///
    /// # Errors
    ///
    /// Returns an error if this task cannot be accepted for execution.
    ///
    /// # See also
    ///
    /// - [`execute`](Self::execute)
    /// - [`delay`](Self::delay)
    pub fn enqueue<F: Future<Output = ()> + Send + 'static>(&self, future: F) -> io::Result<()> {
        self.submit(Box::new(move |manager: &mut ChannelManager| manager.enqueue(Box::pin(future))))
    }

    /// Registers the provided [`SocketHandler`] to this [`SingleThreadManager`] for I/O processing.
    ///
    /// # Arguments
    ///
    /// - `handler` — The function, which creates the [`SocketHandler`] on the thread of this [`SingleThreadManager`]
    ///
    /// # Returns
    ///
    /// The [`Receiver`], which receives the result once the operation completes.
    pub(crate) fn register<F>(&self, handler: F) -> Receiver<io::Result<()>>
    where
        F: FnOnce() -> Box<dyn SocketHandler> + Send + 'static,
    {
        let (sender, receiver) = mpsc::channel();
        let callback = sender.clone();
        let task: Task = Box::new(move |manager: &mut ChannelManager| {
            callback.send(manager.register(handler())).ok();
        });
        if let Err(error) = self.submit(task) {
            sender.send(Err(error)).ok();
        }
        receiver
    }

    /// Adds the provided task to the queue.
    ///
    /// # Arguments
    ///
    /// - `task` — The task to add
    ///
    /// # Errors
    ///
    /// Returns an error if the [`SingleThreadManager`] has been shutdown.
    fn submit(&self, task: Task) -> io::Result<()> {
        self.sender
            .send(task)
            .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;
        self.port().enqueue(DISPATCH)
    }

    /// Shuts down this [`SingleThreadManager`], closing all its connections.
    ///
    /// # Arguments
    ///
    /// - `timeout` — The maximum amount of time to wait until the [`SingleThreadManager`] is shut down
    ///
    /// # See also
    ///
    /// - [`shutdown_now`](Self::shutdown_now)
    /// - [`await_shutdown`](Self::await_shutdown)
    pub fn shutdown(&self, timeout: Duration) {
        self.submit(Box::new(move |manager: &mut ChannelManager| manager.shutdown(timeout)))
            .ok();
    }

    /// Shuts down this [`SingleThreadManager`] instantly, closing all its connections.
    ///
    /// This will also stop the processing of pending tasks.
    ///
    /// # See also
    ///
    /// - [`shutdown`](Self::shutdown)
    /// - [`await_shutdown`](Self::await_shutdown)
    pub fn shutdown_now(&self) -> io::Result<()> {
        self.port().enqueue(SHUTDOWN)
    }

    /// Blocks the current thread until the shutdown has completed.
    ///
    /// **This will not implicitly call [`shutdown`](Self::shutdown), you are responsible to ensure that the
    /// shutdown process has started.**
    ///
    /// # Arguments
    ///
    /// - `timeout` — The maximum time to wait
    ///
    /// # Returns
    ///
    /// `false`, if the timeout has elapsed before the shutdown has completed, `true` otherwise.
    ///
    /// # See also
    ///
    /// - [`shutdown`](Self::shutdown)
    /// - [`shutdown_now`](Self::shutdown_now)
    pub fn await_shutdown(&self, timeout: Duration) -> bool {
        let shutdown_handle = self.shutdown_handle.lock().unwrap_or_else(PoisonError::into_inner);
        matches!(shutdown_handle.recv_timeout(timeout), Err(RecvTimeoutError::Disconnected))
    }
}

impl Drop for SingleThreadManager {
    fn drop(&mut self) {
        self.shutdown_now().ok();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::concurrent::CountingThreadFactory;
    use crate::handle::socket_packet::SocketPacket;
    #[cfg(windows)]
    use crate::handle::socket_packet::{self, Type};
    #[cfg(target_os = "linux")]
    use libc::EPOLLIN;
    #[cfg(any(target_os = "macos", target_os = "freebsd"))]
    use libc::EVFILT_READ;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    #[cfg(unix)]
    use std::os::fd::{AsFd, AsRawFd, BorrowedFd};
    #[cfg(windows)]
    use std::os::windows::io::{AsSocket, BorrowedSocket};
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::thread;
    use std::time::Instant;

    #[test]
    fn execute() {
        let manager = ElasticThreadManager::new(CountingThreadFactory::new(|| "Test", "Loop"));
        let executor = SingleThreadManager::new(&manager, 64, SocketQueueFactory::default()).unwrap();
        let (sender, receiver) = mpsc::channel();

        for index in 1..=3 {
            let sender = sender.clone();
            executor
                .execute(move || {
                    sender
                        .send((index, { thread::current().name().unwrap().to_string() }))
                        .unwrap();
                })
                .unwrap();
        }

        for index in 1..=3 {
            assert_eq!(
                receiver.recv().unwrap(),
                (index, "Test Loop-Worker 1".to_string())
            );
        }
    }

    #[test]
    fn shutdown() {
        let manager = ElasticThreadManager::new(CountingThreadFactory::new(|| "Test", "Loop"));
        let executor = SingleThreadManager::new(&manager, 64, SocketQueueFactory::default()).unwrap();
        let (sender, receiver) = mpsc::channel();

        for index in 1..=3 {
            let sender = sender.clone();
            executor.execute(move || sender.send(index).unwrap()).unwrap();
        }
        drop(sender);
        drop(executor);

        assert_eq!(receiver.iter().collect::<Vec<_>>(), [1, 2, 3]);
    }

    #[test]
    fn schedule() {
        let manager = ElasticThreadManager::new(CountingThreadFactory::new(|| "Test", "Loop"));
        let executor = SingleThreadManager::new(&manager, 64, SocketQueueFactory::default()).unwrap();
        let (sender, receiver) = mpsc::channel();
        let start = Instant::now();

        for delay in [60, 20, 40] {
            let sender = sender.clone();
            executor
                .schedule(Duration::from_millis(delay), move || sender.send(delay).unwrap())
                .unwrap();
        }

        let order: Vec<u64> = (0..3)
            .map(|_| receiver.recv_timeout(Duration::from_secs(5)).unwrap())
            .collect();
        assert_eq!(order, [20, 40, 60]);
        assert!(start.elapsed() >= Duration::from_millis(60));
    }

    struct YieldNow {
        yielded: bool,
    }

    impl Future for YieldNow {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
            if self.yielded {
                return Poll::Ready(());
            }
            self.yielded = true;
            context.waker().wake_by_ref();
            Poll::Pending
        }
    }

    #[test]
    fn enqueue() {
        let manager = ElasticThreadManager::new(CountingThreadFactory::new(|| "Test", "Loop"));
        let executor = SingleThreadManager::new(&manager, 64, SocketQueueFactory::default()).unwrap();
        let (sender, receiver) = mpsc::channel();
        executor
            .enqueue(async {
                YieldNow { yielded: false }.await;
                panic!();
            })
            .unwrap();
        executor
            .enqueue(async move {
                YieldNow { yielded: false }.await;
                sender.send(thread::current().name().unwrap().to_string()).unwrap();
            })
            .unwrap();
        assert_eq!(receiver.recv_timeout(Duration::from_secs(5)).unwrap(), "Test Loop-Worker 1");
    }

    #[test]
    fn delay() {
        let manager = ElasticThreadManager::new(CountingThreadFactory::new(|| "Test", "Loop"));
        let executor = SingleThreadManager::new(&manager, 64, SocketQueueFactory::default()).unwrap();
        let (sender, receiver) = mpsc::channel();
        let start = Instant::now();
        let delay = executor.delay(Duration::from_millis(50)).unwrap();
        executor
            .enqueue(async move {
                delay.await;
                sender.send(start.elapsed()).unwrap();
            })
            .unwrap();
        assert!(receiver.recv_timeout(Duration::from_secs(5)).unwrap() >= Duration::from_millis(50));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn io_uring() {
        let manager = ElasticThreadManager::new(CountingThreadFactory::new(|| "Test", "Loop"));
        let executor = SingleThreadManager::new(&manager, 64, crate::handle::IoUringHandler::new_factory()).unwrap();
        assert!(executor.port().is_io_uring());
        let (sender, receiver) = mpsc::channel();
        let start = Instant::now();
        executor.schedule(Duration::from_millis(20), {
            let sender = sender.clone();
            move || sender.send(()).unwrap()
        }).unwrap();
        executor.execute(move || sender.send(()).unwrap()).unwrap();
        for _ in 0..2 {
            receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        }
        assert!(start.elapsed() >= Duration::from_millis(20));

        executor.shutdown(Duration::from_secs(5));
        assert!(executor.await_shutdown(Duration::from_secs(5)));
    }

    struct SessionControllerAdapter {
        listener: TcpListener,
    }

    impl SocketHandler for SessionControllerAdapter {
        #[cfg(unix)]
        fn socket(&self) -> BorrowedFd<'_> {
            self.listener.as_fd()
        }

        #[cfg(windows)]
        fn socket(&self) -> BorrowedSocket<'_> {
            self.listener.as_socket()
        }

        #[cfg(unix)]
        fn connect(&mut self, manager: &mut ChannelManager) -> io::Result<bool> {
            receive(manager, &self.listener)
        }

        #[cfg(windows)]
        fn connect(&mut self, manager: &mut ChannelManager) -> io::Result<bool> {
            manager.accept(&self.listener).map(|_| true)
        }

        #[cfg(unix)]
        fn handle(&mut self, manager: &mut ChannelManager, data: io::Result<SocketPacket>) -> io::Result<bool> {
            data?;
            let (stream, _) = self.listener.accept()?;
            stream.set_nonblocking(true)?;
            manager.register(Box::new(MessageCreateHandler { stream }))?;
            Ok(true)
        }

        #[cfg(windows)]
        fn handle(&mut self, manager: &mut ChannelManager, data: io::Result<SocketPacket>) -> io::Result<bool> {
            let stream = socket_packet::accepted(&self.listener, data?)?;
            manager.register(Box::new(MessageCreateHandler { stream }))?;
            manager.accept(&self.listener)?;
            Ok(true)
        }
    }

    struct MessageCreateHandler {
        stream: TcpStream,
    }

    impl SocketHandler for MessageCreateHandler {
        #[cfg(unix)]
        fn socket(&self) -> BorrowedFd<'_> {
            self.stream.as_fd()
        }

        #[cfg(windows)]
        fn socket(&self) -> BorrowedSocket<'_> {
            self.stream.as_socket()
        }

        fn connect(&mut self, manager: &mut ChannelManager) -> io::Result<bool> {
            receive(manager, &self.stream)
        }

        #[cfg(unix)]
        fn handle(&mut self, _manager: &mut ChannelManager, data: io::Result<SocketPacket>) -> io::Result<bool> {
            data?;
            let mut buffer = [0; 16];
            match (&self.stream).read(&mut buffer) {
                Ok(0) => Ok(false),
                Ok(length) => (&self.stream).write_all(&buffer[..length]).map(|_| true),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(true),
                Err(error) => Err(error),
            }
        }

        #[cfg(windows)]
        fn handle(&mut self, manager: &mut ChannelManager, data: io::Result<SocketPacket>) -> io::Result<bool> {
            let packet = data?;
            match packet.method() {
                Type::Read if packet.length() == 0 => Ok(false),
                Type::Read => {
                    manager.write(&self.stream, SocketPacket::wrap(vec![packet.into_body()]))?;
                    Ok(true)
                }
                _ => receive(manager, &self.stream),
            }
        }
    }

    struct ErrorChannel {
        listener: TcpListener,
    }

    impl SocketHandler for ErrorChannel {
        #[cfg(unix)]
        fn socket(&self) -> BorrowedFd<'_> {
            self.listener.as_fd()
        }

        #[cfg(windows)]
        fn socket(&self) -> BorrowedSocket<'_> {
            self.listener.as_socket()
        }

        fn connect(&mut self, _manager: &mut ChannelManager) -> io::Result<bool> {
            Err(io::ErrorKind::InvalidInput.into())
        }

        fn handle(&mut self, _manager: &mut ChannelManager, _data: io::Result<SocketPacket>) -> io::Result<bool> {
            Ok(false)
        }
    }

    struct ReconnectChannel {
        listener: TcpListener,
        sender: Sender<usize>,
        count: usize,
    }

    impl SocketHandler for ReconnectChannel {
        #[cfg(unix)]
        fn socket(&self) -> BorrowedFd<'_> {
            self.listener.as_fd()
        }

        #[cfg(windows)]
        fn socket(&self) -> BorrowedSocket<'_> {
            self.listener.as_socket()
        }

        fn connect(&mut self, manager: &mut ChannelManager) -> io::Result<bool> {
            self.count += 1;
            self.sender.send(self.count).ok();
            if self.count < 3 {
                manager.retry(Duration::ZERO);
            }
            Ok(true)
        }

        fn handle(&mut self, _manager: &mut ChannelManager, _data: io::Result<SocketPacket>) -> io::Result<bool> {
            Ok(false)
        }
    }

    struct InvalidChannel {
        stream: TcpStream,
    }

    impl SocketHandler for InvalidChannel {
        #[cfg(unix)]
        fn socket(&self) -> BorrowedFd<'_> {
            self.stream.as_fd()
        }

        #[cfg(windows)]
        fn socket(&self) -> BorrowedSocket<'_> {
            self.stream.as_socket()
        }

        fn connect(&mut self, manager: &mut ChannelManager) -> io::Result<bool> {
            receive(manager, &self.stream)
        }

        fn handle(&mut self, _manager: &mut ChannelManager, _data: io::Result<SocketPacket>) -> io::Result<bool> {
            panic!()
        }
    }

    struct LockedChannel {
        listener: TcpListener,
    }

    impl SocketHandler for LockedChannel {
        #[cfg(unix)]
        fn socket(&self) -> BorrowedFd<'_> {
            self.listener.as_fd()
        }

        #[cfg(windows)]
        fn socket(&self) -> BorrowedSocket<'_> {
            self.listener.as_socket()
        }

        fn connect(&mut self, _manager: &mut ChannelManager) -> io::Result<bool> {
            Ok(true)
        }

        fn handle(&mut self, _manager: &mut ChannelManager, _data: io::Result<SocketPacket>) -> io::Result<bool> {
            Ok(true)
        }

        fn shutdown(&mut self, _manager: &mut ChannelManager) -> io::Result<bool> {
            Ok(true)
        }
    }

    #[cfg(target_os = "linux")]
    fn receive<S: AsRawFd>(manager: &mut ChannelManager, socket: &S) -> io::Result<bool> {
        manager.submit(socket, EPOLLIN as u32).map(|_| true)
    }

    #[cfg(any(target_os = "macos", target_os = "freebsd"))]
    fn receive<S: AsRawFd>(manager: &mut ChannelManager, socket: &S) -> io::Result<bool> {
        manager.submit(socket, EVFILT_READ, true).map(|_| true)
    }

    #[cfg(windows)]
    fn receive(manager: &mut ChannelManager, stream: &TcpStream) -> io::Result<bool> {
        manager.read(stream, SocketPacket::new(16)).map(|_| true)
    }

    fn listener() -> TcpListener {
        TcpListener::bind("127.0.0.1:0").unwrap()
    }

    #[test]
    fn echo() {
        let manager = ElasticThreadManager::new(CountingThreadFactory::new(|| "Test", "Loop"));
        let executor = SingleThreadManager::new(&manager, 64, SocketQueueFactory::default()).unwrap();
        let listener = listener();
        let address = listener.local_addr().unwrap();
        executor.register(move || Box::new(SessionControllerAdapter { listener })).recv().unwrap().unwrap();

        let mut received = [0; 4];
        for _ in 0..2 {
            let mut client = TcpStream::connect(address).unwrap();
            for message in [b"ping", b"pong"] {
                client.write_all(message).unwrap();
                client.read_exact(&mut received).unwrap();
                assert_eq!(&received, message);
            }
        }
    }

    #[test]
    fn register() {
        let manager = ElasticThreadManager::new(CountingThreadFactory::new(|| "Test", "Loop"));
        let executor = SingleThreadManager::new(&manager, 64, SocketQueueFactory::default()).unwrap();
        let data = executor.register(move || Box::new(ErrorChannel { listener: listener() }));
        assert!(data.recv().unwrap().is_err());
    }

    #[test]
    fn retry() {
        let manager = ElasticThreadManager::new(CountingThreadFactory::new(|| "Test", "Loop"));
        let executor = SingleThreadManager::new(&manager, 64, SocketQueueFactory::default()).unwrap();
        let (sender, receiver) = mpsc::channel();
        let channel = ReconnectChannel { listener: listener(), sender, count: 0 };
        executor.register(move || Box::new(channel)).recv().unwrap().unwrap();

        for count in 1..=3 {
            assert_eq!(receiver.recv_timeout(Duration::from_secs(5)).unwrap(), count);
        }
    }

    #[test]
    fn panic() {
        let manager = ElasticThreadManager::new(CountingThreadFactory::new(|| "Test", "Loop"));
        let executor = SingleThreadManager::new(&manager, 64, SocketQueueFactory::default()).unwrap();
        let listener = listener();
        let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (stream, _) = listener.accept().unwrap();
        executor.register(move || Box::new(InvalidChannel { stream })).recv().unwrap().unwrap();

        let mut received = [0; 4];
        client.write_all(b"ping").unwrap();
        assert_eq!(client.read(&mut received).unwrap_or(0), 0);

        executor.execute(|| panic!()).unwrap();
        let (sender, receiver) = mpsc::channel();
        executor.execute(move || sender.send(()).unwrap()).unwrap();
        receiver.recv_timeout(Duration::from_secs(5)).unwrap();
    }

    #[test]
    fn shutdown_timeout() {
        let manager = ElasticThreadManager::new(CountingThreadFactory::new(|| "Test", "Loop"));
        let executor = SingleThreadManager::new(&manager, 64, SocketQueueFactory::default()).unwrap();
        executor.register(move || Box::new(LockedChannel { listener: listener() })).recv().unwrap().unwrap();

        executor.shutdown(Duration::from_millis(100));
        assert!(!executor.await_shutdown(Duration::from_millis(50)));
        assert!(executor.await_shutdown(Duration::from_secs(5)));
    }

    #[test]
    fn close() {
        let manager = ElasticThreadManager::new(CountingThreadFactory::new(|| "Test", "Loop"));
        let executor = SingleThreadManager::new(&manager, 64, SocketQueueFactory::default()).unwrap();
        let listener = listener();
        let address = listener.local_addr().unwrap();
        executor.register(move || Box::new(SessionControllerAdapter { listener })).recv().unwrap().unwrap();

        let mut client = TcpStream::connect(address).unwrap();
        let mut received = [0; 4];
        client.write_all(b"ping").unwrap();
        client.read_exact(&mut received).unwrap();
        drop(executor);

        assert_eq!(client.read(&mut received).unwrap_or(0), 0);
    }
}
