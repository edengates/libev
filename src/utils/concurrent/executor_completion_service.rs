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

use crate::utils::concurrent::{DelayedCompletableFuture, ThreadPerTaskExecutor};
use crate::handle::SocketHandler;
use crate::handle::CompletionService;
use crate::utils::concurrent::{Dispatcher, ScheduledExecutorService, SHUTDOWN, Task};
use log::error;
use std::future::Future;
use std::io;
use std::panic::{self, AssertUnwindSafe};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

/// Executor that executes all its submitted tasks in a single thread using the [`CompletionService`].
///
/// Once it is shut down, new tasks are rejected with an error.
pub struct ExecutorCompletionService {
    port: Arc<CompletionService>,
    dispatcher: Dispatcher,
    shutdown_handle: Mutex<Receiver<()>>,
}

impl ExecutorCompletionService {
    /// Starts the thread of this manager on the `executor`.
    ///
    /// The `capacity` is the maximum number of events handled per poll.
    pub fn new(executor: &ThreadPerTaskExecutor, capacity: usize) -> io::Result<Self> {
        Self::with_factory(executor, capacity, CompletionService::new_factory())
    }

    /// Starts the thread of this manager on the `executor`, with the completion service that the `factory` creates.
    ///
    /// The `capacity` is the maximum number of events handled per poll.
    pub fn with_factory(
        executor: &ThreadPerTaskExecutor,
        capacity: usize,
        factory: fn(u32) -> io::Result<CompletionService>,
    ) -> io::Result<Self> {
        let port = Arc::new(factory(capacity as u32)?);
        let (dispatcher, receiver) = Dispatcher::new(&port);
        let (signal, shutdown_handle) = mpsc::channel::<()>();
        executor.execute({
            let port = Arc::clone(&port);
            let dispatcher = dispatcher.clone();
            move || {
                let _signal = signal;
                let mut manager = ScheduledExecutorService::new(port, dispatcher.clone(), capacity);
                while !manager.is_shutdown() {
                    if let Err(error) = manager.run_worker() {
                        error!(error:% = error; "Encountered error in socket worker");
                        break;
                    }
                    // Now run all tasks before trying to run IO again.
                    dispatcher.reset();
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
            dispatcher,
            shutdown_handle: Mutex::new(shutdown_handle),
        })
    }

    /// The completion service of this manager.
    pub fn port(&self) -> &CompletionService {
        &self.port
    }

    /// Submits the task for execution.
    ///
    /// # Errors
    ///
    /// Returns an error if this task cannot be accepted for execution.
    pub fn execute<F: FnOnce() + Send + 'static>(&self, task: F) -> io::Result<()> {
        self.submit(Box::new(move |_: &mut ScheduledExecutorService| task()))
    }

    /// Schedules the task to be executed after the delay.
    pub fn schedule<F: FnOnce() + Send + 'static>(&self, delay: Duration, task: F) -> io::Result<()> {
        self.submit(Box::new(move |manager: &mut ScheduledExecutorService| {
            manager.schedule(delay, move |_| task());
        }))
    }

    /// Returns a [`DelayedCompletableFuture`] that completes after the delay.
    pub fn delay(&self, delay: Duration) -> io::Result<DelayedCompletableFuture> {
        let future = DelayedCompletableFuture::new();
        self.schedule(delay, {
            let future = future.clone();
            move || future.complete()
        })?;
        Ok(future)
    }

    /// Runs the [`Future`] on the thread of this manager.
    pub fn enqueue<F: Future<Output = ()> + Send + 'static>(&self, future: F) -> io::Result<()> {
        self.submit(Box::new(move |manager: &mut ScheduledExecutorService| manager.enqueue(Box::pin(future))))
    }

    /// The `handler` function creates the [`SocketHandler`] on the thread of this manager.
    pub fn register<F>(&self, handler: F) -> Receiver<io::Result<()>>
    where
        F: FnOnce() -> Box<dyn SocketHandler> + Send + 'static,
    {
        let (sender, receiver) = mpsc::channel();
        let request = Registration { handler, callback: sender.clone() };
        let task: Task = Box::new(move |manager: &mut ScheduledExecutorService| request.run(manager));
        if let Err(error) = self.submit(task) {
            sender.send(Err(error)).ok();
        }
        receiver
    }

    fn submit(&self, task: Task) -> io::Result<()> {
        self.dispatcher.dispatch(task)
    }

    /// Shuts down this [`ExecutorCompletionService`], waiting for at most `timeout` for its connections to close.
    pub fn shutdown(&self, timeout: Duration) {
        self.submit(Box::new(move |manager: &mut ScheduledExecutorService| manager.shutdown(timeout)))
            .ok();
    }

    /// Like [`shutdown`](Self::shutdown), but without waiting. Pending tasks are not processed anymore.
    pub fn shutdown_now(&self) -> io::Result<()> {
        self.port().wakeup(SHUTDOWN)
    }

    /// Blocks the current thread until the shutdown has completed.
    ///
    /// Returns `false` if the `timeout` elapsed first.
    pub fn await_shutdown(&self, timeout: Duration) -> bool {
        let shutdown_handle = self.shutdown_handle.lock().unwrap_or_else(PoisonError::into_inner);
        matches!(shutdown_handle.recv_timeout(timeout), Err(RecvTimeoutError::Disconnected))
    }
}

impl Drop for ExecutorCompletionService {
    fn drop(&mut self) {
        self.shutdown_now().ok();
    }
}

struct Registration<F> {
    handler: F,
    callback: Sender<io::Result<()>>,
}

impl<F: FnOnce() -> Box<dyn SocketHandler>> Registration<F> {
    fn run(self, manager: &mut ScheduledExecutorService) {
        self.callback.send(manager.register((self.handler)())).ok();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::concurrent::CountingThreadFactory;
    use crate::handle::Completion;
    #[cfg(windows)]
    use crate::handle::event::Event;
    #[cfg(windows)]
    use crate::handle::socket;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    #[cfg(unix)]
    use std::os::fd::{AsFd, BorrowedFd};
    #[cfg(any(target_os = "macos", target_os = "freebsd"))]
    use std::os::fd::AsRawFd;
    #[cfg(windows)]
    use std::os::windows::io::{AsSocket, BorrowedSocket};
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::thread;
    use std::time::Instant;

    #[test]
    fn execute() {
        let manager = ThreadPerTaskExecutor::new(CountingThreadFactory::new(|| "Test", "Loop"));
        let executor = executor(&manager);
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
        let manager = ThreadPerTaskExecutor::new(CountingThreadFactory::new(|| "Test", "Loop"));
        let executor = executor(&manager);
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
        let manager = ThreadPerTaskExecutor::new(CountingThreadFactory::new(|| "Test", "Loop"));
        let executor = executor(&manager);
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
        let manager = ThreadPerTaskExecutor::new(CountingThreadFactory::new(|| "Test", "Loop"));
        let executor = executor(&manager);
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
        let manager = ThreadPerTaskExecutor::new(CountingThreadFactory::new(|| "Test", "Loop"));
        let executor = executor(&manager);
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
        let manager = ThreadPerTaskExecutor::new(CountingThreadFactory::new(|| "Test", "Loop"));
        let executor = ExecutorCompletionService::new(&manager, 64).unwrap();
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

    struct ServerSocketChannelAdapter {
        listener: TcpListener,
    }

    impl SocketHandler for ServerSocketChannelAdapter {
        #[cfg(unix)]
        fn socket(&self) -> BorrowedFd<'_> {
            self.listener.as_fd()
        }

        #[cfg(windows)]
        fn socket(&self) -> BorrowedSocket<'_> {
            self.listener.as_socket()
        }

        #[cfg(unix)]
        fn on_registered(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
            receive(manager, &self.listener)
        }

        #[cfg(windows)]
        fn on_registered(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
            manager.accept(&self.listener).map(|_| true)
        }

        #[cfg(unix)]
        fn handle(&mut self, manager: &mut ScheduledExecutorService, data: Completion) -> io::Result<bool> {
            data?;
            let (stream, _) = self.listener.accept()?;
            stream.set_nonblocking(true)?;
            manager.register(Box::new(MessageCreateHandler { stream }))?;
            Ok(true)
        }

        #[cfg(windows)]
        fn handle(&mut self, manager: &mut ScheduledExecutorService, data: Completion) -> io::Result<bool> {
            let stream = socket::accepted(&self.listener, &mut *Event::from_completion(data)?)?;
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

        fn on_registered(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
            receive(manager, &self.stream)
        }

        #[cfg(unix)]
        fn handle(&mut self, _manager: &mut ScheduledExecutorService, data: Completion) -> io::Result<bool> {
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
        fn handle(&mut self, manager: &mut ScheduledExecutorService, data: Completion) -> io::Result<bool> {
            let packet = Event::from_completion(data)?;
            match packet.opcode() {
                Event::READ if packet.length() == 0 => Ok(false),
                Event::READ => {
                    manager.write(&self.stream, Event::wrap(vec![packet.into_body()]))?;
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

        fn on_registered(&mut self, _manager: &mut ScheduledExecutorService) -> io::Result<bool> {
            Err(io::ErrorKind::InvalidInput.into())
        }

        fn handle(&mut self, _manager: &mut ScheduledExecutorService, _data: Completion) -> io::Result<bool> {
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

        fn on_registered(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
            self.count += 1;
            self.sender.send(self.count).ok();
            if self.count < 3 {
                manager.retry(Duration::ZERO);
            }
            Ok(true)
        }

        fn handle(&mut self, _manager: &mut ScheduledExecutorService, _data: Completion) -> io::Result<bool> {
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

        fn on_registered(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
            receive(manager, &self.stream)
        }

        fn handle(&mut self, _manager: &mut ScheduledExecutorService, _data: Completion) -> io::Result<bool> {
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

        fn on_registered(&mut self, _manager: &mut ScheduledExecutorService) -> io::Result<bool> {
            Ok(true)
        }

        fn handle(&mut self, _manager: &mut ScheduledExecutorService, _data: Completion) -> io::Result<bool> {
            Ok(true)
        }

        fn shutdown(&mut self, _manager: &mut ScheduledExecutorService) -> io::Result<bool> {
            Ok(true)
        }
    }

    #[cfg(target_os = "linux")]
    fn receive<S: AsFd>(manager: &mut ScheduledExecutorService, socket: &S) -> io::Result<bool> {
        manager.submit(socket, crate::handle::READABLE).map(|_| true)
    }

    #[cfg(any(target_os = "macos", target_os = "freebsd"))]
    fn receive<S: AsRawFd>(manager: &mut ScheduledExecutorService, socket: &S) -> io::Result<bool> {
        manager.submit(socket, rustix::event::kqueue::EventFilter::Read, true).map(|_| true)
    }

    #[cfg(windows)]
    fn receive(manager: &mut ScheduledExecutorService, stream: &TcpStream) -> io::Result<bool> {
        manager.read(stream, Event::new(16)).map(|_| true)
    }

    fn executor(manager: &ThreadPerTaskExecutor) -> ExecutorCompletionService {
        #[cfg(target_os = "linux")]
        let factory = crate::handle::epoll::CompletionService::new_factory();
        #[cfg(not(target_os = "linux"))]
        let factory = CompletionService::new_factory();
        ExecutorCompletionService::with_factory(manager, 64, factory).unwrap()
    }

    fn listener() -> TcpListener {
        TcpListener::bind("127.0.0.1:0").unwrap()
    }

    #[test]
    fn echo() {
        let manager = ThreadPerTaskExecutor::new(CountingThreadFactory::new(|| "Test", "Loop"));
        let executor = executor(&manager);
        let listener = listener();
        let address = listener.local_addr().unwrap();
        executor.register(move || Box::new(ServerSocketChannelAdapter { listener })).recv().unwrap().unwrap();

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
        let manager = ThreadPerTaskExecutor::new(CountingThreadFactory::new(|| "Test", "Loop"));
        let executor = executor(&manager);
        let data = executor.register(move || Box::new(ErrorChannel { listener: listener() }));
        assert!(data.recv().unwrap().is_err());
    }

    #[test]
    fn retry() {
        let manager = ThreadPerTaskExecutor::new(CountingThreadFactory::new(|| "Test", "Loop"));
        let executor = executor(&manager);
        let (sender, receiver) = mpsc::channel();
        let channel = ReconnectChannel { listener: listener(), sender, count: 0 };
        executor.register(move || Box::new(channel)).recv().unwrap().unwrap();

        for count in 1..=3 {
            assert_eq!(receiver.recv_timeout(Duration::from_secs(5)).unwrap(), count);
        }
    }

    #[test]
    fn panic() {
        let manager = ThreadPerTaskExecutor::new(CountingThreadFactory::new(|| "Test", "Loop"));
        let executor = executor(&manager);
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
        let manager = ThreadPerTaskExecutor::new(CountingThreadFactory::new(|| "Test", "Loop"));
        let executor = executor(&manager);
        executor.register(move || Box::new(LockedChannel { listener: listener() })).recv().unwrap().unwrap();

        executor.shutdown(Duration::from_secs(1));
        assert!(!executor.await_shutdown(Duration::from_millis(50)));
        assert!(executor.await_shutdown(Duration::from_secs(5)));
    }

    #[test]
    fn race() {
        let manager = ThreadPerTaskExecutor::new(CountingThreadFactory::new(|| "Test", "Loop"));
        let executor = executor(&manager);
        let (sender, receiver) = mpsc::channel();
        thread::scope(|scope| {
            for _ in 0..4 {
                let sender = sender.clone();
                let executor = &executor;
                scope.spawn(move || {
                    for _ in 0..1000 {
                        let sender = sender.clone();
                        executor.execute(move || sender.send(()).unwrap()).unwrap();
                    }
                });
            }
        });

        for _ in 0..4000 {
            receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        }
    }

    #[test]
    fn reject() {
        let manager = ThreadPerTaskExecutor::new(CountingThreadFactory::new(|| "Test", "Loop"));
        let executor = executor(&manager);
        executor.shutdown(Duration::ZERO);
        assert!(executor.await_shutdown(Duration::from_secs(5)));

        let listener = listener();
        let address = listener.local_addr().unwrap();
        let data = executor.register(move || Box::new(ServerSocketChannelAdapter { listener }));
        assert!(data.recv().map_or(true, |data| data.is_err()));
        assert!(TcpStream::connect_timeout(&address, Duration::from_secs(1)).is_err());
    }

    #[test]
    fn close() {
        let manager = ThreadPerTaskExecutor::new(CountingThreadFactory::new(|| "Test", "Loop"));
        let executor = executor(&manager);
        let listener = listener();
        let address = listener.local_addr().unwrap();
        executor.register(move || Box::new(ServerSocketChannelAdapter { listener })).recv().unwrap().unwrap();

        let mut client = TcpStream::connect(address).unwrap();
        let mut received = [0; 4];
        client.write_all(b"ping").unwrap();
        client.read_exact(&mut received).unwrap();
        drop(executor);

        assert_eq!(client.read(&mut received).unwrap_or(0), 0);
    }
}
