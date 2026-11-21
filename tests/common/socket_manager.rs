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

use bytes::BytesMut;
use libev::DefaultSocketManagerBuilder;
use libev::entities::channel::middleman::MessageChannel;
use libev::hooks::ListenerAdapter;
use libev::requests::sockactions::SocketAction;
use libev::utils::concurrent::ScheduledThreadPoolExecutor;
use std::io::{self, Read, Write};
use std::mem;
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone)]
struct MessageForwardHandler;

impl ListenerAdapter for MessageForwardHandler {
    async fn on_message_received(&mut self, channel: &mut MessageChannel, message: BytesMut) -> io::Result<()> {
        channel.send_message(message).await
    }
}

struct TestHandler {
    callback: Sender<Vec<u8>>,
}

impl ListenerAdapter for TestHandler {
    async fn on_ready(&mut self, channel: &mut MessageChannel) -> io::Result<()> {
        channel.send_message(BytesMut::from(&b"ping"[..])).await
    }

    async fn on_message_received(&mut self, channel: &mut MessageChannel, message: BytesMut) -> io::Result<()> {
        self.callback.send(message.to_vec()).ok();
        channel.close();
        Ok(())
    }
}

#[derive(Clone)]
struct SessionInvalidateAdapter;

impl ListenerAdapter for SessionInvalidateAdapter {
    async fn on_ready(&mut self, channel: &mut MessageChannel) -> io::Result<()> {
        channel.close();
        Ok(())
    }
}

struct ShardManagerAdapter {
    callback: Sender<usize>,
}

impl ListenerAdapter for ShardManagerAdapter {
    async fn on_ready(&mut self, channel: &mut MessageChannel) -> io::Result<()> {
        self.callback.send(channel.socket_manager().worker_pool().shards_total()).ok();
        channel.close();
        Ok(())
    }
}

#[derive(Clone)]
struct ChannelDisconnectAdapter {
    callback: Sender<bool>,
}

impl ListenerAdapter for ChannelDisconnectAdapter {
    async fn on_ready(&mut self, channel: &mut MessageChannel) -> io::Result<()> {
        self.callback.send(true).ok();
        channel.on_ready();
        Ok(())
    }

    async fn on_channel_disconnect(&mut self, channel: &mut MessageChannel) -> io::Result<()> {
        self.callback.send(false).ok();
        channel.on_channel_disconnect();
        Ok(())
    }
}

#[derive(Clone)]
struct ChannelUpdateSlowmodeAdapter {
    callback: Sender<bool>,
}

impl ListenerAdapter for ChannelUpdateSlowmodeAdapter {
    async fn on_ready(&mut self, channel: &mut MessageChannel) -> io::Result<()> {
        channel.send_message(BytesMut::zeroed(1024 * 1024)).await
    }

    async fn on_channel_update_slowmode(&mut self, channel: &mut MessageChannel) -> io::Result<()> {
        self.callback.send(channel.is_locked()).ok();
        Ok(())
    }
}

#[derive(Clone)]
struct MessageReceivedAdapter;

impl ListenerAdapter for MessageReceivedAdapter {
    async fn on_message_received(&mut self, channel: &mut MessageChannel, message: BytesMut) -> io::Result<()> {
        channel.send_message(message).await?;
        channel.close();
        Ok(())
    }
}

#[derive(Clone)]
struct ThreadListSyncHandler {
    sent: usize,
    callback: Sender<bool>,
}

impl ThreadListSyncHandler {
    async fn chunk(&mut self, channel: &mut MessageChannel) -> io::Result<()> {
        while !channel.is_locked() && self.sent < 16 * 1024 * 1024 {
            let chunk: BytesMut = (self.sent..self.sent + 64 * 1024)
                .map(|index| (index % 251) as u8)
                .collect();
            self.sent += chunk.len();
            channel.send_message(chunk).await?;
        }
        if self.sent >= 16 * 1024 * 1024 {
            channel.close();
        }
        Ok(())
    }
}

impl ListenerAdapter for ThreadListSyncHandler {
    async fn on_ready(&mut self, channel: &mut MessageChannel) -> io::Result<()> {
        self.chunk(channel).await?;
        channel.on_ready();
        Ok(())
    }

    async fn on_channel_update_slowmode(&mut self, channel: &mut MessageChannel) -> io::Result<()> {
        self.callback.send(channel.is_locked()).ok();
        if !channel.is_locked() {
            self.chunk(channel).await?;
        }
        Ok(())
    }
}

#[derive(Clone)]
struct SelfDeafenAdapter {
    undeafen: bool,
    callback: Sender<Vec<u8>>,
}

impl ListenerAdapter for SelfDeafenAdapter {
    async fn on_ready(&mut self, channel: &mut MessageChannel) -> io::Result<()> {
        channel.set_self_deafened(true);
        if self.undeafen {
            channel.send_message(BytesMut::zeroed(16)).await?;
        }
        Ok(())
    }

    async fn on_channel_update_slowmode(&mut self, channel: &mut MessageChannel) -> io::Result<()> {
        if !channel.is_locked() {
            channel.set_self_deafened(false);
        }
        Ok(())
    }

    async fn on_message_received(&mut self, _channel: &mut MessageChannel, message: BytesMut) -> io::Result<()> {
        self.callback.send(message.to_vec()).ok();
        Ok(())
    }
}

#[derive(Clone)]
struct ChannelTimeoutAdapter {
    callback: Sender<()>,
}

impl ListenerAdapter for ChannelTimeoutAdapter {
    async fn on_channel_timeout(&mut self, channel: &mut MessageChannel) -> io::Result<()> {
        self.callback.send(()).ok();
        channel.close();
        Ok(())
    }
}

#[derive(Clone)]
struct StringHandler;

impl ListenerAdapter<BytesMut, String> for StringHandler {
    async fn on_message_received(&mut self, channel: &mut MessageChannel<String>, message: BytesMut) -> io::Result<()> {
        let text = String::from_utf8(message.to_vec()).map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
        channel.on_message_received(text);
        Ok(())
    }

    async fn write(&mut self, channel: &mut MessageChannel<String>, message: String) -> io::Result<()> {
        channel.write(BytesMut::from(message.as_bytes()))
    }
}

#[derive(Clone)]
struct TextHandler;

impl ListenerAdapter<String, String> for TextHandler {
    async fn on_message_received(&mut self, channel: &mut MessageChannel<String, String>, message: String) -> io::Result<()> {
        channel.send_message(message.to_uppercase()).await
    }
}

#[derive(Clone)]
struct MessageUpdateHandler;

impl ListenerAdapter for MessageUpdateHandler {
    async fn on_message_received(&mut self, channel: &mut MessageChannel, message: BytesMut) -> io::Result<()> {
        let delay = channel.socket_manager().worker_pool().provide().delay(Duration::from_millis(50))?;
        delay.await;
        channel.send_message(message).await
    }
}

#[derive(Clone)]
struct SessionDisconnectAdapter {
    delay: Duration,
    callback: Sender<&'static str>,
}

impl ListenerAdapter for SessionDisconnectAdapter {
    async fn on_message_received(&mut self, channel: &mut MessageChannel, _message: BytesMut) -> io::Result<()> {
        self.callback.send("received").ok();
        channel.socket_manager().worker_pool().provide().delay(self.delay)?.await;
        let closed = channel.send_message(BytesMut::from(&b"late"[..])).await.is_err();
        self.callback.send(if closed { "closed" } else { "open" }).ok();
        Ok(())
    }

    async fn on_channel_disconnect(&mut self, channel: &mut MessageChannel) -> io::Result<()> {
        self.callback.send("disconnect").ok();
        channel.on_channel_disconnect();
        Ok(())
    }
}

#[derive(Clone)]
struct ErrorResponseAdapter {
    panic: bool,
}

impl ListenerAdapter for ErrorResponseAdapter {
    async fn on_message_received(&mut self, channel: &mut MessageChannel, _message: BytesMut) -> io::Result<()> {
        channel.socket_manager().worker_pool().provide().delay(Duration::from_millis(50))?.await;
        if self.panic {
            panic!();
        }
        Err(io::ErrorKind::InvalidData.into())
    }
}

#[derive(Clone)]
struct MessageCreateHandler;

impl ListenerAdapter for MessageCreateHandler {
    async fn write(&mut self, channel: &mut MessageChannel, message: BytesMut) -> io::Result<()> {
        channel.socket_manager().worker_pool().provide().delay(Duration::from_millis(50))?.await;
        channel.write(message)
    }
}

#[derive(Clone)]
struct CallbackContext {
    close: bool,
    callback: Sender<bool>,
}

impl ListenerAdapter for CallbackContext {
    async fn on_message_received(&mut self, channel: &mut MessageChannel, message: BytesMut) -> io::Result<()> {
        if self.close {
            channel.close();
        }
        let success = self.callback.clone();
        let failure = self.callback.clone();
        channel.send_message(message).queue(
            move |_| {
                success.send(true).ok();
            },
            move |_| {
                failure.send(false).ok();
            },
        )
    }
}

#[derive(Clone)]
struct MessageBulkAdapter;

impl ListenerAdapter for MessageBulkAdapter {
    async fn on_message_received(&mut self, channel: &mut MessageChannel, message: BytesMut) -> io::Result<()> {
        channel.send_message(message.clone()).queue(|_| (), |_| ())?;
        channel.send_message(message).queue(|_| (), |_| ())
    }
}

struct MessageHistoryAdapter {
    payload: Arc<Vec<u8>>,
    received: Vec<u8>,
    callback: Sender<Vec<u8>>,
}

impl ListenerAdapter for MessageHistoryAdapter {
    async fn on_ready(&mut self, channel: &mut MessageChannel) -> io::Result<()> {
        for chunk in chunks(&self.payload, 64 * 1024) {
            channel.send_message(chunk).queue(|_| (), |_| ())?;
        }
        Ok(())
    }

    async fn on_message_received(&mut self, channel: &mut MessageChannel, message: BytesMut) -> io::Result<()> {
        self.received.extend_from_slice(&message);
        if self.received.len() >= self.payload.len() {
            self.callback.send(mem::take(&mut self.received)).ok();
            channel.close();
        }
        Ok(())
    }
}

#[derive(Clone)]
struct MessageForwardSlowmodeAdapter;

impl ListenerAdapter for MessageForwardSlowmodeAdapter {
    async fn on_message_received(&mut self, channel: &mut MessageChannel, message: BytesMut) -> io::Result<()> {
        channel.send_message(message).queue(|_| (), |_| ())
    }

    async fn on_channel_update_slowmode(&mut self, channel: &mut MessageChannel) -> io::Result<()> {
        channel.set_self_deafened(channel.is_locked());
        Ok(())
    }
}

#[derive(Clone)]
struct SelfDeafenUpdateAdapter;

impl ListenerAdapter for SelfDeafenUpdateAdapter {
    async fn on_message_received(&mut self, channel: &mut MessageChannel, message: BytesMut) -> io::Result<()> {
        channel.set_self_deafened(true);
        channel.send_message(message).await?;
        channel.socket_manager().worker_pool().provide().delay(Duration::from_millis(50))?.await;
        channel.set_self_deafened(false);
        Ok(())
    }
}

#[derive(Clone)]
struct ChannelDeleteAdapter {
    length: usize,
}

impl ListenerAdapter for ChannelDeleteAdapter {
    async fn on_ready(&mut self, channel: &mut MessageChannel) -> io::Result<()> {
        channel.send_message(BytesMut::from(&payload(self.length, 0)[..])).queue(|_| (), |_| ())?;
        channel.close();
        Ok(())
    }
}

#[derive(Clone)]
struct MessageReceiveAdapter {
    received: Vec<u8>,
    callback: Sender<Vec<u8>>,
}

impl ListenerAdapter for MessageReceiveAdapter {
    async fn on_message_received(&mut self, _channel: &mut MessageChannel, message: BytesMut) -> io::Result<()> {
        self.received.extend_from_slice(&message);
        Ok(())
    }

    async fn on_channel_disconnect(&mut self, channel: &mut MessageChannel) -> io::Result<()> {
        self.callback.send(mem::take(&mut self.received)).ok();
        channel.on_channel_disconnect();
        Ok(())
    }
}

struct SessionResumeAdapter {
    received: Vec<u8>,
    paused: bool,
    callback: Sender<Vec<u8>>,
}

impl ListenerAdapter for SessionResumeAdapter {
    async fn on_message_received(&mut self, channel: &mut MessageChannel, message: BytesMut) -> io::Result<()> {
        self.received.extend_from_slice(&message);
        if !self.paused && self.received.len() >= 128 * 1024 {
            self.paused = true;
            channel.set_self_deafened(true);
            channel.socket_manager().worker_pool().provide().delay(Duration::from_millis(750))?.await;
            channel.set_self_deafened(false);
        }
        Ok(())
    }

    async fn on_channel_disconnect(&mut self, _channel: &mut MessageChannel) -> io::Result<()> {
        self.callback.send(mem::take(&mut self.received)).ok();
        Ok(())
    }
}

#[derive(Clone)]
struct ChannelCloseAdapter;

impl ListenerAdapter for ChannelCloseAdapter {
    async fn on_ready(&mut self, channel: &mut MessageChannel) -> io::Result<()> {
        channel.close();
        channel.close();
        Ok(())
    }
}

#[derive(Clone)]
struct MessageBulkSendAdapter;

impl ListenerAdapter for MessageBulkSendAdapter {
    async fn on_ready(&mut self, channel: &mut MessageChannel) -> io::Result<()> {
        channel.send_message(BytesMut::zeroed(4 * 1024 * 1024)).queue(|_| (), |_| ())?;
        channel.close();
        Ok(())
    }
}

struct MessageBulkWriteAdapter {
    payload: Vec<u8>,
}

impl ListenerAdapter for MessageBulkWriteAdapter {
    async fn on_ready(&mut self, channel: &mut MessageChannel) -> io::Result<()> {
        for chunk in chunks(&self.payload, 8 * 1024) {
            channel.write(chunk)?;
        }
        channel.flush();
        Ok(())
    }
}

struct MessagePayloadAdapter {
    received: Vec<u8>,
    callback: Sender<Vec<u8>>,
}

impl ListenerAdapter for MessagePayloadAdapter {
    async fn on_ready(&mut self, channel: &mut MessageChannel) -> io::Result<()> {
        channel.send_message(BytesMut::from(&b"payload"[..])).queue(|_| (), |_| ())?;
        channel.on_ready();
        Ok(())
    }

    async fn on_message_received(&mut self, channel: &mut MessageChannel, message: BytesMut) -> io::Result<()> {
        self.received.extend_from_slice(&message);
        if self.received.len() >= b"payload".len() {
            self.callback.send(mem::take(&mut self.received)).ok();
            channel.close();
        }
        Ok(())
    }
}

#[cfg(unix)]
#[derive(Clone)]
struct GuildReadyAdapter {
    id: usize,
    callback: Sender<usize>,
}

#[cfg(unix)]
impl ListenerAdapter for GuildReadyAdapter {
    async fn on_ready(&mut self, channel: &mut MessageChannel) -> io::Result<()> {
        self.callback.send(self.id).ok();
        channel.close();
        Ok(())
    }
}

fn shard_manager(shards_total: usize) -> ScheduledThreadPoolExecutor {
    ScheduledThreadPoolExecutor::new(shards_total, 64).unwrap()
}

fn payload(length: usize, seed: usize) -> Vec<u8> {
    (0..length).map(|index| ((index * 31 + seed) % 251) as u8).collect()
}

fn chunks(payload: &[u8], limit: usize) -> Vec<BytesMut> {
    let mut chunks = Vec::new();
    let mut offset = 0;
    while offset < payload.len() {
        let length = (1 + (chunks.len() * 7919) % limit).min(payload.len() - offset);
        chunks.push(BytesMut::from(&payload[offset..offset + length]));
        offset += length;
    }
    chunks
}

fn gathering(length: usize, connections: usize) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let seed = AtomicUsize::new(0);
    let manager = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .set_worker_pool(shard_manager(1))
        .add_event_listener_provider(move || MessageBulkWriteAdapter {
            payload: payload(length, seed.fetch_add(1, Ordering::Relaxed)),
        })
        .build()
        .unwrap();
    for _ in 0..connections {
        manager.connect(address).unwrap();
    }

    let mut seeds = Vec::new();
    for _ in 0..connections {
        let (mut server, _) = listener.accept().unwrap();
        server.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
        let mut received = vec![0; length];
        server.read_exact(&mut received).unwrap();
        let seed = received[0] as usize;
        assert_eq!(received, payload(length, seed));
        seeds.push(seed);
    }
    seeds.sort();
    assert_eq!(seeds, (0..connections).collect::<Vec<_>>());
}

#[test]
fn echo() {
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .add_event_listener(MessageForwardHandler)
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();

    let (sender, receiver) = mpsc::channel();
    let manager = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .add_event_listener_provider(move || TestHandler {
            callback: sender.clone(),
        })
        .build()
        .unwrap();
    for _ in 0..3 {
        manager.connect(address).unwrap();
    }

    for _ in 0..3 {
        assert_eq!(
            receiver.recv_timeout(Duration::from_secs(5)).unwrap(),
            b"ping"
        );
    }
}

#[test]
fn flush() {
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .add_event_listener(MessageForwardHandler)
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();

    let mut client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    client.write_all(&vec![7; 16 * 1024 * 1024]).unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut received = Vec::new();
    client.read_to_end(&mut received).unwrap();
    assert_eq!(received.len(), 16 * 1024 * 1024);
}

#[test]
fn socket_manager() {
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .add_event_listener(MessageForwardHandler)
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();

    let (sender, receiver) = mpsc::channel();
    let manager = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .set_worker_pool(shard_manager(3))
        .add_event_listener_provider(move || ShardManagerAdapter {
            callback: sender.clone(),
        })
        .build()
        .unwrap();
    manager.connect(address).unwrap();

    assert_eq!(receiver.recv_timeout(Duration::from_secs(5)).unwrap(), 3);
}

#[test]
fn shutdown_now() {
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .set_worker_pool(shard_manager(1))
        .add_event_listener(MessageForwardHandler)
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();

    let mut client = TcpStream::connect(address).unwrap();
    let mut received = [0; 4];
    client.write_all(b"ping").unwrap();
    client.read_exact(&mut received).unwrap();
    server.shutdown_now().unwrap();

    assert_eq!(client.read(&mut received).unwrap_or(0), 0);
}

#[test]
fn disconnect() {
    let (sender, receiver) = mpsc::channel();
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .set_worker_pool(shard_manager(1))
        .add_event_listener(ChannelDisconnectAdapter { callback: sender })
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();
    let event = || receiver.recv_timeout(Duration::from_secs(5)).unwrap();

    drop(TcpStream::connect(address).unwrap());
    assert!(event());
    assert!(!event());

    let _client = TcpStream::connect(address).unwrap();
    assert!(event());
    server.shutdown_now().unwrap();
    assert!(!event());
    assert!(receiver.recv_timeout(Duration::from_millis(100)).is_err());
}

#[test]
fn is_locked() {
    let (sender, receiver) = mpsc::channel();
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .set_worker_pool(shard_manager(1))
        .add_event_listener(ChannelUpdateSlowmodeAdapter { callback: sender })
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();

    let mut client = TcpStream::connect(address).unwrap();
    let event = || receiver.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(event());
    client.read_exact(&mut vec![0; 1024 * 1024]).unwrap();
    assert!(!event());
}

#[test]
fn await_shutdown() {
    let manager = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .set_worker_pool(shard_manager(1))
        .add_event_listener(MessageReceivedAdapter)
        .build()
        .unwrap();
    manager.shutdown();
    assert!(manager.await_shutdown(Duration::MAX));
}

#[test]
fn shutdown() {
    let (sender, receiver) = mpsc::channel();
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .set_worker_pool(shard_manager(1))
        .add_event_listener(ChannelUpdateSlowmodeAdapter { callback: sender })
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();

    let mut client = TcpStream::connect(address).unwrap();
    assert!(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
    assert!(!server.await_shutdown(Duration::ZERO));
    server.shutdown();

    let mut received = Vec::new();
    client.read_to_end(&mut received).unwrap();
    assert_eq!(received.len(), 1024 * 1024);
    assert!(server.await_shutdown(Duration::from_secs(5)));
    assert!(std::net::TcpListener::bind(address).is_ok());
}

#[test]
fn close() {
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .set_worker_pool(shard_manager(1))
        .add_event_listener(SessionInvalidateAdapter)
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();

    let mut client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(client.read(&mut [0; 1]).unwrap(), 0);
}

#[test]
fn concurrent() {
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .set_worker_pool(shard_manager(1))
        .add_event_listener(MessageReceivedAdapter)
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();
    let barrier = Arc::new(Barrier::new(1000));

    let handles: Vec<_> = (0..1000)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            let client = TcpStream::connect(address);
            thread::Builder::new()
                .stack_size(64 * 1024)
                .spawn(move || {
                    barrier.wait();
                    let mut client = client?;
                    client.set_read_timeout(Some(Duration::from_secs(10)))?;
                    client.write_all(b"ping")?;
                    let mut received = Vec::new();
                    client.read_to_end(&mut received)?;
                    Ok::<_, io::Error>(received)
                })
                .unwrap()
        })
        .collect();
    for handle in handles {
        assert_eq!(handle.join().unwrap().unwrap(), b"ping");
    }
}

#[test]
fn chunking() {
    let (sender, receiver) = mpsc::channel();
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .set_worker_pool(shard_manager(1))
        .add_event_listener(ThreadListSyncHandler { sent: 0, callback: sender })
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();

    let mut client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut buffer = vec![0; 64 * 1024];
    let mut total = 0;
    loop {
        let length = client.read(&mut buffer).unwrap();
        if length == 0 {
            break;
        }
        for (offset, byte) in buffer[..length].iter().enumerate() {
            assert_eq!(*byte, ((total + offset) % 251) as u8);
        }
        total += length;
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(total, 16 * 1024 * 1024);
    assert!(receiver.try_iter().any(|locked| locked));
}

#[test]
fn reset() {
    let (sender, receiver) = mpsc::channel();
    let (callback, _events) = mpsc::channel();
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .set_worker_pool(shard_manager(1))
        .add_event_listener(ThreadListSyncHandler { sent: 0, callback })
        .add_event_listener(ChannelDisconnectAdapter { callback: sender })
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();

    let mut client = TcpStream::connect(address).unwrap();
    client.read_exact(&mut [0; 1024]).unwrap();
    assert!(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
    drop(client);

    assert!(!receiver.recv_timeout(Duration::from_secs(5)).unwrap());
    assert!(receiver.recv_timeout(Duration::from_millis(100)).is_err());
    server.shutdown();
    assert!(server.await_shutdown(Duration::from_secs(5)));
}

#[test]
fn deafen() {
    let (sender, receiver) = mpsc::channel();
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .set_worker_pool(shard_manager(1))
        .add_event_listener(SelfDeafenAdapter { undeafen: false, callback: sender })
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();

    let mut client = TcpStream::connect(address).unwrap();
    client.write_all(b"ping").unwrap();
    assert!(receiver.recv_timeout(Duration::from_millis(200)).is_err());
}

#[test]
fn undeafen() {
    let (sender, receiver) = mpsc::channel();
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .set_worker_pool(shard_manager(1))
        .set_threshold(1)
        .set_large_threshold(1)
        .add_event_listener(SelfDeafenAdapter { undeafen: true, callback: sender })
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();

    let mut client = TcpStream::connect(address).unwrap();
    client.write_all(b"ping").unwrap();
    assert_eq!(receiver.recv_timeout(Duration::from_secs(5)).unwrap(), b"ping");
}

#[test]
fn idle() {
    let (sender, receiver) = mpsc::channel();
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .set_worker_pool(shard_manager(1))
        .set_afk_timeout(Duration::from_secs(1))
        .add_event_listener(ChannelTimeoutAdapter { callback: sender })
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();

    let mut client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    for _ in 0..5 {
        client.write_all(b"ping").unwrap();
        thread::sleep(Duration::from_millis(40));
    }
    assert!(receiver.try_recv().is_err());
    assert_eq!(client.read(&mut [0; 1]).unwrap(), 0);
    assert!(receiver.try_recv().is_ok());
}

#[test]
fn fallback() {
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .add_event_listener(MessageForwardHandler)
        .build()
        .unwrap();
    let address = server.bind("[::]:0").unwrap();

    let mut client = TcpStream::connect(("127.0.0.1", address.port())).unwrap();
    let mut received = [0; 4];
    client.write_all(b"ping").unwrap();
    client.read_exact(&mut received).unwrap();
    assert_eq!(&received, b"ping");
}

#[test]
fn pipeline() {
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .add_event_listener(StringHandler)
        .add_event_listener(TextHandler)
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();

    let mut client = TcpStream::connect(address).unwrap();
    let mut received = [0; 4];
    client.write_all(b"ping").unwrap();
    client.read_exact(&mut received).unwrap();
    assert_eq!(&received, b"PING");
}

#[test]
fn handler() {
    assert!(DefaultSocketManagerBuilder::new().build().is_err());
}

#[test]
fn builder() {
    let manager = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .add_event_listener(MessageForwardHandler)
        .set_backlog(1)
        .set_concurrency(2)
        .set_buffer_size(3)
        .set_reconnect_delay(Duration::from_secs(4))
        .set_max_reconnect_delay(Duration::from_secs(5))
        .set_threshold(6)
        .set_large_threshold(7)
        .set_shutdown_timeout(Duration::from_secs(8))
        .set_chunk_limit(10)
        .set_packet_limit(13)
        .set_tcp_no_delay(false)
        .set_close_timeout(Duration::from_secs(9))
        .set_connect_timeout(Duration::from_secs(14))
        .set_afk_timeout(Duration::from_secs(10))
        .set_address_shared(true)
        .set_fallback_enabled(false)
        .set_timeout(Duration::from_secs(11))
        .build()
        .unwrap();
    assert_eq!(manager.backlog(), 1);
    assert_eq!(manager.concurrency(), 2);
    assert_eq!(manager.buffer_size(), 3);
    assert_eq!(manager.reconnect_delay(), Duration::from_secs(4));
    assert_eq!(manager.max_reconnect_delay(), Duration::from_secs(5));
    assert_eq!(manager.threshold(), 6);
    assert_eq!(manager.large_threshold(), 7);
    assert_eq!(manager.shutdown_timeout(), Duration::from_secs(8));
    assert_eq!(manager.chunk_limit(), 10);
    assert_eq!(manager.packet_limit(), 13);
    assert!(!manager.tcp_no_delay());
    assert_eq!(manager.close_timeout(), Some(Duration::from_secs(9)));
    assert_eq!(manager.connect_timeout(), Duration::from_secs(14));
    assert_eq!(manager.afk_timeout(), Some(Duration::from_secs(10)));
    assert!(manager.is_address_shared());
    assert!(!manager.is_fallback_enabled());
    assert_eq!(manager.timeout(), Duration::from_secs(11));
}

#[test]
fn context_map() {
    let manager = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .add_event_listener(MessageForwardHandler)
        .build()
        .unwrap();
    manager.context_map().write().unwrap().insert(String::from("id"), Arc::new(7u32));
    let value = manager.clone().context_map().read().unwrap().get("id").cloned().unwrap();
    assert!(value.clone().downcast::<String>().is_err());
    assert_eq!(*value.downcast::<u32>().unwrap(), 7);
}

#[test]
fn delay() {
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .add_event_listener(MessageUpdateHandler)
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();

    let mut client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let start = Instant::now();
    client.write_all(b"ping").unwrap();
    let mut received = [0; 4];
    client.read_exact(&mut received).unwrap();
    assert_eq!(&received, b"ping");
    assert!(start.elapsed() >= Duration::from_millis(50));
}

#[test]
fn queued() {
    let (sender, receiver) = mpsc::channel();
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .add_event_listener(SessionDisconnectAdapter { delay: Duration::from_millis(100), callback: sender })
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();

    let mut client = TcpStream::connect(address).unwrap();
    client.write_all(b"ping").unwrap();
    let event = || receiver.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(event(), "received");
    server.shutdown();
    assert_eq!(event(), "closed");
    assert_eq!(event(), "disconnect");
    assert!(server.await_shutdown(Duration::from_secs(5)));
}

#[test]
fn deadline() {
    let (sender, receiver) = mpsc::channel();
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .add_event_listener(SessionDisconnectAdapter { delay: Duration::from_secs(10), callback: sender })
        .set_timeout(Duration::from_millis(100))
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();

    let mut client = TcpStream::connect(address).unwrap();
    client.write_all(b"ping").unwrap();
    assert_eq!(receiver.recv_timeout(Duration::from_secs(5)).unwrap(), "received");
    let start = Instant::now();
    server.shutdown();
    assert!(server.await_shutdown(Duration::from_secs(5)));
    assert!(start.elapsed() < Duration::from_secs(5));
    assert!(receiver.recv_timeout(Duration::from_millis(100)).is_err());
}

#[test]
fn error() {
    for panic in [false, true] {
        let server = DefaultSocketManagerBuilder::new()
            .set_socket_pool(shard_manager(1))
            .add_event_listener(ErrorResponseAdapter { panic })
            .build()
            .unwrap();
        let address = server.bind("127.0.0.1:0").unwrap();

        let mut client = TcpStream::connect(address).unwrap();
        client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        client.write_all(b"ping").unwrap();
        let mut received = [0; 4];
        match client.read(&mut received) {
            Ok(length) => assert_eq!(length, 0),
            Err(error) => assert_eq!(error.kind(), io::ErrorKind::ConnectionReset),
        }
    }
}

#[test]
fn queue() {
    let (sender, receiver) = mpsc::channel();
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .add_event_listener(MessageCreateHandler)
        .add_event_listener(CallbackContext { close: false, callback: sender.clone() })
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();
    let mut client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    client.write_all(b"ping").unwrap();
    let mut received = [0; 4];
    client.read_exact(&mut received).unwrap();
    assert_eq!(&received, b"ping");
    assert!(receiver.recv_timeout(Duration::from_secs(5)).unwrap());

    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .add_event_listener(CallbackContext { close: true, callback: sender })
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();
    let mut client = TcpStream::connect(address).unwrap();
    client.write_all(b"ping").unwrap();
    assert!(!receiver.recv_timeout(Duration::from_secs(5)).unwrap());
}

#[test]
fn lock() {
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .add_event_listener(MessageCreateHandler)
        .add_event_listener(MessageBulkAdapter)
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();

    let mut client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    for _ in 0..2 {
        client.write_all(b"ping").unwrap();
        let mut received = [0; 8];
        client.read_exact(&mut received).unwrap();
        assert_eq!(&received, b"pingping");
    }
}

#[test]
fn bulk() {
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .add_event_listener(MessageForwardHandler)
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();

    let data = Arc::new(payload(1024 * 1024, 0));
    let (sender, receiver) = mpsc::channel();
    let manager = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .add_event_listener_provider({
            let data = Arc::clone(&data);
            move || MessageHistoryAdapter {
                payload: Arc::clone(&data),
                received: Vec::new(),
                callback: sender.clone(),
            }
        })
        .build()
        .unwrap();
    manager.connect(address).unwrap();

    assert_eq!(receiver.recv_timeout(Duration::from_secs(10)).unwrap(), *data);
}

#[test]
fn backpressure() {
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .set_threshold(16 * 1024)
        .set_large_threshold(64 * 1024)
        .add_event_listener(MessageForwardSlowmodeAdapter)
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();

    let data = Arc::new(payload(1024 * 1024, 1));
    let (sender, receiver) = mpsc::channel();
    let manager = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .add_event_listener_provider({
            let data = Arc::clone(&data);
            move || MessageHistoryAdapter {
                payload: Arc::clone(&data),
                received: Vec::new(),
                callback: sender.clone(),
            }
        })
        .build()
        .unwrap();
    manager.connect(address).unwrap();

    assert_eq!(receiver.recv_timeout(Duration::from_secs(10)).unwrap(), *data);
}

#[test]
fn toggle() {
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .add_event_listener(SelfDeafenUpdateAdapter)
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();

    let mut client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    for message in [b"1", b"2"] {
        client.write_all(message).unwrap();
        let mut received = [0; 1];
        client.read_exact(&mut received).unwrap();
        assert_eq!(&received, message);
    }
}

#[test]
fn drain() {
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .add_event_listener(ChannelDeleteAdapter { length: 16 * 1024 })
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();

    let (sender, receiver) = mpsc::channel();
    let manager = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .set_buffer_size(10)
        .set_chunk_limit(1)
        .add_event_listener(MessageReceiveAdapter { received: Vec::new(), callback: sender })
        .build()
        .unwrap();
    manager.connect(address).unwrap();

    assert_eq!(receiver.recv_timeout(Duration::from_secs(5)).unwrap(), payload(16 * 1024, 0));
}

#[test]
fn eof() {
    let (sender, receiver) = mpsc::channel();
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .set_buffer_size(10)
        .set_chunk_limit(1)
        .add_event_listener(MessageReceiveAdapter { received: Vec::new(), callback: sender })
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();

    let mut client = TcpStream::connect(address).unwrap();
    client.write_all(&payload(100, 2)).unwrap();
    drop(client);

    assert_eq!(receiver.recv_timeout(Duration::from_secs(5)).unwrap(), payload(100, 2));
}

#[test]
fn paused() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let data = payload(32 * 1024 * 1024, 3);
    let server = thread::spawn({
        let data = data.clone();
        move || {
            let (mut server, _) = listener.accept().unwrap();
            for chunk in data.chunks(8 * 1024) {
                server.write_all(chunk).unwrap();
            }
            server.shutdown(Shutdown::Write).unwrap();
            server.read_to_end(&mut Vec::new()).ok();
        }
    });

    let (sender, receiver) = mpsc::channel();
    let manager = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .add_event_listener_provider(move || SessionResumeAdapter {
            received: Vec::new(),
            paused: false,
            callback: sender.clone(),
        })
        .build()
        .unwrap();
    manager.connect(address).unwrap();

    assert_eq!(receiver.recv_timeout(Duration::from_secs(30)).unwrap(), data);
    server.join().unwrap();
}

#[test]
fn close_ready() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let (sender, receiver) = mpsc::channel();
    let manager = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .add_event_listener(ChannelDisconnectAdapter { callback: sender })
        .add_event_listener(SessionInvalidateAdapter)
        .build()
        .unwrap();
    manager.connect(listener.local_addr().unwrap()).unwrap();

    let event = || receiver.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(event());
    assert!(!event());
    assert!(receiver.recv_timeout(Duration::from_millis(100)).is_err());
}

#[test]
fn close_twice() {
    let (sender, receiver) = mpsc::channel();
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .add_event_listener(ChannelDisconnectAdapter { callback: sender })
        .add_event_listener(ChannelCloseAdapter)
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();

    let mut client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(client.read(&mut [0; 1]).unwrap(), 0);
    let event = || receiver.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(event());
    assert!(!event());
    assert!(receiver.recv_timeout(Duration::from_millis(100)).is_err());
}

#[test]
fn close_pending() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let (sender, receiver) = mpsc::channel();
    let manager = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .set_shutdown_timeout(Duration::from_millis(100))
        .add_event_listener(ChannelDisconnectAdapter { callback: sender })
        .add_event_listener(MessageBulkSendAdapter)
        .build()
        .unwrap();
    manager.connect(listener.local_addr().unwrap()).unwrap();

    let event = || receiver.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(event());
    assert!(!event());
    let (mut server, _) = listener.accept().unwrap();
    server.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    if let Err(error) = server.read_to_end(&mut Vec::new()) {
        assert!(!matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut));
    }
    manager.shutdown();
    assert!(manager.await_shutdown(Duration::from_secs(5)));
}

#[test]
fn refused() {
    let address = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
    let (sender, receiver) = mpsc::channel();
    let manager = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .add_event_listener(ChannelDisconnectAdapter { callback: sender })
        .build()
        .unwrap();
    for _ in 0..2 {
        manager.connect(address).ok();
        assert!(receiver.recv_timeout(Duration::from_millis(500)).is_err());
    }

    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .add_event_listener(MessageForwardHandler)
        .build()
        .unwrap();
    manager.connect(server.bind("127.0.0.1:0").unwrap()).unwrap();
    assert!(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
}

#[test]
fn linger() {
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .set_close_timeout(Duration::ZERO)
        .add_event_listener(SessionInvalidateAdapter)
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();

    let mut client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(client.read(&mut [0; 1]).unwrap_err().kind(), io::ErrorKind::ConnectionReset);
}

#[test]
fn gather() {
    gathering(1024 * 1024, 1);
}

#[test]
fn gather_big() {
    gathering(50 * 1024 * 1024, 1);
}

#[test]
fn gather_shared() {
    gathering(1024 * 1024, 2);
}

#[test]
fn churn() {
    let (events, server_events) = mpsc::channel();
    let server = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .set_worker_pool(shard_manager(2))
        .add_event_listener(ChannelDisconnectAdapter { callback: events })
        .add_event_listener(MessageForwardHandler)
        .build()
        .unwrap();
    let address = server.bind("127.0.0.1:0").unwrap();

    let (sender, receiver) = mpsc::channel();
    let (events, client_events) = mpsc::channel();
    let manager = DefaultSocketManagerBuilder::new()
        .set_socket_pool(shard_manager(1))
        .set_worker_pool(shard_manager(2))
        .add_event_listener(ChannelDisconnectAdapter { callback: events })
        .add_event_listener_provider(move || MessagePayloadAdapter {
            received: Vec::new(),
            callback: sender.clone(),
        })
        .build()
        .unwrap();
    for _ in 0..100 {
        manager.connect(address).unwrap();
    }

    for _ in 0..100 {
        assert_eq!(receiver.recv_timeout(Duration::from_secs(10)).unwrap(), b"payload");
    }
    for events in [server_events, client_events] {
        let ready = (0..200).filter(|_| events.recv_timeout(Duration::from_secs(10)).unwrap()).count();
        assert_eq!(ready, 100);
        assert!(events.recv_timeout(Duration::from_millis(100)).is_err());
    }
}

#[test]
fn rebind() {
    let mut address = "127.0.0.1:0".parse().unwrap();
    for _ in 0..2 {
        let server = DefaultSocketManagerBuilder::new()
            .set_socket_pool(shard_manager(1))
            .add_event_listener(MessageForwardHandler)
            .build()
            .unwrap();
        address = server.bind(address).unwrap();

        let mut client = TcpStream::connect(address).unwrap();
        let mut received = [0; 4];
        client.write_all(b"ping").unwrap();
        client.read_exact(&mut received).unwrap();
        assert_eq!(&received, b"ping");
        drop(client);
        server.shutdown();
        assert!(server.await_shutdown(Duration::from_secs(5)));
    }
}

#[cfg(unix)]
#[test]
fn shared() {
    let (sender, receiver) = mpsc::channel();
    let mut address = "127.0.0.1:0".parse().unwrap();
    let mut servers = Vec::new();
    for id in 0..2 {
        let server = DefaultSocketManagerBuilder::new()
            .set_socket_pool(shard_manager(1))
            .set_address_shared(true)
            .add_event_listener(GuildReadyAdapter { id, callback: sender.clone() })
            .build()
            .unwrap();
        address = server.bind(address).unwrap();
        servers.push(server);
    }

    let mut ids = Vec::new();
    for _ in 0..64 {
        let mut client = TcpStream::connect(address).unwrap();
        client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        assert_eq!(client.read(&mut [0; 1]).unwrap(), 0);
        ids.push(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
    }
    #[cfg(any(target_os = "linux", target_os = "freebsd", windows))]
    assert!(ids.contains(&0) && ids.contains(&1));
}
