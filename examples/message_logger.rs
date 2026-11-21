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
use std::{env, io};

// Every channel uses its own clone of the listener, so it has to implement Clone
#[derive(Clone)]
struct MessageLoggerExample;

impl ListenerAdapter for MessageLoggerExample {
    // This overrides the method called on_message_received in the ListenerAdapter trait
    // Your IDE (such as RustRover) can automatically generate this override for you, by simply typing
    // "on_message" and auto-completing it!
    async fn on_message_received(&mut self, _channel: &mut MessageChannel, message: BytesMut) -> io::Result<()> {
        // The message is the raw bytes received from the socket,
        // this converts it to a readable string
        println!("[message] {}", String::from_utf8_lossy(&message));
        Ok(())
    }

    // Gets called once the slow mode of the channel changed, the new slow mode is returned by is_locked()
    async fn on_channel_update_slowmode(&mut self, channel: &mut MessageChannel) -> io::Result<()> {
        println!("The channel slow mode is {}", channel.is_locked());
        Ok(())
    }
}

fn main() -> io::Result<()> {
    // Possible ways to provide the address:

    // 1. From the command line directly
    let address = env::args().nth(1).unwrap_or_else(|| String::from("127.0.0.1:8080"));

    // 2. Using environment variable:
    // let address = env::var("ADDRESS").unwrap();

    // To start the socket manager, you have to use the DefaultSocketManagerBuilder.
    let manager = DefaultSocketManagerBuilder::new()
        // On this builder, you are adding all your event listeners and socket configuration
        .add_event_listener(MessageLoggerExample)
        // You can do lots of configuration before starting, checkout all the setters on the
        // DefaultSocketManagerBuilder struct!
        // Once you're done configuring your socket manager, call build to start it.
        .build()?;

    // Here you can now start using the socket manager,
    // bind(...) starts accepting connections on the address and returns the address it is bound to
    println!("Listening on address: {}", manager.bind(address)?);

    // The connections are handled on the thread pools, so the main thread can wait here until Enter is pressed
    io::stdin().read_line(&mut String::new())?;

    // Already enqueued messages are still going to be sent before the connections are closed
    manager.shutdown();
    Ok(())
}
