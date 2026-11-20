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
use std::{env, io};

// Every channel uses its own clone of the listener, so it has to implement Clone
#[derive(Clone)]
struct MessageCreateExample {
    content: String,
}

impl ListenerAdapter for MessageCreateExample {
    // This overrides the method called on_ready in the ListenerAdapter trait
    // It gets called once the connection to the address is established
    async fn on_ready(&mut self, channel: &mut MessageChannel) -> io::Result<()> {
        // send_message(...) creates a MessageCreateAction, nothing is sent until you queue it
        // queue(...) sends the message in the background and calls one of the callbacks once it is done
        channel.send_message(BytesMut::from(self.content.as_bytes())).queue(
            |_| println!("[sent] {}", self.content),
            |error| println!("The message could not be sent: {}", error),
        )
    }

    // Gets called for every reply of the address, this one prints it and closes the channel
    async fn on_message_received(&mut self, channel: &mut MessageChannel, message: BytesMut) -> io::Result<()> {
        println!("[reply] {}", String::from_utf8_lossy(&message));
        channel.close();
        Ok(())
    }
}

fn main() -> io::Result<()> {
    // The address to connect to and the message to send, both can be provided from the command line
    let address = env::args().nth(1).unwrap_or_else(|| String::from("127.0.0.1:8080"));
    let content = env::args().nth(2).unwrap_or_else(|| String::from("ping"));

    // To start the socket manager, you have to use the DefaultSocketManagerBuilder.
    let manager = DefaultSocketManagerBuilder::new()
        // On this builder, you are adding all your event listeners and socket configuration
        .add_event_listener(MessageCreateExample { content })
        // Once you're done configuring your socket manager, call build to start it.
        .build()?;

    // connect(...) starts connecting to the address, on_ready is called once the connection is established
    println!("Connecting to address: {}", address);
    manager.connect(address.as_str())?;

    // The connections are handled on the thread pools, so the main thread can wait here until Enter is pressed
    io::stdin().read_line(&mut String::new())?;

    // Already enqueued messages are still going to be sent before the connections are closed
    manager.shutdown();
    Ok(())
}
