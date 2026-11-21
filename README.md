# libev

This open source library is intended for implementing sockets in Rust using the native I/O APIs
of each operating system. It provides event based functionality to implement protocols of any kind, allowing for
effective and scalable applications.

## Overview

The core concepts of libev have been developed to make building scalable sockets easy:

1. Event System  
    Providing simple events for every connection, to respond to new messages, disconnects and timeouts without much hassle.
2. Socket Actions  
    Easy to use way of sending messages, letting you choose between callbacks and blocking.
3. Event Loops  
    Using the native I/O API of each system: IOCP on Windows, io_uring or epoll on Linux, and kqueue on macOS and FreeBSD.

## Installation

This library is available on crates.io. The latest version is always shown in the
[GitHub Release](https://github.com/java-nio-toolkit/libev/releases/latest).

The minimum Rust version supported by libev is Rust 1.86.

```toml
[dependencies]
libev = "$version" # replace $version with the latest version
bytes = "1" # required for the BytesMut messages received and sent by the listeners
```

## Getting started

There are a number of [examples](https://github.com/java-nio-toolkit/libev/tree/master/examples) to introduce you to
libev. You can also take a look at the [Documentation](https://java-nio-toolkit.github.io/libev/).

Every socket implemented by libev starts out using the
[DefaultSocketManagerBuilder](https://java-nio-toolkit.github.io/libev/libev/struct.DefaultSocketManagerBuilder.html). The builder
provides a set of settings for the thread pools, buffers and timeouts it wants to use:

* `set_socket_pool` and `set_worker_pool` - Choose the thread pools that accept and serve the connections
* `set_threshold` and `set_large_threshold` - Choose when a channel enters and leaves slow mode
* `set_connect_timeout`, `set_afk_timeout` and `set_shutdown_timeout` - Choose how long a connection may take to
  connect, stay idle and flush its messages

Here are some possible use-cases:

### Example: Message Logging

Simply logging messages to the console. Making use of `DefaultSocketManagerBuilder`, the intended entry point for every
socket, whether it accepts connections or connects to an address.

Starting your server and attaching an event listener:

```rust
// Every channel uses its own clone of the listener, so it has to implement Clone
#[derive(Clone)]
struct MessageLoggerExample;

impl ListenerAdapter for MessageLoggerExample {
    // Gets called for every message received from the socket
    async fn on_message_received(&mut self, _channel: &mut MessageChannel, message: BytesMut) -> io::Result<()> {
        // The message is the raw bytes received from the socket, this converts it to a readable string
        println!("[message] {}", String::from_utf8_lossy(&message));
        Ok(())
    }
}

fn main() -> io::Result<()> {
    // On this builder, you are adding all your event listeners and socket configuration
    let manager = DefaultSocketManagerBuilder::new()
        .add_event_listener(MessageLoggerExample)
        .build()?;

    // bind(...) starts accepting connections on the address and returns the address it is bound to
    println!("Listening on address: {}", manager.bind("127.0.0.1:8080")?);

    // The connections are handled on the thread pools, so the main thread can wait here until Enter is pressed
    io::stdin().read_line(&mut String::new())?;

    // Already enqueued messages are still going to be sent before the connections are closed
    manager.shutdown();
    Ok(())
}
```
You can find a more thorough example with the
[MessageLoggerExample](https://github.com/java-nio-toolkit/libev/blob/master/examples/message_logger.rs) struct.

### Example: Message Create

Simply sending a message to an address. Making use of the same `DefaultSocketManagerBuilder`, this time connecting to
the address instead of accepting connections on it.

Connecting your client and sending a message once the connection is ready:

```rust
// Every channel uses its own clone of the listener, so it has to implement Clone
#[derive(Clone)]
struct MessageCreateExample;

impl ListenerAdapter for MessageCreateExample {
    // Gets called once the connection to the address is established
    async fn on_ready(&mut self, channel: &mut MessageChannel) -> io::Result<()> {
        // send_message(...) creates a MessageCreateAction, nothing is sent until you queue it
        channel.send_message(BytesMut::from(&b"ping"[..])).queue(
            |_| println!("[queued] ping"),
            |error| eprintln!("The message could not be sent: {}", error),
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
    // On this builder, you are adding all your event listeners and socket configuration
    let manager = DefaultSocketManagerBuilder::new()
        .add_event_listener(MessageCreateExample)
        .build()?;

    // connect(...) starts connecting to the address, on_ready is called once the connection is established
    manager.connect("127.0.0.1:8080")?;

    // The connections are handled on the thread pools, so the main thread can wait here until Enter is pressed
    io::stdin().read_line(&mut String::new())?;

    // Already enqueued messages are still going to be sent before the connections are closed
    manager.shutdown();
    Ok(())
}
```

You can find a more thorough example with the
[MessageCreateExample](https://github.com/java-nio-toolkit/libev/blob/master/examples/message_create.rs) struct.

## SocketAction

In this library, the `SocketAction` trait is used as a message builder for all operations on a channel. This trait
represents a lazy message builder, as shown in this simple example:

```rust
channel.send_message(BytesMut::from("Hello Friend!")) // Create the message
    .queue(|_| (), |_| ())?; // Send the message
```

> [!IMPORTANT]
> The final call to `queue(...)` sends the message. You can also send the message synchronously with `complete()` or
> as a future with `submit()`.

The `SocketAction` trait also supports a number of operators to avoid callback hell:

* `map`
  Convert the result of the `SocketAction` to a different value
* `flat_map`
  Chain another `SocketAction` on the result
* `on_error_map`
  Supply a fallback value when the `SocketAction` fails
* `on_error_flat_map`
  Supply a fallback `SocketAction` when the `SocketAction` fails

As well as combinators like:

* `and`
  Require another `SocketAction` to complete successfully

Example:

```rust
fn greet<'a>(channel: &'a mut MessageChannel, target: &'a mut MessageChannel) -> impl SocketAction<String> + 'a {
    channel.send_message(BytesMut::from("Hello Friend!")) // send the greeting
        .flat_map(move |_| target.send_message(BytesMut::from("A friend was greeted!"))) // then tell the target
        .map(|_| String::from("The friend was greeted")) // then return the result
        .on_error_map(|error| error.to_string()) // or the reason why it failed
}
```

This could then be used in code:

```rust
greet(channel, target).queue(|result| println!("{result}"), |_| ())?;
```

## Contributing to libev

If you want to contribute to libev, make sure to base your branch off of the master branch (or a feature-branch) and
create your PR into that same branch.

Please follow the [Contributing Guidelines](.github/CONTRIBUTING.md).
