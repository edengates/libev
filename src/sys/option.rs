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

use socket2::{SockRef, TcpKeepalive};
use std::io;
use std::net::TcpStream;
use std::time::Duration;

pub(crate) fn init(
    stream: &TcpStream,
    tcp_no_delay: bool,
    is_keep_alive_enabled: bool,
    keep_alive: Option<Duration>,
    close_timeout: Option<Duration>,
) -> io::Result<()> {
    if tcp_no_delay {
        stream.set_nodelay(true)?;
    }
    if is_keep_alive_enabled {
        let socket = SockRef::from(stream);
        match keep_alive {
            Some(time) => socket.set_tcp_keepalive(&TcpKeepalive::new().with_time(time))?,
            None => socket.set_keepalive(true)?,
        }
    }
    if close_timeout.is_some() {
        SockRef::from(stream).set_linger(close_timeout)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    fn accepted() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        (client, server)
    }

    #[test]
    fn init() {
        let (_client, server) = accepted();
        super::init(&server, true, true, Some(Duration::from_secs(60)), Some(Duration::from_secs(5))).unwrap();

        assert!(server.nodelay().unwrap());
        assert!(SockRef::from(&server).keepalive().unwrap());
        assert_eq!(SockRef::from(&server).linger().unwrap(), Some(Duration::from_secs(5)));
    }

    #[test]
    fn keep_alive() {
        let (_client, server) = accepted();
        super::init(&server, false, false, Some(Duration::from_secs(60)), None).unwrap();
        assert!(!SockRef::from(&server).keepalive().unwrap());

        super::init(&server, false, true, None, None).unwrap();
        assert!(SockRef::from(&server).keepalive().unwrap());
    }
}
