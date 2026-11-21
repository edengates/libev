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

use crate::requests::sockactions::SocketAction;
use crate::requests::sockactions::operator::catcher;
use std::cell::Cell;
use std::io;
use std::rc::Rc;

#[must_use]
pub struct MapErrorObjectAction<A, M> {
    action: A,
    map: M,
}

impl<A, M> MapErrorObjectAction<A, M> {
    pub(crate) fn new(action: A, map: M) -> Self {
        Self { action, map }
    }
}

impl<A, M, T> SocketAction<T> for MapErrorObjectAction<A, M>
where
    A: SocketAction<T>,
    M: FnOnce(io::Error) -> T,
{
    fn complete(self) -> io::Result<T> {
        Ok(self.action.complete().unwrap_or_else(self.map))
    }

    fn queue<S, F>(self, success: S, _failure: F) -> io::Result<()>
    where
        S: FnOnce(T),
        F: FnOnce(io::Error),
    {
        let map = self.map;
        let success = Rc::new(Cell::new(Some(success)));
        let then = catcher(&success);
        self.action.queue(then, move |error| catcher(&success)(map(error)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct CompletedSocketAction {
        result: io::Result<String>,
    }

    impl SocketAction<String> for CompletedSocketAction {
        fn complete(self) -> io::Result<String> {
            self.result
        }
    }

    fn completed(result: io::Result<String>) -> CompletedSocketAction {
        CompletedSocketAction { result }
    }

    #[test]
    fn complete() {
        let action = completed(Ok(String::from("sent"))).on_error_map(|error| error.kind().to_string());
        assert_eq!(action.complete().unwrap(), "sent");

        let action = completed(Err(io::ErrorKind::BrokenPipe.into())).on_error_map(|_| String::from("fallback"));
        assert_eq!(action.complete().unwrap(), "fallback");
    }

    #[test]
    fn queue() {
        for (result, expected) in [
            (Ok(String::from("sent")), "sent"),
            (Err(io::ErrorKind::BrokenPipe.into()), "fallback"),
        ] {
            let mut received = None;
            completed(result)
                .on_error_map(|_| String::from("fallback"))
                .queue(|value| received = Some(value), |_| panic!())
                .unwrap();
            assert_eq!(received.as_deref(), Some(expected));
        }
    }
}
