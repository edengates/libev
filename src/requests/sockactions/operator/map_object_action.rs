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
use std::io;
use std::marker::PhantomData;

#[must_use]
pub struct MapObjectAction<A, M, I> {
    action: A,
    function: M,
    marker: PhantomData<fn() -> I>,
}

impl<A, M, I> MapObjectAction<A, M, I> {
    pub(crate) fn new(action: A, function: M) -> Self {
        Self {
            action,
            function,
            marker: PhantomData,
        }
    }
}

impl<A, M, I, O> SocketAction<O> for MapObjectAction<A, M, I>
where
    A: SocketAction<I>,
    M: FnOnce(I) -> O,
{
    fn complete(self) -> io::Result<O> {
        self.action.complete().map(self.function)
    }

    fn queue<S, F>(self, success: S, failure: F) -> io::Result<()>
    where
        S: FnOnce(O),
        F: FnOnce(io::Error),
    {
        let function = self.function;
        self.action.queue(move |result| success(function(result)), failure)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct CompletedSocketAction {
        result: io::Result<u32>,
    }

    impl SocketAction<u32> for CompletedSocketAction {
        fn complete(self) -> io::Result<u32> {
            self.result
        }
    }

    #[test]
    fn complete() {
        let action = CompletedSocketAction { result: Ok(2) }.map(|value| value * 10);
        assert_eq!(action.complete().unwrap(), 20);

        let action = CompletedSocketAction { result: Err(io::ErrorKind::BrokenPipe.into()) }.map(|value| value * 10);
        assert_eq!(action.complete().unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    }

    #[test]
    fn queue() {
        let mut received = None;
        CompletedSocketAction { result: Ok(2) }
            .map(|value| value.to_string())
            .queue(|value| received = Some(value), |_| panic!())
            .unwrap();
        assert_eq!(received.as_deref(), Some("2"));

        let mut failed = None;
        CompletedSocketAction { result: Err(io::ErrorKind::BrokenPipe.into()) }
            .map(|value| value.to_string())
            .queue(|_| panic!(), |error| failed = Some(error.kind()))
            .unwrap();
        assert_eq!(failed, Some(io::ErrorKind::BrokenPipe));
    }
}
