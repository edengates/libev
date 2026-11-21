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
pub struct FlatMapErrorObjectAction<A, M> {
    action: A,
    map: M,
}

impl<A, M> FlatMapErrorObjectAction<A, M> {
    pub(crate) fn new(action: A, map: M) -> Self {
        Self { action, map }
    }
}

impl<A, M, B, T> SocketAction<T> for FlatMapErrorObjectAction<A, M>
where
    A: SocketAction<T>,
    M: FnOnce(io::Error) -> B,
    B: SocketAction<T>,
{
    fn complete(self) -> io::Result<T> {
        match self.action.complete() {
            Ok(result) => Ok(result),
            Err(error) => (self.map)(error).complete(),
        }
    }

    fn queue<S, F>(self, success: S, failure: F) -> io::Result<()>
    where
        S: FnOnce(T),
        F: FnOnce(io::Error),
    {
        let map = self.map;
        let success = Rc::new(Cell::new(Some(success)));
        let then = catcher(&success);
        self.action.queue(then, move |error| {
            let failure = Rc::new(Cell::new(Some(failure)));
            if let Err(error) = map(error).queue(catcher(&success), catcher(&failure)) {
                catcher(&failure)(error);
            }
        })
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

    fn completed(result: io::Result<u32>) -> CompletedSocketAction {
        CompletedSocketAction { result }
    }

    #[test]
    fn complete() {
        let action = completed(Ok(2)).on_error_flat_map(|_| completed(Ok(0)));
        assert_eq!(action.complete().unwrap(), 2);

        let action = completed(Err(io::ErrorKind::BrokenPipe.into())).on_error_flat_map(|_| completed(Ok(0)));
        assert_eq!(action.complete().unwrap(), 0);

        let action =
            completed(Err(io::ErrorKind::BrokenPipe.into())).on_error_flat_map(|_| completed(Err(io::ErrorKind::TimedOut.into())));
        assert_eq!(action.complete().unwrap_err().kind(), io::ErrorKind::TimedOut);
    }

    #[test]
    fn queue() {
        for (result, expected) in [(Ok(2), 2), (Err(io::ErrorKind::BrokenPipe.into()), 0)] {
            let mut received = None;
            completed(result)
                .on_error_flat_map(|_| completed(Ok(0)))
                .queue(|value| received = Some(value), |_| panic!())
                .unwrap();
            assert_eq!(received, Some(expected));
        }

        let mut failed = None;
        completed(Err(io::ErrorKind::BrokenPipe.into()))
            .on_error_flat_map(|_| completed(Err(io::ErrorKind::TimedOut.into())))
            .queue(|_| panic!(), |error| failed = Some(error.kind()))
            .unwrap();
        assert_eq!(failed, Some(io::ErrorKind::TimedOut));
    }
}
