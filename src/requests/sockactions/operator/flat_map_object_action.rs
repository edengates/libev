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
use std::marker::PhantomData;
use std::rc::Rc;

#[must_use]
pub struct FlatMapObjectAction<A, M, I> {
    action: A,
    function: M,
    marker: PhantomData<fn() -> I>,
}

impl<A, M, I> FlatMapObjectAction<A, M, I> {
    pub(crate) fn new(action: A, function: M) -> Self {
        Self {
            action,
            function,
            marker: PhantomData,
        }
    }
}

impl<A, M, I, B, O> SocketAction<O> for FlatMapObjectAction<A, M, I>
where
    A: SocketAction<I>,
    M: FnOnce(I) -> B,
    B: SocketAction<O>,
{
    fn complete(self) -> io::Result<O> {
        (self.function)(self.action.complete()?).complete()
    }

    fn queue<S, F>(self, success: S, failure: F) -> io::Result<()>
    where
        S: FnOnce(O),
        F: FnOnce(io::Error),
    {
        let function = self.function;
        let failure = Rc::new(Cell::new(Some(failure)));
        let catch = catcher(&failure);
        self.action.queue(
            move |result| {
                if let Err(error) = function(result).queue(success, catcher(&failure)) {
                    catcher(&failure)(error);
                }
            },
            catch,
        )
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
        let action = completed(Ok(2)).flat_map(|value| completed(Ok(value * 10)));
        assert_eq!(action.complete().unwrap(), 20);

        let action = completed(Err(io::ErrorKind::BrokenPipe.into())).flat_map(|value| completed(Ok(value * 10)));
        assert_eq!(action.complete().unwrap_err().kind(), io::ErrorKind::BrokenPipe);

        let action = completed(Ok(2)).flat_map(|_| completed(Err(io::ErrorKind::TimedOut.into())));
        assert_eq!(action.complete().unwrap_err().kind(), io::ErrorKind::TimedOut);
    }

    #[test]
    fn queue() {
        let mut received = None;
        completed(Ok(2))
            .flat_map(|value| completed(Ok(value * 10)))
            .queue(|value| received = Some(value), |_| panic!())
            .unwrap();
        assert_eq!(received, Some(20));

        for (first, second, expected) in [
            (Err(io::ErrorKind::BrokenPipe), Ok(()), io::ErrorKind::BrokenPipe),
            (Ok(()), Err(io::ErrorKind::TimedOut), io::ErrorKind::TimedOut),
        ] {
            let mut failed = None;
            completed(first.map(|_| 2).map_err(io::Error::from))
                .flat_map(move |value| completed(second.map(|_| value).map_err(io::Error::from)))
                .queue(|_| panic!(), |error| failed = Some(error.kind()))
                .unwrap();
            assert_eq!(failed, Some(expected));
        }
    }
}
