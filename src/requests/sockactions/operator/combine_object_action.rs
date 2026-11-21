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
use crate::requests::sockactions::operator::{catcher, fail};
use std::cell::Cell;
use std::io;
use std::marker::PhantomData;
use std::rc::Rc;

#[must_use]
pub struct CombineObjectAction<A, B, I, U> {
    action: A,
    other: B,
    marker: PhantomData<fn() -> (I, U)>,
}

impl<A, B, I, U> CombineObjectAction<A, B, I, U> {
    pub(crate) fn new(action: A, other: B) -> Self {
        Self {
            action,
            other,
            marker: PhantomData,
        }
    }
}

impl<A, B, I, U> SocketAction<()> for CombineObjectAction<A, B, I, U>
where
    A: SocketAction<I>,
    B: SocketAction<U>,
{
    fn complete(self) -> io::Result<()> {
        self.action.complete()?;
        self.other.complete()?;
        Ok(())
    }

    fn queue<S, F>(self, success: S, failure: F) -> io::Result<()>
    where
        S: FnOnce(()),
        F: FnOnce(io::Error),
    {
        let success = Rc::new(Cell::new(Some(success)));
        let failure = Rc::new(Cell::new(Some(failure)));
        let count = Rc::new(Cell::new(0));
        if let Err(error) = self.action.queue(done(&count, &success), fail(&success, &failure)) {
            fail(&success, &failure)(error);
        }
        if let Err(error) = self.other.queue(done(&count, &success), fail(&success, &failure)) {
            fail(&success, &failure)(error);
        }
        Ok(())
    }
}

fn done<V, S: FnOnce(())>(count: &Rc<Cell<u8>>, success: &Rc<Cell<Option<S>>>) -> impl FnOnce(V) + use<V, S> {
    let count = Rc::clone(count);
    let success = catcher(success);
    move |_| {
        count.set(count.get() + 1);
        if count.get() == 2 {
            success(());
        }
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
        assert!(completed(Ok(1)).and(completed(Ok(2))).complete().is_ok());

        let action = completed(Err(io::ErrorKind::BrokenPipe.into())).and(completed(Ok(2)));
        assert_eq!(action.complete().unwrap_err().kind(), io::ErrorKind::BrokenPipe);

        let action = completed(Ok(1)).and(completed(Err(io::ErrorKind::TimedOut.into())));
        assert_eq!(action.complete().unwrap_err().kind(), io::ErrorKind::TimedOut);
    }

    #[test]
    fn queue() {
        let mut succeeded = 0;
        completed(Ok(1))
            .and(completed(Ok(2)))
            .queue(|_| succeeded += 1, |_| panic!())
            .unwrap();
        assert_eq!(succeeded, 1);

        for (first, second, expected) in [
            (Err(io::ErrorKind::BrokenPipe), Ok(()), io::ErrorKind::BrokenPipe),
            (Ok(()), Err(io::ErrorKind::TimedOut), io::ErrorKind::TimedOut),
            (Err(io::ErrorKind::BrokenPipe), Err(io::ErrorKind::TimedOut), io::ErrorKind::BrokenPipe),
        ] {
            let mut failed = Vec::new();
            completed(first.map(|_| 1).map_err(io::Error::from))
                .and(completed(second.map(|_| 2).map_err(io::Error::from)))
                .queue(|_| panic!(), |error| failed.push(error.kind()))
                .unwrap();
            assert_eq!(failed, [expected]);
        }
    }
}
