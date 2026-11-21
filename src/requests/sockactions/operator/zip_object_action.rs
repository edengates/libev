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
use std::cell::{Cell, RefCell};
use std::io;
use std::rc::Rc;

#[must_use]
pub struct ZipObjectAction<A, I> {
    action: A,
    other: I,
}

impl<A, I> ZipObjectAction<A, I> {
    pub(crate) fn new(action: A, other: I) -> Self {
        Self { action, other }
    }
}

impl<A, I, T> SocketAction<Vec<T>> for ZipObjectAction<A, I>
where
    A: SocketAction<T>,
    I: Iterator,
    I::Item: SocketAction<T>,
{
    fn complete(self) -> io::Result<Vec<T>> {
        let mut data = vec![self.action.complete()?];
        for action in self.other {
            data.push(action.complete()?);
        }
        Ok(data)
    }

    fn queue<S, F>(self, success: S, failure: F) -> io::Result<()>
    where
        S: FnOnce(Vec<T>),
        F: FnOnce(io::Error),
    {
        let other: Vec<I::Item> = self.other.collect();
        let data = Rc::new(RefCell::new((0..=other.len()).map(|_| None).collect()));
        let success = Rc::new(Cell::new(Some(success)));
        let failure = Rc::new(Cell::new(Some(failure)));
        if let Err(error) = self.action.queue(done(0, &data, &success), fail(&success, &failure)) {
            fail(&success, &failure)(error);
        }
        for (index, action) in other.into_iter().enumerate() {
            if let Err(error) = action.queue(done(index + 1, &data, &success), fail(&success, &failure)) {
                fail(&success, &failure)(error);
            }
        }
        Ok(())
    }
}

fn done<T, S: FnOnce(Vec<T>)>(
    index: usize,
    data: &Rc<RefCell<Vec<Option<T>>>>,
    success: &Rc<Cell<Option<S>>>,
) -> impl FnOnce(T) + use<T, S> {
    let data = Rc::clone(data);
    let success = catcher(success);
    move |value| {
        data.borrow_mut()[index] = Some(value);
        if data.borrow().iter().all(Option::is_some) {
            success(data.take().into_iter().flatten().collect());
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
        let action = completed(Ok(1)).zip([completed(Ok(2)), completed(Ok(3))]);
        assert_eq!(action.complete().unwrap(), [1, 2, 3]);

        assert_eq!(completed(Ok(1)).zip(Vec::<CompletedSocketAction>::new()).complete().unwrap(), [1]);

        let action = completed(Err(io::ErrorKind::BrokenPipe.into())).zip([completed(Ok(2))]);
        assert_eq!(action.complete().unwrap_err().kind(), io::ErrorKind::BrokenPipe);

        let action = completed(Ok(1)).zip([completed(Ok(2)), completed(Err(io::ErrorKind::TimedOut.into()))]);
        assert_eq!(action.complete().unwrap_err().kind(), io::ErrorKind::TimedOut);
    }

    #[test]
    fn queue() {
        let mut succeeded = Vec::new();
        completed(Ok(1))
            .zip([completed(Ok(2)), completed(Ok(3))])
            .queue(|data| succeeded.push(data), |_| panic!())
            .unwrap();
        assert_eq!(succeeded, [[1, 2, 3]]);

        for (first, second, expected) in [
            (Err(io::ErrorKind::BrokenPipe), Ok(()), io::ErrorKind::BrokenPipe),
            (Ok(()), Err(io::ErrorKind::TimedOut), io::ErrorKind::TimedOut),
            (Err(io::ErrorKind::BrokenPipe), Err(io::ErrorKind::TimedOut), io::ErrorKind::BrokenPipe),
        ] {
            let mut failed = Vec::new();
            completed(first.map(|_| 1).map_err(io::Error::from))
                .zip([completed(second.map(|_| 2).map_err(io::Error::from))])
                .queue(|_| panic!(), |error| failed.push(error.kind()))
                .unwrap();
            assert_eq!(failed, [expected]);
        }
    }
}
