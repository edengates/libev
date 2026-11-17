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

use std::sync::atomic::{AtomicUsize, Ordering};

pub trait ThreadPoolProvider<T>: Send + Sync {
    fn provide(&self) -> &T;

    fn shards(&self) -> &[T];
}

/// Round-robin over the shards.
struct LazySharedProvider<T> {
    count: AtomicUsize,
    shards: Vec<T>,
}

impl<T> LazySharedProvider<T> {
    fn new(shards: Vec<T>) -> Self {
        Self {
            count: AtomicUsize::new(0),
            shards,
        }
    }
}

impl<T: Send + Sync> ThreadPoolProvider<T> for LazySharedProvider<T> {
    fn provide(&self) -> &T {
        let count = self.count.fetch_add(1, Ordering::Relaxed);
        let length = self.shards.len();
        &self.shards[if length.is_power_of_two() { count & (length - 1) } else { count % length }]
    }

    fn shards(&self) -> &[T] {
        &self.shards
    }
}

pub fn lazy<T: Send + Sync + 'static>(shards: Vec<T>) -> Box<dyn ThreadPoolProvider<T>> {
    Box::new(LazySharedProvider::new(shards))
}

#[cfg(test)]
mod tests {
    #[test]
    fn eager() {
        let provider = super::lazy(vec!["a", "b"]);
        assert_eq!(*provider.provide(), "a");
        assert_eq!(*provider.provide(), "b");
        assert_eq!(*provider.provide(), "a");
    }

    #[test]
    fn lazy() {
        let provider = super::lazy(vec!["a", "b", "c"]);
        assert_eq!(*provider.provide(), "a");
        assert_eq!(*provider.provide(), "b");
        assert_eq!(*provider.provide(), "c");
        assert_eq!(*provider.provide(), "a");
    }
}