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

use std::any::type_name;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::{Builder, JoinHandle};

/// Creates the threads used by the internal executors.
pub struct CountingThreadFactory {
    identifier: Box<dyn Fn() -> String + Send + Sync>,
    specifier: String,
    count: AtomicUsize,
}

impl CountingThreadFactory {
    /// Creates a new [`CountingThreadFactory`] with the provided identifier and specifier.
    ///
    /// The thread name is `{identifier} {specifier}-Worker {id}`, with the id starting at 1.
    ///
    /// # Arguments
    ///
    /// - `identifier` — The function, which provides the identifier of each thread name
    /// - `specifier` — The specifier of each thread name
    ///
    /// # See also
    ///
    /// - [`of`](Self::of)
    pub fn new<F, S>(identifier: F, specifier: &str) -> Self
    where
        F: Fn() -> S + Send + Sync + 'static,
        S: Into<String>,
    {
        Self {
            identifier: Box::new(move || identifier().into()),
            specifier: specifier.to_string(),
            count: AtomicUsize::new(1),
        }
    }

    /// Creates a new [`CountingThreadFactory`] with the identifier of the type `T`.
    ///
    /// # Arguments
    ///
    /// - `specifier` — The specifier of each thread name
    ///
    /// # See also
    ///
    /// - [`new`](Self::new)
    pub fn of<T>(specifier: &str) -> Self {
        Self::new(move || identifier_string::<T>(), specifier)
    }

    /// Creates a new thread which executes the specified task.
    ///
    /// # Arguments
    ///
    /// - `task` — The task to execute
    ///
    /// # Errors
    ///
    /// Returns an error if the thread could not be created.
    ///
    /// # Returns
    ///
    /// The [`JoinHandle`] of the new thread.
    pub fn new_thread<F, R>(&self, task: F) -> io::Result<JoinHandle<R>>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let id = self.count.fetch_add(1, Ordering::Relaxed);
        Builder::new()
            .name(format!(
                "{} {}-Worker {}",
                (self.identifier)(),
                self.specifier,
                id
            ))
            .spawn(task)
    }
}

/// The identifier of the type `T`: the name without the path and generic types.
pub fn identifier_string<T>() -> String {
    lowercase({
        type_name::<T>()
            .split('<')
            .next()
            .unwrap()
            .rsplit("::")
            .next()
            .unwrap()
    })
}

/// Converts the first letter to lowercase, if it is followed by a lowercase letter or by nothing.
///
/// Returns `unknown` if the text is empty.
///
/// # Arguments
///
/// - `text` — The text to convert
fn lowercase(text: &str) -> String {
    if text.is_empty() {
        return "unknown".to_string();
    }

    let mut chars = text.chars();
    let current = chars.next().unwrap();
    let suffix = chars.as_str();

    if suffix.is_empty() || (current.is_uppercase() && suffix.starts_with(char::is_lowercase)) {
        current.to_lowercase().chain(suffix.chars()).collect()
    } else {
        text.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    fn thread_name(factory: &CountingThreadFactory) -> String {
        factory
            .new_thread(|| thread::current().name().unwrap().to_string())
            .unwrap()
            .join()
            .unwrap()
    }

    #[test]
    fn name() {
        let factory = CountingThreadFactory::new(|| "Test".to_string(), "Timer");
        assert_eq!(thread_name(&factory), "Test Timer-Worker 1");
        assert_eq!(thread_name(&factory), "Test Timer-Worker 2");
    }

    struct TestWorker;

    #[test]
    fn identifier() {
        assert_eq!(identifier_string::<TestWorker>(), "testWorker");
        assert_eq!(identifier_string::<Vec<String>>(), "vec");
    }

    #[test]
    fn of() {
        let factory = CountingThreadFactory::of::<TestWorker>("Io");
        assert_eq!(thread_name(&factory), "testWorker Io-Worker 1");
    }
}
