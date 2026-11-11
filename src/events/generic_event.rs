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

use std::any::Any;

/// Interface for events supported by [`MessageChannel`].
///
/// # See also
///
/// - [`ListenerAdapter::on_generic_event`]
///
/// [`MessageChannel`]: crate::entities::channel::middleman::MessageChannel
/// [`ListenerAdapter::on_generic_event`]: crate::hooks::ListenerAdapter::on_generic_event
pub trait GenericEvent: Any + Send {}

// We do the downcasting on dyn GenericEvent because type parameters would make the trait not dyn-compatible.
impl dyn GenericEvent {
    /// Returns `true` if the inner type is the same as `T`.
    pub fn is<T: GenericEvent>(&self) -> bool {
        (self as &dyn Any).is::<T>()
    }

    /// Returns some mutable reference to the inner value if it is of type `T`, or
    /// `None` if it isn't.
    pub fn downcast_mut<T: GenericEvent>(&mut self) -> Option<&mut T> {
        (self as &mut dyn Any).downcast_mut::<T>()
    }
}
