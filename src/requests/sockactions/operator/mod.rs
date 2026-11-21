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

use std::cell::Cell;
use std::io;
use std::rc::Rc;

mod combine_object_action;
pub use combine_object_action::CombineObjectAction;
mod flat_map_error_object_action;
pub use flat_map_error_object_action::FlatMapErrorObjectAction;
mod flat_map_object_action;
pub use flat_map_object_action::FlatMapObjectAction;
mod map_error_object_action;
pub use map_error_object_action::MapErrorObjectAction;
mod map_object_action;
pub use map_object_action::MapObjectAction;
mod zip_object_action;
pub use zip_object_action::ZipObjectAction;

pub(crate) fn catcher<V, C: FnOnce(V)>(callback: &Rc<Cell<Option<C>>>) -> impl FnOnce(V) + use<V, C> {
    let callback = Rc::clone(callback);
    move |value| {
        if let Some(callback) = callback.take() {
            callback(value);
        }
    }
}

pub(crate) fn fail<S, F: FnOnce(io::Error)>(
    success: &Rc<Cell<Option<S>>>,
    failure: &Rc<Cell<Option<F>>>,
) -> impl FnOnce(io::Error) + use<S, F> {
    let success = Rc::clone(success);
    let failure = catcher(failure);
    move |error| {
        success.take();
        failure(error);
    }
}
