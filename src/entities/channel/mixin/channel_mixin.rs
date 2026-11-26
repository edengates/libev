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

use crate::utils::concurrent::ScheduledExecutorService;
use std::any::Any;
use std::cell::RefCell;
use std::rc::Rc;

/// The state of a channel that a [`ScheduledExecutorService`] needs to run actions on the channel.
pub trait ChannelMixin {
    /// The channels of the listeners of this mixin, by position.
    fn channels(&self) -> &RefCell<Vec<Box<dyn Any>>>;

    /// Starts to handle the queued events of the channel, unless that is already running.
    fn wake(self: Rc<Self>, manager: &mut ScheduledExecutorService);
}
