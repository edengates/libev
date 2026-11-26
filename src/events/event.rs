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

use crate::events::GenericEvent;

pub trait Event: GenericEvent {
    fn identifier(&self) -> usize;
}

impl GenericEvent for Box<dyn GenericEvent> {}

impl Event for Box<dyn GenericEvent> {
    fn identifier(&self) -> usize {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::channel::ChannelReadyEvent;
    use crate::events::buffer::BufferReceivedEvent;
    use std::any::Any;

    struct CustomEvent;

    impl GenericEvent for CustomEvent {}

    #[test]
    fn identifier() {
        assert_eq!(ChannelReadyEvent.identifier(), ChannelReadyEvent::IDENTIFIER);

        let event: Box<dyn Event> = Box::new(BufferReceivedEvent::new(7u32));
        assert_eq!(event.identifier(), BufferReceivedEvent::IDENTIFIER);
        assert_eq!((event as Box<dyn Any>).downcast::<BufferReceivedEvent<u32>>().unwrap().buffer(), 7);

        let event: Box<dyn Event> = Box::new(Box::new(CustomEvent) as Box<dyn GenericEvent>);
        assert_eq!(event.identifier(), 0);
        let event = (event as Box<dyn Any>).downcast::<Box<dyn GenericEvent>>().unwrap();
        assert!((**event).is::<CustomEvent>());
    }
}
