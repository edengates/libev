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

use std::time::{Duration, SystemTime};

/// The epoch millisecond timestamp the Snowflake ids are counted from, the first second of February 11, 2025 (UTC).
pub const EPOCH: u64 = 1_739_232_000_000;
/// The number of bits the timestamp is shifted to the left in a Snowflake id.
pub const TIMESTAMP_OFFSET: u64 = 22;

/// Marks a snowflake entity. Snowflake entities are ones that have an id that uniquely identifies them.
pub trait ISnowflake {
    /// The Snowflake id of this entity. This is unique to every entity and will never change.
    fn id(&self) -> u64;

    /// The time this entity was created. Calculated through the Snowflake in [`id`](Self::id).
    fn time_created(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_millis((self.id() >> TIMESTAMP_OFFSET) + EPOCH)
    }
}
