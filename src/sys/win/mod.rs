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

use crate::SocketManager;
use crate::utils::concurrent::ExecutorCompletionService;
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
pub use std::os::windows::io::BorrowedSocket;
use std::os::windows::io::AsSocket;
use std::sync::mpsc::Receiver;
use windows_sys::Win32::System::Threading::{
    GetCurrentThread, SetThreadInformation, THREAD_POWER_THROTTLING_CURRENT_VERSION,
    THREAD_POWER_THROTTLING_EXECUTION_SPEED, THREAD_POWER_THROTTLING_STATE, ThreadPowerThrottling,
};

/// Asks Windows to run the current thread at full speed, unless `is_power_suppressed_enabled` allows to suppress the
/// power of the thread to save energy.
pub fn set_power_suppressed(is_power_suppressed_enabled: bool) {
    let state = THREAD_POWER_THROTTLING_STATE {
        Version: THREAD_POWER_THROTTLING_CURRENT_VERSION,
        ControlMask: if is_power_suppressed_enabled { 0 } else { THREAD_POWER_THROTTLING_EXECUTION_SPEED },
        StateMask: 0,
    };
    unsafe {
        SetThreadInformation(
            GetCurrentThread(),
            ThreadPowerThrottling,
            (&raw const state).cast(),
            size_of::<THREAD_POWER_THROTTLING_STATE>() as u32,
        );
    }
}

#[path = "../option.rs"]
mod option;
pub(crate) mod socket;
pub mod event;
mod completion;
pub use completion::Completion;
mod completion_service;
pub use completion_service::CompletionService;
mod scheduled_executor_service;
pub use scheduled_executor_service::EventArray;
mod socket_channel;
pub use socket_channel::SocketChannel;
mod server_socket_channel;
pub use server_socket_channel::ServerSocketChannel;
pub(crate) fn as_socket(stream: &TcpStream) -> BorrowedSocket<'_> {
    stream.as_socket()
}

pub(crate) fn bind(
    pool: &ExecutorCompletionService,
    listener: TcpListener,
    socket_manager: SocketManager,
) -> Receiver<io::Result<()>> {
    pool.register(move || Box::new(ServerSocketChannel::new(listener, socket_manager)))
}

pub(crate) fn connect(
    pool: &ExecutorCompletionService,
    socket: TcpStream,
    address: SocketAddr,
    socket_manager: SocketManager,
) -> Receiver<io::Result<()>> {
    pool.register(move || Box::new(SocketChannel::connect(socket, address, socket_manager)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use windows_sys::Win32::System::Threading::GetThreadInformation;

    fn control_mask() -> u32 {
        let mut state = THREAD_POWER_THROTTLING_STATE {
            Version: THREAD_POWER_THROTTLING_CURRENT_VERSION,
            ControlMask: 0,
            StateMask: 0,
        };
        let data = unsafe {
            GetThreadInformation(
                GetCurrentThread(),
                ThreadPowerThrottling,
                (&raw mut state).cast(),
                size_of::<THREAD_POWER_THROTTLING_STATE>() as u32,
            )
        };
        assert_ne!(data, 0);
        state.ControlMask
    }

    #[test]
    fn set_power_suppressed() {
        super::set_power_suppressed(false);
        assert_eq!(control_mask(), THREAD_POWER_THROTTLING_EXECUTION_SPEED);
        super::set_power_suppressed(true);
        assert_eq!(control_mask(), 0);
    }
}
