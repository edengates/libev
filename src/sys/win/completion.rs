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

use crate::handle::Request;
use std::io;
use windows_sys::Win32::Foundation::{NTSTATUS, RtlNtStatusToDosError};

/// An operation that has completed, together with the [`Request`] that was submitted for it.
pub struct Completion {
    request: Box<dyn Request>,
    transferred: usize,
    error: Option<io::Error>,
}

impl Completion {
    pub(crate) fn new(mut request: Box<dyn Request>, transferred: usize) -> Self {
        let status = unsafe { (*request.overlapped()).Internal } as NTSTATUS;
        let error = (status != 0)
            .then(|| io::Error::from_raw_os_error(unsafe { RtlNtStatusToDosError(status) } as i32));
        Self { request, transferred, error }
    }

    /// The number of bytes that the operation has transferred.
    pub fn transferred(&self) -> usize {
        self.transferred
    }

    /// Returns the request, or the error that the operation has failed with.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidInput`](io::ErrorKind::InvalidInput) if the request is not an `R`.
    pub fn into_request<R: Request>(self) -> io::Result<Box<R>> {
        let (request, _, error) = self.into_parts()?;
        match error {
            Some(error) => Err(error),
            None => Ok(request),
        }
    }

    /// Returns the request, the number of bytes that the operation has transferred, and the error that the operation
    /// has failed with, if any.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidInput`](io::ErrorKind::InvalidInput) if the request is not an `R`.
    pub fn into_parts<R: Request>(self) -> io::Result<(Box<R>, usize, Option<io::Error>)> {
        let request = self
            .request
            .into_any()
            .downcast::<R>()
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        Ok((request, self.transferred, self.error))
    }
}
