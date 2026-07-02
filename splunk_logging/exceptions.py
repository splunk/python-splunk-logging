# Copyright 2021 Splunk Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


class HecError(Exception):
    """Base exception for HEC delivery errors."""


class HecAckError(HecError):
    """Raised when HEC indexer acknowledgment fails."""


class HecAckTimeoutError(HecAckError):
    """Raised when HEC does not acknowledge a request before its deadline."""


class HecBatchError(HecError):
    """Raised when HEC accepts only the events before an invalid batch event."""

    def __init__(self, invalid_event_number: int, total_count: int, response):
        self.invalid_event_number = invalid_event_number
        self.accepted_count = invalid_event_number
        self.total_count = total_count
        self.response = response
        super().__init__(
            f"HEC rejected event {invalid_event_number} after accepting {self.accepted_count} "
            f"of {total_count} batched events"
        )


class HecEventTooLargeError(HecError):
    """Raised when one event exceeds the configured batch request limit."""

    def __init__(self, event_size: int, max_batch_bytes: int):
        self.event_size = event_size
        self.max_batch_bytes = max_batch_bytes
        super().__init__(f"Serialized HEC event is {event_size} bytes; limit is {max_batch_bytes} bytes")


class HecQueueFullError(HecError):
    """Raised when a batch queue cannot accept more events before its deadline."""

    def __init__(self, enqueued_count: int, next_event_index: int):
        self.enqueued_count = enqueued_count
        self.next_event_index = next_event_index
        super().__init__(
            f"HEC batch queue is full after accepting {enqueued_count} events; "
            f"resume at event index {next_event_index}"
        )


class HecWorkerError(HecError):
    """Raised after a background batch delivery fails."""

    def __init__(self, message: str, cause: Exception, failed_events: tuple[dict, ...]):
        self.cause = cause
        self.failed_events = failed_events
        super().__init__(message)
