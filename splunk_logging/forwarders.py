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

import json
import queue
import random
import socket
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Optional, Union

import httpx
from dateutil.parser import isoparse

from .exceptions import (
    HecAckError,
    HecAckTimeoutError,
    HecBatchError,
    HecEventTooLargeError,
    HecQueueFullError,
    HecWorkerError,
)


@dataclass(frozen=True)
class _QueuedEvent:
    envelope: dict
    payload: str
    size: int


@dataclass
class _FlushRequest:
    completed: bool = False


class HecForwarder:
    _RETRY_STATUS_CODES = frozenset(
        [
            httpx.codes.REQUEST_TIMEOUT,
            httpx.codes.TOO_MANY_REQUESTS,
            httpx.codes.INTERNAL_SERVER_ERROR,
            httpx.codes.SERVICE_UNAVAILABLE,
        ]
    )

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8088,
        token: Optional[str] = None,
        use_ssl: bool = True,
        verify_ssl: bool = True,
        default_host: Optional[str] = None,
        default_source: Optional[str] = None,
        default_sourcetype: Optional[str] = None,
        default_index: Optional[str] = None,
        *,
        indexer_ack: bool = False,
        channel_id: Optional[str] = None,
        ack_poll_interval: float = 10.0,
        ack_timeout: float = 300.0,
    ):
        """
        Initializes a HecForwarder instance for sending events to a Splunk HEC listener.

        Args:
            host (str):
                IP or domain name of the Splunk server (default: "localhost").
            port (int, optional):
                Port on the Splunk server the HEC is listening on (default: 8088).
            token (str, optional):
                HEC token (default: None).
            use_ssl (bool, optional):
                Whether to connect with HTTPS. (default: True)
            verify_ssl (bool, optional):
                Whether to verify the server's SSL certificate. (default: True)
            default_host (str, optional):
                Default host Splunk sets for this event. (default: system hostname)
            default_source (str, optional):
                Default source to set when forwarding events. (default: "")
            default_sourcetype (str, optional):
                Default sourcetype to use when forwarding events. (default: "")
            default_index (str, optional):
                Default index to send events. (default: "")
            indexer_ack (bool, optional):
                Wait for Splunk indexer acknowledgment after sending events. (default: False)
            channel_id (str, optional):
                GUID used for indexer acknowledgment requests. (default: generated UUID)
            ack_poll_interval (float, optional):
                Seconds to wait between acknowledgment queries. (default: 10)
            ack_timeout (float, optional):
                Seconds to wait for an acknowledgment before raising. (default: 300)
        """
        self._host = host
        self._port = port
        self._token = token or ""
        self._use_ssl = use_ssl
        self._verify_ssl = verify_ssl
        self._default_host = default_host or socket.gethostname()
        self._default_source = default_source or ""
        self._default_sourcetype = default_sourcetype or ""
        self._default_index = default_index or ""
        if channel_id is not None:
            uuid.UUID(channel_id)
        self._channel_id = channel_id or str(uuid.uuid4())
        self._indexer_ack = indexer_ack
        self._ack_poll_interval = ack_poll_interval
        self._ack_timeout = ack_timeout
        self._backoff_factor = 1
        self._max_retries = 3

        self._client = self._create_client()
        return

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        self._client.close()

    def _create_client(self) -> httpx.Client:
        scheme = "https" if self._use_ssl else "http"
        return httpx.Client(
            base_url=httpx.URL(scheme=scheme, host=self._host, port=self._port),
            verify=self._verify_ssl,
            headers={"Authorization": f"Splunk {self._token}"},
        )

    def _request(self, method: str, path: str, **kwargs) -> httpx.Response:
        retry_attempt = 0
        while True:
            response = self._client.request(method, path, **kwargs)
            if response.status_code not in self._RETRY_STATUS_CODES:
                response.raise_for_status()
                return response

            if retry_attempt >= self._max_retries:
                response.raise_for_status()

            retry_after = response.headers.get("Retry-After")
            try:
                delay = float(retry_after) if retry_after is not None else None
            except ValueError:
                delay = None

            if delay is None:
                delay = self._backoff_factor * 2**retry_attempt + random.uniform(0, retry_attempt)

            time.sleep(delay)
            retry_attempt += 1

    def _parse_timestamp(self, timeinfo: Union[str, int, float, datetime], timefmt: Optional[str] = None) -> float:
        """
        Parses various timestamp formats into a float.

        Args:
            timeinfo (str, int, float, datetime): The timestamp to parse.
            timefmt (str, optional): Format string to parse timeinfo if it is a string.

        Returns:
            float: Parsed timestamp.

        Raises:
            ValueError: If the timestamp parsing has failed.
        """
        if isinstance(timeinfo, datetime):
            timestamp = timeinfo.timestamp()
        elif type(timeinfo) in [int, float]:
            # parsing and reconverting ensures we have a valid timestamp
            timestamp = datetime.fromtimestamp(timeinfo).timestamp()
        elif type(timeinfo) is str:
            if timefmt:
                timestamp = datetime.strptime(timeinfo, timefmt).timestamp()
            else:
                timestamp = isoparse(timeinfo).timestamp()
        else:
            raise ValueError(f"Invalid timestamp: {timeinfo}")
        return timestamp

    def forward_event(
        self,
        event: dict,
        eventtime: Optional[Union[str, int, float, datetime]] = None,
        timefmt: Optional[str] = None,
        **kwargs,
    ):
        """
        Forwards a single event to Splunk.

        Args:
            event (dict):
                JSON serializable event to forward
            eventtime (str, int, float, datetime, optional):
                Time of the event. Should be a valid timestamp or datetime object. (default: datetime.now())
            timefmt (str, optional):
                If eventtime is a string, this is the format to use to parse the eventtime string.
                By default, eventtime will be parsed as an isoformatted string.
                (default: None)
            kwargs:
                host (str, optional):
                    host field to set in Splunk. This will overwrite the default host configured for this event only
                    (default: default_host)
                source (str, optional):
                    source field to set in Splunk. This will overwrite the default source configured for this event
                    only (default: default_source)
                sourcetype (str, optional):
                    sourcetype field to set in Splunk. This will overwrite the default sourcetype configured for this
                    event only (default: default_sourcetype)
                index (str, optional):
                    index to send this event to. This will overwrite the default index configured for this event only
                    (default: default_index)
        """
        hec_event = self._build_hec_event(event, eventtime=eventtime, timefmt=timefmt, **kwargs)

        headers = self._event_headers()

        response = self._request("POST", "/services/collector/event", headers=headers, json=hec_event)
        self._wait_for_response_acknowledgment(response)
        return

    def _event_headers(self) -> dict[str, str]:
        if self._indexer_ack:
            return {"X-Splunk-Request-Channel": self._channel_id}
        return {}

    def _wait_for_response_acknowledgment(self, response: httpx.Response):
        if not self._indexer_ack:
            return

        try:
            response_data = response.json()
            ack_id = response_data.get("ackId", response_data.get("ackID"))
        except (AttributeError, TypeError, ValueError) as error:
            raise HecAckError("HEC returned an invalid acknowledgment response") from error
        if ack_id is None:
            raise HecAckError("HEC response did not include an acknowledgment ID")
        try:
            normalized_ack_id = int(ack_id)
        except (TypeError, ValueError) as error:
            raise HecAckError(f"HEC returned an invalid acknowledgment ID: {ack_id!r}") from error
        self._wait_for_acknowledgments([normalized_ack_id])

    def _wait_for_acknowledgments(self, ack_ids: list[int]):
        deadline = time.monotonic() + self._ack_timeout
        headers = {"X-Splunk-Request-Channel": self._channel_id}
        while True:
            response = self._request(
                "POST",
                "/services/collector/ack",
                headers=headers,
                json={"acks": ack_ids},
            )
            try:
                ack_statuses = response.json()["acks"]
                statuses = [ack_statuses[str(ack_id)] for ack_id in ack_ids]
            except (KeyError, TypeError, ValueError) as error:
                raise HecAckError("HEC returned an invalid acknowledgment response") from error
            if all(statuses):
                return
            if time.monotonic() >= deadline:
                raise HecAckTimeoutError(f"Timed out waiting for acknowledgments: {ack_ids}")
            time.sleep(self._ack_poll_interval)

    def _build_hec_event(
        self,
        event: dict,
        eventtime: Optional[Union[str, int, float, datetime]] = None,
        timefmt: Optional[str] = None,
        **kwargs,
    ) -> dict:
        eventtime = eventtime or datetime.now()
        host = kwargs.get("host", self._default_host)
        source = kwargs.get("source", self._default_source)
        sourcetype = kwargs.get("sourcetype", self._default_sourcetype)
        index = kwargs.get("index", self._default_index)

        hec_event = {}
        hec_event["event"] = event

        if host:
            hec_event["host"] = host
        if source:
            hec_event["source"] = source
        if sourcetype:
            hec_event["sourcetype"] = sourcetype
        if index:
            hec_event["index"] = index

        hec_event["time"] = str(self._parse_timestamp(eventtime, timefmt=timefmt))
        return hec_event

    def forward_events(
        self,
        events: list[dict],
        eventtime: Callable = lambda _: datetime.now(),
        timefmt: Optional[str] = None,
        **kwargs,
    ):
        """
        Forwards multiple events to Splunk.

        Args:
            event (dict):
                JSON serializable event to forward
            eventtime (callable, optional):
                A callable (func or lambda) that should return an event time if given an event from the list.
                (default: datetime.now() for all events)
            timefmt (str, optional):
                If eventtime is a string, this is the format to use to parse the eventtime string.
                By default, eventtime will be parsed as an isoformatted string.
                (default: None)
            kwargs:
                host (str, optional):
                    host field to set in Splunk. This will overwrite the default host configured for all events in this
                    list. (default: default_host)
                source (str, optional):
                    source field to set in Splunk. This will overwrite the default source configured for all events in
                    this list. (default: default_source)
                sourcetype (str, optional):
                    sourcetype field to set in Splunk. This will overwrite the default sourcetype configured for all
                    events in this list. (default: default_sourcetype)
                index (str, optional):
                    index to send this event to. This will overwrite the default index configured for all events in
                    this list. (default: default_index)
        """
        if not events:
            return

        hec_events = [
            self._build_hec_event(event, eventtime=eventtime(event), timefmt=timefmt, **kwargs) for event in events
        ]
        self._send_hec_events(hec_events)
        return

    def _send_hec_events(self, hec_events: list[dict]):
        payload = "".join(self._serialize_hec_event(hec_event) for hec_event in hec_events)
        self._send_hec_payload(payload, event_count=len(hec_events))

    def _serialize_hec_event(self, hec_event: dict) -> str:
        return json.dumps(hec_event)

    def _send_hec_payload(self, payload: str, *, event_count: int):
        headers = {
            "Content-Type": "application/json",
            **self._event_headers(),
        }
        try:
            response = self._request("POST", "/services/collector/event", headers=headers, content=payload)
        except httpx.HTTPStatusError as error:
            try:
                invalid_event_number = int(error.response.json()["invalid-event-number"])
            except (KeyError, TypeError, ValueError):
                raise error
            if not 0 <= invalid_event_number < event_count:
                raise error
            raise HecBatchError(invalid_event_number, event_count, error.response) from error
        self._wait_for_response_acknowledgment(response)


class BatchHecForwarder(HecForwarder):
    def __init__(
        self,
        *args,
        batch_size: int = 100,
        max_batch_bytes: int = 1_048_576,
        flush_interval: float = 2.0,
        max_queue_size: int = 10_000,
        max_queue_bytes: int = 10_485_760,
        enqueue_timeout: Optional[float] = None,
        **kwargs,
    ):
        limits = {
            "batch_size": batch_size,
            "max_batch_bytes": max_batch_bytes,
            "flush_interval": flush_interval,
            "max_queue_size": max_queue_size,
            "max_queue_bytes": max_queue_bytes,
        }
        for name, value in limits.items():
            if value <= 0:
                raise ValueError(f"{name} must be greater than zero")
        if enqueue_timeout is not None and enqueue_timeout < 0:
            raise ValueError("enqueue_timeout cannot be negative")

        super().__init__(*args, **kwargs)
        self._batch_size = batch_size
        self._max_batch_bytes = max_batch_bytes
        self._flush_interval = flush_interval
        self._max_queue_size = max_queue_size
        self._max_queue_bytes = max_queue_bytes
        self._enqueue_timeout = enqueue_timeout
        self._queue = queue.Queue()
        self._capacity = threading.Condition()
        self._producer_lock = threading.RLock()
        self._pending_count = 0
        self._pending_bytes = 0
        self._worker_error = None
        self._failed_batch = ()
        self._accepting = True
        self._stop = object()
        self._closed = False
        self._worker_thread = threading.Thread(target=self._worker, name="splunk-hec-batch", daemon=True)
        self._worker_thread.start()

    def forward_event(
        self,
        event: dict,
        eventtime: Optional[Union[str, int, float, datetime]] = None,
        timefmt: Optional[str] = None,
        **kwargs,
    ):
        queued_event = self._prepare_queued_event(event, eventtime=eventtime, timefmt=timefmt, **kwargs)
        deadline = None if self._enqueue_timeout is None else time.monotonic() + self._enqueue_timeout
        with self._producer_lock:
            self._enqueue(queued_event, deadline=deadline, enqueued_count=0, next_event_index=0)

    def forward_events(
        self,
        events: list[dict],
        eventtime: Callable = lambda _: datetime.now(),
        timefmt: Optional[str] = None,
        **kwargs,
    ):
        deadline = None if self._enqueue_timeout is None else time.monotonic() + self._enqueue_timeout
        with self._producer_lock:
            for index, event in enumerate(events):
                queued_event = self._prepare_queued_event(
                    event,
                    eventtime=eventtime(event),
                    timefmt=timefmt,
                    **kwargs,
                )
                self._enqueue(
                    queued_event,
                    deadline=deadline,
                    enqueued_count=index,
                    next_event_index=index,
                )

    def _enqueue(
        self,
        queued_event: _QueuedEvent,
        *,
        deadline: Optional[float],
        enqueued_count: int,
        next_event_index: int,
    ):
        with self._capacity:
            self._raise_if_worker_failed()
            if not self._accepting:
                raise RuntimeError("BatchHecForwarder is closed")
            while (
                self._pending_count + 1 > self._max_queue_size
                or self._pending_bytes + queued_event.size > self._max_queue_bytes
            ):
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise HecQueueFullError(enqueued_count, next_event_index)
                else:
                    remaining = None
                self._capacity.wait(timeout=remaining)
                self._raise_if_worker_failed()
                if not self._accepting:
                    raise RuntimeError("BatchHecForwarder is closed")

            self._pending_count += 1
            self._pending_bytes += queued_event.size
            self._queue.put(queued_event)

    def _prepare_queued_event(
        self,
        event: dict,
        eventtime: Optional[Union[str, int, float, datetime]] = None,
        timefmt: Optional[str] = None,
        **kwargs,
    ) -> _QueuedEvent:
        envelope = self._build_hec_event(event, eventtime=eventtime, timefmt=timefmt, **kwargs)
        payload = self._serialize_hec_event(envelope)
        event_size = len(payload.encode("utf-8"))
        event_limit = min(self._max_batch_bytes, self._max_queue_bytes)
        if event_size > event_limit:
            raise HecEventTooLargeError(event_size, event_limit)
        return _QueuedEvent(envelope=envelope, payload=payload, size=event_size)

    def flush(self):
        with self._producer_lock:
            with self._capacity:
                self._raise_if_worker_failed()
                if self._pending_count == 0:
                    return
            request = _FlushRequest()
            self._queue.put(request)

        with self._capacity:
            while not request.completed:
                self._raise_if_worker_failed()
                self._capacity.wait()
            self._raise_if_worker_failed()

    def close(self):
        if self._closed:
            self._raise_if_worker_failed()
            return

        with self._producer_lock:
            self._accepting = False

        delivery_error = None
        try:
            self.flush()
        except HecWorkerError as error:
            delivery_error = error

        if self._worker_thread.is_alive():
            self._queue.put(self._stop)
            self._worker_thread.join()
        super().close()
        self._closed = True
        if delivery_error is not None:
            raise delivery_error

    def _raise_if_worker_failed(self):
        if self._worker_error is not None:
            raise HecWorkerError(
                "Background HEC batch delivery failed",
                self._worker_error,
                tuple(queued_event.envelope for queued_event in self._failed_batch),
            ) from self._worker_error

    def _worker(self):
        deferred_item = None
        while True:
            if deferred_item is None:
                item = self._queue.get()
            else:
                item = deferred_item
                deferred_item = None
            if item is self._stop:
                return
            if isinstance(item, _FlushRequest):
                with self._capacity:
                    item.completed = True
                    self._capacity.notify_all()
                continue

            batch = [item]
            batch_bytes = item.size
            deadline = time.monotonic() + self._flush_interval
            flush_request = None
            while len(batch) < self._batch_size:
                timeout = deadline - time.monotonic()
                if timeout <= 0:
                    break
                try:
                    next_item = self._queue.get(timeout=timeout)
                except queue.Empty:
                    break
                if isinstance(next_item, _FlushRequest):
                    flush_request = next_item
                    break
                if batch_bytes + next_item.size > self._max_batch_bytes:
                    deferred_item = next_item
                    break
                batch.append(next_item)
                batch_bytes += next_item.size

            try:
                self._send_hec_payload(
                    "".join(queued_event.payload for queued_event in batch),
                    event_count=len(batch),
                )
            except Exception as error:
                with self._capacity:
                    self._worker_error = error
                    self._failed_batch = tuple(batch)
                    self._capacity.notify_all()
                return

            with self._capacity:
                self._pending_count -= len(batch)
                self._pending_bytes -= sum(queued_event.size for queued_event in batch)
                if flush_request is not None:
                    flush_request.completed = True
                self._capacity.notify_all()
        return
