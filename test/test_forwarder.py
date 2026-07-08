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
import random
import threading
import unittest
from datetime import datetime
from unittest.mock import patch

import httpx
import respx

from splunk_logging.forwarders import HecForwarder


class TestHecForwarder(unittest.TestCase):
    def create_forwarder(self) -> HecForwarder:
        return HecForwarder(host="localhost", port=8088, token="", use_ssl=False)

    @respx.mock
    def test_forward_event(self):
        hec = self.create_forwarder()
        route = respx.post("http://localhost:8088/services/collector/event")
        event = {"message": "test"}

        # test successful forward
        route.return_value = httpx.Response(200, json={"text": "Success", "code": 0})
        hec.forward_event(event)
        self.assertTrue(route.called)

        # test failed forward
        route.return_value = httpx.Response(400, json={"text": "Incorrect data format", "code": 5})
        with self.assertRaises(httpx.HTTPStatusError):
            hec.forward_event(event)
        return

    @respx.mock
    def test_forward_event_preserves_channel_header_without_ack_polling(self):
        channel_id = "fe0ecfad-13d5-401b-847d-77833bd77131"
        hec = HecForwarder(
            host="localhost",
            port=8088,
            token="",
            use_ssl=False,
            channel_id=channel_id,
        )
        route = respx.post("http://localhost:8088/services/collector/event")
        route.return_value = httpx.Response(200, json={"text": "Success", "code": 0, "ackID": 7})

        hec.forward_event({"message": "test"})

        self.assertEqual(route.calls.last.request.headers["X-Splunk-Request-Channel"], channel_id)

    @respx.mock
    def test_forward_events_sends_one_batched_request(self):
        hec = self.create_forwarder()
        route = respx.post("http://localhost:8088/services/collector/event")
        route.return_value = httpx.Response(200, json={"text": "Success", "code": 0})
        events = [{"message": "first"}, {"message": "second"}]
        event_times = {
            "first": datetime(2026, 1, 1, 12, 0, 0),
            "second": datetime(2026, 1, 1, 12, 0, 1),
        }

        hec.forward_events(events, eventtime=lambda event: event_times[event["message"]])

        self.assertEqual(route.call_count, 1)
        payload = route.calls.last.request.content.decode()
        decoder = json.JSONDecoder()
        first, offset = decoder.raw_decode(payload)
        second, offset = decoder.raw_decode(payload, offset)
        self.assertEqual(offset, len(payload))
        self.assertEqual(first["event"], events[0])
        self.assertEqual(second["event"], events[1])
        self.assertEqual(first["time"], str(event_times["first"].timestamp()))
        self.assertEqual(second["time"], str(event_times["second"].timestamp()))

    @respx.mock
    def test_forward_events_with_no_events_sends_no_request(self):
        hec = self.create_forwarder()
        route = respx.post("http://localhost:8088/services/collector/event")

        hec.forward_events([])

        self.assertFalse(route.called)

    @respx.mock
    def test_forward_events_reports_partial_batch_acceptance(self):
        from splunk_logging.exceptions import HecBatchError

        hec = self.create_forwarder()
        route = respx.post("http://localhost:8088/services/collector/event")
        route.return_value = httpx.Response(
            400,
            json={"text": "Incorrect data format", "code": 5, "invalid-event-number": 1},
        )

        with self.assertRaises(HecBatchError) as raised:
            hec.forward_events([{"message": "first"}, {"invalid": True}, {"message": "third"}])

        self.assertEqual(raised.exception.invalid_event_number, 1)
        self.assertEqual(raised.exception.accepted_count, 1)
        self.assertEqual(raised.exception.total_count, 3)

    @respx.mock
    def test_forward_event_waits_for_indexer_acknowledgment(self):
        channel_id = "fe0ecfad-13d5-401b-847d-77833bd77131"
        hec = HecForwarder(
            host="localhost",
            port=8088,
            token="",
            use_ssl=False,
            indexer_ack=True,
            channel_id=channel_id,
            ack_poll_interval=0,
        )
        event_route = respx.post("http://localhost:8088/services/collector/event")
        event_route.return_value = httpx.Response(200, json={"text": "Success", "code": 0, "ackId": 7})
        ack_route = respx.post("http://localhost:8088/services/collector/ack")
        ack_route.return_value = httpx.Response(200, json={"acks": {"7": True}})

        hec.forward_event({"message": "test"})

        self.assertEqual(event_route.calls.last.request.headers["X-Splunk-Request-Channel"], channel_id)
        self.assertEqual(ack_route.calls.last.request.headers["X-Splunk-Request-Channel"], channel_id)
        self.assertEqual(json.loads(ack_route.calls.last.request.content), {"acks": [7]})

    @respx.mock
    @patch("splunk_logging.forwarders.time.sleep")
    def test_forward_event_polls_until_indexer_acknowledges(self, sleep):
        hec = HecForwarder(
            host="localhost",
            port=8088,
            token="",
            use_ssl=False,
            indexer_ack=True,
        )
        event_route = respx.post("http://localhost:8088/services/collector/event")
        event_route.return_value = httpx.Response(200, json={"text": "Success", "code": 0, "ackId": 7})
        ack_route = respx.post("http://localhost:8088/services/collector/ack")
        ack_route.side_effect = [
            httpx.Response(200, json={"acks": {"7": False}}),
            httpx.Response(200, json={"acks": {"7": True}}),
        ]

        hec.forward_event({"message": "test"})

        self.assertEqual(ack_route.call_count, 2)
        sleep.assert_called_once_with(10.0)

    @respx.mock
    @patch("splunk_logging.forwarders.time.monotonic", side_effect=[0.0, 0.0, 1.0])
    def test_forward_event_raises_typed_acknowledgment_timeout(self, _monotonic):
        from splunk_logging.exceptions import HecAckTimeoutError

        hec = HecForwarder(
            host="localhost",
            port=8088,
            token="",
            use_ssl=False,
            indexer_ack=True,
            ack_timeout=1.0,
        )
        event_route = respx.post("http://localhost:8088/services/collector/event")
        event_route.return_value = httpx.Response(200, json={"text": "Success", "code": 0, "ackId": 7})
        ack_route = respx.post("http://localhost:8088/services/collector/ack")
        ack_route.return_value = httpx.Response(200, json={"acks": {"7": False}})

        with self.assertRaises(HecAckTimeoutError):
            hec.forward_event({"message": "test"})

    @respx.mock
    def test_ack_timeout_caps_poll_interval(self):
        from splunk_logging.exceptions import HecAckTimeoutError

        clock = [0.0]
        hec = HecForwarder(
            host="localhost",
            port=8088,
            token="",
            use_ssl=False,
            indexer_ack=True,
            ack_poll_interval=10.0,
            ack_timeout=1.0,
        )
        event_route = respx.post("http://localhost:8088/services/collector/event")
        event_route.return_value = httpx.Response(200, json={"text": "Success", "code": 0, "ackID": 7})
        ack_route = respx.post("http://localhost:8088/services/collector/ack")
        ack_route.return_value = httpx.Response(200, json={"acks": {"7": False}})

        with (
            patch("splunk_logging.forwarders.time.monotonic", side_effect=lambda: clock[0]),
            patch(
                "splunk_logging.forwarders.time.sleep", side_effect=lambda delay: clock.__setitem__(0, clock[0] + delay)
            ) as sleep,
            self.assertRaises(HecAckTimeoutError),
        ):
            hec.forward_event({"message": "test"})

        self.assertEqual(ack_route.call_count, 1)
        sleep.assert_called_once_with(1.0)

    @respx.mock
    def test_ack_timeout_caps_retry_after(self):
        from splunk_logging.exceptions import HecAckTimeoutError

        clock = [0.0]
        hec = HecForwarder(
            host="localhost",
            port=8088,
            token="",
            use_ssl=False,
            indexer_ack=True,
            ack_timeout=1.0,
        )
        event_route = respx.post("http://localhost:8088/services/collector/event")
        event_route.return_value = httpx.Response(200, json={"text": "Success", "code": 0, "ackID": 7})
        ack_route = respx.post("http://localhost:8088/services/collector/ack")
        ack_route.return_value = httpx.Response(503, headers={"Retry-After": "10"})

        with (
            patch("splunk_logging.forwarders.time.monotonic", side_effect=lambda: clock[0]),
            patch(
                "splunk_logging.forwarders.time.sleep", side_effect=lambda delay: clock.__setitem__(0, clock[0] + delay)
            ) as sleep,
            self.assertRaises(HecAckTimeoutError),
        ):
            hec.forward_event({"message": "test"})

        self.assertEqual(ack_route.call_count, 1)
        sleep.assert_called_once_with(1.0)

    @respx.mock
    def test_forward_event_raises_ack_error_when_response_has_no_ack_id(self):
        from splunk_logging.exceptions import HecAckError

        hec = HecForwarder(
            host="localhost",
            port=8088,
            token="",
            use_ssl=False,
            indexer_ack=True,
        )
        event_route = respx.post("http://localhost:8088/services/collector/event")
        event_route.return_value = httpx.Response(200, json={"text": "Success", "code": 0})

        with self.assertRaises(HecAckError):
            hec.forward_event({"message": "test"})

    @respx.mock
    def test_forward_event_raises_ack_error_when_response_is_not_json(self):
        from splunk_logging.exceptions import HecAckError

        hec = HecForwarder(
            host="localhost",
            port=8088,
            token="",
            use_ssl=False,
            indexer_ack=True,
        )
        event_route = respx.post("http://localhost:8088/services/collector/event")
        event_route.return_value = httpx.Response(200, text="not json")

        with self.assertRaises(HecAckError):
            hec.forward_event({"message": "test"})

    @respx.mock
    def test_forward_event_raises_ack_error_when_status_is_missing(self):
        from splunk_logging.exceptions import HecAckError

        hec = HecForwarder(
            host="localhost",
            port=8088,
            token="",
            use_ssl=False,
            indexer_ack=True,
        )
        event_route = respx.post("http://localhost:8088/services/collector/event")
        event_route.return_value = httpx.Response(200, json={"text": "Success", "code": 0, "ackId": 7})
        ack_route = respx.post("http://localhost:8088/services/collector/ack")
        ack_route.return_value = httpx.Response(200, json={"acks": {}})

        with self.assertRaises(HecAckError):
            hec.forward_event({"message": "test"})

    @respx.mock
    def test_forward_event_wraps_ack_request_http_failure(self):
        from splunk_logging.exceptions import HecAckError

        hec = HecForwarder(
            host="localhost",
            port=8088,
            token="",
            use_ssl=False,
            indexer_ack=True,
        )
        event_route = respx.post("http://localhost:8088/services/collector/event")
        event_route.return_value = httpx.Response(200, json={"text": "Success", "code": 0, "ackID": 7})
        ack_route = respx.post("http://localhost:8088/services/collector/ack")
        ack_route.return_value = httpx.Response(400, json={"text": "ACK is disabled", "code": 14})

        with self.assertRaises(HecAckError) as raised:
            hec.forward_event({"message": "test"})

        self.assertIsInstance(raised.exception.__cause__, httpx.HTTPStatusError)

    @respx.mock
    def test_forward_event_wraps_ack_request_transport_failure(self):
        from splunk_logging.exceptions import HecAckError

        hec = HecForwarder(
            host="localhost",
            port=8088,
            token="",
            use_ssl=False,
            indexer_ack=True,
        )
        event_route = respx.post("http://localhost:8088/services/collector/event")
        event_route.return_value = httpx.Response(200, json={"text": "Success", "code": 0, "ackID": 7})
        ack_route = respx.post("http://localhost:8088/services/collector/ack")
        ack_route.side_effect = httpx.ConnectError("connection failed")

        with self.assertRaises(HecAckError) as raised:
            hec.forward_event({"message": "test"})

        self.assertIsInstance(raised.exception.__cause__, httpx.ConnectError)

    def test_context_manager_closes_forwarder(self):
        hec = self.create_forwarder()

        with hec as entered:
            self.assertIs(entered, hec)

        with self.assertRaises(RuntimeError):
            hec.forward_event({"message": "test"})

    def test_batch_forwarder_rejects_invalid_limits(self):
        from splunk_logging.forwarders import BatchHecForwarder

        invalid_options = [
            {"batch_size": 0},
            {"max_batch_bytes": 0},
            {"flush_interval": 0},
            {"max_queue_size": 0},
            {"max_queue_bytes": 0},
            {"enqueue_timeout": -1},
        ]
        for options in invalid_options:
            with self.subTest(options=options), self.assertRaises(ValueError):
                BatchHecForwarder(host="localhost", token="", use_ssl=False, **options)

    @respx.mock
    def test_batch_forwarder_flushes_one_request_at_count_threshold(self):
        from splunk_logging.forwarders import BatchHecForwarder

        route = respx.post("http://localhost:8088/services/collector/event")
        route.return_value = httpx.Response(200, json={"text": "Success", "code": 0})

        with BatchHecForwarder(
            host="localhost",
            port=8088,
            token="",
            use_ssl=False,
            batch_size=2,
            flush_interval=60,
        ) as hec:
            hec.forward_event({"message": "first"})
            hec.forward_event({"message": "second"})
            hec.flush()

        self.assertEqual(route.call_count, 1)
        payload = route.calls.last.request.content.decode()
        decoder = json.JSONDecoder()
        first, offset = decoder.raw_decode(payload)
        second, offset = decoder.raw_decode(payload, offset)
        self.assertEqual(offset, len(payload))
        self.assertEqual(first["event"], {"message": "first"})
        self.assertEqual(second["event"], {"message": "second"})

    @respx.mock
    def test_batch_forwarder_explicit_flush_does_not_wait_for_interval(self):
        from splunk_logging.forwarders import BatchHecForwarder

        sent = threading.Event()
        route = respx.post("http://localhost:8088/services/collector/event")

        def mark_sent(_request):
            sent.set()
            return httpx.Response(200, json={"text": "Success", "code": 0})

        route.side_effect = mark_sent
        hec = BatchHecForwarder(
            host="localhost",
            port=8088,
            token="",
            use_ssl=False,
            batch_size=2,
            flush_interval=60,
        )
        hec.forward_event({"message": "first"})
        flush_thread = threading.Thread(target=hec.flush)
        flush_thread.start()

        sent_before_interval = sent.wait(0.2)
        if not sent_before_interval:
            hec.forward_event({"message": "release worker"})
        flush_thread.join(1)
        hec.close()

        self.assertTrue(sent_before_interval)

    @respx.mock
    def test_batch_forwarder_flushes_after_interval(self):
        from splunk_logging.forwarders import BatchHecForwarder

        sent = threading.Event()
        route = respx.post("http://localhost:8088/services/collector/event")

        def mark_sent(_request):
            sent.set()
            return httpx.Response(200, json={"text": "Success", "code": 0})

        route.side_effect = mark_sent
        hec = BatchHecForwarder(
            host="localhost",
            port=8088,
            token="",
            use_ssl=False,
            batch_size=100,
            flush_interval=0.05,
        )
        hec.forward_event({"message": "test"})

        sent_after_interval = sent.wait(0.2)
        if not sent_after_interval:
            hec.flush()
        hec.close()

        self.assertTrue(sent_after_interval)

    @respx.mock
    def test_batch_forwarder_splits_requests_at_byte_limit(self):
        from splunk_logging.forwarders import BatchHecForwarder

        route = respx.post("http://localhost:8088/services/collector/event")
        route.return_value = httpx.Response(200, json={"text": "Success", "code": 0})
        hec = BatchHecForwarder(
            host="localhost",
            port=8088,
            token="",
            use_ssl=False,
            batch_size=100,
            max_batch_bytes=700,
            flush_interval=60,
        )

        hec.forward_events(
            [{"message": "x" * 500}, {"message": "y" * 500}],
            eventtime=lambda _: datetime(2026, 1, 1),
        )
        hec.flush()
        hec.close()

        self.assertEqual(route.call_count, 2)

    @respx.mock
    def test_batch_forwarder_rejects_event_larger_than_batch_limit(self):
        from splunk_logging.exceptions import HecEventTooLargeError
        from splunk_logging.forwarders import BatchHecForwarder

        route = respx.post("http://localhost:8088/services/collector/event")
        route.return_value = httpx.Response(200, json={"text": "Success", "code": 0})
        hec = BatchHecForwarder(
            host="localhost",
            port=8088,
            token="",
            use_ssl=False,
            max_batch_bytes=100,
        )

        try:
            with self.assertRaises(HecEventTooLargeError) as raised:
                hec.forward_event({"message": "x" * 500})
        finally:
            hec.close()

        self.assertGreater(raised.exception.event_size, raised.exception.max_batch_bytes)
        self.assertFalse(route.called)

    @respx.mock
    def test_batch_forwarder_applies_backpressure_to_in_flight_events(self):
        from splunk_logging.exceptions import HecQueueFullError
        from splunk_logging.forwarders import BatchHecForwarder

        request_started = threading.Event()
        release_request = threading.Event()
        route = respx.post("http://localhost:8088/services/collector/event")

        def block_request(_request):
            request_started.set()
            release_request.wait(1)
            return httpx.Response(200, json={"text": "Success", "code": 0})

        route.side_effect = block_request
        hec = BatchHecForwarder(
            host="localhost",
            port=8088,
            token="",
            use_ssl=False,
            batch_size=1,
            max_queue_size=1,
            max_queue_bytes=1_024,
            enqueue_timeout=0,
        )

        try:
            hec.forward_event({"message": "first"})
            self.assertTrue(request_started.wait(1))
            with self.assertRaises(HecQueueFullError) as raised:
                hec.forward_event({"message": "second"})
        finally:
            release_request.set()
            hec.flush()
            hec.close()

        self.assertEqual(raised.exception.enqueued_count, 0)
        self.assertEqual(raised.exception.next_event_index, 0)

    @respx.mock
    def test_batch_forwarder_applies_byte_backpressure_to_in_flight_events(self):
        from splunk_logging.exceptions import HecQueueFullError
        from splunk_logging.forwarders import BatchHecForwarder

        request_started = threading.Event()
        release_request = threading.Event()
        route = respx.post("http://localhost:8088/services/collector/event")

        def block_request(_request):
            request_started.set()
            release_request.wait(1)
            return httpx.Response(200, json={"text": "Success", "code": 0})

        route.side_effect = block_request
        hec = BatchHecForwarder(
            host="localhost",
            port=8088,
            token="",
            use_ssl=False,
            batch_size=1,
            max_batch_bytes=600,
            max_queue_size=10,
            max_queue_bytes=600,
            enqueue_timeout=0,
        )

        try:
            hec.forward_event({"message": "x" * 300})
            self.assertTrue(request_started.wait(1))
            with self.assertRaises(HecQueueFullError):
                hec.forward_event({"message": "y" * 300})
        finally:
            release_request.set()
            hec.flush()
            hec.close()

    @respx.mock
    def test_batch_forwarder_reports_partial_bulk_admission(self):
        from splunk_logging.exceptions import HecQueueFullError
        from splunk_logging.forwarders import BatchHecForwarder

        release_request = threading.Event()
        route = respx.post("http://localhost:8088/services/collector/event")

        def block_request(_request):
            release_request.wait(1)
            return httpx.Response(200, json={"text": "Success", "code": 0})

        route.side_effect = block_request
        hec = BatchHecForwarder(
            host="localhost",
            port=8088,
            token="",
            use_ssl=False,
            batch_size=1,
            max_queue_size=1,
            enqueue_timeout=0,
        )

        try:
            with self.assertRaises(HecQueueFullError) as raised:
                hec.forward_events([{"message": "first"}, {"message": "second"}])
        finally:
            release_request.set()
            hec.flush()
            hec.close()

        self.assertEqual(raised.exception.enqueued_count, 1)
        self.assertEqual(raised.exception.next_event_index, 1)

    @respx.mock
    def test_batch_forwarder_prepares_all_events_before_admission(self):
        from splunk_logging.forwarders import BatchHecForwarder

        route = respx.post("http://localhost:8088/services/collector/event")
        route.return_value = httpx.Response(200, json={"text": "Success", "code": 0})
        hec = BatchHecForwarder(
            host="localhost",
            port=8088,
            token="",
            use_ssl=False,
            batch_size=100,
            flush_interval=60,
        )

        with self.assertRaises(TypeError):
            hec.forward_events([{"message": "valid"}, {"message": object()}])
        hec.close()

        self.assertFalse(route.called)

    @respx.mock
    def test_batch_forwarder_waits_for_ack_before_sending_next_batch(self):
        from splunk_logging.forwarders import BatchHecForwarder

        timeline = []
        ack_attempts = {1: 0}
        event_route = respx.post("http://localhost:8088/services/collector/event")
        ack_route = respx.post("http://localhost:8088/services/collector/ack")

        def accept_event(_request):
            ack_id = event_route.call_count + 1
            timeline.append(f"event-{ack_id}")
            return httpx.Response(200, json={"text": "Success", "code": 0, "ackID": ack_id})

        def acknowledge_event(request):
            ack_id = json.loads(request.content)["acks"][0]
            timeline.append(f"ack-{ack_id}")
            if ack_id == 1:
                ack_attempts[1] += 1
                acknowledged = ack_attempts[1] > 1
            else:
                acknowledged = True
            return httpx.Response(200, json={"acks": {str(ack_id): acknowledged}})

        event_route.side_effect = accept_event
        ack_route.side_effect = acknowledge_event
        hec = BatchHecForwarder(
            host="localhost",
            port=8088,
            token="",
            use_ssl=False,
            batch_size=1,
            indexer_ack=True,
            ack_poll_interval=0,
        )

        hec.forward_events([{"message": "first"}, {"message": "second"}])
        hec.flush()
        hec.close()

        self.assertEqual(timeline, ["event-1", "ack-1", "ack-1", "event-2", "ack-2"])

    @respx.mock
    def test_batch_forwarder_reports_only_unaccepted_events_after_partial_failure(self):
        from splunk_logging.exceptions import HecBatchError, HecWorkerError
        from splunk_logging.forwarders import BatchHecForwarder

        route = respx.post("http://localhost:8088/services/collector/event")
        route.return_value = httpx.Response(
            400,
            json={"text": "Invalid data format", "code": 6, "invalid-event-number": 1},
        )
        hec = BatchHecForwarder(
            host="localhost",
            port=8088,
            token="",
            use_ssl=False,
            batch_size=3,
            flush_interval=60,
        )
        events = [{"message": "accepted"}, {"message": "invalid"}, {"message": "not processed"}]

        hec.forward_events(events)
        with self.assertRaises(HecWorkerError) as raised:
            hec.flush()
        with self.assertRaises(HecWorkerError):
            hec.close()

        self.assertIsInstance(raised.exception.cause, HecBatchError)
        self.assertEqual(
            [envelope["event"] for envelope in raised.exception.failed_events],
            events[1:],
        )

    @respx.mock
    def test_batch_forwarder_failed_events_match_attempted_payload(self):
        from splunk_logging.exceptions import HecWorkerError
        from splunk_logging.forwarders import BatchHecForwarder

        request_started = threading.Event()
        release_request = threading.Event()
        route = respx.post("http://localhost:8088/services/collector/event")

        def reject_request(_request):
            request_started.set()
            release_request.wait(1)
            return httpx.Response(400, json={"text": "Invalid data format", "code": 6})

        route.side_effect = reject_request
        hec = BatchHecForwarder(
            host="localhost",
            port=8088,
            token="",
            use_ssl=False,
            batch_size=1,
        )
        event = {"message": {"value": "attempted"}}

        hec.forward_event(event)
        self.assertTrue(request_started.wait(1))
        event["message"]["value"] = "mutated"
        release_request.set()
        with self.assertRaises(HecWorkerError) as raised:
            hec.flush()
        with self.assertRaises(HecWorkerError):
            hec.close()

        self.assertEqual(raised.exception.failed_events[0]["event"]["message"]["value"], "attempted")

    @respx.mock
    def test_batch_forwarder_surfaces_background_delivery_failure(self):
        from splunk_logging.exceptions import HecWorkerError
        from splunk_logging.forwarders import BatchHecForwarder

        request_finished = threading.Event()
        route = respx.post("http://localhost:8088/services/collector/event")

        def reject_request(_request):
            request_finished.set()
            return httpx.Response(400, json={"text": "Incorrect data format", "code": 5})

        route.side_effect = reject_request
        hec = BatchHecForwarder(
            host="localhost",
            port=8088,
            token="",
            use_ssl=False,
            batch_size=1,
        )
        hec.forward_event({"message": "first"})
        self.assertTrue(request_finished.wait(1))

        with self.assertRaises(HecWorkerError):
            hec.flush()
        with self.assertRaises(HecWorkerError):
            hec.forward_event({"message": "second"})
        with self.assertRaises(HecWorkerError):
            hec.close()

    @respx.mock
    @patch("splunk_logging.forwarders.time.sleep")
    def test_retry(self, sleep):
        hec = self.create_forwarder()
        route = respx.post("http://localhost:8088/services/collector/event")
        event = {"message": "test"}

        # test successful forward
        route.return_value = httpx.Response(httpx.codes.SERVICE_UNAVAILABLE)
        with self.assertRaises(httpx.HTTPStatusError):
            hec.forward_event(event)

        self.assertTrue(route.called)
        self.assertEqual(route.call_count, 4)
        self.assertEqual(sleep.call_count, 3)
        return

    @respx.mock
    @patch("splunk_logging.forwarders.time.sleep")
    def test_retry_returns_successful_response(self, _sleep):
        hec = self.create_forwarder()
        route = respx.post("http://localhost:8088/services/collector/event")
        route.side_effect = [
            httpx.Response(httpx.codes.SERVICE_UNAVAILABLE),
            httpx.Response(httpx.codes.OK, json={"text": "Success", "code": 0}),
        ]

        hec.forward_event({"message": "test"})

        self.assertEqual(route.call_count, 2)

    @respx.mock
    @patch("splunk_logging.forwarders.time.sleep")
    def test_retry_honors_retry_after(self, sleep):
        hec = self.create_forwarder()
        route = respx.post("http://localhost:8088/services/collector/event")
        route.side_effect = [
            httpx.Response(httpx.codes.TOO_MANY_REQUESTS, headers={"Retry-After": "7"}),
            httpx.Response(httpx.codes.OK, json={"text": "Success", "code": 0}),
        ]

        hec.forward_event({"message": "test"})

        sleep.assert_called_once_with(7.0)

    def test_parse_timestamp(self):
        hec = self.create_forwarder()
        dt = datetime(
            year=random.randint(2000, 2024),
            month=random.randint(1, 12),
            day=random.randint(1, 28),
            hour=random.randint(1, 23),
            minute=random.randint(1, 59),
            second=random.randint(1, 59),
        )
        ts = dt.timestamp()

        self.assertEqual(hec._parse_timestamp(dt), ts)
        self.assertEqual(hec._parse_timestamp(ts), ts)
        self.assertEqual(hec._parse_timestamp(dt.isoformat()), ts)
        self.assertEqual(hec._parse_timestamp(dt.isoformat(), timefmt="%Y-%m-%dT%H:%M:%S"), ts)

        with self.assertRaises(ValueError):
            hec._parse_timestamp("invalid timestamp")
        return
