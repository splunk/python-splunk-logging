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
    @patch("splunk_logging.forwarders.time.monotonic", side_effect=[0.0, 1.0])
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

    def test_context_manager_closes_forwarder(self):
        hec = self.create_forwarder()

        with hec as entered:
            self.assertIs(entered, hec)

        with self.assertRaises(RuntimeError):
            hec.forward_event({"message": "test"})

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
