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
import logging
import unittest

import httpx
import respx

from splunk_logging.formatters import JsonFormatter
from splunk_logging.handlers import HecHandler

TEST_CHANNEL_ID = "00000000-0000-4000-8000-000000000001"


class TestHecHandler(unittest.TestCase):
    def create_logger(self):
        self.log = logging.Logger("root")
        hec_handler = HecHandler(host="localhost", port=8088, token="", use_ssl=False)
        hec_handler.setFormatter(JsonFormatter())
        self.log.addHandler(hec_handler)
        self.log.setLevel(logging.DEBUG)
        return

    @respx.mock
    def test_hec_logger(self):
        self.create_logger()
        route = respx.post("http://localhost:8088/services/collector/event")
        route.return_value = httpx.Response(200, json={"text": "Success", "code": 0})
        event = {"message": "test"}

        self.log.info(event)
        self.assertTrue(route.called)
        sent_event = json.loads(route.calls.last.request.content)

        self.assertIn("event", sent_event)
        self.assertIn("host", sent_event)
        self.assertIn("time", sent_event)
        self.assertEqual(json.dumps(sent_event["event"]), json.dumps(event))
        return

    @respx.mock
    def test_hec_logger_supports_indexer_acknowledgment(self):
        log = logging.Logger("ack")
        handler = HecHandler(
            host="localhost",
            port=8088,
            token="",
            use_ssl=False,
            indexer_ack=True,
            channel_id=TEST_CHANNEL_ID,
            ack_poll_interval=0,
        )
        log.addHandler(handler)
        event_route = respx.post("http://localhost:8088/services/collector/event")
        event_route.return_value = httpx.Response(200, json={"text": "Success", "code": 0, "ackId": 7})
        ack_route = respx.post("http://localhost:8088/services/collector/ack")
        ack_route.return_value = httpx.Response(200, json={"acks": {"7": True}})

        log.warning({"message": "test"})

        self.assertEqual(event_route.calls.last.request.headers["X-Splunk-Request-Channel"], TEST_CHANNEL_ID)
        self.assertEqual(ack_route.call_count, 1)

    @respx.mock
    def test_hec_logger_supports_opt_in_batching(self):
        log = logging.Logger("batch")
        handler = HecHandler(
            host="localhost",
            port=8088,
            token="",
            use_ssl=False,
            batch_enabled=True,
            batch_size=2,
            flush_interval=60,
            ignore_exceptions=False,
        )
        log.addHandler(handler)
        route = respx.post("http://localhost:8088/services/collector/event")
        route.return_value = httpx.Response(200, json={"text": "Success", "code": 0})

        try:
            log.info({"message": "first"})
            log.info({"message": "second"})
            handler.flush()
        finally:
            handler.close()

        self.assertEqual(route.call_count, 1)
        payload = route.calls.last.request.content.decode()
        decoder = json.JSONDecoder()
        first, offset = decoder.raw_decode(payload)
        second, offset = decoder.raw_decode(payload, offset)
        self.assertEqual(offset, len(payload))
        self.assertEqual(first["event"]["message"], "first")
        self.assertEqual(second["event"]["message"], "second")
