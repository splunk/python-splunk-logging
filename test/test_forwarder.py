import random
import unittest
from datetime import datetime

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
    def test_retry(self):
        hec = self.create_forwarder()
        route = respx.post("http://localhost:8088/services/collector/event")
        event = {"message": "test"}

        # test successful forward
        route.return_value = httpx.Response(httpx.codes.SERVICE_UNAVAILABLE)
        with self.assertRaises(httpx.HTTPStatusError):
            hec.forward_event(event)

        self.assertTrue(route.called)
        self.assertGreaterEqual(route.call_count, 3)
        return

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
