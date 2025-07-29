import json
import logging
import unittest

import httpx
import respx

from splunk_logging.formatters import JsonFormatter
from splunk_logging.handlers import HecHandler


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
