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

import io
import json
import logging
import traceback
import unittest

from splunk_logging.formatters import JsonFormatter


class TestJsonFormatter(unittest.TestCase):
    def create_logger(self, formatter):
        self.log = logging.Logger("root")
        self.stream = io.StringIO()
        self.handler = logging.StreamHandler(self.stream)
        self.log.addHandler(self.handler)
        self.log.setLevel(logging.DEBUG)
        self.handler.setFormatter(formatter)

    def test_readme_empty(self):
        self.create_logger(JsonFormatter())
        self.log.info({"a": 1, "b": 2})
        self.assertEqual(self.stream.getvalue(), '{"a": 1, "b": 2}\n')

    def test_readme_format(self):
        self.create_logger(
            JsonFormatter(
                level="%(levelname)s",
                name="%(name)s",
                message="%(message)s",
            )
        )
        self.log.info("hello world")
        self.log.info({"a": 1, "b": 2})
        self.assertEqual(
            self.stream.getvalue(),
            '{"level": "INFO", "name": "root", "message": "hello world"}\n'
            '{"a": 1, "b": 2, "level": "INFO", "name": "root"}\n',
        )

    def test_readme_prune(self):
        self.create_logger(JsonFormatter(prune_keys=False))
        self.log.info({"a": 1, "b": 2, "c": ""})
        self.assertEqual(self.stream.getvalue(), '{"a": 1, "b": 2, "c": ""}\n')

    def test_readme_format_prune(self):
        self.create_logger(
            JsonFormatter(
                level="%(levelname)s",
                name="%(name)s",
                message="%(message)s",
                prune_keys=False,
            )
        )
        self.log.info({"a": 1, "b": 2})
        expected = '{"a": 1, "b": 2, "level": "INFO", "name": "root", "message": ""}\n'
        self.assertEqual(self.stream.getvalue(), expected)

    def test_log_exception(self):
        """
        Make sure log.exception() logs the traceback.
        """
        self.create_logger(JsonFormatter(prune_keys=False))
        try:
            ()()
        except TypeError:
            self.log.exception("exception!")
            exc = traceback.format_exc()
        obj = json.loads(self.stream.getvalue())
        self.assertIn("exception", obj)
        self.assertEqual(obj["exception"], exc.rstrip("\n"))

    def test_log_stack(self):
        """
        Make sure log.info(..., stack_info=True) logs the stack.
        """
        self.create_logger(JsonFormatter(prune_keys=False))
        # The following two statements measure the call stack and need to be on the same line.
        self.log.info("X", stack_info=True); stack = traceback.format_stack();  # fmt: skip
        obj = json.loads(self.stream.getvalue())
        self.assertIn("stack", obj)
        self.assertEqual(
            obj["stack"],
            "Stack (most recent call last):\n" + "".join(stack).rstrip("\n"),
        )

    def test_log_exc_info_false(self):
        """
        Make sure exc_info=False is handled correctly. Default is exc_info=None.
        """
        self.create_logger(JsonFormatter(prune_keys=False))
        self.log.error({"a": "b"}, exc_info=False, stack_info=False)
        self.log.error({"c": "d"}, exc_info=[], stack_info=[])
        self.assertEqual(self.stream.getvalue(), '{"a": "b"}\n{"c": "d"}\n')

    def test_override(self):
        """
        Make sure default fields can be overriden by the user.
        """
        self.create_logger(
            JsonFormatter(
                level="%(levelname)s",
                name="%(name)s",
                message="%(message)s",
                prune_keys=False,
            )
        )
        self.log.error("message 1")
        self.log.error({"hello": "world", "message": "msg 2", "name": "qwer"})
        self.log.error({"hello": "world", "message": ["msg 3"]})
        expected = (
            '{"level": "ERROR", "name": "root", "message": "message 1"}\n'
            '{"hello": "world", "message": "msg 2", "name": "qwer", "level": "ERROR"}\n'
            '{"hello": "world", "message": ["msg 3"], "level": "ERROR", "name": "root"}\n'
        )
        self.assertEqual(self.stream.getvalue(), expected)


if __name__ == "__main__":
    unittest.main()
