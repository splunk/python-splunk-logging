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
from typing import Optional


class JsonFormatter(logging.Formatter):
    def __init__(
        self,
        fmt: Optional[str] = None,
        datefmt: Optional[str] = None,
        prune_keys: bool = True,
        serializer_args: Optional[dict] = None,
        **default_keys,
    ):
        """
        The JsonFormatter class takes json objects as a logging input, and outputs a json formatted log record.

        Args:
            fmt (str, optional):
                Format string for the log message.
            datefmt (str, optional):
                Format string to use when formatting the log record time.
            prune_keys (bool, optional):
                If True, will prune keys with empty values from the log record (default: True).
            serializer_args (dict, optional):
                Args that should be passed to `json.dumps`.
            default_keys:
                Default keys to be included in every log record. Can be formatted with any log record attribute.
        """
        super().__init__(fmt=fmt, datefmt=datefmt)
        self._default_keys = default_keys or {}
        self._prune_keys = prune_keys
        self._serializer_args = serializer_args or {}
        return

    def format(self, record: logging.LogRecord) -> str:
        """
        Takes a log record and returns a JSON formatted string

        Args:
            record (logging.LogRecord): Log record to format.

        Returns:
            str: JSON formatted string representing the log record.
        """
        json_record = {}
        record.asctime = self.formatTime(record, self.datefmt)

        if record.exc_info:
            json_record["exception"] = self.formatException(record.exc_info)

        if record.stack_info:
            json_record["stack"] = self.formatStack(record.stack_info)

        if isinstance(record.msg, dict):
            json_record.update(**record.msg)
            record.message = ""
        else:
            record.message = record.getMessage()

        for k, v in self._default_keys.items():
            if k not in json_record:
                json_record[k] = v % record.__dict__

        if self._prune_keys:
            json_record = {k: v for k, v in json_record.items() if v}

        return json.dumps(json_record, **self._serializer_args)
