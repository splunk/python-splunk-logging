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
import os
import socket
from typing import Optional

from .formatters import JsonFormatter
from .forwarders import HecForwarder


class HecHandler(logging.Handler):
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
        ignore_exceptions: bool = True,
        **kwargs,
    ):
        """
        The HecHandler class takes log records and forwards them to a Splunk HTTP Event Collector.

        Args:
            host (str):
                IP or domain name of the HEC Listener. (default: localhost)
            port (int, optional):
                Port of the HEC listener. (default: 8088)
            token (str, optional):
                HEC token. (default: os.environ['HEC_TOKEN'])
            use_ssl (bool, optional):
                Whether to connect with https to the HEC listener. (default: True)
            verify_ssl (bool, optional):
                Whether to verify HEC servers ssl certificate. (default: True)
            default_host (str, optional):
                Default host Splunk uses for this even. (default: hostname)
            default_source (str, optional):
                Default source to use when forwarding events. (default: None)
            default_sourcetype (str, optional):
                Default sourcetype to use when forwarding events. (default: None)
            default_index (str, optional):
                Default index to send events to. (default: None)
            ignore_exceptions (bool, optional):
                Ignore exceptions thrown when sending an events. (default: False)
        """
        super().__init__(**kwargs)
        self.setFormatter(JsonFormatter())

        self._hec = HecForwarder(
            host=host,
            port=port,
            token=token or os.environ.get("HEC_TOKEN", ""),
            use_ssl=use_ssl,
            verify_ssl=verify_ssl,
            default_host=default_host or socket.gethostname(),
            default_source=default_source or "",
            default_sourcetype=default_sourcetype or "",
            default_index=default_index or "",
        )

        self._ignore_exceptions = ignore_exceptions

        # urllib3/httpx message creates log recortds when an http connection is established.
        # This can lead to an infinite loop when sending logs to HEC, so we disable it
        # if using a hec logger.
        logging.getLogger("urllib3").propagate = False
        logging.getLogger("httpx").propagate = False
        logging.getLogger("httpcore").propagate = False
        return

    def emit(self, record: logging.LogRecord):
        """
        Sends the log record to splunk via HEC. The default host, source, sourcetype, and index can be
        overridden if provided in the log record.

        Args:
            record (logging.LogRecord): Log record to send to Splunk via HEC
        """
        hec_args = {}
        if hasattr(record, "host"):
            hec_args["host"] = record.host
        if hasattr(record, "source"):
            hec_args["source"] = record.source
        if hasattr(record, "sourcetype"):
            hec_args["sourcetype"] = record.sourcetype
        if hasattr(record, "index"):
            hec_args["index"] = record.index

        event = json.loads(self.format(record))
        try:
            self._hec.forward_event(event, eventtime=record.created, **hec_args)
        except Exception as e:
            if self._ignore_exceptions:
                print(f"Exception when logging to Splunk: {e}")
            else:
                raise e
        return
