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
from .forwarders import BatchHecForwarder, HecForwarder


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
        *,
        indexer_ack: bool = False,
        channel_id: Optional[str] = None,
        ack_poll_interval: float = 10.0,
        ack_timeout: float = 300.0,
        batch_enabled: bool = False,
        batch_size: int = 100,
        max_batch_bytes: int = 1_048_576,
        flush_interval: float = 2.0,
        max_queue_size: int = 10_000,
        max_queue_bytes: int = 10_485_760,
        enqueue_timeout: Optional[float] = None,
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
                Ignore exceptions thrown when sending an events. (default: True)
            indexer_ack (bool, optional):
                Wait for Splunk indexer acknowledgment after sending events. (default: False)
            channel_id (str, optional):
                GUID used for indexer acknowledgment requests. (default: generated UUID)
            ack_poll_interval (float, optional):
                Seconds to wait between acknowledgment queries. (default: 10)
            ack_timeout (float, optional):
                Seconds to wait for an acknowledgment before raising. (default: 300)
            batch_enabled (bool, optional):
                Queue log records and send them in background batches. (default: False)
            batch_size (int, optional):
                Maximum events per request when batching. (default: 100)
            max_batch_bytes (int, optional):
                Maximum serialized bytes per batch request. (default: 1048576)
            flush_interval (float, optional):
                Maximum seconds to hold a partial batch. (default: 2)
            max_queue_size (int, optional):
                Maximum queued and in-flight event count. (default: 10000)
            max_queue_bytes (int, optional):
                Maximum queued and in-flight serialized bytes. (default: 10485760)
            enqueue_timeout (float, optional):
                Maximum seconds to wait for queue capacity. (default: wait indefinitely)
        """
        super().__init__(**kwargs)
        self.setFormatter(JsonFormatter())
        self._ignore_exceptions = ignore_exceptions

        forwarder_class = BatchHecForwarder if batch_enabled else HecForwarder
        forwarder_kwargs = {
            "host": host,
            "port": port,
            "token": token or os.environ.get("HEC_TOKEN", ""),
            "use_ssl": use_ssl,
            "verify_ssl": verify_ssl,
            "default_host": default_host or socket.gethostname(),
            "default_source": default_source or "",
            "default_sourcetype": default_sourcetype or "",
            "default_index": default_index or "",
            "indexer_ack": indexer_ack,
            "channel_id": channel_id,
            "ack_poll_interval": ack_poll_interval,
            "ack_timeout": ack_timeout,
        }
        if batch_enabled:
            forwarder_kwargs.update(
                batch_size=batch_size,
                max_batch_bytes=max_batch_bytes,
                flush_interval=flush_interval,
                max_queue_size=max_queue_size,
                max_queue_bytes=max_queue_bytes,
                enqueue_timeout=enqueue_timeout,
            )
        self._hec = forwarder_class(**forwarder_kwargs)

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

    def flush(self):
        forwarder = getattr(self, "_hec", None)
        flush = getattr(forwarder, "flush", None)
        if flush is None:
            return
        try:
            flush()
        except Exception as error:
            if getattr(self, "_ignore_exceptions", True):
                print(f"Exception when flushing logs to Splunk: {error}")
            else:
                raise error

    def close(self):
        delivery_error = None
        forwarder = getattr(self, "_hec", None)
        if forwarder is not None:
            try:
                forwarder.close()
            except Exception as error:
                delivery_error = error
        super().close()
        if delivery_error is not None:
            if getattr(self, "_ignore_exceptions", True):
                print(f"Exception when closing Splunk logging handler: {delivery_error}")
            else:
                raise delivery_error
