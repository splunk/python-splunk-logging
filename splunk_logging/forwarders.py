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

import random
import socket
import time
import uuid
from datetime import datetime
from typing import Callable, Optional, Union

import httpx
from dateutil.parser import isoparse


class HecForwarder:
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

        self._client = self._create_client()
        return

    def __del__(self):
        if self._client:
            self._client.close()
        return

    def _create_client(self) -> httpx.Client:
        scheme = "https" if self._use_ssl else "http"
        client = httpx.Client(
            base_url=httpx.URL(scheme=scheme, host=self._host, port=self._port),
            verify=self._verify_ssl,
            headers={"Authorization": f"Splunk {self._token}"},
            event_hooks={"response": [self._client_retry]},
        )

        client.backoff_factor = 1
        client.max_retries = 3
        client.retry_attempt = 0
        return client

    def _client_retry(self, response: httpx.Response):
        """
        Handles retry logic for HTTP requests based on response status codes.

        Args:
            response (httpx.Response): The HTTP response object.

        Retries the request if the response status code indicates a transient error,
        otherwise raises for status.
        """
        retry_codes = [
            httpx.codes.REQUEST_TIMEOUT,
            httpx.codes.TOO_MANY_REQUESTS,
            httpx.codes.INTERNAL_SERVER_ERROR,
            httpx.codes.SERVICE_UNAVAILABLE,
        ]
        if response.status_code in retry_codes and self._client.retry_attempt < self._client.max_retries:
            time.sleep(
                self._client.backoff_factor * 2**self._client.retry_attempt
                + random.uniform(0, self._client.retry_attempt)
            )
            self._client.retry_attempt += 1
            self._client.send(response.request)
        else:
            self._client.retry_attempt = 0
            response.raise_for_status()
        return

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

        headers = {"X-Splunk-Request-Channel": str(uuid.uuid1())}

        resp = self._client.post("/services/collector/event", headers=headers, json=hec_event)
        resp.raise_for_status()
        return

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
        for event in events:
            self.forward_event(event, eventtime=eventtime(event), timefmt=timefmt, **kwargs)
        return
