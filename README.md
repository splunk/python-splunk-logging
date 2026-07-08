# Splunk logging for Python

The splunk-logging python module provides logging handlers for sending log messages directly to a configured
splunk instance using the HTTP Event Collector (HEC).

## Installation

```
pip install python-splunk-logging
```

Python 3.9 or newer is required.

## Usage

### JsonFormatter

When logging to a file that will be ingested by Splunk, JSON is a great format to use since Splunk already knows how to parse it.
The `JsonFormatter` class is meant to be used as a formatter for any python logging handler.
Attaching this formatter to a log handler with emit json formatted log records to your configured source.

The following code examples assume the following setup:
```python
import logging
import sys

from splunk_logging.formatters import JsonFormatter

root = logging.getLogger()
logger = logging.getLogger(__name__)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonFormatter())

root.addHandler(handler)
root.setLevel(logging.DEBUG)
```

The most basic configuration will take any JSON serializable object and log it to the configured destination:
```
>>> logger.info({"a": 1, "b": 2})
{"a": 1, "b": 2}
```

You can also configure default keys when setting up the formatter, these keys will be included in every log messages.
You can format these keys with any logging variables:
```python
JsonFormatter(
    level="%(levelname)s",
    name="%(name)s",
    message="%(message)s"
)
```

since `message` was included in one of the default keys, you can log a normal string message,
and the default keys will be formatted correctly:
```
>>> logger.info("hello world")
{"level": "INFO", "name": "root", "message": "hello world"}
```

You can also log a json object this way as well. The default keys will be added to the object:
```
>>> logger.info({"a": 1, "b": 2})
{"a": 1, "b": 2, "level": "INFO", "name": "root"}
```

Notice that the `message` key was not included in the above log record, since a dict wast passed instead of a string.
By default, the `JsonFormatter` class will prune empty keys. For example:
```
>>> logger.info({"a": 1, "b": 2, "c": ""})
{"a": 1, "b": 2}
```

This can be disabled when creating the `JsonFormatter`:
```python
JsonFormatter(prune_keys=False)
```
```
>>> logger.info({"a": 1, "b": 2, "c": ""})
{"a": 1, "b": 2, "c": ""}
```

This will also include an empty `message` key from earlier, since it is a default key:
```python
JsonFormatter(
    level="%(levelname)s",
    name="%(name)s",
    message="%(message)s",
    prune_keys=False
)
```
```
>>> logger.info({"a": 1, "b": 2})
{"a": 1, "b": 2, "level": "INFO", "name": "root", "message": ""}
```

### HecForwarder

The HecForwarder will forward events to Splunk using the http event collector.

Initialize the collector:
```python
import os
import socket
from splunk_logging.forwarders import HecForwarder

hec = HecForwarder(
    host="localhost", # Splunk hostname or ip
    port=8088, # port of event listener
    token=os.environ.get("HEC_TOKEN", ""), # hec token, defaults to the HEC_TOKEN environment variable
    use_ssl=True, # connect to the Splunk collector with https, default True
    verify_ssl=True, # verify the collector certificate, default True
    default_host=socket.gethostname(), # default host for the event, can be blank
    default_source="http", # default source to set in splunk when forwarding events, can be blank
    default_sourcetype="_json", # default sourcetype to use, can be blank
    default_index="main", # default index to send events into, can be blank
)
```

Then to forward an event:
```python
hec.forward_event(
    event=event,
    eventtime=datetime.now(), # this is the timestamp for the event, defaults to the current time
    # you can also override the default host/source/sourcetype/index for this event only if needed
    host="",
    source="",
    sourcetype="",
    index=""
)
```

`forward_events()` sends a sequence of events in one HEC request using Splunk's
[JSON event batching protocol](https://help.splunk.com/en/splunk-enterprise/get-started/get-data-in/9.2/get-data-with-http-event-collector/format-events-for-http-event-collector):

```python
hec.forward_events(events)
```

Forwarders are context managers. A context manager or an explicit `close()` call should be used to release the
underlying HTTP client:

```python
with HecForwarder(host="localhost", token=os.environ["HEC_TOKEN"]) as hec:
    hec.forward_event(event)
```

#### Indexer acknowledgment

Indexer acknowledgment is opt-in and remains blocking: each forwarding call waits until Splunk confirms that its
request has been indexed. The default 10-second polling interval and 5-minute timeout follow Splunk's
[HEC indexer acknowledgment guidance](https://help.splunk.com/en/data-management/get-data-in/get-data-into-splunk-enterprise/9.3/get-data-with-http-event-collector/about-http-event-collector-indexer-acknowledgment).

```python
with HecForwarder(
    host="localhost",
    token=os.environ["HEC_ACK_TOKEN"],
    indexer_ack=True,
    ack_poll_interval=10,
    ack_timeout=300,
) as hec:
    hec.forward_events(events)
```

The HEC token must have indexer acknowledgment enabled. A UUID channel is generated per forwarder unless
`channel_id` is supplied. Missing, malformed, failed, or timed-out acknowledgments raise typed exceptions from
`splunk_logging.exceptions`.

#### Background batching

`BatchHecForwarder` adds an in-memory bounded queue and one background delivery worker while preserving the
`forward_event()` and `forward_events()` interfaces. It sends a batch when it reaches the event or byte limit, when
the flush interval expires, or when `flush()`/`close()` is called.

```python
from splunk_logging.forwarders import BatchHecForwarder

with BatchHecForwarder(
    host="localhost",
    token=os.environ["HEC_TOKEN"],
    batch_size=100,
    max_batch_bytes=1_048_576,
    flush_interval=2,
    max_queue_size=10_000,
    max_queue_bytes=10_485_760,
    enqueue_timeout=None,
) as hec:
    hec.forward_events(events)
    hec.flush()
```

`enqueue_timeout=None` applies backpressure until capacity is available. Set it to a number of seconds, including
zero for a non-blocking attempt, to raise `HecQueueFullError` instead. After a background delivery failure, the next
forwarding call, `flush()`, or `close()` raises `HecWorkerError`. With `indexer_ack=True`, the same worker waits for
the current batch acknowledgment before sending the next batch.

> [!WARNING]
> The batch queue is memory-only. Events that have not been successfully flushed before process termination are
> lost. Always use the context manager or call `close()` (which performs a final flush) from the application's
> graceful shutdown path. Abrupt termination, including `SIGKILL`, cannot flush the queue.

### HecHandler

This is a logging handler that will forward a logging record directly to a HEC handler using python's built in logging library.
This handler uses the `HecForwarder` under the hood, so the configuration will be the same:

```python
import logging
import os
import sys
import socket

from splunk_logging.handlers import HecHandler
from splunk_logging.formatters import JsonFormatter

root = logging.getLogger()
logger = logging.getLogger(__name__)

hec_handler = HecHandler(
    host="localhost", # Splunk hostname or ip
    port=8088, # port of event listener
    token=os.environ.get("HEC_TOKEN", ""), # hec token, defaults to the HEC_TOKEN environment variable
    use_ssl=True, # connect to the Splunk collector with https, default True
    verify_ssl=True, # verify the collector certificate, default True
    default_host=socket.gethostname(), # default host for the event, can be blank
    default_source="http", # default source to set in splunk when forwarding events, can be blank
    default_sourcetype="_json", # default sourcetype to use, can be blank
    default_index="main", # default index to send events into, can be blank
)
hec_handler.setFormatter(JsonFormatter())

root.addHandler(hec_handler)
root.setLevel(logging.DEBUG)
```

Now anytime you want to log something with `logging.log(...)` it will be forwarded to Splunk using HEC.
```python
logger.info({"a": 1, "b": 2})
```

You can also override the default host/source/sourcetype/index for a specific log event if needed using the `extra` arg:
```python
logger.info(
    {"a": 1, "b": 2},
    extra={
        "host": "",
        "source": "",
        "sourcetype": "",
        "index": ""
    }
)
```

The handler can opt into the same background batching behavior without changing its default synchronous behavior:

```python
hec_handler = HecHandler(
    host="localhost",
    token=os.environ["HEC_TOKEN"],
    batch_enabled=True,
    batch_size=100,
    flush_interval=2,
)

try:
    root.addHandler(hec_handler)
    logger.info({"a": 1, "b": 2})
finally:
    hec_handler.close()
```

`HecHandler.flush()` and `HecHandler.close()` delegate to the batch forwarder when batching is enabled. All
forwarder acknowledgment and queue options can also be passed to the handler.

Python calls [`logging.shutdown()`](https://docs.python.org/3/library/logging.html#logging.shutdown) during normal
interpreter shutdown, which flushes and closes registered handlers. An unhandled `SIGINT` normally reaches that path
through `KeyboardInterrupt`; the default `SIGTERM` behavior does
[not run Python exit handlers](https://docs.python.org/3/library/atexit.html). Service applications should handle
`SIGTERM`, stop producing logs (and stop any `QueueListener`), then call `logging.shutdown()` from their graceful
shutdown path. Hard termination cannot be made lossless with an in-memory queue.

An application-level logging queue can also be used when logging dispatch itself needs to be isolated from the
calling thread.
See [Dealing with handlers that block](https://docs.python.org/3/howto/logging-cookbook.html#dealing-with-handlers-that-block) for details.

```python
import logging
import queue
import os
import sys
import socket
from logging.handlers import QueueHandler, QueueListener

from splunk_logging.handlers import HecHandler
from splunk_logging.formatters import JsonFormatter

root = logging.getLogger()
logger = logging.getLogger(__name__)

hec_handler = HecHandler(
    host="localhost", # Splunk hostname or ip
    port=8088, # port of event listener
    token=os.environ.get("HEC_TOKEN", ""), # hec token, defaults to the HEC_TOKEN environment variable
    use_ssl=True, # connect to the Splunk collector with https, default True
    verify_ssl=True, # verify the collector certificate, default True
    default_host=socket.gethostname(), # default host for the event, can be blank
    default_source="http", # default source to set in splunk when forwarding events, can be blank
    default_sourcetype="_json", # default sourcetype to use, can be blank
    default_index="main", # default index to send events into, can be blank
)
hec_handler.setFormatter(logging.Formatter("%(message)s"))

que: queue.Queue = queue.Queue(-1)
queue_handler = QueueHandler(que)
queue_handler.setFormatter(JsonFormatter())
root.addHandler(queue_handler)
queue_listener = QueueListener(que, hec_handler)
queue_listener.start()

root.setLevel(logging.DEBUG)
```

## License

Splunk logging for Python is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.
