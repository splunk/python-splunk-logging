# Splunk logging for Python

The splunk-logging python module provides logging handlers for sending log messages directly to a configured
splunk instance using the HTTP Event Collector (HEC).

## Installation

```
pip install python-splunk-logging
```

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
import time
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

To prevent Splunk logging from slowing down the application, a queue can be used to buffer the log messages.
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
