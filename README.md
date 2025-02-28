<!--- Top of README Badges (automated) --->
[![PyPI](https://img.shields.io/pypi/v/oms-mqclient)](https://pypi.org/project/oms-mqclient/) [![GitHub release (latest by date including pre-releases)](https://img.shields.io/github/v/release/Observation-Management-Service/MQClient?include_prereleases)](https://github.com/Observation-Management-Service/MQClient/) [![PyPI - License](https://img.shields.io/pypi/l/oms-mqclient)](https://github.com/Observation-Management-Service/MQClient/blob/master/LICENSE) [![Lines of code](https://img.shields.io/tokei/lines/github/Observation-Management-Service/MQClient)](https://github.com/Observation-Management-Service/MQClient/) [![GitHub issues](https://img.shields.io/github/issues/Observation-Management-Service/MQClient)](https://github.com/Observation-Management-Service/MQClient/issues?q=is%3Aissue+sort%3Aupdated-desc+is%3Aopen) [![GitHub pull requests](https://img.shields.io/github/issues-pr/Observation-Management-Service/MQClient)](https://github.com/Observation-Management-Service/MQClient/pulls?q=is%3Apr+sort%3Aupdated-desc+is%3Aopen)
<!--- End of README Badges (automated) --->

# MQClient

MQClient is a versatile message-queue client API that provides a unified interface for interacting with multiple messaging systems, including Apache Pulsar, RabbitMQ, and NATS.io.

## Features

- **Unified API**: Interact with multiple message brokers through a single, consistent interface.
- **Extensible Design**: Easily add support for additional messaging systems as needed.
- **Asynchronous Support**: Leverage asynchronous operations for improved performance and scalability.

## Installation

You must choose the message broker protocol at install time, these are `pulsar`, `rabbitmq`,and `nats`:

```bash
pip install oms-mqclient[pulsar]  
```

or

```bash
pip install oms-mqclient[rabbitmq]  
```

or

```bash
pip install oms-mqclient[nats]  
```

## Usage

### Initializing a Queue

To use MQClient, instantiate a `Queue` with the required broker client:

```python
from mqclient.queue import Queue
import os

broker_client = "rabbitmq"  # this must match what was used at install
queue = Queue(broker_client=broker_client, name="my_queue", auth_token=os.getenv('MY_QUEUE_AUTH'))
```

### Use Cases / Patterns / Recipes

The most common use case of MQClient is to open a pub and/or sub stream.

#### **Streaming Publisher**

Use `open_pub()` to open a pub stream.

```python
async def stream_publisher(queue: Queue):
    """Publish all messages."""
    async with queue.open_pub() as pub:
        while True:
            msg = await generate_message()
            await pub.send(msg)
            print(f"Sent: {msg}")
```

#### **Streaming Consumer**

Use `open_sub()` to open a sub stream. Each message will be automatically acknowledged upon the following iteration. If an `Exception` is raised, the message will immediately be nacked. By default, any un-excepted exceptions will be excepted by the `open_sub()` context manager. This can be turned off by setting `Queue.except_errors` to `False`.

```python
async def stream_consumer(queue: Queue):
    """Consume messages until timeout."""
    async with queue.open_sub() as sub:
        async for msg in sub:
            print(f"Received: {msg}")
            await process_message(msg)
```

### Less Common Use Cases / Patterns / Recipes

#### **Consuming a Single Message**

The most common use case is to open an `open_sub()` stream to receive messages due to the overhead of opening a sub. Nonetheless, `open_sub_one()` can be used to consume a single message.

```python
async def consume_one(queue: Queue):
    """Get one message only."""
    async with queue.open_sub_one() as msg:
        print(f"Received: {msg}")
```

#### **Multitasking Consumer with Manual Acknowledgement**

Since `open_sub()`'s built-in ack/nack mechanism enforces one-by-one message consumption—i.e., the previous message must be acked/nacked before an additional message can be consumed—you will need to manually acknowledge (or nack) messages.

**Warning:** If a message is not acked/nacked within a certain time, it may be re-enqueued. Client code will need to account for this.

```python
async def stream_consumer_manual_ack(queue: Queue):
    """Stream many messages, manually."""
    messages_pending_ack = []
    async with queue.open_sub_manual_acking() as sub:
        async for m in sub.iter_messages():
            messages_pending_ack.append(m)
            # Process many messages -- another example might use an async-multitasking pool
            some_msg_obj = next_done_message(messages_pending_ack)
            try:
                print(f"Processed: {some_msg_obj.data}")
                raise_exception_if_failed(some_msg_obj)
            except Exception:
                await sub.nack(some_msg_obj)  # Mark message for redelivery
            else:
                await sub.ack(some_msg_obj)  # Manually acknowledge
```

## Configuration

MQClient supports various configurations via environment variables or direct parameters:

| Parameter    | Description                           | Default Value                |
|--------------|---------------------------------------|------------------------------|
| `broker_url` | Connection URL for the message broker | `localhost`                  |
| `queue_name` | Name of the message queue             | autogenerated                |
| `prefetch`   | Number of messages to prefetch        | `1`                          |
| `timeout`    | Time in seconds to wait for a message | `60`                         |
| `retries`    | Number of retry attempts on failure   | `2` (i.e., 3 attempts total) |

## Contributing

Contributions are welcome! Feel free to submit issues or pull requests to improve MQClient.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

For more details, visit the [repository](https://github.com/Observation-Management-Service/MQClient).
