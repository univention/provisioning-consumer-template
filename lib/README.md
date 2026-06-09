# provisioning-consumer-lib

A Python library that provides base classes and helpers for writing Provisioning Consumers for the [Univention Nubus](https://www.univention.com/products/nubus/) provisioning service.

The library is fully `async` and uses `asyncio` with `httpx` for non-blocking HTTP.

## Installation

```bash
pip install provisioning-consumer-lib
```

Or with `uv`:

```bash
uv add provisioning-consumer-lib
```

## Concepts

A **Provisioning Consumer** subscribes to one or more UDM object topics (e.g. `users/user`, `groups/group`) at the Nubus provisioning service. Whenever a subscribed object is created, modified, or deleted in the directory, the consumer receives an event and processes it.

The library provides three main building blocks:

| Class | Purpose |
|---|---|
| `ConsumerModule` | Manages the subscription lifecycle and the async event polling loop |
| `UDMEventHandler` | Base class — subclass this to react to create/modify/remove events |
| `EventHandler` | Lower-level base class for non-UDM event handling |

## Usage

### 1. Subclass `UDMEventHandler`

All handler methods are `async`, so you can freely `await` within them:

```python
from provisioning_consumer_lib import UDMEventHandler, Metadata, AttributeMapping

class MyEventHandler(UDMEventHandler):
    async def _handle_create(self, metadata: Metadata, new: AttributeMapping) -> None:
        self.logger.info("Created: %s", new["dn"])

    async def _handle_modify(self, metadata: Metadata, old: AttributeMapping,
                             new: AttributeMapping, has_moved: bool) -> None:
        self.logger.info("Modified: %s (moved=%s)", new["dn"], has_moved)

    async def _handle_remove(self, metadata: Metadata, old: AttributeMapping) -> None:
        self.logger.info("Removed: %s", old["dn"])
```

All three methods must be implemented. They receive:

- `metadata` — event metadata (sequence number, topic, realm, etc.) without the object body
- `old` — previous UDM object attributes (present on modify and remove events)
- `new` — new UDM object attributes (present on create and modify events)
- `has_moved` — `True` if the object's DN changed during a modify event

### 2. Optionally filter events with `is_relevant()`

Override `is_relevant()` to skip events before they reach `handle_event()`. The method is `async`, so you can `await` within it:

```python
async def is_relevant(self, event: dict) -> bool:
    # Only process events for objects inside a specific OU
    return "ou=employees" in event["body"]["new"].get("dn", "")
```

### 3. Create `ConsumerModule` and subscribe

`ConsumerModule` is an async context manager that closes the underlying HTTP client on exit:

```python
import asyncio
from loguru import logger
from provisioning_consumer_lib import ConsumerModule

async def main():
    handler = MyEventHandler(logger)
    async with ConsumerModule(
        handler=handler,
        name="my-consumer",                  # unique name for this consumer instance
        provisioning_url="https://<nubus-fqdn>/univention/provisioning/",
        config_dir="/var/lib/univention/consumer",  # directory for storing credentials
    ) as consumer:
        await consumer.subscribe(
            admin_username="provisioning_admin",
            admin_password="<password>",
            topics=[
                {"realm": "udm", "topic": "users/user"},
                {"realm": "udm", "topic": "groups/group"},
            ],
            prefill=True,   # replay existing objects on first subscription
        )
        await consumer.consume_loop()

asyncio.run(main())
```

`subscribe()` is idempotent: if a `provisioning_config.json` with valid credentials already exists in `config_dir`, the existing subscription is reused and no new subscription is created at the server.

### 4. Run the event loop

```python
await consumer.consume_loop()   # runs forever, handles QueueAccessError with async back-off
```

For custom control flow, call `process_one_event()` directly:

```python
while True:
    await consumer.process_one_event(long_polling_timeout=10)
```

## `ConsumerModule` configuration

All configuration is passed as keyword arguments to the constructor:

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `name` | `str` | yes | — | Unique name for this consumer |
| `provisioning_url` | `str` | yes | — | Base URL of the provisioning service |
| `config_dir` | `str` | no | `/var/lib/univention/consumer` | Directory where `provisioning_config.json` is stored |
| `error_timeout` | `int` | no | `60` | Seconds to wait (via `asyncio.sleep`) after a `QueueAccessError` before retrying |
| `session` | `httpx.AsyncClient` | no | auto-created | Custom async HTTP client (useful for testing) |

## `provisioning_config.json`

`ConsumerModule` stores subscription credentials in a file named `provisioning_config.json` inside `config_dir`. The file has the following format:

```json
{
  "subscription_name": "sub-f5e10d16-d7d9-4fd7-9f94-48e6efcb4030",
  "subscription_password": "eCPrJCc80jCQr3n9tONYYT05QqhaOFNBB191sutHrbY"
}
```

| Field | Type | Description |
|---|---|---|
| `subscription_name` | `str` | Client-generated identifier (`<consumer_name>-<random_hex>`) used to register the subscription if no `subscription_name` was provided |
| `subscription_password` | `str` | Client-generated random password (`secrets.token_urlsafe(32)`) used to authenticate the subscription |

Both values are generated by `ConsumerModule` during `subscribe()` and sent to the provisioning service when creating the subscription. The file is reused on subsequent runs.

## Detecting attribute changes

`UDMEventHandler.diff()` compares old and new UDM attributes and returns only the changed ones:

```python
class MyEventHandler(UDMEventHandler):
    async def _handle_modify(self, metadata, old, new, has_moved):
        changes = self.diff({"body": {"old": old, "new": new}}, keys=["mail", "groups"])
        for attr, (old_val, new_val) in changes.items():
            self.logger.info("%s changed: %r -> %r", attr, old_val, new_val)
```

## Error handling

If `_handle_create`, `_handle_modify`, or `_handle_remove` raises an exception, `_handle_error()` is called. The default implementation logs the exception and re-raises it, which causes `handle_event()` to return `False` and the event is **not** acknowledged (so it will be retried).

Override `_handle_error()` to implement custom error handling. The method is `async`, so you can perform I/O such as writing to a dead-letter queue or sending an alert:

```python
async def _handle_error(self, metadata, old, new, exc_type, exc_value, exc_traceback):
    self.logger.error("Ignoring error: %s", exc_value)
    # returning without re-raising suppresses the error
```

## DN helper

The library also exports a `DN` class for working with LDAP Distinguished Names:

```python
from provisioning_consumer_lib import DN

dn = DN("uid=jdoe,ou=employees,dc=example,dc=com")
dn.rdn          # ('uid', 'jdoe')
dn.parent       # DN('ou=employees,dc=example,dc=com')
dn.endswith("dc=example,dc=com")   # True
DN("CN=Foo") == DN("cn=foo")       # True (case-insensitive for common attributes)
```

## Public API

```python
from provisioning_consumer_lib import (
    ConsumerModule,
    EventHandler,
    Metadata,
    UDMEventHandler,
    QueueAccessError,
    SubscriptionError,
    AttributeMapping,
    DN,
)
```
