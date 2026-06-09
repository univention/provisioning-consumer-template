# Provisioning Consumer — library and Docker template

This repository contains two things:

1. **`lib/`** — a pip-installable Python library (`provisioning-consumer-lib`) with base classes for writing Provisioning Consumers
2. **`src/`** — a minimal template application that demonstrates how to use the library

Container images built with the provided `Dockerfile` will be [distroless](https://github.com/GoogleContainerTools/distroless).

## Repository layout

```
.
├── Dockerfile
├── pyproject.toml          # Template application (provisioning-consumer)
├── appcenter/
│   ├── inst                # Join script performing the subscription
│   └── uinst               # Unjoin script performing the desubscription
├── src/
│   └── provisioning_consumer/
│       ├── __init__.py
│       └── main.py         # Entry point — customise this
└── lib/
    ├── pyproject.toml      # Library (provisioning-consumer-lib)
    └── src/
        └── provisioning_consumer_lib/
            ├── consumer.py # ConsumerModule, EventHandler, UDMEventHandler
            └── dn.py       # DN helper class
```

## Quick start

### 1. Implement your event handler

Edit `src/provisioning_consumer/main.py` and subclass `UDMEventHandler`. All handler methods are `async`:

```python
import asyncio
from provisioning_consumer_lib import ConsumerModule, UDMEventHandler, Metadata, AttributeMapping

class MyEventHandler(UDMEventHandler):
    async def _handle_create(self, metadata: Metadata, new: AttributeMapping) -> None:
        ...

    async def _handle_modify(self, metadata: Metadata, old: AttributeMapping,
                             new: AttributeMapping, has_moved: bool) -> None:
        ...

    async def _handle_remove(self, metadata: Metadata, old: AttributeMapping) -> None:
        ...

    # Optional — filter events before processing:
    async def is_relevant(self, event) -> bool:
        ...

    # Optional — custom error handling:
    async def _handle_error(self, metadata, old, new, exc_type, exc_value, exc_traceback) -> None:
        ...
```

### 2. Wire up `ConsumerModule` and run

```python
import asyncio
from loguru import logger

async def main() -> None:
    handler = MyEventHandler(logger)
    async with ConsumerModule(
        handler=handler,
        name="my-consumer",
        provisioning_url="https://<nubus-fqdn>/univention/provisioning/",
        config_dir="/var/lib/univention/consumer",
    ) as consumer:
        await consumer.subscribe(
            admin_username="provisioning_admin",
            admin_password="<password>",
            topics=[{"realm": "udm", "topic": "users/user"}],
            prefill=True,
        )
        await consumer.consume_loop()

if __name__ == "__main__":
    asyncio.run(main())
```

## Prerequisites

| Tool   | Minimum version |
|--------|-----------------|
| Python | 3.11            |
| uv     | 0.4             |
| Docker | 20.10           |

## Running from the CLI

### 1. Create a virtual environment and install dependencies

```bash
uv venv  # --python python3.11
uv sync
```

### 2. Start the service

```bash
. .venv/bin/activate
provisioning-consumer
deactivate
```

Or without activating the venv:

```bash
uv run provisioning-consumer
```

Stop the process at any time with **Ctrl+C**.

## Docker

### Build the image

The image is built in two stages:

1. A virtual env with the project's dependencies is built in a standard Python container (`python:3.12-slim`).
2. The virtual env is copied into a distroless base image (`gcr.io/distroless/python3-debian12:nonroot`).

The result is a minimal image containing only your application and its dependencies.

```bash
docker build -t provisioning-consumer:latest .
```

### Run the container

```bash
docker run --rm provisioning-consumer:latest
```

Stop with **Ctrl+C** or, if running detached:

```bash
docker stop provisioning-consumer && docker rm provisioning-consumer
```

### Run detached

```bash
docker run -d --name provisioning-consumer provisioning-consumer:latest
```

View output:

```bash
docker logs provisioning-consumer
```

## App Center scripts

The `appcenter/` directory contains two scripts that automatically manage the provisioning subscription when the app is installed or removed. Both use `univention-provisioning-tool` — the standard command-line helper for subscribing and unsubscribing from the Provisioning Service.

### `inst` — subscribe on app installation

Runs when the app is installed. It subscribes to provisioning topics (e.g. `users/user`) and saves the resulting subscription config to disk so the consumer can pick it up at runtime. It also requests prefill data, allowing the consumer to catch up on events that occurred before the subscription existed. If the subscription fails, the join script is aborted.

### `uinst` — unsubscribe on app removal

Runs when the app is removed. It reads the subscription config written by `inst` and unsubscribes from all topics. Failures are silently tolerated so that app removal always succeeds.

## Library documentation

See [`lib/README.md`](lib/README.md) for the full API reference of `provisioning-consumer-lib`.
