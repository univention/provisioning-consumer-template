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

Edit `src/provisioning_consumer/main.py` and subclass `UDMEventHandler`:

```python
from provisioning_consumer_lib import ConsumerModule, UDMEventHandler, AttributeMapping

class MyEventHandler(UDMEventHandler):
    def _handle_create(self, metadata: AttributeMapping, new: AttributeMapping) -> None:
        ...

    def _handle_modify(self, metadata: AttributeMapping, old: AttributeMapping,
                       new: AttributeMapping, has_moved: bool) -> None:
        ...

    def _handle_remove(self, metadata: AttributeMapping, old: AttributeMapping) -> None:
        ...
```

### 2. Wire up `ConsumerModule` and run

```python
from loguru import logger

def main() -> None:
    handler = MyEventHandler(logger)
    consumer = ConsumerModule(
        handler=handler,
        name="my-consumer",
        provisioning_url="https://<nubus-fqdn>/univention/provisioning/",
        config_dir="/var/lib/univention/consumer",
    )
    consumer.subscribe(
        admin_username="provisioning_admin",
        admin_password="<password>",
        topics=[{"realm": "udm", "topic": "users/user"}],
        prefill=True,
    )
    consumer.consume_loop()
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

## Library documentation

See [`lib/README.md`](lib/README.md) for the full API reference of `provisioning-consumer-lib`.
