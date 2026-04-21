# Provisioning Consumer library and Docker template

This repository contains a pip-installable Python framework and a `Dockerfile`
that you can use to quickly create a Provisioning Consumer.

Container images build with the provided `Dockerfile` will be [distroless](https://github.com/GoogleContainerTools/distroless).

## Usage

TODO

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

Or, without activating the venv:

```bash
uv run provisioning-consumer
```

Stop the process at any time with **Ctrl + C**.

## Docker

### Build the image

The image will be built in two stages:

1. A virtual env with the project's dependencies and the project itself is built in a standard Python container (`python:3.12-slim`).
2. The virtual env is copied into a distroless base container (`gcr.io/distroless/python3-debian12:nonroot`).

The result is a small image with only your application.

```bash
docker build -t provisioning-consumer:latest .
```

### Run the container

```bash
docker run --rm provisioning-consumer:latest
```

The container prints `Hello, World!` and then keeps running. Stop it with **Ctrl + C**
(or `docker stop <container-id>` if running detached).

### Run detached

```bash
docker run -d --name provisioning-consumer provisioning-consumer:latest
```

View output:

```bash
docker logs provisioning-consumer
```

Stop and remove:

```bash
docker stop provisioning-consumer && docker rm provisioning-consumer
```

---

## Project layout

```
.
├── Dockerfile
├── pyproject.toml
└── src/
    └── provisioning_consumer/
        ├── __init__.py
        └── main.py         # Entry point
```
