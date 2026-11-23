👋 Welcome to the Examples folder! Here, you'll find a collection of examples and use cases demonstrating various features and functionalities of the project.

## Docker: dynamic blocklist

When running the identity service with Docker (no Kubernetes API), the blocklist is loaded from the file referenced by the `BLOCKLIST_FILE` environment variable. Edit `run-with-docker/config/blocklist` (one IPv4 address per line, `#` comments allowed) and the pinned `Blocklist` map is reconciled every 30 seconds.

`cfcli policy ...` is Kubernetes-oriented: in Docker, manage the blocklist through `run-with-docker/config/blocklist`.
