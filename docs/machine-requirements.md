# Machine Requirements

This project runs Spark, Delta, MinIO, ClickHouse, and the event-log loader inside Docker. The host machine only needs the local CLI tools below. Java, Spark, Hadoop, Scala, MinIO, ClickHouse, and Go do not need to be installed on the host.

## Validated Host Tool Versions

These are the versions currently validated on this machine:

| Tool | Validated version | Used for |
| --- | --- | --- |
| Docker Engine | `29.2.1` | Running all containers and local image builds. |
| Docker Compose | `v5.0.2` | Running the `docker compose` stack. |
| uv | `0.10.0` | Python dependency sync and test runner environment. |
| GNU Make | `4.3` | Project command interface. |
| Bash | `5.2.21` | Project shell scripts. |
| curl | `8.5.0` | Bootstrap downloads and readiness checks. |
| GNU coreutils / sha256sum | `9.4` | Bootstrap checksum validation. |
| iproute2 / ss | `6.1.0` | Port validation before Compose startup. |

Nearby newer versions should be fine. If behavior changes, validate with `make bootstrap`, `make build`, `make validate`, and `make tests`.

## Install If Missing

### Docker Engine And Compose

Install Docker Engine or Docker Desktop using the official Docker instructions for your OS:

- https://docs.docker.com/engine/install/
- https://docs.docker.com/compose/install/linux/

On Ubuntu/WSL2, after Docker's apt repository is configured, the relevant packages are typically:

```bash
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

Verify:

```bash
docker --version
docker compose version
```

### uv

`make bootstrap` installs `uv` automatically when it is missing and `curl` is available. Manual install:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Verify:

```bash
uv --version
```

### Make, Bash, curl, sha256sum, ss

On Ubuntu/WSL2:

```bash
sudo apt-get update
sudo apt-get install make bash curl coreutils iproute2
```

Verify:

```bash
make --version
bash --version
curl --version
sha256sum --version
ss --version
```
