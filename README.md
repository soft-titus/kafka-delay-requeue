# Kafka Delay Requeue

Worker to process retriable Kafka messages with delay and requeue logic.

This worker is intended to run inside a Kubernetes cluster with KEDA.

It will:
- Pull messages from Kafka
- Delay processing of messages based on process-after header
- Requeue to origin topic when the time exceed

---

## Features
- Each messages can have individual delay intervals based on process-after header
- Each messages can be requeue to different origin topic based on header origin-topic
- Real-time kafka lag, to optimize pulling of messages in shorter interval when there is more than 1 message to process, and pulling the message in longer interval when there is only 1 message to process


---

## Requirements
- Go (tested with version 1.25.5)
- golangci-lint (tested with version 2.7.2)
- Docker & Docker Compose (for local development)

---

## Setup

1. Clone the repository:

```bash
git clone https://github.com/soft-titus/kafka-delay-requeue.git
cd kafka-delay-requeue
```

2. Install Go dependencies:

```bash
go mod tidy
```

3. Install Git pre-commit hooks:

```bash
cp hooks/pre-commit .git/hooks/
```

4. Run the application locally with Docker:

```bash
docker compose build
docker compose up -d
```

5. Send Kafka messages:

```bash
docker compose run --rm producer --retry-count 0 --process-after "2025-12-31T23:59:59Z"
```

| Parameter     | Required | Notes                                |
| ------------- | -------- | ------------------------------------ |
| retry-count   | no       | Default 0                            |
| process-after | no       | Default is current time + 10 seconds |

the origin-topic will always be set to `test.topic` for simplicity.

---

## Check Worker Logs

```bash
docker compose logs consumer -f
```

---

## Code Formatting and Linting

```bash
go fmt ./...
golangci-lint run ./...
```

---

## GitHub Actions

- `ci.yaml` : Runs linting and tests on all branches and PRs
- `build-and-publish-branch-docker-image.yaml` : Builds Docker images for
  branches
- `build-and-publish-production-docker-image.yaml` : Builds production
  images on `main`

---

### Branch Builds

Branch-specific Docker images are built with timestamped tags.  

Example: `1.0.0-dev-1762431`

---

### Production Builds

Merges to `main` trigger a production Docker image build.  
Versioning follows semantic versioning based on commit messages:

- `BREAKING CHANGE:` : major version bump
- `feat:` : minor version bump
- `fix:` : patch version bump

Example: `1.0.0`
