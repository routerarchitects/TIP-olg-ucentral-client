# uCentral Client Daemon (`olg-ucentral-client`)

The uCentral client is a lightweight, Go-based gateway daemon that bridges a cloud management platform (via the uCentral WebSocket/JSON-RPC 2.0 protocol) with local device microservices using a local **NATS message bus**.

---

## 1. Quick Start

### 1.1 Prerequisites
*   Go (version 1.20 or later)
*   A local NATS broker (running on NATS port 4222)

### 1.2 Build & Run
1.  Initialize Go dependencies:
    ```bash
    go mod tidy
    ```
2.  Build the daemon binary:
    ```bash
    go build -o ucentral-client ./cmd/ucentral-client
    ```
3.  Run the client:
    ```bash
    ./ucentral-client -config config.json
    ```

---

## 2. Configuration Schema (`config.json`)

```json
{
  "serial": "00:11:22:33:44:55",
  "compression_threshold_bytes": 2048,
  "cloud": {
    "url": "wss://cloud.gateway.example.com:15002",
    "connect_timeout_seconds": 10
  },
  "nats": {
    "servers": ["tls://127.0.0.1:4222"],
    "credentials_file": "/etc/ucentral/nats.creds",
    "tls_required": true,
    "ca_file": "/etc/ucentral/ca.pem"
  },
  "queues": {
    "ws_writer_capacity": 500,
    "nats_publish_capacity": 100,
    "command_result_capacity": 50
  }
}
```

---

## 3. Running Test Suites

Verify all components are functioning using the standard Go test command:
```bash
go test -v ./...
```
To run tests with code coverage:
```bash
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```