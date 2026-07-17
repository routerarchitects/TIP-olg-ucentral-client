# uCentral Client Daemon (`TIP-olg-ucentral-client`)

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
    "ca_file": "/etc/ucentral/ca.pem"
  },
  "queues": {
    "ws_writer_capacity": 500,
    "emergency_capacity": 100,
    "nats_publish_capacity": 100,
    "command_result_capacity": 50,
    "telemetry_capacity": 500
  }
}
```

---

## 3. Environment Variables

The daemon utilizes container environment variables to configure operational timeouts safely. All durations must be specified in valid Go duration syntax (e.g., `5s`, `1m30s`). If an environment variable is omitted, the daemon uses the default value. If a variable is malformed or zero/negative, the daemon triggers a fatal startup error.

*   `OLG_TIMEOUT_DISPATCH` (Default: `5s`): Bounded timeout for the local preparation and NATS dispatch phases.
*   `OLG_TIMEOUT_CONFIGURE` (Default: `30s`): Maximum downstream response wait time for `configure`.
*   `OLG_TIMEOUT_ACTION_EXTENDED` (Default: `120s`): Extended response wait time for heavy actions (`upgrade`, `certupdate`, `script`, `trace`).
*   `OLG_TIMEOUT_ACTION_DEFAULT` (Default: `60s`): Maximum downstream response wait time for all other standard actions (e.g., `ping`, `reboot`, `factory`).

---

## 4. Running Test Suites

Verify all components are functioning using the standard Go test command:
```bash
go test -v ./...
```
To run tests with code coverage:
```bash
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```