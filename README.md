# uCentral Client Daemon (`TIP-olg-ucentral-client`)

The uCentral client is a lightweight, Go-based gateway daemon that bridges a cloud management platform (via the uCentral WebSocket/JSON-RPC 2.0 protocol) with local device microservices using a local **NATS message bus**.

---

## 1. Quick Start

### 1.1 Prerequisites
*   Go (version 1.20 or later)
*   A local NATS broker configured strictly for **TLS 1.3** and **NKey/JWT Authentication**. (Plaintext connections and insecure `nats://` URLs are architecturally prohibited by the daemon).
    *   You must generate a valid CA certificate (`ca.pem`) and a NATS credential file (`nats.creds`) and map them in your `config.json` before the daemon will start.

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
  "cloud": {
    "url": "wss://cloud.gateway.example.com:15002",
    "connect_timeout_seconds": 10,
    "write_timeout_seconds": 10,
    "ping_interval_seconds": 30,
    "pong_timeout_seconds": 60,
    "stable_session_threshold_seconds": 300,
    "compression_threshold_bytes": 2048,
    "tls": {
      "ca_file": "/etc/ucentral/cloud-ca.pem",
      "client_cert_file": "/etc/ucentral/cloud-cert.pem",
      "client_key_file": "/etc/ucentral/cloud-key.pem",
      "server_name": "cloud.gateway.example.com"
    }
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

The daemon utilizes container environment variables to configure operational timeouts and cache durations safely. All durations must be specified in valid Go duration syntax (e.g., `5s`, `1m30s`, `1h`). If an environment variable is omitted, the daemon uses the default value. If a variable is malformed or zero/negative, the daemon triggers a fatal startup error.

**Timeouts:**
*   `OLG_TIMEOUT_DISPATCH` (Default: `5s`): Bounded timeout for the local preparation and NATS dispatch phases.
*   `OLG_TIMEOUT_CONFIGURE` (Default: `30s`): Maximum downstream response wait time for `configure`.
*   `OLG_TIMEOUT_ACTION_EXTENDED` (Default: `120s`): Extended response wait time for heavy actions (`upgrade`, `certupdate`, `script`, `trace`).
*   `OLG_TIMEOUT_ACTION_DEFAULT` (Default: `60s`): Maximum downstream response wait time for all other standard actions (e.g., `ping`, `reboot`, `factory`).

**Cache TTLs:**
*   `OLG_CACHE_TTL_DEFAULT` (Default: `2m`): TTL for read-only diagnostics (`ping`, `trace`, `telemetry`, `capabilities.get`, `status.get`).
*   `OLG_CACHE_TTL_CONFIGURE` (Default: `5m`): TTL for `configure`.
*   `OLG_CACHE_TTL_LEDS` (Default: `5m`): TTL for `leds`.
*   `OLG_CACHE_TTL_REBOOT` (Default: `10m`): TTL for `reboot`.
*   `OLG_CACHE_TTL_REMOTE_ACCESS` (Default: `10m`): TTL for `remote_access`.
*   `OLG_CACHE_TTL_FACTORY` (Default: `30m`): TTL for `factory`.
*   `OLG_CACHE_TTL_CERTUPDATE` (Default: `30m`): TTL for `certupdate`.
*   `OLG_CACHE_TTL_REENROLL` (Default: `30m`): TTL for `reenroll`.
*   `OLG_CACHE_TTL_SCRIPT` (Default: `30m`): TTL for `script`.
*   `OLG_CACHE_TTL_UPGRADE` (Default: `60m`): TTL for `upgrade` (measured from completion).

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