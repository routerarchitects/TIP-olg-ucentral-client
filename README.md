# TIP-olg-ucentral-client

`TIP-olg-ucentral-client` is a Go daemon that acts as a secure Cloud Gateway bridging the OpenWiFi Cloud Controller (via WebSocket/JSON-RPC 2.0 mTLS) and local device microservices using a local **NATS message bus**.

The default mode relies on strict TLS and NKey validation for production security, but supports an explicit `allow_insecure_local_dev` flag for CI and local development.

## What this client does
* Loads runtime settings from `config.json`.
* Establishes a strict mTLS WebSocket connection to the OpenWiFi Cloud Controller.
* Connects to the local NATS message bus.
* Subscribes to the `result.vyos` (or configured target) topic to handle asynchronous downstream replies.
* Translates incoming OpenWiFi JSON-RPC requests into standardized NATS envelopes.
* Publishes configuration payloads to `cmd.configure.vyos`.
* Publishes operational actions to `cmd.action.vyos.<action>`.
* Maintains strict payload size limits (e.g., 10MB configure limit) to prevent memory exhaustion.
* Maintains an active state machine lock for heavy actions (like `upgrade` or `reboot`) to prevent conflicting concurrent state changes.
* Manages request timeouts based on configurable environment variables.
* Caches completed transaction responses for a configurable TTL so duplicate requests can be safely replayed without re-executing the downstream operation.
* Subscribes to downstream device operational state exclusively on `state.<target>` via `agentcore.StateEnvelope`, coalesces snapshots via `StateCoalescer`, and pushes periodic/on-demand JSON-RPC state messages to the Cloud Controller over Priority 2 queues.
* Strictly enforces subject separation: `state.<target>` is used exclusively for periodic operational telemetry, while `status.<target>` remains reserved for lifecycle and execution progress.
* Pushes standard telemetry directly from NATS to the Cloud Controller (To be implemented).

## Configuration
The client uses JSON as the runtime source of truth.

Default config path:
`config.json`

Config path resolution:
1. The path supplied with `-config`.
2. `config.json` when `-config` is omitted.

Example config file:
`config.example.json`
*(Note: This file is provided purely to illustrate the structural schema of the configuration. It contains placeholder values that will fail strict validation out-of-the-box. You must configure it with consistent credentials and network schemes for your specific environment.)*

Important rule:
- Environment variables override operational timeouts, payload limits, and cache TTLs. A complete list of all supported operational variables and their defaults is documented in the provided `.env.example` file.
- `config.json` dictates network routes, queue capacities, and TLS paths. The `serial` field is strictly ignored in `config.json` to prevent identity spoofing.

The agent must not hardcode Cloud URLs, NATS servers, or TLS paths.

mTLS enforcement:
By default, the client enforces strict TLS on the Cloud connection requiring `ca_file`, `client_cert_file`, and `client_key_file` to be specified and cryptographically valid.

## Setup Instructions
To successfully configure and connect the client to an OpenWiFi Cloud Controller:

1. **Obtain mTLS Certificates:** You must provision the device with a valid `cert.pem` and `key.pem` signed by the Cloud's operational Certificate Authority. You will also need the public Root CA bundle (`ca.pem` or system certificates).
2. **Set the Target URL:** In `config.json`, configure the `cloud.url` to point to the Cloud Controller's WebSocket port (typically `15002`, not `443`), for example: `wss://openwifi.example.com:15002`.
3. **Configure the Identity:** The device identity is securely loaded from the path specified by the `OW_INTERFACE_MAP_FILE` environment variable, which defaults to `/etc/ucentral/interface_map.json` at boot. This file must be provisioned by the host hardware and must contain a serial number matching what is expected by the Cloud Controller (and bound to the client certificate CN).
4. **Start the Local Bus:** Ensure a local NATS server is running and accessible.
   * **Production:** Must use secure `tls://...` connections with a valid `credentials_file` and `ca_file`.
   * **Local/CI Development:** Can use plaintext `nats://127.0.0.1:4222` provided `"allow_insecure_local_dev": true` is explicitly set in the configuration.
5. **Run the Daemon:** Start the client using `go run ./cmd/ucentral-client -config ./config.json`.

## Local state
The runtime config is JSON, and local persistent state (like the active state lock) is managed by an `OperationStore` disk backend, ensuring robust state recovery after restarts and mitigating stale locks.

## Minimal repository layout
```
TIP-olg-ucentral-client/
  README.md
  SPEC.md
  config.example.json
  .env.example
  go.mod

  cmd/
    ucentral-client/
      main.go
      handler.go
      main_test.go
      handler_test.go

  pkg/
    config/
      config.go
    contracts/
      enums.go
      rpc.go
    nats/
      client.go
    queues/
      scheduler.go
    reqmgr/
      manager.go
      cache.go
      disk_store.go
    websocket/
      client.go

  tests/
    integration_test.go
    system_e2e/
      system_test.go
```

## Common commands
```bash
go test ./...
UNFORMATTED=$(gofmt -l $(find . -type f -name '*.go' -not -path './.git/*'))
test -z "$UNFORMATTED"
go build ./...
go run ./cmd/ucentral-client -config ./config.json
```

## Testing
This client has been tested and validated against the opensource OpenLan Cloud Controller [Mango Cloud](https://www.mangowifi.cloud/) deployment version 1.0. This real-world integration confirms compatibility with core OpenWiFi Cloud Controller specifications, including strict mTLS WebSocket handshakes, JSON-RPC 2.0 schema validation, and real-time NATS state synchronization under production-like conditions.

## CI coverage
`.github/workflows/ci.yml` validates:
* Module verification
* `gofmt` formatting check
* `go vet` and `staticcheck` for static analysis
* `govulncheck` and advisory `gosec` for security vulnerabilities
* `go test -v -race -p=1 ./...` (with data race detection and sequential execution)
* `go build ./...`

## Binary usage
The current binary supports:
* long-running runtime mode using `websocket.WSClient` and `nats.NATSClient` (Start, handler registration, graceful Close)
* strict JSON-RPC payload limit parsing and memory clamping via `OLG_PAYLOAD_LIMIT_*` environment variables
* configuration workflow dispatching to `cmd.configure.vyos`
* action workflow dispatching to `cmd.action.vyos.<action>`
* automatic lock release and transaction timeouts when downstream agents fail to respond
* deep JSON-RPC error translations leveraging standard `-32603` schemas for application failures

```bash
go run ./cmd/ucentral-client -config ./config.json
```

**Options**
| Option | Description |
| --- | --- |
| `-config <path>` | Path to the JSON configuration file. |

## Current behavior
Running the client loads the configuration, initializes the `CapabilityCache`, `PriorityScheduler`, `OperationStore`, and `NATSClient`. It then dials the OpenWiFi Cloud WebSocket.

Once the `WSClient` achieves a `connected and verified` state, it begins receiving JSON-RPC frames. 
It uses the `ReqManager` to create transactions and locks. If the request is state-changing (like `upgrade` or `reboot`), it sets a lock. It publishes the command to NATS, sets a dispatch timeout, and waits for a response on `result.vyos`.

When the downstream agent publishes a result, the NATS handler validates the result envelope against expected schema constraints. It parses it via `BuildDeviceResultObject()`, translates it into JSON-RPC, and pushes it to the Outbound Scheduler back to the Cloud.

## Design principle
`TIP-olg-ucentral-client` handles Cloud-facing behavior (WebSocket stability, JSON-RPC schema compliance, queueing, request management). `vyos-nats-agent` (or similar agents) handles device-specific orchestration. Real VyOS behavior stays behind the NATS boundary so the client remains purely a lightweight, high-performance protocol bridge.
