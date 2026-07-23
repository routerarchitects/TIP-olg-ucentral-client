module github.com/routerarchitects/TIP-olg-ucentral-client

go 1.25.0

require github.com/Telecominfraproject/olg-nats-agent-core v0.1.0

require (
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/nats-io/nats.go v1.51.0 // indirect
	github.com/nats-io/nkeys v0.4.15 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.49.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
)

replace github.com/Telecominfraproject/olg-nats-agent-core => ../TIP-olg-nats-agent-core
