package websocket

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	gws "github.com/gorilla/websocket"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/config"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/contracts"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/queues"
)

// CloudConnectParams defines the required fields for the connect JSON-RPC handshake.
type CloudConnectParams struct {
	Serial       string         `json:"serial"`
	UUID         uint64         `json:"uuid"`
	Firmware     string         `json:"firmware"`
	Capabilities map[string]any `json:"capabilities"`
}

type ConnectMetadataProvider interface {
	ConnectParams(ctx context.Context) (CloudConnectParams, error)
}

type InboundFrame struct {
	SessionID string
	Type      int
	Payload   []byte
}

type FrameDisposition int

const (
	FrameAccepted FrameDisposition = iota
	FrameRejectedKeepConnection
	FrameFatalCloseConnection
)

// FrameHandler represents the upstream component that processes incoming frames.
//
// HandleFrame is invoked synchronously by the WebSocket reader loop. To ensure
// the session can be cleanly torn down on failure, implementations MUST promptly
// return when the provided context is canceled and MUST propagate the context
// to any blocking operations.
type FrameHandler interface {
	HandleFrame(ctx context.Context, frame InboundFrame) (FrameDisposition, error)
}

type HandshakeResult int

const (
	HandshakeAccepted HandshakeResult = iota
	HandshakeRetryableFailure
	HandshakeRejected
)

const (
	defaultCompressionThresholdBytes     = 2048
	defaultMaxFrameSize                  = 11 * 1024 * 1024
	defaultConnectTimeout                = 10 * time.Second
	defaultWriteTimeout                  = 10 * time.Second
	defaultPongTimeout                   = 60 * time.Second
	defaultPingInterval                  = 30 * time.Second
	defaultStableSessionThresholdSeconds = 60 * time.Second
)

var (
	initialBackoff = 2 * time.Second
	maxBackoff     = 60 * time.Second
)

type WSClient struct {
	running       atomic.Bool
	mu            sync.Mutex
	conn          *gws.Conn
	generation    uint64
	cancel        context.CancelFunc
	config        config.CloudConfig
	scheduler     queues.OutboundScheduler
	metaProvider  ConnectMetadataProvider
	onStateChange StateChangeFunc
	pendingMsgs   []queues.OutboundMessage // Retains dequeued messages across reconnects if write fails

	writeMu sync.Mutex
}

// StateChangeFunc is a callback invoked synchronously on the networking path whenever
// the connection or protocol state changes.
//
// To prevent stalling the reconnect engine, implementations MUST return promptly
// and MUST NOT perform blocking I/O operations. Heavier processing (such as
// NATS publishing or persistence) should be enqueued and handled asynchronously
// outside the transport layer.
type StateChangeFunc func(cloud contracts.LinkState, protocol contracts.ProtocolState)

func NewWSClient(cfg config.CloudConfig, scheduler queues.OutboundScheduler, metaProvider ConnectMetadataProvider, onStateChange StateChangeFunc) (*WSClient, error) {
	if scheduler == nil {
		return nil, errors.New("scheduler cannot be nil")
	}
	if metaProvider == nil {
		return nil, errors.New("metadata provider cannot be nil")
	}
	if onStateChange == nil {
		onStateChange = func(contracts.LinkState, contracts.ProtocolState) {}
	}

	// Normalize runtime default for compression threshold
	if cfg.CompressionThresholdBytes == 0 {
		cfg.CompressionThresholdBytes = defaultCompressionThresholdBytes
	}

	return &WSClient{
		config:        cfg,
		scheduler:     scheduler,
		metaProvider:  metaProvider,
		onStateChange: onStateChange,
	}, nil
}

// ReconnectLoop manages the persistent WebSocket connection to the cloud.
// This function takes ownership of the physical connection state.
// It is designed for single-run ownership and MUST NOT be called concurrently
// by multiple goroutines.
func (c *WSClient) ReconnectLoop(ctx context.Context, handler FrameHandler) error {
	if !c.running.CompareAndSwap(false, true) {
		return fmt.Errorf("ws: ReconnectLoop is already running")
	}
	defer c.running.Store(false)

	if handler == nil {
		return errors.New("ws: frame handler cannot be nil")
	}

	backoff := initialBackoff

	waitForRetry := func() bool {
		jitter := time.Duration(rand.Float64() * float64(backoff))
		jitteredBackoff := initialBackoff + jitter
		if jitteredBackoff > maxBackoff {
			jitteredBackoff = maxBackoff
		}

		select {
		case <-ctx.Done():
			return false
		case <-time.After(jitteredBackoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
		return true
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("ws: reconnect loop stopped by context")
			return nil
		default:
		}

		c.onStateChange(contracts.LinkConnecting, contracts.ProtocolUnknown)
		log.Printf("ws: dialing %s", c.config.URL)

		// Create dialer
		dialerCopy := *gws.DefaultDialer
		dialer := &dialerCopy

		dialer.EnableCompression = true

		hasCA := c.config.TLS.CAFile != ""
		hasCert := c.config.TLS.ClientCertFile != "" || c.config.TLS.ClientKeyFile != ""

		if hasCA || hasCert || c.config.TLS.ServerName != "" {
			tlsConfig := &tls.Config{
				ServerName: c.config.TLS.ServerName,
			}

			if hasCA {
				caCert, err := os.ReadFile(c.config.TLS.CAFile)
				if err != nil {
					log.Printf("ws: failed to read CA file: %v", err)
					if !waitForRetry() {
						return nil
					}
					continue
				}
				caCertPool := x509.NewCertPool()
				if !caCertPool.AppendCertsFromPEM(caCert) {
					log.Printf("ws: failed to parse any valid certificates from CA file")
					if !waitForRetry() {
						return nil
					}
					continue
				}
				tlsConfig.RootCAs = caCertPool
			}

			if hasCert {
				if c.config.TLS.ClientCertFile == "" || c.config.TLS.ClientKeyFile == "" {
					return errors.New("ws: fatal: incomplete client certificate configuration")
				}
				cert, err := tls.LoadX509KeyPair(c.config.TLS.ClientCertFile, c.config.TLS.ClientKeyFile)
				if err != nil {
					log.Printf("ws: failed to load client cert: %v", err)
					if !waitForRetry() {
						return nil
					}
					continue
				}
				tlsConfig.Certificates = []tls.Certificate{cert}
			}

			dialer.TLSClientConfig = tlsConfig
		}

		connectTimeout := defaultConnectTimeout
		if c.config.ConnectTimeoutSeconds > 0 {
			connectTimeout = time.Duration(c.config.ConnectTimeoutSeconds) * time.Second
		}
		dialCtx, cancelDial := context.WithTimeout(ctx, connectTimeout)

		conn, _, err := dialer.DialContext(dialCtx, c.config.URL, nil)
		cancelDial()
		if err != nil {
			log.Printf("ws: dial failed: %v", err)
			c.onStateChange(contracts.LinkConnecting, contracts.ProtocolUnknown)
			if !waitForRetry() {
				return nil
			}
			continue
		}

		maxFrameSize := int64(defaultMaxFrameSize)
		if c.config.MaxFrameSizeBytes > 0 {
			maxFrameSize = int64(c.config.MaxFrameSizeBytes)
		}

		// Enforce transport hard maximum frame size (default 11MB) to prevent OOM across all reads
		conn.SetReadLimit(maxFrameSize)

		c.mu.Lock()
		c.conn = conn
		c.generation++
		c.mu.Unlock()

		c.onStateChange(contracts.LinkConnected, contracts.ProtocolVerifying)
		log.Printf("ws: connected, performing handshake...")

		sessionCtx, sessionCancel := context.WithCancel(ctx)
		c.mu.Lock()
		c.cancel = sessionCancel
		sessionID := fmt.Sprintf("sess-%d", c.generation)
		c.mu.Unlock()

		hsResult := c.performConnectHandshake(sessionCtx, conn)
		if hsResult == HandshakeRejected {
			log.Printf("ws: fatal: handshake rejected (e.g. invalid empty serial), aborting reconnect loop")
			c.onStateChange(contracts.LinkConnected, contracts.ProtocolRejected)
			c.Close()
			return fmt.Errorf("ws: fatal: handshake rejected")
		} else if hsResult == HandshakeRetryableFailure {
			log.Printf("ws: handshake failed, closing connection and retrying")
			c.Close()
			c.onStateChange(contracts.LinkConnecting, contracts.ProtocolUnknown)
			if !waitForRetry() {
				return nil
			}
			continue
		}

		c.onStateChange(contracts.LinkConnected, contracts.ProtocolTransportVerified)
		log.Printf("ws: session %s active, connected and verified", sessionID)
		sessionStartTime := time.Now()

		g, gCtx := errgroup.WithContext(sessionCtx)

		// Teardown watcher: violently close the physical socket to unblock
		// ReadMessage/WriteMessage the instant any loop crashes or the context is canceled!
		go func() error {
			<-gCtx.Done()
			conn.Close()
			c.mu.Lock()
			if c.conn == conn {
				c.conn = nil
			}
			c.mu.Unlock()
			return nil
		}()

		g.Go(func() error {
			return c.startReaderLoop(gCtx, conn, handler)
		})

		g.Go(func() error {
			return c.startWriterLoop(gCtx, conn)
		})

		err = g.Wait()
		sessionCancel()
		c.onStateChange(contracts.LinkConnecting, contracts.ProtocolUnknown)
		log.Printf("ws: session %s ended: %v", sessionID, err)

		// Only reset backoff if the session was stable
		threshold := time.Duration(c.config.StableSessionThresholdSeconds) * time.Second
		if threshold <= 0 {
			threshold = defaultStableSessionThresholdSeconds
		}
		if time.Since(sessionStartTime) > threshold {
			backoff = initialBackoff
			continue
		}

		// Apply backoff before the next dial to prevent rapid accept/drop churn
		if !waitForRetry() {
			return nil
		}
	}
}

type jsonrpcRequest struct {
	JSONRPC string         `json:"jsonrpc"`
	Method  string         `json:"method"`
	Params  map[string]any `json:"params"`
}

func (c *WSClient) performConnectHandshake(ctx context.Context, conn *gws.Conn) HandshakeResult {
	if conn == nil {
		return HandshakeRetryableFailure
	}

	timeout := defaultConnectTimeout
	if c.config.ConnectTimeoutSeconds > 0 {
		timeout = time.Duration(c.config.ConnectTimeoutSeconds) * time.Second
	}

	handshakeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	params, err := c.metaProvider.ConnectParams(handshakeCtx)
	if err != nil {
		log.Printf("ws: failed to get connect params: %v", err)
		return HandshakeRetryableFailure
	}

	params.Serial = strings.TrimSpace(params.Serial)
	if params.Serial == "" {
		log.Printf("ws: aborting handshake, local serial number is empty or whitespace")
		return HandshakeRejected
	}

	if params.Capabilities == nil {
		params.Capabilities = make(map[string]any)
	}

	paramsMap := map[string]any{
		"serial":       params.Serial,
		"uuid":         params.UUID,
		"firmware":     params.Firmware,
		"capabilities": params.Capabilities,
	}

	req := jsonrpcRequest{
		JSONRPC: "2.0",
		Method:  "connect",
		Params:  paramsMap,
	}

	// Force-close the socket if context cancels during the blocking handshake!
	watchCtx, cancelWatch := context.WithCancel(context.Background())
	defer cancelWatch()
	go func() {
		select {
		case <-watchCtx.Done(): // Handshake finished normally
			return
		case <-ctx.Done(): // Daemon was killed!
			conn.Close()
		}
	}()

	deadline, ok := handshakeCtx.Deadline()
	if !ok {
		return HandshakeRetryableFailure
	}

	if err := conn.SetWriteDeadline(deadline); err != nil {
		log.Printf("ws: failed to set handshake write deadline: %v", err)
		return HandshakeRetryableFailure
	}

	payload, err := json.Marshal(req)
	if err != nil {
		log.Printf("ws: failed to marshal connect request: %v", err)
		return HandshakeRetryableFailure
	}

	if len(payload) >= c.config.CompressionThresholdBytes {
		conn.EnableWriteCompression(true)
	} else {
		conn.EnableWriteCompression(false)
	}

	if err := conn.WriteMessage(gws.TextMessage, payload); err != nil {
		log.Printf("ws: failed to write connect request: %v", err)
		return HandshakeRetryableFailure
	}

	log.Printf("ws: connect handshake frame sent")
	return HandshakeAccepted
}

func (c *WSClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}

	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}

func (c *WSClient) startReaderLoop(ctx context.Context, conn *gws.Conn, handler FrameHandler) error {
	pongTimeout := defaultPongTimeout
	if c.config.PongTimeoutSeconds > 0 {
		pongTimeout = time.Duration(c.config.PongTimeoutSeconds) * time.Second
	}

	pingInterval := defaultPingInterval
	if c.config.PingIntervalSeconds > 0 {
		pingInterval = time.Duration(c.config.PingIntervalSeconds) * time.Second
	}

	// The reader must wait long enough for the writer to send a Ping, PLUS the PongTimeout
	readDeadlineDuration := pingInterval + pongTimeout

	conn.SetReadDeadline(time.Now().Add(readDeadlineDuration))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(readDeadlineDuration))
		return nil
	})
	conn.SetPingHandler(func(appData string) error {

		err := conn.WriteControl(
			gws.PongMessage,
			[]byte(appData),
			time.Now().Add(2*time.Second),
		)
		if err == gws.ErrCloseSent {
			return nil
		}
		return err
	})

	c.mu.Lock()
	sessID := fmt.Sprintf("sess-%d", c.generation)
	c.mu.Unlock()

	consecutiveErrors := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msgType, reader, err := conn.NextReader()
		if err != nil {
			log.Printf("ws: reader loop socket error: %v", err)
			return err
		}

		maxFrameSize := int64(defaultMaxFrameSize)
		if c.config.MaxFrameSizeBytes > 0 {
			maxFrameSize = int64(c.config.MaxFrameSizeBytes)
		}

		limited := io.LimitReader(reader, maxFrameSize+1)
		payload, err := io.ReadAll(limited)
		if err != nil {
			return fmt.Errorf("failed to read decompressed frame: %w", err)
		}
		if int64(len(payload)) > maxFrameSize {
			return fmt.Errorf("decompressed websocket message exceeds limit")
		}

		frame := InboundFrame{
			SessionID: sessID,
			Type:      msgType,
			Payload:   payload,
		}

		disp, err := handler.HandleFrame(ctx, frame)

		if disp == FrameFatalCloseConnection {
			if err != nil {
				log.Printf("ws: fatal disposition accompanied by error (ignoring error): %v", err)
			}
			return errors.New("handler requested fatal socket termination")
		}

		if err != nil {
			consecutiveErrors++
			log.Printf("ws: frame handler error: %v (consecutive: %d)", err, consecutiveErrors)

			maxErrors := 20
			if c.config.MaxConsecutiveFrameErrors > 0 {
				maxErrors = c.config.MaxConsecutiveFrameErrors
			}

			if consecutiveErrors >= maxErrors {
				return fmt.Errorf("ws: fatal: exceeded maximum consecutive frame handler errors")
			}
			continue
		}

		consecutiveErrors = 0

		switch disp {
		case FrameAccepted:
			// Frame was processed successfully.
		case FrameRejectedKeepConnection:
			// Handler rejected the frame, but transport remains active.
		default:
			return fmt.Errorf("invalid frame disposition: %d", disp)
		}
	}
}

func (c *WSClient) startWriterLoop(ctx context.Context, conn *gws.Conn) error {
	pingInterval := defaultPingInterval
	if c.config.PingIntervalSeconds > 0 {
		pingInterval = time.Duration(c.config.PingIntervalSeconds) * time.Second
	}

	writeTimeout := defaultWriteTimeout
	if c.config.WriteTimeoutSeconds > 0 {
		writeTimeout = time.Duration(c.config.WriteTimeoutSeconds) * time.Second
	}

	pingTicker := time.NewTicker(pingInterval)
	defer pingTicker.Stop()

	c.mu.Lock()
	sessID := fmt.Sprintf("sess-%d", c.generation)
	c.mu.Unlock()

	type nextResult struct {
		msg queues.OutboundMessage
		err error
	}
	msgCh := make(chan nextResult)

	// Spawn a background reader for the blocking PriorityQueue
	go func() {
		// First drain any pending messages retained from a previous failed session write
		c.mu.Lock()
		pending := c.pendingMsgs
		c.pendingMsgs = nil
		c.mu.Unlock()

		for i, msg := range pending {
			select {
			case <-ctx.Done():
				// Session dropped before we could even attempt to write it, put it back in pending
				c.mu.Lock()
				// Prepend unhandled messages back to pendingMsgs (they are oldest)
				c.pendingMsgs = append(pending[i:], c.pendingMsgs...)
				c.mu.Unlock()
				return
			case msgCh <- nextResult{msg: msg, err: nil}:
			}
		}

		for {
			msg, err := c.scheduler.Next(ctx)
			if err == nil {
				select {
				case <-ctx.Done():
					if msg.Priority != queues.PriorityHighest {
						// Session is shutting down, retain it for the next session
						c.mu.Lock()
						c.pendingMsgs = append(c.pendingMsgs, msg)
						c.mu.Unlock()
					}
					return
				case msgCh <- nextResult{msg: msg, err: err}:
				}
			} else {
				select {
				case <-ctx.Done():
					return
				case msgCh <- nextResult{msg: msg, err: err}:
				}
				return // Stop the goroutine on terminal queue errors
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-pingTicker.C:
			// Writer loop exclusively sends Ping
			// Set deadline for writing the ping itself based on config
			c.writeMu.Lock()
			conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			err := conn.WriteMessage(gws.PingMessage, nil)
			c.writeMu.Unlock()
			if err != nil {
				return fmt.Errorf("failed to write ping: %v", err)
			}
		case res := <-msgCh:
			if res.err != nil {
				if res.err != context.Canceled && res.err != context.DeadlineExceeded {
					log.Printf("ws: terminal queue drain error: %v", res.err)
				}
				return res.err // Crash the writer loop so the errgroup tears down the session
			}
			msg := res.msg

			// Discard Priority-0 OutboundMessages whose SessionID does not match the active connection
			if msg.Priority == queues.PriorityHighest && msg.SessionID != sessID {
				log.Printf("ws: discarding stale Priority-0 message from session %s", msg.SessionID)
				continue
			}

			c.writeMu.Lock()
			conn.SetWriteDeadline(time.Now().Add(writeTimeout)) // Using configured write deadline

			// Enable permessage-deflate compression if payload exceeds the configured threshold
			if len(msg.Payload) >= c.config.CompressionThresholdBytes {
				conn.EnableWriteCompression(true)
			} else {
				conn.EnableWriteCompression(false)
			}

			err := conn.WriteMessage(gws.TextMessage, []byte(msg.Payload))
			c.writeMu.Unlock()

			if err != nil {
				if msg.Priority != queues.PriorityHighest {
					log.Printf("ws: retaining priority-%d message for retry after write failure", msg.Priority)
					c.mu.Lock()
					c.pendingMsgs = append(c.pendingMsgs, msg)
					c.mu.Unlock()
				}
				return fmt.Errorf("failed to write outbound message: %v", err)
			}
		}
	}
}
