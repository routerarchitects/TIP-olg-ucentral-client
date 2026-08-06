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
	"os"
	"strings"
	"sync"
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
// SECURITY CONTRACT: The FrameHandler is only invoked after the transport layer
// has completed the WebSocket handshake verification (ProtocolTransportVerified). It does not need to
// track ProtocolVerifying, as all pre-acceptance frames are owned and explicitly
// discarded by the transport's handshake routine. Pre-acceptance commands are
// never buffered or replayed.
type FrameHandler interface {
	HandleFrame(ctx context.Context, frame InboundFrame) (FrameDisposition, error)
}

type HandshakeResult int

const (
	HandshakeAccepted HandshakeResult = iota
	HandshakeRetryableFailure
)

type WSClient struct {
	mu            sync.Mutex
	conn          *gws.Conn
	generation    uint64
	cancel        context.CancelFunc
	config        config.CloudConfig
	scheduler     queues.OutboundScheduler
	metaProvider  ConnectMetadataProvider
	onStateChange func(cloud contracts.LinkState, protocol contracts.ProtocolState)
}

func NewWSClient(cfg config.CloudConfig, scheduler queues.OutboundScheduler, metaProvider ConnectMetadataProvider, onStateChange func(contracts.LinkState, contracts.ProtocolState)) (*WSClient, error) {
	if scheduler == nil {
		return nil, errors.New("scheduler cannot be nil")
	}
	if metaProvider == nil {
		return nil, errors.New("metadata provider cannot be nil")
	}
	if onStateChange == nil {
		onStateChange = func(contracts.LinkState, contracts.ProtocolState) {}
	}
	return &WSClient{
		config:        cfg,
		scheduler:     scheduler,
		metaProvider:  metaProvider,
		onStateChange: onStateChange,
	}, nil
}

func (c *WSClient) ReconnectLoop(ctx context.Context, handler FrameHandler) error {
	if handler == nil {
		return errors.New("ws: frame handler cannot be nil")
	}

	defer c.onStateChange(contracts.LinkConnecting, contracts.ProtocolUnknown)

	backoff := 2 * time.Second
	maxBackoff := 60 * time.Second

	waitForRetry := func() bool {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(backoff):
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

		if c.config.CompressionThresholdBytes > 0 {
			dialer.EnableCompression = true
		}

		hasCA := c.config.TLS.CAFile != ""
		hasCert := c.config.TLS.ClientCertFile != "" || c.config.TLS.ClientKeyFile != ""

		if hasCA || hasCert || c.config.TLS.ServerName != "" {
			tlsConfig := &tls.Config{
				ServerName: c.config.TLS.ServerName,
			}

			if hasCA {
				caCert, err := os.ReadFile(c.config.TLS.CAFile)
				if err != nil {
					return fmt.Errorf("ws: fatal: failed to read CA file: %w", err)
				}
				caCertPool := x509.NewCertPool()
				if !caCertPool.AppendCertsFromPEM(caCert) {
					return errors.New("ws: fatal: failed to parse any valid certificates from CA file")
				}
				tlsConfig.RootCAs = caCertPool
			}

			if hasCert {
				if c.config.TLS.ClientCertFile == "" || c.config.TLS.ClientKeyFile == "" {
					return errors.New("ws: fatal: incomplete client certificate configuration")
				}
				cert, err := tls.LoadX509KeyPair(c.config.TLS.ClientCertFile, c.config.TLS.ClientKeyFile)
				if err != nil {
					return fmt.Errorf("ws: fatal: failed to load client cert: %w", err)
				}
				tlsConfig.Certificates = []tls.Certificate{cert}
			}

			dialer.TLSClientConfig = tlsConfig
		}

		connectTimeout := 10 * time.Second
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

		maxFrameSize := int64(11 * 1024 * 1024)
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
		if hsResult == HandshakeRetryableFailure {
			log.Printf("ws: handshake failed, closing connection and retrying")
			c.Close()
			sessionCancel()
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
		g.Go(func() error {
			<-gCtx.Done()
			conn.Close()
			c.mu.Lock()
			if c.conn == conn {
				c.conn = nil
			}
			c.mu.Unlock()
			return nil
		})

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
			threshold = 60 * time.Second
		}
		if time.Since(sessionStartTime) > threshold {
			backoff = 2 * time.Second
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

	params, err := c.metaProvider.ConnectParams(ctx)
	if err != nil {
		log.Printf("ws: failed to get connect params: %v", err)
		return HandshakeRetryableFailure
	}

	params.Serial = strings.TrimSpace(params.Serial)
	if params.Serial == "" {
		log.Printf("ws: aborting handshake, local serial number is empty or whitespace")
		return HandshakeRetryableFailure
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

	timeout := 10 * time.Second
	if c.config.ConnectTimeoutSeconds > 0 {
		timeout = time.Duration(c.config.ConnectTimeoutSeconds) * time.Second
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

	handshakeDeadline := time.Now().Add(timeout)

	payload, err := json.Marshal(req)
	if err != nil {
		log.Printf("ws: failed to marshal connect request: %v", err)
		return HandshakeRetryableFailure
	}

	if c.config.CompressionThresholdBytes > 0 && len(payload) >= c.config.CompressionThresholdBytes {
		conn.EnableWriteCompression(true)
	} else {
		conn.EnableWriteCompression(false)
	}

	conn.SetWriteDeadline(handshakeDeadline)
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
	pongTimeout := 60 * time.Second
	if c.config.PongTimeoutSeconds > 0 {
		pongTimeout = time.Duration(c.config.PongTimeoutSeconds) * time.Second
	}

	conn.SetReadDeadline(time.Now().Add(pongTimeout))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongTimeout))
		return nil
	})
	conn.SetPingHandler(func(appData string) error {
		conn.SetReadDeadline(time.Now().Add(pongTimeout))
		err := conn.WriteControl(gws.PongMessage, []byte(appData), time.Now().Add(10*time.Second))
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

		maxFrameSize := int64(11 * 1024 * 1024)
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
		} else {
			consecutiveErrors = 0
		}

		switch disp {
		case FrameAccepted:
			// Frame was processed successfully.
		case FrameRejectedKeepConnection:
			// Handler rejected the frame, but transport remains active.
		case FrameFatalCloseConnection:
			return errors.New("handler requested fatal socket termination")
		default:
			return fmt.Errorf("invalid frame disposition: %d", disp)
		}
	}
}

func (c *WSClient) startWriterLoop(ctx context.Context, conn *gws.Conn) error {
	pingInterval := 30 * time.Second
	if c.config.PingIntervalSeconds > 0 {
		pingInterval = time.Duration(c.config.PingIntervalSeconds) * time.Second
	}

	writeTimeout := 10 * time.Second
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
		for {
			msg, err := c.scheduler.Next(ctx)
			if err == nil {
				select {
				case <-ctx.Done():
					if msg.Priority != queues.PriorityHighest {
						if err := c.scheduler.Push(msg); err != nil {
							log.Printf("ws: fatal: failed to requeue priority-%d message (queue full), dropping payload: %v", msg.Priority, err)
						}
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
			conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			err := conn.WriteMessage(gws.PingMessage, nil)
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

			conn.SetWriteDeadline(time.Now().Add(writeTimeout)) // Using configured write deadline

			// Enable permessage-deflate compression if payload exceeds the configured threshold
			if c.config.CompressionThresholdBytes > 0 && len(msg.Payload) >= c.config.CompressionThresholdBytes {
				conn.EnableWriteCompression(true)
			} else {
				conn.EnableWriteCompression(false)
			}

			err := conn.WriteMessage(gws.TextMessage, []byte(msg.Payload))

			if err != nil {
				if msg.Priority != queues.PriorityHighest {
					log.Printf("ws: requeuing priority-%d message after write failure", msg.Priority)
					if pushErr := c.scheduler.Push(msg); pushErr != nil {
						log.Printf("ws: fatal: failed to requeue priority-%d message (queue full), dropping payload: %v", msg.Priority, pushErr)
					}
				}
				return fmt.Errorf("failed to write outbound message: %v", err)
			}
		}
	}
}
