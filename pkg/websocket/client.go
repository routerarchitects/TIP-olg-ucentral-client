package websocket

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
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

type CloudConnectResult struct {
	Error int    `json:"error"`
	Text  string `json:"text,omitempty"`
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

type FrameHandler interface {
	HandleFrame(ctx context.Context, frame InboundFrame) (FrameDisposition, error)
}

type HandshakeResult int

const (
	HandshakeAccepted HandshakeResult = iota
	HandshakeRejectedKeepOpen
	HandshakeRetryableFailure
	HandshakeFatalClose
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

func NewWSClient(cfg config.CloudConfig, scheduler queues.OutboundScheduler, metaProvider ConnectMetadataProvider, onStateChange func(contracts.LinkState, contracts.ProtocolState)) *WSClient {
	return &WSClient{
		config:        cfg,
		scheduler:     scheduler,
		metaProvider:  metaProvider,
		onStateChange: onStateChange,
	}
}

func (c *WSClient) ReconnectLoop(ctx context.Context, handler FrameHandler) {
	backoff := 2 * time.Second
	maxBackoff := 60 * time.Second

	for {
		select {
		case <-ctx.Done():
			log.Printf("ws: reconnect loop stopped by context")
			return
		default:
		}

		c.onStateChange(contracts.LinkConnecting, contracts.ProtocolUnknown)
		log.Printf("ws: dialing %s", c.config.URL)

		// Create dialer
		dialer := gws.DefaultDialer
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
					select {
					case <-ctx.Done():
						return
					case <-time.After(backoff):
					}
					continue
				}
				caCertPool := x509.NewCertPool()
				if !caCertPool.AppendCertsFromPEM(caCert) {
					log.Printf("ws: failed to parse any valid certificates from CA file")
					select {
					case <-ctx.Done():
						return
					case <-time.After(backoff):
					}
					continue
				}
				tlsConfig.RootCAs = caCertPool
			}

			if hasCert {
				if c.config.TLS.ClientCertFile == "" || c.config.TLS.ClientKeyFile == "" {
					log.Printf("ws: incomplete client certificate configuration")
					select {
					case <-ctx.Done():
						return
					case <-time.After(backoff):
					}
					continue
				}
				cert, err := tls.LoadX509KeyPair(c.config.TLS.ClientCertFile, c.config.TLS.ClientKeyFile)
				if err != nil {
					log.Printf("ws: failed to load client cert: %v", err)
					select {
					case <-ctx.Done():
						return
					case <-time.After(backoff):
					}
					continue
				}
				tlsConfig.Certificates = []tls.Certificate{cert}
			}

			dialer = &gws.Dialer{
				TLSClientConfig: tlsConfig,
			}
		}

		conn, _, err := dialer.DialContext(ctx, c.config.URL, nil)
		if err != nil {
			log.Printf("ws: dial failed: %v", err)
			c.onStateChange(contracts.LinkConnecting, contracts.ProtocolUnknown)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		// Enforce transport hard maximum frame size (11MB) to prevent OOM across all reads
		conn.SetReadLimit(11 * 1024 * 1024)

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

		hsResult := c.performConnectHandshake(sessionCtx, conn, sessionID)
		if hsResult != HandshakeAccepted {
			log.Printf("ws: handshake failed, closing connection")
			c.Close()
			sessionCancel()
			c.onStateChange(contracts.LinkConnecting, contracts.ProtocolUnknown)

			if hsResult == HandshakeFatalClose {
				// Fatal failure, perhaps stop entirely or use max backoff
				log.Printf("ws: fatal handshake failure, applying max backoff")
				backoff = maxBackoff
			} else if hsResult == HandshakeRejectedKeepOpen {
				// Keep connection open theoretically, but spec says reconnect loop manages retries
				backoff = maxBackoff
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		c.onStateChange(contracts.LinkConnected, contracts.ProtocolAccepted)
		log.Printf("ws: session %s active", sessionID)
		sessionStartTime := time.Now()

		g, gCtx := errgroup.WithContext(sessionCtx)

		// Teardown watcher: violently close the physical socket to unblock
		// ReadMessage/WriteMessage the instant any loop crashes or the context is canceled!
		g.Go(func() error {
			<-gCtx.Done()
			c.Close()
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
		log.Printf("ws: session %s ended: %v", sessionID, err)

		// Only reset backoff if the session was stable (connected for > 60s)
		if time.Since(sessionStartTime) > 60*time.Second {
			backoff = 2 * time.Second
		}

		// Apply backoff before the next dial to prevent rapid accept/drop churn
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

type jsonrpcRequest struct {
	JSONRPC string         `json:"jsonrpc"`
	Method  string         `json:"method"`
	ID      uint64         `json:"id"`
	Params  map[string]any `json:"params"`
}

type jsonrpcResponse struct {
	JSONRPC string              `json:"jsonrpc"`
	ID      uint64              `json:"id"`
	Result  *CloudConnectResult `json:"result,omitempty"`
	Error   *jsonrpcError       `json:"error,omitempty"`
	Method  string              `json:"method,omitempty"`
}

type jsonrpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (c *WSClient) performConnectHandshake(ctx context.Context, conn *gws.Conn, sessionID string) HandshakeResult {
	if conn == nil {
		return HandshakeRetryableFailure
	}

	params, err := c.metaProvider.ConnectParams(ctx)
	if err != nil {
		log.Printf("ws: failed to get connect params: %v", err)
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
		ID:      1,
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

	conn.SetWriteDeadline(handshakeDeadline)
	if err := conn.WriteJSON(req); err != nil {
		log.Printf("ws: failed to write connect request: %v", err)
		return HandshakeRetryableFailure
	}

	conn.SetReadDeadline(handshakeDeadline)
	for {
		var resp jsonrpcResponse
		if err := conn.ReadJSON(&resp); err != nil {
			log.Printf("ws: failed to read connect response: %v", err)
			return HandshakeRetryableFailure
		}

		if resp.Method != "" {
			if resp.Method == "ping" {
				// Reply to ping
				pingReply := map[string]any{
					"jsonrpc": "2.0",
					"id":      resp.ID,
					"result":  map[string]any{"serial": params.Serial, "uuid": params.UUID},
				}
				conn.SetWriteDeadline(handshakeDeadline)
				if err := conn.WriteJSON(pingReply); err != nil {
					log.Printf("ws: failed to write ping reply during handshake: %v", err)
					return HandshakeRetryableFailure
				}
			} else {
				// Reject non-ping
				rejectReply := map[string]any{
					"jsonrpc": "2.0",
					"id":      resp.ID,
					"error": map[string]any{
						"code":    -32603,
						"message": "Protocol Verifying",
						"data":    map[string]any{"application_code": 3},
					},
				}
				conn.SetWriteDeadline(handshakeDeadline)
				if err := conn.WriteJSON(rejectReply); err != nil {
					log.Printf("ws: failed to write reject reply during handshake: %v", err)
					return HandshakeRetryableFailure
				}
			}
			continue
		}

		if resp.ID == 1 {
			if resp.Error != nil && resp.Error.Code == -32600 {
				log.Printf("ws: fatal rejection from cloud: %v", resp.Error)
				return HandshakeFatalClose
			}
			if resp.Error != nil {
				log.Printf("ws: rejected by cloud (jsonrpc error): %v", resp.Error)
				return HandshakeRejectedKeepOpen
			}
			if resp.Result != nil && resp.Result.Error != 0 {
				log.Printf("ws: rejected by cloud (result error %d): %s", resp.Result.Error, resp.Result.Text)
				return HandshakeRejectedKeepOpen
			}
			if resp.Result == nil && resp.Error == nil {
				log.Printf("ws: malformed empty response from cloud")
				return HandshakeRetryableFailure
			}

			log.Printf("ws: connect handshake accepted by cloud")
			return HandshakeAccepted
		}
	}
}

func (c *WSClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
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

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msgType, payload, err := conn.ReadMessage()
		if err != nil {
			log.Printf("ws: reader loop socket error: %v", err)
			return err
		}

		// Update deadline aggressively on any successful read (pong or payload)
		conn.SetReadDeadline(time.Now().Add(pongTimeout))

		frame := InboundFrame{
			SessionID: sessID,
			Type:      msgType,
			Payload:   payload,
		}

		disp, err := handler.HandleFrame(ctx, frame)
		if disp == FrameFatalCloseConnection || err != nil {
			return fmt.Errorf("fatal frame disposition: %v", err)
		}
	}
}

func (c *WSClient) startWriterLoop(ctx context.Context, conn *gws.Conn) error {
	pingInterval := 30 * time.Second
	if c.config.PingIntervalSeconds > 0 {
		pingInterval = time.Duration(c.config.PingIntervalSeconds) * time.Second
	}

	writeTimeout := 60 * time.Second
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
			select {
			case <-ctx.Done():
				return
			case msgCh <- nextResult{msg: msg, err: err}:
			}
			if err != nil {
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
			err := conn.WriteMessage(gws.TextMessage, msg.Payload)

			if err != nil {
				return fmt.Errorf("failed to write outbound message: %v", err)
			}
		}
	}
}
