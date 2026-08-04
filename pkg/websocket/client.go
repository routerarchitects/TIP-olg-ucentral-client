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
		if c.config.TLS.CAFile != "" {
			caCert, err := os.ReadFile(c.config.TLS.CAFile)
			if err != nil {
				log.Printf("ws: failed to read CA file: %v", err)
				time.Sleep(backoff)
				continue
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)

			cert, err := tls.LoadX509KeyPair(c.config.TLS.ClientCertFile, c.config.TLS.ClientKeyFile)
			if err != nil {
				log.Printf("ws: failed to load client cert: %v", err)
				time.Sleep(backoff)
				continue
			}

			tlsConfig := &tls.Config{
				RootCAs:      caCertPool,
				Certificates: []tls.Certificate{cert},
				ServerName:   c.config.TLS.ServerName,
			}
			dialer = &gws.Dialer{
				TLSClientConfig: tlsConfig,
			}
		}

		conn, _, err := dialer.DialContext(ctx, c.config.URL, nil)
		if err != nil {
			log.Printf("ws: dial failed: %v", err)
			c.onStateChange(contracts.LinkConnecting, contracts.ProtocolUnknown)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

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

		hsResult := c.performConnectHandshake(sessionCtx, sessionID)
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

			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		c.onStateChange(contracts.LinkConnected, contracts.ProtocolAccepted)
		log.Printf("ws: session %s active", sessionID)
		backoff = 2 * time.Second // Reset backoff on success

		g, gCtx := errgroup.WithContext(sessionCtx)

		g.Go(func() error {
			return c.startReaderLoop(gCtx, handler)
		})

		g.Go(func() error {
			return c.startWriterLoop(gCtx)
		})

		err = g.Wait()
		log.Printf("ws: session %s ended: %v", sessionID, err)
		c.Close()
		sessionCancel()
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

func (c *WSClient) performConnectHandshake(ctx context.Context, sessionID string) HandshakeResult {
	if c.conn == nil {
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

	c.conn.SetWriteDeadline(time.Now().Add(timeout))
	if err := c.conn.WriteJSON(req); err != nil {
		log.Printf("ws: failed to write connect request: %v", err)
		return HandshakeRetryableFailure
	}

	c.conn.SetReadDeadline(time.Now().Add(timeout))
	for {
		var resp jsonrpcResponse
		if err := c.conn.ReadJSON(&resp); err != nil {
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
				c.conn.SetWriteDeadline(time.Now().Add(timeout))
				_ = c.conn.WriteJSON(pingReply)
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
				c.conn.SetWriteDeadline(time.Now().Add(timeout))
				_ = c.conn.WriteJSON(rejectReply)
			}
			c.conn.SetReadDeadline(time.Now().Add(timeout))
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

func (c *WSClient) startReaderLoop(ctx context.Context, handler FrameHandler) error {
	// 1. Enforce transport hard maximum frame size (11MB) to prevent OOM
	c.conn.SetReadLimit(11 * 1024 * 1024)

	pongTimeout := 60 * time.Second
	if c.config.PongTimeoutSeconds > 0 {
		pongTimeout = time.Duration(c.config.PongTimeoutSeconds) * time.Second
	}

	c.conn.SetReadDeadline(time.Now().Add(pongTimeout))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongTimeout))
		return nil
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

		msgType, payload, err := c.conn.ReadMessage()
		if err != nil {
			log.Printf("ws: reader loop socket error: %v", err)
			return err
		}

		// Update deadline aggressively on any successful read (pong or payload)
		c.conn.SetReadDeadline(time.Now().Add(pongTimeout))

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

func (c *WSClient) startWriterLoop(ctx context.Context) error {
	pingInterval := 30 * time.Second
	if c.config.PingIntervalSeconds > 0 {
		pingInterval = time.Duration(c.config.PingIntervalSeconds) * time.Second
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
			c.mu.Lock()
			// Set a short deadline for writing the ping itself
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			err := c.conn.WriteMessage(gws.PingMessage, nil)
			c.mu.Unlock()
			if err != nil {
				return fmt.Errorf("failed to write ping: %v", err)
			}
		case res := <-msgCh:
			if res.err != nil {
				if res.err == context.Canceled || res.err == context.DeadlineExceeded {
					return ctx.Err()
				}
				log.Printf("ws: queue drain error: %v", res.err)
				continue
			}
			msg := res.msg

			// Discard Priority-0 OutboundMessages whose SessionID does not match the active connection
			if msg.Priority == queues.PriorityHighest && msg.SessionID != sessID {
				log.Printf("ws: discarding stale Priority-0 message from session %s", msg.SessionID)
				continue
			}

			c.mu.Lock()
			c.conn.SetWriteDeadline(time.Now().Add(60 * time.Second)) // Using a generous write deadline
			err := c.conn.WriteMessage(gws.TextMessage, msg.Payload)
			c.mu.Unlock()

			if err != nil {
				return fmt.Errorf("failed to write outbound message: %v", err)
			}
		}
	}
}
