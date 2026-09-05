package service

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// openAIWSChatBridgeWriter turns the Responses SSE emitted by Forward into an
// io.Reader. The Chat Completions compatibility converter consumes that reader
// concurrently, preserving streaming and time-to-first-token behavior.
type openAIWSChatBridgeWriter struct {
	base   gin.ResponseWriter
	pipe   *io.PipeWriter
	header http.Header

	mu     sync.Mutex
	status int
	size   int

	ready     chan struct{}
	readyOnce sync.Once
	onReady   func()
}

func newOpenAIWSChatBridgeWriter(base gin.ResponseWriter, pipe *io.PipeWriter, onReady func()) *openAIWSChatBridgeWriter {
	return &openAIWSChatBridgeWriter{
		base:    base,
		pipe:    pipe,
		header:  make(http.Header),
		status:  http.StatusOK,
		size:    -1,
		ready:   make(chan struct{}),
		onReady: onReady,
	}
}

func (w *openAIWSChatBridgeWriter) signalReady() {
	w.readyOnce.Do(func() {
		if w.onReady != nil {
			w.onReady()
		}
		close(w.ready)
	})
}

func (w *openAIWSChatBridgeWriter) Header() http.Header { return w.header }

func (w *openAIWSChatBridgeWriter) WriteHeader(code int) {
	w.mu.Lock()
	if w.size < 0 {
		w.status = code
		w.size = 0
	}
	w.mu.Unlock()
}

func (w *openAIWSChatBridgeWriter) WriteHeaderNow() {
	w.WriteHeader(http.StatusOK)
	w.signalReady()
}

func (w *openAIWSChatBridgeWriter) Write(payload []byte) (int, error) {
	w.WriteHeaderNow()
	n, err := w.pipe.Write(payload)
	w.mu.Lock()
	w.size += n
	w.mu.Unlock()
	return n, err
}

func (w *openAIWSChatBridgeWriter) WriteString(value string) (int, error) {
	return w.Write([]byte(value))
}

func (w *openAIWSChatBridgeWriter) Status() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.status
}

func (w *openAIWSChatBridgeWriter) Size() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

func (w *openAIWSChatBridgeWriter) Written() bool { return w.Size() >= 0 }

func (w *openAIWSChatBridgeWriter) Flush() { w.WriteHeaderNow() }

func (w *openAIWSChatBridgeWriter) Pusher() http.Pusher {
	if w.base == nil {
		return nil
	}
	return w.base.Pusher()
}

func (w *openAIWSChatBridgeWriter) CloseNotify() <-chan bool {
	if w.base == nil {
		ch := make(chan bool)
		return ch
	}
	return w.base.CloseNotify()
}

func (w *openAIWSChatBridgeWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if w.base == nil {
		return nil, nil, errors.New("websocket chat bridge writer cannot hijack")
	}
	return w.base.Hijack()
}

var _ gin.ResponseWriter = (*openAIWSChatBridgeWriter)(nil)

type openAIWSChatBridgeOutcome struct {
	result *OpenAIForwardResult
	err    error
}

func copyOpenAIWSBridgeContext(dst, src *gin.Context) {
	if dst == nil || src == nil {
		return
	}
	snapshot := src.Copy()
	for key, value := range snapshot.Keys {
		dst.Set(key, value)
	}
}

func mergeOpenAIWSChatResult(converted, upstream *OpenAIForwardResult) *OpenAIForwardResult {
	if converted == nil {
		return upstream
	}
	converted.OpenAIWSMode = true
	if upstream == nil {
		return converted
	}
	converted.RequestID = upstream.RequestID
	converted.ResponseID = upstream.ResponseID
	converted.UpstreamHeaders = upstream.UpstreamHeaders
	converted.ResponseHeaders = upstream.ResponseHeaders
	converted.UpstreamEndpoint = upstream.UpstreamEndpoint
	converted.UpstreamTerminalEvent = upstream.UpstreamTerminalEvent
	converted.UpstreamResponseModel = upstream.UpstreamResponseModel
	converted.UpstreamResponseModelConflict = upstream.UpstreamResponseModelConflict
	converted.UpstreamResponseServiceTier = upstream.UpstreamResponseServiceTier
	converted.ServiceTier = upstream.ServiceTier
	converted.ReasoningEffort = upstream.ReasoningEffort
	converted.Usage = upstream.Usage
	if converted.FirstTokenMs == nil {
		converted.FirstTokenMs = upstream.FirstTokenMs
	}
	converted.ClientDisconnect = converted.ClientDisconnect || upstream.ClientDisconnect
	return converted
}

func (s *OpenAIGatewayService) forwardChatCompletionsViaOpenAIWS(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	responsesBody []byte,
	clientStream bool,
	originalModel string,
	billingModel string,
	upstreamModel string,
	startTime time.Time,
	requestBodyLen int,
) (*OpenAIForwardResult, error) {
	reader, writer := io.Pipe()
	defer func() { _ = reader.Close() }()

	bridgeCtx := c.Copy()
	SetOpenAIClientTransport(bridgeCtx, OpenAIClientTransportHTTP)
	bridgeWriter := newOpenAIWSChatBridgeWriter(c.Writer, writer, func() {
		copyOpenAIWSBridgeContext(c, bridgeCtx)
	})
	bridgeCtx.Writer = bridgeWriter

	outcomeCh := make(chan openAIWSChatBridgeOutcome, 1)
	go func() {
		result, err := s.Forward(ctx, bridgeCtx, account, responsesBody)
		copyOpenAIWSBridgeContext(c, bridgeCtx)
		if err != nil {
			_ = writer.CloseWithError(err)
		} else {
			_ = writer.Close()
		}
		outcomeCh <- openAIWSChatBridgeOutcome{result: result, err: err}
	}()

	var earlyOutcome *openAIWSChatBridgeOutcome
	select {
	case <-bridgeWriter.ready:
	case outcome := <-outcomeCh:
		earlyOutcome = &outcome
	}
	if earlyOutcome != nil {
		return mergeOpenAIWSChatResult(nil, earlyOutcome.result), earlyOutcome.err
	}

	SetActualOpenAIUpstreamEndpoint(c, "/v1/responses")
	response := &http.Response{
		StatusCode: bridgeWriter.Status(),
		Header:     bridgeWriter.Header().Clone(),
		Body:       reader,
	}

	var converted *OpenAIForwardResult
	var convertErr error
	if clientStream {
		converted, convertErr = s.handleChatStreamingResponse(
			response, c, account, originalModel, billingModel, upstreamModel, startTime, requestBodyLen,
		)
	} else {
		converted, convertErr = s.handleChatBufferedStreamingResponse(
			response, c, account, originalModel, billingModel, upstreamModel, startTime,
		)
	}
	if convertErr != nil {
		_ = reader.CloseWithError(convertErr)
	}
	outcome := <-outcomeCh
	converted = mergeOpenAIWSChatResult(converted, outcome.result)
	if outcome.err != nil {
		return converted, outcome.err
	}
	return converted, convertErr
}
