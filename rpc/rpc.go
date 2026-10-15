// Package rpc is an intentionally incomplete RMI assignment template.
package rpc

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"time"
	"unicode"
)

const (
	maxArguments       = 8
	maxArgumentBytes   = 1 << 20
	maxRequestBytes    = 4 << 20
	defaultCallTimeout = 5 * time.Second
)

type request struct {
	Method string            `json:"method"`
	Args   []json.RawMessage `json:"args"`
}

type response struct {
	OK    bool            `json:"ok"`
	Value json.RawMessage `json:"value,omitempty"`
	Error string          `json:"error,omitempty"`
}

// Server exposes exported methods of a receiver over TCP.
type Server struct {
	receiver   any
	listener   net.Listener
	stopped    chan struct{}
	acceptDone chan struct{}
	workers    sync.WaitGroup
	mu         sync.Mutex
}

// NewServer creates a server for a non-nil pointer receiver.
func NewServer(receiver any) (*Server, error) {
	if receiver == nil {
		return nil, errors.New("receiver cannot be nil")
	}
	value := reflect.ValueOf(receiver)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return nil, errors.New("receiver must be a non-nil pointer")
	}
	return &Server{receiver: receiver}, nil
}

// TODO 1: Create the TCP listener, reject duplicate starts, save lifecycle state,
// and launch the accept loop without blocking the caller.
// Start begins accepting remote calls at address. Use port 0 to request an available port.

 // Start begins accepting remote calls at the specified TCP address.
// Use port 0 to ask the operating system to select an available port.
func (s *Server) Start(address string) error {
	// Lock the server state so two goroutines cannot start the
	// same server simultaneously.
	s.mu.Lock()
	defer s.mu.Unlock()

	// A non-nil listener means the server is already running.
	if s.listener != nil {
		return errors.New("server is already running")
	}

	// Create a TCP listener using the requested address.
	// Example: "127.0.0.1:8080"
	// Port 0 allows the operating system to choose an available port.
	listener, err := net.Listen("tcp", address)
	if err != nil {
		// Return the original error if the address cannot be opened.
		return err
	}

	// Save the listener so Address() and Stop() can use it.
	s.listener = listener

	// Create a fresh stop signal for this server run.
	// A new channel is required if the server is restarted later.
	s.stopped = make(chan struct{})

	// This channel is closed when the accept loop finishes.
	s.acceptDone = make(chan struct{})

	// Capture the current run's listener and channels.
	// This prevents a later restart from changing which resources
	// this accept loop is using.
	currentListener := s.listener
	currentStopped := s.stopped
	currentAcceptDone := s.acceptDone

	// Run the accept loop in the background so Start() returns
	// without waiting for any client to connect.
	go func() {
		// Notify Stop() when the accept loop has finished.
		defer close(currentAcceptDone)

		for {
			// Wait for the next client connection.
			connection, err := currentListener.Accept()

			if err != nil {
				// Stop() signals shutdown and closes the listener.
				// In that case, exit the accept loop.
				select {
				case <-currentStopped:
					return
				default:
					return
				}
			}

		
			s.workers.Add(1)

			// Handle each client concurrently.
			go func(conn net.Conn) {
				defer s.workers.Done()
				s.handle(conn)
			}(connection)
		}
	}()

	// The server is listening; return without waiting for clients.
	return nil
}

// TODO 2: Return the listener address safely while allowing port 0 discovery.
// Address returns the address assigned to the running server.

 // Address returns the actual listening address of the running server.
// It returns an empty string when the server is not running.
func (s *Server) Address() string {
	// Lock the mutex because Start() and Stop() modify the listener.
	s.mu.Lock()
	defer s.mu.Unlock()

	// There is no active listener
	if s.listener == nil {
		return ""
	}

	// Addr() returns the actual address, including the port
	// selected by the operating system when port 0 was requested.
	return s.listener.Addr().String()
}


// TODO 3: Close the listener, signal the accept loop, wait for its workers,
// and leave the server in a state where Start can be called again.
// Stop stops accepting new connections and waits for active handlers to finish.

 // Stop stops accepting new connections and waits for active handlers.
// It is safe to call Stop before Start or more than once.
func (s *Server) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If no listener exists, the server is already stopped.
	// This also makes Stop() safe before startup and on repeated calls.
	if s.listener == nil {
		return nil
	}

	// Keep references to the current run's resources.
	// These are the exact channels and listener created by Start().
	currentListener := s.listener
	currentStopped := s.stopped
	currentAcceptDone := s.acceptDone

	// Signal the accept loop that shutdown has started.
	close(currentStopped)
	err := currentListener.Close()

	// Wait until the accept loop has completely exited.
	// After this finishes, it will not register any more workers.
	<-currentAcceptDone
	s.workers.Wait()

	// Clear the listener and channels so Address() reports an
	// empty address and Start() can initialize a fresh server.
	s.listener = nil
	s.stopped = nil
	s.acceptDone = nil

	// Return any listener-closing error to the caller.
	return err
}

// TODO 4: Study this starter request handler, then complete and test the protocol
// contract. Add tests for malformed JSON, oversized input, remote errors, panics,
// and successful calls, and make sure one bad request cannot crash the server or
// leave a connection hanging. Connect this handler to your completed Start and
// Stop lifecycle so each accepted connection is processed by a worker safely.
// handle reads one request, invokes it, and writes one response.

func (s *Server) handle(connection net.Conn) {
	defer connection.Close()

	//  give this whole exchange a time limit. Any read or write that is
	// still blocked when the deadline passes fails with a timeout error.
	_ = connection.SetDeadline(time.Now().Add(defaultCallTimeout))

	encoder := json.NewEncoder(connection)
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = encoder.Encode(response{Error: fmt.Sprintf("remote method panicked: %v", recovered)})
		}
	}()

	//  +4096 bytes of slack for the method name and JSON punctuation, so
	// "too large" requests are rejected by validateRequest with a clear message.

	decoder := json.NewDecoder(bufio.NewReader(io.LimitReader(connection, maxRequestBytes+4096)))

	var req request
	
	if err := decoder.Decode(&req); err != nil {
		_ = encoder.Encode(response{Error: fmt.Sprintf("invalid request: %v", err)})
		return
	}
	if err := validateRequest(req); err != nil {
		_ = encoder.Encode(response{Error: err.Error()})
		return
	}
	result, err := s.invoke(req)
	if err != nil {
		_ = encoder.Encode(response{Error: err.Error()})
		return
	}
	payload, err := json.Marshal(result)
	if err != nil {
		_ = encoder.Encode(response{Error: fmt.Sprintf("encode result: %v", err)})
		return
	}
	_ = encoder.Encode(response{OK: true, Value: payload})
}

// TODO 5: Reject empty or unsafe method names, too many arguments, arguments
// larger than maxArgumentBytes, and requests larger than maxRequestBytes.
// validateRequest rejects malformed or abusive requests before reflection runs.
func validateRequest(req request) error {
	// Rule 1: the method name must not be empty.
	if req.Method == "" {
		return errors.New("method cannot be empty")
	}

	// Rule 2: no whitespace or control characters in the method name.
	// "for _, r := range string" visits one character (rune) at a time.
	for _, r := range req.Method {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return fmt.Errorf("method %q contains whitespace or control characters", req.Method)
		}
	}

	// Rule 3: too many arguments? (exactly maxArguments is still allowed)
	if len(req.Args) > maxArguments {
		return fmt.Errorf("too many arguments: got %d, maximum is %d", len(req.Args), maxArguments)
	}

	// Rules 4 and 5: check each argument's size and keep a running total.
	// Each arg is a json.RawMessage (a []byte), so len() is its size in bytes.
	total := 0
	for index, argument := range req.Args {
		if len(argument) > maxArgumentBytes {
			return fmt.Errorf("argument %d is larger than %d bytes", index, maxArgumentBytes)
		}
		total += len(argument)
		if total > maxRequestBytes {
			return fmt.Errorf("request arguments are larger than %d bytes", maxRequestBytes)
		}
	}

	// All checks passed.
	return nil
}

// TODO 6: Study and test this starter reflection bridge. Your required work is to
// support and test exported methods with zero arguments, typed arguments, no
// return value, one return value, and a final error return; unknown methods,
// wrong argument counts, invalid JSON arguments, returned errors, and panics must
// become clear errors instead of crashing the server. Extend the bridge only when
// your chosen assignment protocol requires another result or argument rule.
// invoke finds a method, converts JSON arguments to its parameter types, and calls it.
func (s *Server) invoke(req request) (any, error) {
	method := reflect.ValueOf(s.receiver).MethodByName(req.Method)
	if !method.IsValid() {
		return nil, fmt.Errorf("unknown method %q", req.Method)
	}
	typeOfMethod := method.Type()
	if typeOfMethod.NumIn() != len(req.Args) {
		return nil, fmt.Errorf("method %q expects %d arguments, got %d", req.Method, typeOfMethod.NumIn(), len(req.Args))
	}
	arguments := make([]reflect.Value, len(req.Args))
	for index, raw := range req.Args {
		argument := reflect.New(typeOfMethod.In(index))
		if err := json.Unmarshal(raw, argument.Interface()); err != nil {
			return nil, fmt.Errorf("argument %d: %v", index, err)
		}
		arguments[index] = argument.Elem()
	}
	outputs, err := callMethodSafely(method, arguments)
	if err != nil {
		return nil, err
	}
	if len(outputs) == 0 {
		return nil, nil
	}
	if last := outputs[len(outputs)-1]; last.Type().Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		if !isNilValue(last) {
			return nil, last.Interface().(error)
		}
		outputs = outputs[:len(outputs)-1]
	}
	if len(outputs) == 0 {
		return nil, nil
	}
	return outputs[0].Interface(), nil
}

func isNilValue(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func callMethodSafely(method reflect.Value, arguments []reflect.Value) (outputs []reflect.Value, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("remote method panicked: %v", recovered)
		}
	}()
	return method.Call(arguments), nil
}

// Client calls methods on a remote Server.
type Client struct {
	Address string
	Timeout time.Duration
}

// NewClient creates a client for a server address.
func NewClient(address string) (*Client, error) {
	if address == "" {
		return nil, errors.New("address cannot be empty")
	}
	return &Client{Address: address, Timeout: defaultCallTimeout}, nil
}

// TODO 7: Open one connection, apply a deadline, encode the request, read one
// response, return remote errors, and decode the response into out.
// Call invokes method and decodes its result into out. Pass nil for no result.

func (c *Client) Call(method string, out any, args ...any) error {
	// Reject an empty method name before opening any connection.
	if method == "" {
		return errors.New("method cannot be empty")
	}

	// Use the default timeout if none is configured.
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = defaultCallTimeout
	}

	// 1. Connect to the server.
	conn, err := net.DialTimeout("tcp", c.Address, timeout)
	if err != nil {
		return fmt.Errorf("connect to server: %w", err)
	}
	defer conn.Close()

	// 2. Set a deadline for the whole request and response.
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return fmt.Errorf("set connection deadline: %w", err)
	}

	// 3. Convert the arguments to JSON.
	jsonArgs := make([]json.RawMessage, len(args))
	for i, arg := range args {
		data, err := json.Marshal(arg)
		if err != nil {
			return fmt.Errorf("encode argument %d: %w", i, err)
		}
		jsonArgs[i] = data
	}

	// 4. Send the request.
	req := request{
		Method: method,
		Args:   jsonArgs,
	}
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return fmt.Errorf("send request: %w", err)
	}

	// 5. Read the server's response.
	var resp response
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	// 6. Return any error reported by the server.
	if !resp.OK {
		if resp.Error != "" {
			return errors.New(resp.Error)
		}
		return errors.New("remote call failed")
	}

	// 7. If no result is needed, we're done.
	if out == nil {
		return nil
	}

	// 8. Decode the result into the caller's variable.
	if len(resp.Value) == 0 {
		return errors.New("response contains no result")
	}
	if err := json.Unmarshal(resp.Value, out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	return nil
}
