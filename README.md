# Net-Centric
Net-Centric Computing course labs and assignments


# Lab 1: Building a Reflection-Based RPC System

**Course:** Net-Centric Computing  
**Faculty:** Faculty of Engineering, Alexandria University  
**Semester:** Fall 2026  
**Language:** Go

## Overview

This lab focuses on building a small Remote Procedure Call (RPC) system in Go. RPC allows a client to request the execution of a method on a remote server as if it were a local method call.

The client and server communicate over a TCP connection, exchanging requests and responses encoded in JSON. The server uses Go reflection to discover and invoke exported methods at runtime.

## Objectives

- Build a TCP client-server application using Go.
- Encode and decode requests and responses using JSON.
- Handle multiple client connections concurrently.
- Implement safe server startup, shutdown, and restart.
- Validate incoming requests and handle errors.
- Convert JSON arguments into Go types.
- Use reflection to invoke methods dynamically.
- Implement client-side RPC calls with timeouts.
- Test network communication and error handling.

## Main Tasks

1. **Start the RPC Server** — Create a TCP listener and accept connections in the background.
2. **Return the Server Address** — Retrieve the active listener address, including an automatically assigned port.
3. **Stop the Server** — Shut down the server safely and wait for active handlers to finish.
4. **Validate Requests** — Reject invalid method names and requests that exceed protocol limits.
5. **Handle RPC Calls** — Process JSON requests and return successful or error responses.
6. **Invoke Methods** — Use reflection to find methods, decode arguments, and handle return values and errors.
7. **Implement the RPC Client** — Send requests, receive responses, apply timeouts, and decode results.

## RPC Protocol

### Example Request

```json
{
  "method": "Add",
  "args": [2, 3]
}
```

### Successful Response

```json
{
  "ok": true,
  "value": 5
}
```

### Error Response

```json
{
  "ok": false,
  "error": "unknown method \"Missing\""
}
```

## Supported Methods

The RPC system supports methods with different argument and return-value patterns, including:

- `NoArgs()` — No arguments.
- `Add(left, right int)` — Typed arguments.
- `Echo(value string)` — String arguments and return values.
- `OnlyError(shouldFail bool)` — Error-only return.
- `Ping(id int)` — A value followed by an error.

## Testing

Run the tests from the `lab1` directory:

```bash
cd lab1
go test ./...
```

The tests cover server lifecycle management, successful RPC calls, invalid requests, reflection, error handling, malformed JSON, oversized input, concurrent calls, and client timeouts.

## Technologies Used

- **Go (Golang)**
- TCP networking
- JSON encoding and decoding
- Reflection
- Goroutines and concurrency
- Automated testing
