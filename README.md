# gold-go

gold-go is a Go-first backend framework that generates a type-safe TypeScript client and serves your frontend through the backend in both development and production.

Key features:
- **Zero manual entrypoint**: APIs auto-register via `init()` functions, no central configuration
- **Type-safe clients**: TypeScript client auto-generated at startup with full type information
- **Request operations**: `POST /gold_request` for immediate request/response
- **Stream operations**: `POST /gold_sse` for Server-Sent Events (SSE) subscriptions
- **File uploads**: Native multipart form parsing with temp-file helpers
- **Dev/Prod modes**: Seamless Vite HMR in dev, static SPA serving in production

## Installation

```sh
go get github.com/dotmonk/gold-go@<version>
```

## Quick Start

### 1. Create backend entrypoint — `backend/main.go`

```go
package main

import (
    "log"
    "os"
    "strconv"

    gold "github.com/dotmonk/gold-go"
    _ "your-module/backend/api"
)

func main() {
    opts := gold.DefaultOptions()
    opts.Dev = os.Getenv("ENV") != "production"
    opts.WorkDir = "."
    opts.GeneratedClientPath = "frontend/client.ts"
    opts.FrontendDir = "static"
    opts.ViteConfig = "vite.config.ts"

    if portStr := os.Getenv("PORT"); portStr != "" {
        if p, err := strconv.Atoi(portStr); err == nil {
            opts.Port = p
        }
    }

    app := gold.New(opts)

    if err := app.ListenAndServe(); err != nil {
        log.Fatal(err)
    }
}
```

### 2. Define API operations — `backend/api/users.go`

Each API file automatically registers itself via `init()`:

```go
package api

import (
    "context"
    gold "github.com/dotmonk/gold-go"
)

type User struct {
    Name string `json:"name"`
    Age  int    `json:"age"`
}

type CreateUserInput struct {
    User User `json:"user"`
}

func init() {
    gold.RegisterAPIFunc(func(app *gold.App) {
        app.Register(gold.Request(
            gold.OperationMeta{Namespace: "Users", Name: "createUser"},
            func(_ context.Context, in CreateUserInput) (User, error) {
                return in.User, nil
            },
        ))
    })
}
```

### 3. Use generated client in frontend

When the backend starts, gold-go writes `frontend/client.ts` with fully typed operations:

```ts
import { Users } from "./client";

// Request operation
const user = await Users.createUser({ user: { name: "Alice", age: 30 } });

// Stream operation (SSE)
const stop = Clock.ticking((tick) => console.log(tick));
// later…
stop();
```

## How It Works

### Automatic API Discovery

1. Each `*.go` file in `backend/api/` defines operations with `gold.Register()`
2. Each file calls `gold.RegisterAPIFunc()` in an `init()` function
3. When the app imports `backend/api`, all `init()` functions run and register themselves
4. On startup, `gold.New()` wires up all registered operations and generates the TypeScript client

### Operation Patterns

**Request operation** — Request/response handled immediately:

```go
app.Register(gold.Request(
    gold.OperationMeta{Namespace: "Users", Name: "createUser"},
    func(_ context.Context, in CreateUserInput) (User, error) {
        return user, nil
    },
))
```

**Stream operation** — Server-Sent Events (SSE):

```go
app.Register(gold.Stream(
    gold.OperationMeta{Namespace: "Clock", Name: "ticking"},
    func(ctx context.Context, _ struct{}, out chan<- int64) error {
        out <- time.Now().UnixMilli()
        ticker := time.NewTicker(time.Second)
        defer ticker.Stop()
        for {
            select {
            case <-ctx.Done():
                return nil
            case t := <-ticker.C:
                out <- t.UnixMilli()
            }
        }
    },
))
```

## File Uploads

Use gold.UploadedFile inside the operation input struct.

```go
type SetImageInput struct {
    File gold.UploadedFile `json:"file"`
}

app.Register(gold.Request(
    gold.OperationMeta{Namespace: "Chat", Name: "setImage"},
    func(_ context.Context, in SetImageInput) (gold.Void, error) {
        data, err := in.File.ReadAsBuffer()
        if err != nil {
            return gold.Void{}, err
        }
        // use data...
        _ = data
        return gold.Void{}, nil
    },
))
```

## Options

DefaultOptions() provides these defaults:
- Port: 3000
- Dev: false
- GeneratedClientPath: frontend/client.ts
- FrontendDir: static
- WorkDir: .
- ViteConfig: vite.config.ts
- VitePort: 5173
- MaxMultipartBytes: 8 GB
- MaxMultipartFieldBytes: 1 MB
- MultipartTmpDir: /tmp/gold-uploads

## Interceptors

Interceptors provide a generic way to inject cross-cutting concerns like authentication, logging, request/response transformation, or proxying:

**RequestInterceptor** — Runs before operation handlers. Use for auth, validation, adding context, etc:

```go
opts.RequestInterceptors = append(opts.RequestInterceptors, func(r *http.Request) (*http.Request, error) {
    token := r.Header.Get("Authorization")
    if token == "" {
        return nil, errors.New("missing token")
    }
    // Validate token and add to context
    ctx := context.WithValue(r.Context(), "user", parseToken(token))
    return r.WithContext(ctx), nil
})
```

**ResponseInterceptor** — Runs after operation handlers. Use for logging, transforming responses, etc:

```go
opts.ResponseInterceptors = append(opts.ResponseInterceptors, func(w http.ResponseWriter, r *http.Request, result any) (any, error) {
    log.Printf("Operation completed: %v", result)
    // Can modify or reject the response
    return result, nil
})
```

Interceptors run in order for each request. If any interceptor returns an error, the request is rejected immediately.

## Dev vs Production Frontend Serving

- Dev mode (Dev=true): gold-go starts Vite and proxies frontend requests through the backend. This keeps one backend entrypoint while preserving HMR.
- Production mode (Dev=false): gold-go serves static assets from FrontendDir with SPA fallback to index.html.

## Example App

The [`gold-go-example`](https://github.com/dotmonk/gold-go-example) repo shows a complete app with three API modules:

- **Chat**: Bidirectional text and image updates via streams + requests
- **Clock**: Server time pushed every second via stream
- **Users**: Full CRUD with real-time list stream

All APIs are auto-discovered and the frontend is built with React + Vite.

## Repository Layout

```text
gold-go/
├── go.mod               # Go module definition
├── router.go            # HTTP routing, request/SSE handlers, client generation
├── types.go             # Type definitions, marshalling, TypeScript codegen
├── setup.go             # Auto-discovery registry (gold.RegisterAPIFunc, RegisterAll)
└── README.md
```
