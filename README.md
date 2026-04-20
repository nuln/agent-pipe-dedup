# Message Dedup Pipe

> [中文文档](README.zh.md)

`github.com/nuln/agent-pipe-dedup` — Message Dedup Pipe plugin for [nuln/agent-core](https://github.com/nuln/agent-core).

## Overview

| Field | Value |
|-------|-------|
| **Plugin Type** | `pipe` |
| **Module** | `github.com/nuln/agent-pipe-dedup` |
| **Key Dependency** | *(stdlib only)* |

## Installation

```bash
go get github.com/nuln/agent-pipe-dedup
```

Import the package in your `main.go` (side-effect import triggers `init()`):

```go
import _ "github.com/nuln/agent-pipe-dedup"
```

## Configuration

Configure via environment variables or the Web UI.  
See `RegisterPluginConfigSpec` in the plugin source for the full field list.

## Development

```bash
make fmt     # format code
make lint    # run golangci-lint
make test    # run tests
make build   # go build ./...
```

## License

MIT
