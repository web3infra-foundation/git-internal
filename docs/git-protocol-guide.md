# Git Protocol Abstraction Design and Implementation

## 1. Project Overview

The git-internal library implements a transport layer abstraction for the Git smart protocol, separating HTTP and SSH protocol handling from monorepo business code and adapting to any business system through Trait interfaces.

## 2. Architecture Design

### 2.1 Layered Architecture

```
┌─────────────────────────────────────┐
│     Business System Integration     │  ← Implement Trait interfaces
├─────────────────────────────────────┤
│     Transport Protocol Adapters     │  ← HTTP/SSH handlers
├─────────────────────────────────────┤
│     Git Smart Protocol Core         │  ← Protocol logic implementation
├─────────────────────────────────────┤
│     Pack File Processing Layer      │  ← Object packing/unpacking
└─────────────────────────────────────┘
```

### 2.2 Core Abstraction Interfaces

**RepositoryAccess Trait**

- Provides storage layer abstraction, isolating business logic
- Supports reference management, object access, and Pack operations
- Can adapt to any storage backend (filesystem, database, etc.)

**AuthenticationService Trait**

- Unified authentication interface supporting HTTP and SSH
- Can integrate with any authentication system (OAuth, JWT, public key, etc.)

**GitProtocol Core**

- Transport-agnostic protocol implementation
- Unified info/refs, upload-pack, receive-pack interfaces

## 3. Implementation Status

### 3.1 Completed Features

**Core Protocol**

- Complete Git smart protocol v1 implementation
- Reference advertisement and capability negotiation
- upload-pack service (clone/fetch operations)
- receive-pack service (push operations)
- Pack file generation and parsing

**Transport Layer**

- HTTP transport adapter (request parsing, streaming responses)
- SSH transport adapter (command parsing, authentication integration)
- Transport protocol abstraction (unified interface)

**Data Processing**

- Side-band multiplexing
- Progress reporting mechanism
- Object parsing (blob, commit, tree)
- Reference updates and validation

**Authentication System**

- HTTP authentication (header-based)
- SSH authentication (public key verification)
- Pluggable authentication architecture

## 4. Module Organization

```
src/protocol/
├── core.rs          # Main abstractions (Trait definitions, GitProtocol)
├── http.rs          # HTTP transport adapter
├── ssh.rs           # SSH transport adapter
├── smart.rs         # Git smart protocol implementation
├── pack.rs          # Pack generation and processing
├── types.rs         # Protocol types and error definitions
├── utils.rs         # Protocol utility functions
└── mod.rs           # Module exports
```

## 5. HTTP Protocol Abstraction

### 5.1 Design Features

- Request path parsing and repository location
- Standard Git HTTP content type handling
- Streaming responses for large repository transfers
- Error mapping to HTTP status codes

### 5.2 Main Functions

- `handle_info_refs`: Process reference query requests
- `handle_upload_pack`: Process clone/fetch requests
- `handle_receive_pack`: Process push requests
- `authenticate_http`: HTTP authentication integration

## 6. SSH Protocol Abstraction

### 6.1 Design Features

- Git command line parsing (git-upload-pack, git-receive-pack)
- Repository path extraction and validation
- Direct protocol mapping without HTTP overhead
- Public key authentication integration

### 6.2 Main Functions

- Command parsing and dispatching
- Repository path extraction
- Protocol operation mapping
- SSH authentication integration

## 7. Trait Adaptation Solution

### 7.1 Storage Adaptation

Adapt any storage system through RepositoryAccess Trait:

- Filesystem storage
- Database storage
- Cloud storage services
- Distributed storage

### 7.2 Authentication Adaptation

Integrate any authentication system through AuthenticationService Trait:

- Traditional username/password
- OAuth/JWT tokens
- SSH public key authentication
- Enterprise SSO systems

### 7.3 Framework Agnostic

- No dependency on specific web frameworks
- No binding to specific SSH libraries
- No database choice restrictions
- No forced authentication schemes

## 8. Error Handling and Types

### 8.1 Protocol Error Types

- InvalidService: Invalid service request
- RepositoryNotFound: Repository does not exist
- Unauthorized: Authentication failure
- InvalidRequest: Request format error
- Other I/O and internal errors

### 8.2 Transport Mapping

Each transport layer is responsible for mapping protocol errors to appropriate transport error formats (HTTP status codes, SSH error messages, etc.).

## 9. Capabilities and Features

### 9.1 Supported Git Capabilities

- side-band-64k: Multiplexed data streams
- ofs-delta: Offset delta objects
- report-status: Push status reporting
- multi_ack_detailed: Detailed acknowledgment negotiation
- no-done: Optimized negotiation flow

### 9.2 Protocol Features

- Complete want/have negotiation
- Incremental Pack transmission
- Progress reporting
- Reference update validation

### 9.3 Protocol Evolution: v0 / v1 / v2 Comparison

#### Version Overview

Git Smart Protocol has evolved through three major versions.  
Each version refines how the client and server exchange repository data,  
especially for large-scale or latency-sensitive environments.

| Feature | v0 (Legacy) | v1 (Mainstream) | v2 (Modern) |
|----------|--------------|----------------|-------------|
| Capability Negotiation | ❌ None | ✅ Introduced | ✅ Refined command-based negotiation |
| Protocol Framing | Raw Stream | pkt-line framing | pkt-line with structured commands |
| HTTP Support | Partial | Full | Optimized and proxy-friendly |
| Extensibility | Limited | Basic | Modular and command-oriented |
| Command Granularity | Combined flow | Sequential phases | Independent commands |
| Performance | Low | Medium | High (fewer RTTs, less data transfer) |
| Server Complexity | Simple | Moderate | Structured and modular |
| Status in Git Ecosystem | Deprecated | Widely used | Supported in modern Git servers (>=2.18) |

#### Version Highlights

##### **v0 (Legacy Smart Protocol)**
- Early “smart” mode over TCP or SSH before capability negotiation existed.  
- Used simple request/response sequences (`upload-pack` / `receive-pack`)  
  without flexible negotiation or side-band streaming.  
- Now considered obsolete and unsupported in most Git servers.

##### **v1 (Current Mainstream)**
- Introduced in Git 1.7+.  
- Added capability negotiation, side-band multiplexing, and better error mapping.  
- Enables features such as:
  - `multi_ack_detailed`, `side-band-64k`, `ofs-delta`, `report-status`, `no-done`
- Currently the most widely deployed version (e.g., GitHub, GitLab, Gitea).

##### **v2 (Command-Based Modern Protocol)**
- Introduced in Git 2.18 (2018), designed for extensibility and performance.  
- Transforms the protocol from stream-based negotiation to a **command-driven request model**:
  - Client sends specific commands instead of phase-based state machines.
  - Server replies with structured sections per command.
- Ideal for HTTP(S) transports and proxy environments.


### 9.4 Protocol v2 Negotiation and Commands

#### 1. Initial Capability Exchange

The client initiates a request with:
```
GET /info/refs?service=git-upload-pack
Git-Protocol: version=2
```

Server responds with a list of supported capabilities and commands:
```
version 2
ls-refs
fetch=filter
server-option
session-id=deadbeef
agent=git/2.45.0
```

#### 2. Supported v2 Commands

| Command | Purpose | Description |
|----------|----------|-------------|
| `ls-refs` | List references | Returns branches, tags, and HEAD information with filtering. |
| `fetch` | Clone / fetch data | Supports partial and shallow clones; incremental packfile delivery. |
| `push` | Push updates | Negotiates reference updates and receives new objects. |
| `server-option` | Custom server parameters | Allows additional server-side settings before command execution. |
| `agent` | Version identification | Reports client/server version for debugging and analytics. |

#### 3. Example: v2 Fetch Flow

```
C: command=ls-refs
C: agent=git/2.45.0
C: end
S: ref refs/heads/main 1234abcd
S: symref=HEAD refs/heads/main
S: end

C: command=fetch
C: want 1234abcd
C: filter blob:none
C: done
S: packfile data...
S: end
```

### 9.5 Migration and Upgrade Recommendations

#### For Protocol Implementers
- Maintain backward compatibility: support v1 and v2 simultaneously.
- Allow version negotiation via the `Git-Protocol` header or SSH command environment.
- Gradually deprecate v0 handling to reduce code complexity.

#### For System Integrators
- Use v2 for large-scale repositories or latency-sensitive deployments.
- v2’s `ls-refs` and `fetch=filter` reduce bandwidth and server CPU usage.
- Implement adaptive fallback to v1 for older Git clients.

#### For `git-internal` Library
- Current status: **Full v1 compliance**
- Recommended next steps:
  1. Introduce protocol negotiation layer detecting `Git-Protocol: version=2`
  2. Implement minimal `ls-refs` and `fetch` command handlers
  3. Extend `RepositoryAccess` and `GitProtocol` traits to support v2 command flow
  4. Add benchmark suite comparing v1 vs v2 RTT and bandwidth efficiency

### 9.6 Summary

| Category | v1 | v2 |
|-----------|----|----|
| Negotiation | Capabilities exchange via `info/refs` | Explicit version header (`Git-Protocol: version=2`) |
| Command Model | Sequential phase-based | Independent modular commands |
| Extensibility | Limited (capability list) | Open-ended (new commands possible) |
| Efficiency | Moderate | High (fewer round trips, less data) |
| Implementation Complexity | Medium | Higher but cleaner abstraction |

## 10. Integration Guide

### 10.1 Implementation Steps

1. Implement RepositoryAccess Trait to connect storage system
2. Implement AuthenticationService Trait to connect authentication system
3. Create HTTP/SSH handler instances
4. Route requests to appropriate handlers in framework

### 10.2 Design Principles

- Separation of concerns: Protocol logic decoupled from business logic
- Interface abstraction: Pluggable architecture through Traits
- Transport agnostic: Same protocol logic supports multiple transports
- Performance focused: Streaming processing, memory efficient

## 11. Summary

The git-internal library successfully implements Git protocol transport layer abstraction, separating protocol handling from business logic through clear Trait interfaces. This design supports:

- **Complete protocol implementation**: Full Git smart protocol v1 functionality
- **Flexible integration solution**: Can adapt to any storage and authentication system
- **Transport layer abstraction**: Unified HTTP and SSH handling
- **High-performance design**: Streaming processing and memory optimization
