# Mini Relational Database Engine (SQLite-lite)

A lightweight, disk-based relational database engine implemented in C++17.

## Architecture Overview

```
src/
├── common/          # Shared utilities and types
├── storage/         # Storage engine (pages, buffer pool, files)
├── catalog/         # Schema management (tables, columns)
├── execution/       # Query execution engine
├── parser/          # SQL parser and AST
├── index/           # B+ Tree indexing
└── db/              # Main database interface

include/             # Public headers
tests/               # Unit tests
examples/            # Usage examples
CMakeLists.txt       # Build configuration
```

## Core Components

- **Storage Engine**: Fixed-size pages with slotted layout
- **Buffer Pool Manager**: Page caching and LRU eviction
- **Execution Engine**: Iterator-based query processing
- **SQL Parser**: Tokenizer and AST generator
- **Index Manager**: B+ Tree implementation

## Build Requirements

- C++17 or later
- CMake 3.12+
- Make or compatible build system
