# Architecture Documentation

## Module Responsibilities

### Storage Layer (`src/storage/`)

**Page**: Fixed-size 4KB data structure with slotted page layout
- Manages record storage within a page using slot array
- Handles insert/delete/get operations for records
- Tracks free space and slot usage

**DiskManager**: Low-level file I/O operations
- Reads/writes pages to disk files
- Manages page allocation and deallocation
- Provides thread-safe file operations

**BufferPoolManager**: In-memory page caching
- Implements LRU replacement policy
- Manages page frames and pin counts
- Coordinates between memory and disk

### Catalog Layer (`src/catalog/`)

**Schema**: Table structure definition
- Defines columns, data types, and constraints
- Provides column lookup by name/ID
- Calculates row sizes for storage

**TableInfo**: Table metadata
- Stores table ID, name, and schema reference
- Maintains root page ID for table data
- Acts as catalog entry point

### Execution Layer (`src/execution/`)

**Executor**: Abstract base for query operators
- Implements iterator interface with `Next()`
- Provides output schema information

**SeqScanExecutor**: Sequential table scan
- Iterates through all records in a table
- Handles page-by-page record retrieval

**FilterExecutor**: WHERE clause processing
- Filters records based on comparison conditions
- Wraps child executor with predicate evaluation

**ProjectionExecutor**: Column selection
- Implements SELECT column list functionality
- Reorders and filters output columns

### Database Interface (`src/db/`)

**Database**: Main API entry point
- Coordinates all subsystems
- Provides high-level database operations
- Manages table creation and DML operations

## Data Flow

1. **Table Creation**: Database → Schema → TableInfo → Page allocation
2. **Insert**: Database → BufferPool → Page → Record storage
3. **Scan**: Database → SeqScan → BufferPool → Page → Record retrieval
4. **Filter**: Filter → Child executor → Predicate evaluation

## Key Design Decisions

- **Fixed-size pages**: Simplifies storage management and disk I/O
- **Slotted page layout**: Efficient variable-length record storage
- **Iterator model**: Enables pipelined query execution
- **Separation of concerns**: Clear boundaries between storage, catalog, and execution
- **Extensible design**: Easy to add new operators and data types
