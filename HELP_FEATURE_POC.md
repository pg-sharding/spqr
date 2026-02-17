# HELP Command PoC Implementation

## Overview
This is a proof of concept implementation of the HELP command for the SPQR console, as requested in issue #1968.

## Implementation Summary

### 1. Help Documentation Files
**Location**: `yacc/console/help/`

Created a modular help file structure where each command has its own `.txt` file:
- `CREATE_KEY_RANGE.txt` - Help for CREATE KEY RANGE command

The help files contain:
- NAME: Brief description of the command
- SYNTAX: Command syntax with parameters
- DESCRIPTION: Detailed explanation
- PARAMETERS: Parameter descriptions
- EXAMPLES: Real-world usage examples
- TIPS: Helpful guidance
- RELATED COMMANDS: Related commands

### 2. Grammar Modifications
**Files Modified**: `yacc/console/gram.y`

Added:
- `HELP` token to token declarations (line 132)
- Help union type in `%union` section (line 47)
- Type declarations for `help_stmt` and `help_command_name` (lines 228-229)
- `help_stmt` parsing rule (lines 1092-1102)
- Integration into `command` rule (lines 359-363)

### 3. AST Definitions
**File Modified**: `yacc/console/ast.go`

Added:
```go
type Help struct {
    CommandName string
}

func (*Help) iStatement() {}
```

### 4. Keyword Registration
**File Modified**: `yacc/console/reserved_keyword.go`

Added:
- `"help"` keyword mapping to `HELP` token

### 5. Help Registry System
**File Created**: `yacc/console/help_registry.go`

Implements:
- `InitHelpRegistry(helpDir string)` - Loads all help files from a directory at startup
- `GetHelp(commandName string)` - Retrieves help for a specific command
- `ListAvailableCommands()` - Lists all available help commands
- Thread-safe map-based storage with sync.RWMutex

Features:
- Loads `.txt` files from the help directory
- Converts file names to command names (e.g., `CREATE_KEY_RANGE.txt` → `CREATE KEY RANGE`)
- Lazy initialization with `sync.Once` for thread safety

### 6. Command Handler
**File Modified**: `pkg/meta/meta.go`

Added:
- HELP case in `ProcMetadataCommand()` dispatcher (lines 843-850)
- `ProcessHelp()` function to handle HELP statements (lines 1901-1918)

The handler:
- Checks user permissions (RoleReader, like SHOW)
- Retrieves help content from registry
- Formats results as a TupleTableSlot with two columns: COMMAND, HELP TEXT
- Returns formatted text to the client via PostgreSQL protocol

### 7. Router Initialization
**File Modified**: `cmd/router/main.go`

Added:
- Import of spqrparser package
- Help registry initialization in the router startup sequence (lines 144-147)
- Initializes from path: `{config_dir}/../../yacc/console/help`

## Usage Example

After starting the router:

```sql
psql -h localhost -p 6432 -U admin -d admin

admin=> HELP CREATE KEY RANGE;
                        COMMAND                        |                         HELP TEXT
────────────────────────────────────────────────────────┼────────────────────────────────────────────────────────────
CREATE KEY RANGE                                       | NAME
                                                       |     CREATE KEY RANGE - Create a new key range that maps value range to a shard
                                                       |
                                                       | SYNTAX
                                                       |     CREATE KEY RANGE <id> FROM <bound> ROUTE TO <shard> [FOR DISTRIBUTION <dist>]
                                                       |
                                                       | DESCRIPTION
                                                       |     Creates a new key range that routes values within the specified boundary
                                                       |     to the given shard. If no distribution is specified, uses the default.
                                                       |     ...
                                                       |     [rest of help text]
```

## Future Enhancements

1. **Extend help_command_name rule** to support all commands:
   - Currently only supports "CREATE KEY RANGE"
   - Can add: CREATE DISTRIBUTION, ALTER DISTRIBUTION, DROP KEY RANGE, SHOW, LOCK KEY RANGE, etc.

2. **Add more help files** as needed for each command

3. **Search/Filter functionality**:
   - `HELP CREATE` - show all CREATE commands
   - `HELP DISTRIBUTION` - show all distribution-related commands

4. **Dynamic help from catalog**:
   - Could pull help from a database table instead of files for easier updates

5. **Fuzzy matching**:
   - Handle typos: `HELP CREATE RANGE` could suggest `CREATE KEY RANGE`

6. **Web-based help viewer**:
   - Could expose help via HTTP API for documentation site

## Build Instructions

1. Generate the parser:
   ```bash
   cd yacc/console && goyacc -o gram.go -p yy gram.y
   cd ../..
   ```

2. Fix vendoring:
   ```bash
   go mod vendor
   ```

3. Build the router:
   ```bash
   make build_router
   ```

## Testing

The PoC can be tested by:

1. Starting the router with the help registry initialized
2. Connecting to the admin console
3. Executing: `HELP CREATE KEY RANGE;`
4. Verifying the help text is returned correctly

## Implementation Notes

- The help system is read-only and doesn't require admin privileges beyond what SHOW already requires
- Help files are loaded once at router startup for performance
- The system is modular - new help files can be added without code changes
- Using separate files per command makes it PostgreSQL-like and easier to maintain
- The structure follows PostgreSQL's man page conventions (NAME, SYNTAX, DESCRIPTION, EXAMPLES, etc.)

## Files Modified/Created

1. **Created**:
   - `yacc/console/help/CREATE_KEY_RANGE.txt`
   - `yacc/console/help_registry.go`
   - `HELP_FEATURE_POC.md` (this file)

2. **Modified**:
   - `yacc/console/gram.y`
   - `yacc/console/ast.go`
   - `yacc/console/reserved_keyword.go`
   - `pkg/meta/meta.go`
   - `cmd/router/main.go`
   - `yacc/console/gram.go` (auto-generated)
   - `go.mod` (vendoring updated)
   - `go.sum` (vendoring updated)
   - `vendor/` (vendoring updated)
