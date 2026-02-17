# SPQR Console Help System

This directory contains help documentation for SPQR console commands. Help files are embedded into the `spqr-router` and `spqr-coordinator` binaries at build time using Go's `embed` package.

## How It Works

1. **Help files** are stored as `.txt` files in this directory
2. **At build time**, all `.txt` files are embedded into the binary using Go's `//go:embed` directive
3. **At runtime**, the help registry is initialized by reading from the embedded filesystem
4. **No external files** are required - the binary is completely self-contained

This approach ensures:
- ✅ Production-ready single binary (no runtime file dependencies)
- ✅ Fast startup (no I/O overhead)
- ✅ Easy to maintain (edit `.txt` files during development)
- ✅ Standard Go best practices

## File Naming Convention

Help files use the command name in UPPER_CASE with underscores:

- `CREATE_KEY_RANGE.txt` → Maps to command `CREATE KEY RANGE`
- `CREATE_DISTRIBUTION.txt` → Maps to command `CREATE DISTRIBUTION`
- `ALTER_DISTRIBUTION.txt` → Maps to command `ALTER DISTRIBUTION`
- `DROP_KEY_RANGE.txt` → Maps to command `DROP KEY RANGE`

The filename is automatically converted to the command name:
1. Remove `.txt` extension
2. Replace underscores with spaces
3. Use for command lookup

## Help File Format

Each help file follows PostgreSQL-style documentation format with sections:

```
NAME
    COMMAND_NAME - Brief description

SYNTAX
    COMMAND syntax with parameters

DESCRIPTION
    Detailed explanation of what the command does
    Multi-line description is supported

PARAMETERS
    <param1> - Description
    <param2> - Description

EXAMPLES
    Example usage 1:
        SQL COMMAND HERE;

    Example usage 2:
        SQL COMMAND HERE;

TIPS
    - Helpful tip 1
    - Helpful tip 2
    - Helpful tip 3

RELATED COMMANDS
    COMMAND1 - Description
    COMMAND2 - Description
```

See `CREATE_KEY_RANGE.txt` for a complete example.

## Adding New Help Commands

### Step 1: Create Help File

Create a new file in this directory following the naming convention:

```bash
touch yacc/console/help/YOUR_COMMAND.txt
```

Example for `CREATE DISTRIBUTION`:

```bash
touch yacc/console/help/CREATE_DISTRIBUTION.txt
```

### Step 2: Write Help Content

Edit the file and follow the format above. Example:

```
NAME
    CREATE DISTRIBUTION - Create a new distribution for sharding

SYNTAX
    CREATE DISTRIBUTION <id> COLUMN TYPES <type1>, <type2>, ...

DESCRIPTION
    Creates a new distribution that defines how data is sharded.
    ...
```

### Step 3: Add Grammar Rule

Update `yacc/console/gram.y` to support the new command in the `help_command_name` rule:

```yacc
help_command_name:
    CREATE KEY RANGE
    {
        $$ = "CREATE KEY RANGE"
    }
    | CREATE DISTRIBUTION
    {
        $$ = "CREATE DISTRIBUTION"
    }
    | ...
```

### Step 4: Regenerate Parser

```bash
cd yacc/console && goyacc -o gram.go -p yy gram.y
```

### Step 5: Build and Test

Build the binary:

```bash
make build_router
```

Test the help command:

```bash
psql -h localhost -p 6432 -U admin -d admin
admin=> HELP CREATE DISTRIBUTION;
```

The help text should display formatted and wrapped for the console.

## Implementation Details

### Help Registry (`help_embedded.go`)

The help registry system:

- Uses Go's `embed` package to embed all `.txt` files at build time
- Reads from the embedded filesystem on startup
- Caches help entries in a thread-safe map
- Provides lookup functions: `GetHelp()`, `ListAvailableCommands()`

### Parser Integration (`gram.y`)

The YACC grammar:

- Parses `HELP <command>` syntax
- Supports multi-word commands (e.g., `CREATE KEY RANGE`)
- Creates `Help` AST nodes

### Handler (`pkg/meta/meta.go`)

The `ProcessHelp()` function:

- Retrieves help text from registry
- Formats as PostgreSQL protocol result set
- Returns two columns: COMMAND, HELP TEXT

### Build Time

During `go build`:

1. The `//go:embed help/*.txt` directive is processed
2. All `.txt` files are compiled into the binary
3. No file I/O occurs at runtime

## Testing

Help commands are tested in the regression test suite:

- Test file: `test/regress/tests/console/sql/help.sql`
- Expected output: `test/regress/tests/console/expected/help.out`
- Run tests: `make regress`

## Maintenance

### Local Development

1. Edit `.txt` files directly
2. Rebuild: `make build_router`
3. Test: `make regress`

### Production Deployment

No changes needed! The compiled binary contains all help text. Simply distribute the single binary:

```bash
./spqr-router run --config router.yaml
```

Users can access help via:

```sql
HELP CREATE KEY RANGE;
HELP CREATE DISTRIBUTION;
```

## Limitations

- Help commands can only be viewed, not modified at runtime
- Adding new commands requires rebuilding the binary
- Help text size adds to binary size (typically < 100KB for all commands)

## Future Enhancements

- Support for `HELP CREATE` (show all CREATE commands)
- Fuzzy matching for typo tolerance
- Dynamic help from database table (future feature)
- Web-based help viewer integration
