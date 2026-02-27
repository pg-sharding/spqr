# SPQR Console Help

This directory contains help documentation for SPQR console commands. Help files are embedded into the `spqr-router` and `spqr-coordinator` binaries at build time using Go's `embed` package.

## How It Works

1. **Help files** are stored as `.txt` files in this directory
2. **At build time**, all `.txt` files are embedded into the binary using Go's `//go:embed` directive
3. **At runtime**, the help registry is initialized by reading from the embedded filesystem

