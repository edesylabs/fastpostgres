# FastPostgres Tests

This directory contains test scripts for FastPostgres.

## Test Scripts

### test_persistence.sh

Tests the data persistence functionality including:
- Write-Ahead Logging (WAL)
- Checkpointing
- Recovery from disk
- Data survival across restarts

**Usage:**
```bash
cd /path/to/fastpostgres
./tests/test_persistence.sh
```

**Requirements:**
- FastPostgres binary must be built: `go build -o bin/fastpostgres cmd/fastpostgres/main.go`
- PostgreSQL client (`psql`) must be installed
- Ports 5435 (default) must be available

**What it tests:**
1. Creates a fresh database with sample data
2. Verifies WAL files are created
3. Stops the server
4. Restarts the server
5. Verifies data persists across restart

## Running Tests

### Build the binary first
```bash
go build -o bin/fastpostgres cmd/fastpostgres/main.go
```

### Run persistence test
```bash
./tests/test_persistence.sh
```

## Test Output

Test logs and server output are written to `/tmp/`:
- `/tmp/test1.log` - First server run
- `/tmp/test2.log` - Second server run (after restart)
- `/tmp/server.log` - General server logs

These files are temporary and excluded from git via `.gitignore`.

## Adding New Tests

To add new test scripts:

1. Create a new `.sh` file in this directory
2. Make it executable: `chmod +x tests/your_test.sh`
3. Update this README with test description
4. Follow the existing test pattern for consistency

## Cleanup

After running tests, you can clean up with:
```bash
rm -rf ./data    # Remove test data
pkill fastpostgres  # Kill any running servers
```