package server

import (
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"
)

// PostgreSQL-compatible FastPostgres Server
type PGServer struct {
	listener           net.Listener
	database           *Database
	protocolHandler    *PGProtocolHandler
	activeConnections  int64
	totalConnections   int64
	totalQueries       int64
	errors             int64
	maxConnections     int64
	startTime          time.Time
	isRunning          bool
}

func NewPGServer(maxConnections int64) *PGServer {
	database := NewDatabase("fastpostgres")

	return &PGServer{
		database:         database,
		protocolHandler:  NewPGProtocolHandler(database),
		maxConnections:   maxConnections,
		startTime:        time.Now(),
		isRunning:        false,
	}
}

func (s *PGServer) Start(port int) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}

	s.listener = listener
	s.isRunning = true

	fmt.Printf("🐘 FastPostgres Server (PostgreSQL Wire Protocol) started on port %d\n", port)
	fmt.Printf("📊 Max Connections: %d\n", s.maxConnections)
	fmt.Printf("🔌 PostgreSQL clients can connect using:\n")
	fmt.Printf("   psql -h localhost -p %d -U fastpostgres -d fastpostgres\n", port)
	fmt.Printf("⚡ Ready to handle PostgreSQL connections!\n\n")

	go s.printStats()

	for s.isRunning {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.isRunning {
				atomic.AddInt64(&s.errors, 1)
				continue
			}
			break
		}

		// Check connection limit
		if atomic.LoadInt64(&s.activeConnections) >= s.maxConnections {
			conn.Write([]byte("ERROR: Connection limit reached\n"))
			conn.Close()
			atomic.AddInt64(&s.errors, 1)
			continue
		}

		atomic.AddInt64(&s.totalConnections, 1)
		atomic.AddInt64(&s.activeConnections, 1)

		go s.handleConnection(conn)
	}

	return nil
}

func (s *PGServer) handleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		atomic.AddInt64(&s.activeConnections, -1)
	}()

	// Use PostgreSQL protocol handler
	err := s.protocolHandler.HandleConnection(conn)
	if err != nil {
		atomic.AddInt64(&s.errors, 1)
		// Error is logged, but don't print every connection error to avoid spam
	}

	atomic.AddInt64(&s.totalQueries, 1)
}

func (s *PGServer) printStats() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for s.isRunning {
		select {
		case <-ticker.C:
			active := atomic.LoadInt64(&s.activeConnections)
			total := atomic.LoadInt64(&s.totalConnections)
			queries := atomic.LoadInt64(&s.totalQueries)
			errors := atomic.LoadInt64(&s.errors)

			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			fmt.Printf("📊 [%v] Active: %d | Total: %d | Queries: %d | Errors: %d | Memory: %.1fMB\n",
				time.Now().Format("15:04:05"),
				active, total, queries, errors,
				float64(m.Alloc)/(1024*1024))

			// Show connection rate
			uptime := time.Since(s.startTime)
			if uptime.Seconds() > 0 {
				connRate := float64(total) / uptime.Seconds()
				queryRate := float64(queries) / uptime.Seconds()
				fmt.Printf("   📈 Rates: %.1f conn/sec | %.1f queries/sec | Uptime: %v\n",
					connRate, queryRate, uptime.Round(time.Second))
			}
		}
	}
}

func (s *PGServer) Stop() {
	s.isRunning = false
	if s.listener != nil {
		s.listener.Close()
	}
}

func mainPGServer() { // Renamed to avoid conflict
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run pg_server.go pg_protocol.go <max_connections> [port]")
		fmt.Println("Example: go run pg_server.go pg_protocol.go 50000 5432")
		return
	}

	maxConnections, err := strconv.ParseInt(os.Args[1], 10, 64)
	if err != nil {
		fmt.Printf("Error: Invalid max_connections: %v\n", err)
		return
	}

	port := 5432 // Default PostgreSQL port
	if len(os.Args) > 2 {
		if p, err := strconv.Atoi(os.Args[2]); err == nil {
			port = p
		}
	}

	server := NewPGServer(maxConnections)

	fmt.Println("🚀 Starting FastPostgres with PostgreSQL Wire Protocol...")
	fmt.Println("   This server is compatible with:")
	fmt.Println("   • psql (PostgreSQL command line)")
	fmt.Println("   • pgAdmin (GUI administration tool)")
	fmt.Println("   • All PostgreSQL drivers (Go, Python, Java, etc.)")
	fmt.Println("   • PostgreSQL tools (pg_dump, pg_restore, etc.)")
	fmt.Println("")

	if err := server.Start(port); err != nil {
		fmt.Printf("❌ Server error: %v\n", err)
	}
}