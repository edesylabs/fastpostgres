package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// FastPostgres Server with actual TCP connections
type FastPostgresServer struct {
	listener        net.Listener
	database        *Database
	connectionPool  *ServerConnectionPool
	isRunning       bool
	stats          *ServerStats
	config         *ServerConfig
}

type ServerConfig struct {
	Host            string
	Port            int
	MaxConnections  int64
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	BufferSize      int
}

type ServerStats struct {
	totalConnections    int64
	activeConnections   int64
	totalRequests       int64
	totalResponses      int64
	bytesRead          int64
	bytesWritten       int64
	errors             int64
	startTime          time.Time
}

type ServerConnectionPool struct {
	connections     sync.Map // map[net.Conn]*ServerConnection
	connectionCount int64
	maxConnections  int64
	bufferPool      sync.Pool
}

type ServerConnection struct {
	conn         net.Conn
	id           string
	created      time.Time
	lastActivity time.Time
	requestCount int64
	bytesRead    int64
	bytesWritten int64
}

func NewFastPostgresServer(config *ServerConfig) *FastPostgresServer {
	return &FastPostgresServer{
		database:       NewDatabase("fastpostgres"),
		connectionPool: NewServerConnectionPool(config.MaxConnections),
		stats:         &ServerStats{startTime: time.Now()},
		config:        config,
	}
}

func NewServerConnectionPool(maxConnections int64) *ServerConnectionPool {
	return &ServerConnectionPool{
		maxConnections: maxConnections,
		bufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 4096)
			},
		},
	}
}

func (s *FastPostgresServer) Start() error {
	addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to start server on %s: %v", addr, err)
	}

	s.listener = listener
	s.isRunning = true

	fmt.Printf("🚀 FastPostgres Server started on %s\n", addr)
	fmt.Printf("📊 Max Connections: %d\n", s.config.MaxConnections)
	fmt.Printf("⚡ Ready to handle concurrent connections!\n\n")

	go s.printStats()

	for s.isRunning {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.isRunning {
				atomic.AddInt64(&s.stats.errors, 1)
				fmt.Printf("❌ Error accepting connection: %v\n", err)
			}
			continue
		}

		// Check connection limit
		if atomic.LoadInt64(&s.connectionPool.connectionCount) >= s.config.MaxConnections {
			conn.Write([]byte("ERROR: Connection limit reached\n"))
			conn.Close()
			atomic.AddInt64(&s.stats.errors, 1)
			continue
		}

		go s.handleConnection(conn)
	}

	return nil
}

func (s *FastPostgresServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	// Create server connection
	serverConn := &ServerConnection{
		conn:         conn,
		id:           fmt.Sprintf("%s-%d", conn.RemoteAddr().String(), time.Now().UnixNano()),
		created:      time.Now(),
		lastActivity: time.Now(),
	}

	// Add to pool
	s.connectionPool.connections.Store(conn, serverConn)
	atomic.AddInt64(&s.connectionPool.connectionCount, 1)
	atomic.AddInt64(&s.stats.totalConnections, 1)
	atomic.AddInt64(&s.stats.activeConnections, 1)

	// Set timeouts
	conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout))
	conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))

	// Handle requests
	reader := bufio.NewReader(conn)

	for {
		// Read request
		request, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				atomic.AddInt64(&s.stats.errors, 1)
			}
			break
		}

		request = strings.TrimSpace(request)
		atomic.AddInt64(&s.stats.totalRequests, 1)
		atomic.AddInt64(&s.stats.bytesRead, int64(len(request)))
		atomic.AddInt64(&serverConn.bytesRead, int64(len(request)))
		atomic.AddInt64(&serverConn.requestCount, 1)

		serverConn.lastActivity = time.Now()

		// Process request and send response
		response := s.processRequest(request)

		_, err = conn.Write([]byte(response + "\n"))
		if err != nil {
			atomic.AddInt64(&s.stats.errors, 1)
			break
		}

		atomic.AddInt64(&s.stats.totalResponses, 1)
		atomic.AddInt64(&s.stats.bytesWritten, int64(len(response)+1))
		atomic.AddInt64(&serverConn.bytesWritten, int64(len(response)+1))

		// Update timeouts
		conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout))
		conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))
	}

	// Remove from pool
	s.connectionPool.connections.Delete(conn)
	atomic.AddInt64(&s.connectionPool.connectionCount, -1)
	atomic.AddInt64(&s.stats.activeConnections, -1)
}

func (s *FastPostgresServer) processRequest(request string) string {
	if request == "" {
		return "ERROR: Empty request"
	}

	parts := strings.Fields(request)
	if len(parts) == 0 {
		return "ERROR: Invalid request"
	}

	command := strings.ToUpper(parts[0])

	switch command {
	case "PING":
		return "PONG"
	case "INFO":
		return s.getServerInfo()
	case "STATS":
		return s.getServerStats()
	case "SELECT":
		return s.handleSelectQuery(request)
	case "INSERT":
		return s.handleInsertQuery(request)
	case "CONNECTIONS":
		return fmt.Sprintf("ACTIVE_CONNECTIONS:%d", atomic.LoadInt64(&s.stats.activeConnections))
	case "QUIT":
		return "BYE"
	default:
		return fmt.Sprintf("OK: Processed command %s", command)
	}
}

func (s *FastPostgresServer) handleSelectQuery(query string) string {
	// Simulate query processing
	time.Sleep(100 * time.Microsecond) // Simulate query execution time
	return "RESULT: Query executed successfully"
}

func (s *FastPostgresServer) handleInsertQuery(query string) string {
	// Simulate insert processing
	time.Sleep(50 * time.Microsecond) // Simulate insert execution time
	return "INSERT: Record inserted successfully"
}

func (s *FastPostgresServer) getServerInfo() string {
	return fmt.Sprintf("FastPostgres v1.0 | Connections: %d/%d | Uptime: %v",
		atomic.LoadInt64(&s.stats.activeConnections),
		s.config.MaxConnections,
		time.Since(s.stats.startTime).Round(time.Second))
}

func (s *FastPostgresServer) getServerStats() string {
	return fmt.Sprintf("STATS: Connections=%d Requests=%d Responses=%d Errors=%d BytesRead=%d BytesWritten=%d",
		atomic.LoadInt64(&s.stats.totalConnections),
		atomic.LoadInt64(&s.stats.totalRequests),
		atomic.LoadInt64(&s.stats.totalResponses),
		atomic.LoadInt64(&s.stats.errors),
		atomic.LoadInt64(&s.stats.bytesRead),
		atomic.LoadInt64(&s.stats.bytesWritten))
}

func (s *FastPostgresServer) printStats() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for s.isRunning {
		select {
		case <-ticker.C:
			active := atomic.LoadInt64(&s.stats.activeConnections)
			total := atomic.LoadInt64(&s.stats.totalConnections)
			requests := atomic.LoadInt64(&s.stats.totalRequests)
			responses := atomic.LoadInt64(&s.stats.totalResponses)
			errors := atomic.LoadInt64(&s.stats.errors)

			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			fmt.Printf("📊 [%v] Active: %d | Total: %d | Requests: %d | Responses: %d | Errors: %d | Memory: %.2f MB\n",
				time.Now().Format("15:04:05"),
				active, total, requests, responses, errors,
				float64(m.Alloc)/(1024*1024))
		}
	}
}

func (s *FastPostgresServer) Stop() {
	s.isRunning = false
	if s.listener != nil {
		s.listener.Close()
	}
}

// Database stub for server
type Database struct {
	name string
}

func NewDatabase(name string) *Database {
	return &Database{name: name}
}

func mainServer() { // Renamed to avoid conflict
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run server.go <max_connections>")
		fmt.Println("Example: go run server.go 50000")
		return
	}

	maxConnections, err := strconv.ParseInt(os.Args[1], 10, 64)
	if err != nil {
		fmt.Printf("Error: Invalid max_connections value: %v\n", err)
		return
	}

	config := &ServerConfig{
		Host:           "localhost",
		Port:           5433, // Different from PostgreSQL's 5432
		MaxConnections: maxConnections,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		BufferSize:     4096,
	}

	server := NewFastPostgresServer(config)

	// Handle graceful shutdown
	go func() {
		fmt.Println("Press Ctrl+C to stop the server")
		var input string
		fmt.Scanln(&input)
		server.Stop()
	}()

	if err := server.Start(); err != nil {
		fmt.Printf("❌ Server error: %v\n", err)
	}
}