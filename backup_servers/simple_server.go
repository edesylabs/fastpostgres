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
	"sync/atomic"
	"time"
)

type SimpleServer struct {
	listener           net.Listener
	activeConnections  int64
	totalConnections   int64
	totalRequests      int64
	totalResponses     int64
	errors             int64
	maxConnections     int64
	startTime          time.Time
	isRunning          bool
}

func NewSimpleServer(maxConnections int64) *SimpleServer {
	return &SimpleServer{
		maxConnections: maxConnections,
		startTime:      time.Now(),
		isRunning:      false,
	}
}

func (s *SimpleServer) Start(port int) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}

	s.listener = listener
	s.isRunning = true

	fmt.Printf("🚀 FastPostgres Simple Server started on port %d\n", port)
	fmt.Printf("📊 Max Connections: %d\n", s.maxConnections)
	fmt.Printf("⚡ Ready to handle concurrent connections!\n\n")

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

func (s *SimpleServer) handleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		atomic.AddInt64(&s.activeConnections, -1)
	}()

	conn.SetDeadline(time.Now().Add(30 * time.Second))
	reader := bufio.NewReader(conn)

	for {
		request, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				atomic.AddInt64(&s.errors, 1)
			}
			break
		}

		request = strings.TrimSpace(request)
		atomic.AddInt64(&s.totalRequests, 1)

		var response string
		switch {
		case strings.HasPrefix(request, "PING"):
			response = "PONG"
		case request == "INFO":
			response = s.getInfo()
		case request == "STATS":
			response = s.getStats()
		case request == "QUIT":
			response = "BYE"
			conn.Write([]byte(response + "\n"))
			return
		default:
			response = "OK"
		}

		_, err = conn.Write([]byte(response + "\n"))
		if err != nil {
			atomic.AddInt64(&s.errors, 1)
			break
		}

		atomic.AddInt64(&s.totalResponses, 1)
		conn.SetDeadline(time.Now().Add(30 * time.Second))
	}
}

func (s *SimpleServer) getInfo() string {
	return fmt.Sprintf("FastPostgres | Active: %d/%d | Uptime: %v",
		atomic.LoadInt64(&s.activeConnections),
		s.maxConnections,
		time.Since(s.startTime).Round(time.Second))
}

func (s *SimpleServer) getStats() string {
	return fmt.Sprintf("Connections:%d Requests:%d Responses:%d Errors:%d",
		atomic.LoadInt64(&s.totalConnections),
		atomic.LoadInt64(&s.totalRequests),
		atomic.LoadInt64(&s.totalResponses),
		atomic.LoadInt64(&s.errors))
}

func (s *SimpleServer) printStats() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for s.isRunning {
		select {
		case <-ticker.C:
			active := atomic.LoadInt64(&s.activeConnections)
			total := atomic.LoadInt64(&s.totalConnections)
			requests := atomic.LoadInt64(&s.totalRequests)
			responses := atomic.LoadInt64(&s.totalResponses)
			errors := atomic.LoadInt64(&s.errors)

			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			fmt.Printf("📊 [%v] Active: %d | Total: %d | Req: %d | Resp: %d | Err: %d | Mem: %.1fMB\n",
				time.Now().Format("15:04:05"),
				active, total, requests, responses, errors,
				float64(m.Alloc)/(1024*1024))
		}
	}
}

func mainSimpleServer() { // Renamed to avoid conflict
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run simple_server.go <max_connections>")
		return
	}

	maxConnections, err := strconv.ParseInt(os.Args[1], 10, 64)
	if err != nil {
		fmt.Printf("Error: Invalid max_connections: %v\n", err)
		return
	}

	server := NewSimpleServer(maxConnections)

	if err := server.Start(5433); err != nil {
		fmt.Printf("❌ Server error: %v\n", err)
	}
}