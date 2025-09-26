package engine

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type StressTestConfig struct {
	ServerHost         string
	ServerPort         int
	MaxConnections     int64
	ConcurrentClients  int64
	RequestsPerClient  int64
	ConnectionDuration time.Duration
	RequestDelay       time.Duration
}

type StressTestResults struct {
	TotalConnections      int64
	SuccessfulConnections int64
	FailedConnections     int64
	TotalRequests         int64
	SuccessfulRequests    int64
	FailedRequests        int64
	TotalResponseTime     int64
	MinResponseTime       int64
	MaxResponseTime       int64
	BytesSent            int64
	BytesReceived        int64
	StartTime            time.Time
	EndTime              time.Time
	Errors               []string
	errorsMutex          sync.Mutex
}

func (r *StressTestResults) AddError(err string) {
	r.errorsMutex.Lock()
	defer r.errorsMutex.Unlock()
	if len(r.Errors) < 100 { // Limit error storage
		r.Errors = append(r.Errors, err)
	}
}

func NewStressTestConfig() *StressTestConfig {
	return &StressTestConfig{
		ServerHost:         "localhost",
		ServerPort:         5433,
		MaxConnections:     10000,
		ConcurrentClients:  1000,
		RequestsPerClient:  10,
		ConnectionDuration: 30 * time.Second,
		RequestDelay:       100 * time.Millisecond,
	}
}

func runConnectionStressTest(config *StressTestConfig) *StressTestResults {
	results := &StressTestResults{
		StartTime:       time.Now(),
		MinResponseTime: int64(^uint64(0) >> 1), // Max int64
		Errors:         make([]string, 0),
	}

	fmt.Printf("🚀 STARTING FASTPOSTGRES CONNECTION STRESS TEST\n")
	fmt.Printf("   Server: %s:%d\n", config.ServerHost, config.ServerPort)
	fmt.Printf("   Target Connections: %d\n", config.MaxConnections)
	fmt.Printf("   Concurrent Clients: %d\n", config.ConcurrentClients)
	fmt.Printf("   Requests per Client: %d\n", config.RequestsPerClient)
	fmt.Println("   " + strings.Repeat("-", 60))

	var wg sync.WaitGroup

	// Launch concurrent clients
	for i := int64(0); i < config.ConcurrentClients; i++ {
		wg.Add(1)
		go func(clientID int64) {
			defer wg.Done()
			runClient(clientID, config, results)
		}(i)

		// Stagger connection attempts to avoid overwhelming the server
		if i%1000 == 0 && i > 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Monitor progress
	go monitorProgress(results, config)

	wg.Wait()
	results.EndTime = time.Now()

	return results
}

func runClient(clientID int64, config *StressTestConfig, results *StressTestResults) {
	// Connect to server
	addr := fmt.Sprintf("%s:%d", config.ServerHost, config.ServerPort)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		atomic.AddInt64(&results.FailedConnections, 1)
		results.AddError(fmt.Sprintf("Client %d: Connection failed: %v", clientID, err))
		return
	}
	defer conn.Close()

	atomic.AddInt64(&results.TotalConnections, 1)
	atomic.AddInt64(&results.SuccessfulConnections, 1)

	reader := bufio.NewReader(conn)

	// Send requests
	for i := int64(0); i < config.RequestsPerClient; i++ {
		startTime := time.Now()

		// Send request
		request := fmt.Sprintf("PING client-%d-req-%d", clientID, i)
		_, err := conn.Write([]byte(request + "\n"))
		if err != nil {
			atomic.AddInt64(&results.FailedRequests, 1)
			results.AddError(fmt.Sprintf("Client %d: Write failed: %v", clientID, err))
			continue
		}

		atomic.AddInt64(&results.BytesSent, int64(len(request)+1))
		atomic.AddInt64(&results.TotalRequests, 1)

		// Read response
		response, err := reader.ReadString('\n')
		if err != nil {
			atomic.AddInt64(&results.FailedRequests, 1)
			results.AddError(fmt.Sprintf("Client %d: Read failed: %v", clientID, err))
			continue
		}

		responseTime := time.Since(startTime).Nanoseconds()
		atomic.AddInt64(&results.TotalResponseTime, responseTime)
		atomic.AddInt64(&results.BytesReceived, int64(len(response)))
		atomic.AddInt64(&results.SuccessfulRequests, 1)

		// Update min/max response times
		for {
			currentMin := atomic.LoadInt64(&results.MinResponseTime)
			if responseTime >= currentMin || atomic.CompareAndSwapInt64(&results.MinResponseTime, currentMin, responseTime) {
				break
			}
		}
		for {
			currentMax := atomic.LoadInt64(&results.MaxResponseTime)
			if responseTime <= currentMax || atomic.CompareAndSwapInt64(&results.MaxResponseTime, currentMax, responseTime) {
				break
			}
		}

		// Optional delay between requests
		if config.RequestDelay > 0 {
			time.Sleep(config.RequestDelay)
		}
	}

	// Keep connection alive for specified duration
	if config.ConnectionDuration > 0 {
		time.Sleep(config.ConnectionDuration)
	}
}

func monitorProgress(results *StressTestResults, config *StressTestConfig) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			successful := atomic.LoadInt64(&results.SuccessfulConnections)
			failed := atomic.LoadInt64(&results.FailedConnections)
			requests := atomic.LoadInt64(&results.TotalRequests)
			successfulReqs := atomic.LoadInt64(&results.SuccessfulRequests)

			if successful+failed >= config.ConcurrentClients {
				return // Test completed
			}

			elapsed := time.Since(results.StartTime)
			fmt.Printf("⏱️  [%v] Connections: %d✅ %d❌ | Requests: %d total, %d✅\n",
				elapsed.Round(time.Second), successful, failed, requests, successfulReqs)

			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			fmt.Printf("   Memory: %.2f MB | Goroutines: %d\n",
				float64(m.Alloc)/(1024*1024), runtime.NumGoroutine())
		}
	}
}

func printResults(results *StressTestResults) {
	duration := results.EndTime.Sub(results.StartTime)

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("📊 FASTPOSTGRES CONNECTION STRESS TEST RESULTS")
	fmt.Println(strings.Repeat("=", 80))

	fmt.Printf("\n🕐 TIMING:\n")
	fmt.Printf("   Duration: %v\n", duration.Round(time.Millisecond))
	fmt.Printf("   Start: %v\n", results.StartTime.Format("15:04:05.000"))
	fmt.Printf("   End: %v\n", results.EndTime.Format("15:04:05.000"))

	fmt.Printf("\n🔗 CONNECTIONS:\n")
	fmt.Printf("   Total Attempted: %d\n", results.TotalConnections)
	fmt.Printf("   Successful: %d\n", results.SuccessfulConnections)
	fmt.Printf("   Failed: %d\n", results.FailedConnections)
	if results.TotalConnections > 0 {
		successRate := float64(results.SuccessfulConnections) / float64(results.TotalConnections) * 100
		fmt.Printf("   Success Rate: %.2f%%\n", successRate)
	}

	fmt.Printf("\n📨 REQUESTS:\n")
	fmt.Printf("   Total Sent: %d\n", results.TotalRequests)
	fmt.Printf("   Successful: %d\n", results.SuccessfulRequests)
	fmt.Printf("   Failed: %d\n", results.FailedRequests)
	if results.TotalRequests > 0 {
		successRate := float64(results.SuccessfulRequests) / float64(results.TotalRequests) * 100
		fmt.Printf("   Success Rate: %.2f%%\n", successRate)
	}

	fmt.Printf("\n⚡ PERFORMANCE:\n")
	if results.SuccessfulRequests > 0 {
		avgResponseTime := float64(results.TotalResponseTime) / float64(results.SuccessfulRequests) / 1000000 // Convert to ms
		fmt.Printf("   Average Response Time: %.2f ms\n", avgResponseTime)
		fmt.Printf("   Min Response Time: %.2f ms\n", float64(results.MinResponseTime)/1000000)
		fmt.Printf("   Max Response Time: %.2f ms\n", float64(results.MaxResponseTime)/1000000)

		requestsPerSec := float64(results.SuccessfulRequests) / duration.Seconds()
		fmt.Printf("   Throughput: %.0f requests/sec\n", requestsPerSec)

		connectionsPerSec := float64(results.SuccessfulConnections) / duration.Seconds()
		fmt.Printf("   Connection Rate: %.0f connections/sec\n", connectionsPerSec)
	}

	fmt.Printf("\n💾 DATA TRANSFER:\n")
	fmt.Printf("   Bytes Sent: %d (%.2f MB)\n", results.BytesSent, float64(results.BytesSent)/(1024*1024))
	fmt.Printf("   Bytes Received: %d (%.2f MB)\n", results.BytesReceived, float64(results.BytesReceived)/(1024*1024))
	fmt.Printf("   Total Transfer: %.2f MB\n", float64(results.BytesSent+results.BytesReceived)/(1024*1024))

	if len(results.Errors) > 0 {
		fmt.Printf("\n❌ ERRORS (%d total, showing first 10):\n", len(results.Errors))
		for i, err := range results.Errors {
			if i >= 10 {
				break
			}
			fmt.Printf("   %d. %s\n", i+1, err)
		}
	}

	// Final system stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("\n🖥️  SYSTEM STATS:\n")
	fmt.Printf("   Final Memory: %.2f MB\n", float64(m.Alloc)/(1024*1024))
	fmt.Printf("   Goroutines: %d\n", runtime.NumGoroutine())
	fmt.Printf("   CPUs: %d\n", runtime.NumCPU())

	fmt.Println("\n" + strings.Repeat("=", 80))
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run connection_stress_test.go <max_connections> <concurrent_clients> [requests_per_client]")
		fmt.Println("Example: go run connection_stress_test.go 10000 5000 5")
		return
	}

	maxConnections, err := strconv.ParseInt(os.Args[1], 10, 64)
	if err != nil {
		fmt.Printf("Error: Invalid max_connections: %v\n", err)
		return
	}

	concurrentClients, err := strconv.ParseInt(os.Args[2], 10, 64)
	if err != nil {
		fmt.Printf("Error: Invalid concurrent_clients: %v\n", err)
		return
	}

	config := NewStressTestConfig()
	config.MaxConnections = maxConnections
	config.ConcurrentClients = concurrentClients

	if len(os.Args) > 3 {
		requestsPerClient, err := strconv.ParseInt(os.Args[3], 10, 64)
		if err == nil {
			config.RequestsPerClient = requestsPerClient
		}
	}

	// Run the stress test
	results := runConnectionStressTest(config)
	printResults(results)

	fmt.Printf("\n🎉 Test completed! FastPostgres handled %d concurrent connections with %.2f%% success rate.\n",
		results.SuccessfulConnections,
		float64(results.SuccessfulConnections)/float64(results.TotalConnections)*100)
}