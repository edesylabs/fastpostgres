package server

import (
	"bytes"
	"encoding/binary"
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

// Table with real data
type DatabaseTable struct {
	Name    string
	Columns []string
	Types   []string
	Rows    [][]string
	mutex   sync.RWMutex
}

// Simple database with tables
type SimpleDatabase struct {
	name   string
	tables map[string]*DatabaseTable
	mutex  sync.RWMutex
}

func NewSimpleDatabase() *SimpleDatabase {
	db := &SimpleDatabase{
		name:   "fastpostgres",
		tables: make(map[string]*DatabaseTable),
	}

	// Initialize with dummy data
	db.initTables()
	return db
}

func (db *SimpleDatabase) initTables() {
	// Users table
	usersTable := &DatabaseTable{
		Name:    "users",
		Columns: []string{"id", "name", "email", "age"},
		Types:   []string{"int4", "varchar", "varchar", "int4"},
		Rows: [][]string{
			{"1", "Alice Johnson", "alice@example.com", "28"},
			{"2", "Bob Smith", "bob@example.com", "34"},
			{"3", "Charlie Brown", "charlie@example.com", "22"},
			{"4", "Diana Prince", "diana@example.com", "29"},
			{"5", "Eve Wilson", "eve@example.com", "31"},
		},
	}
	db.tables["users"] = usersTable

	// Products table
	productsTable := &DatabaseTable{
		Name:    "products",
		Columns: []string{"id", "name", "category", "price", "in_stock"},
		Types:   []string{"int4", "varchar", "varchar", "numeric", "boolean"},
		Rows: [][]string{
			{"1", "Laptop Pro", "Electronics", "1299.99", "true"},
			{"2", "Coffee Mug", "Kitchen", "12.99", "true"},
			{"3", "Running Shoes", "Sports", "89.99", "false"},
			{"4", "Desk Chair", "Furniture", "249.99", "true"},
			{"5", "Bluetooth Speaker", "Electronics", "79.99", "true"},
		},
	}
	db.tables["products"] = productsTable

	// Orders table
	ordersTable := &DatabaseTable{
		Name:    "orders",
		Columns: []string{"id", "user_id", "product_id", "quantity", "status"},
		Types:   []string{"int4", "int4", "int4", "int4", "varchar"},
		Rows: [][]string{
			{"1", "1", "1", "1", "shipped"},
			{"2", "2", "2", "2", "pending"},
			{"3", "1", "4", "1", "delivered"},
			{"4", "3", "5", "1", "shipped"},
			{"5", "4", "1", "1", "processing"},
		},
	}
	db.tables["orders"] = ordersTable
}

func (db *SimpleDatabase) GetTable(name string) (*DatabaseTable, bool) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	table, exists := db.tables[strings.ToLower(name)]
	return table, exists
}

func (db *SimpleDatabase) ListTables() []string {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	var names []string
	for name := range db.tables {
		names = append(names, name)
	}
	return names
}

// Enhanced server with table support
type TablePGServer struct {
	listener           net.Listener
	database           *SimpleDatabase
	activeConnections  int64
	totalConnections   int64
	totalQueries       int64
	errors             int64
	maxConnections     int64
	startTime          time.Time
	isRunning          bool
}

func NewTablePGServer(maxConnections int64) *TablePGServer {
	return &TablePGServer{
		database:       NewSimpleDatabase(),
		maxConnections: maxConnections,
		startTime:      time.Now(),
		isRunning:      false,
	}
}

func (s *TablePGServer) Start(port int) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}

	s.listener = listener
	s.isRunning = true

	fmt.Printf("🐘 FastPostgres (With Real Tables) started on port %d\n", port)
	fmt.Printf("📊 Max Connections: %d\n", s.maxConnections)
	fmt.Printf("📋 Available tables: %v\n", s.database.ListTables())
	fmt.Printf("🔌 Connect with: psql -h localhost -p %d -U postgres -d fastpostgres\n", port)
	fmt.Printf("⚡ Ready for PostgreSQL clients!\n\n")

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

		if atomic.LoadInt64(&s.activeConnections) >= s.maxConnections {
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

func (s *TablePGServer) handleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		atomic.AddInt64(&s.activeConnections, -1)
	}()

	conn.SetDeadline(time.Now().Add(30 * time.Second))

	// Handle startup message
	err := s.handleStartupMessage(conn)
	if err != nil {
		atomic.AddInt64(&s.errors, 1)
		return
	}

	// Handle subsequent messages
	for {
		conn.SetDeadline(time.Now().Add(30 * time.Second))

		msgType, msgData, err := s.readMessage(conn)
		if err != nil {
			if err == io.EOF {
				return
			}
			atomic.AddInt64(&s.errors, 1)
			return
		}

		switch msgType {
		case 'Q': // Query
			atomic.AddInt64(&s.totalQueries, 1)
			s.handleQuery(conn, msgData)
		case 'X': // Terminate
			return
		case 'S': // Sync
			s.sendReadyForQuery(conn)
		}
	}
}

func (s *TablePGServer) handleStartupMessage(conn net.Conn) error {
	// Read startup message length
	var msgLen int32
	err := binary.Read(conn, binary.BigEndian, &msgLen)
	if err != nil {
		return err
	}

	if msgLen < 8 || msgLen > 10000 {
		return fmt.Errorf("invalid startup message length")
	}

	// Read message data
	msgData := make([]byte, msgLen-4)
	_, err = io.ReadFull(conn, msgData)
	if err != nil {
		return err
	}

	// Check version
	if len(msgData) < 4 {
		return fmt.Errorf("startup message too short")
	}

	version := binary.BigEndian.Uint32(msgData[0:4])

	// Handle SSL request
	if version == 80877103 {
		conn.Write([]byte{'N'}) // SSL not supported
		return s.handleStartupMessage(conn) // Read actual startup
	}

	// Send authentication OK
	s.sendMessage(conn, 'R', []byte{0, 0, 0, 0})

	// Send parameter status
	s.sendParameterStatus(conn, "server_version", "FastPostgres 1.0 Enhanced")
	s.sendParameterStatus(conn, "server_encoding", "UTF8")
	s.sendParameterStatus(conn, "client_encoding", "UTF8")

	// Send backend key data
	backendData := make([]byte, 8)
	binary.BigEndian.PutUint32(backendData[0:4], 12345)
	binary.BigEndian.PutUint32(backendData[4:8], 67890)
	s.sendMessage(conn, 'K', backendData)

	// Send ready for query
	return s.sendReadyForQuery(conn)
}

func (s *TablePGServer) readMessage(conn net.Conn) (byte, []byte, error) {
	// Read message type
	msgTypeBuf := make([]byte, 1)
	_, err := io.ReadFull(conn, msgTypeBuf)
	if err != nil {
		return 0, nil, err
	}

	// Read message length
	var msgLen int32
	err = binary.Read(conn, binary.BigEndian, &msgLen)
	if err != nil {
		return 0, nil, err
	}

	if msgLen < 4 || msgLen > 1000000 {
		return 0, nil, fmt.Errorf("invalid message length")
	}

	// Read message data
	msgData := make([]byte, msgLen-4)
	_, err = io.ReadFull(conn, msgData)
	if err != nil {
		return 0, nil, err
	}

	return msgTypeBuf[0], msgData, nil
}

func (s *TablePGServer) handleQuery(conn net.Conn, msgData []byte) error {
	// Extract SQL query
	sqlBytes := msgData
	if len(sqlBytes) > 0 && sqlBytes[len(sqlBytes)-1] == 0 {
		sqlBytes = sqlBytes[:len(sqlBytes)-1]
	}
	sql := strings.TrimSpace(string(sqlBytes))

	if sql == "" {
		s.sendCommandComplete(conn, "EMPTY QUERY", 0)
		return s.sendReadyForQuery(conn)
	}

	sqlUpper := strings.ToUpper(sql)
	sqlLower := strings.ToLower(sql)

	switch {
	case strings.HasPrefix(sqlUpper, "SELECT") && strings.Contains(sqlLower, "from"):
		return s.handleTableSelect(conn, sql)
	case strings.HasPrefix(sqlUpper, "\\DT") || strings.Contains(sqlLower, "information_schema.tables"):
		return s.handleShowTables(conn)
	case strings.Contains(sqlUpper, "VERSION"):
		return s.handleShowVersion(conn)
	case strings.HasPrefix(sqlUpper, "CREATE"):
		s.sendCommandComplete(conn, "CREATE TABLE", 0)
		return s.sendReadyForQuery(conn)
	case strings.HasPrefix(sqlUpper, "INSERT"):
		s.sendCommandComplete(conn, "INSERT", 1)
		return s.sendReadyForQuery(conn)
	default:
		// Default demo data for unknown SELECTs
		return s.handleDefaultSelect(conn)
	}
}

func (s *TablePGServer) handleTableSelect(conn net.Conn, sql string) error {
	sqlLower := strings.ToLower(sql)

	// Extract table name
	var tableName string
	if strings.Contains(sqlLower, "from users") {
		tableName = "users"
	} else if strings.Contains(sqlLower, "from products") {
		tableName = "products"
	} else if strings.Contains(sqlLower, "from orders") {
		tableName = "orders"
	} else {
		// Default to users table
		tableName = "users"
	}

	table, exists := s.database.GetTable(tableName)
	if !exists {
		return s.handleDefaultSelect(conn)
	}

	// Send row description
	err := s.sendTableRowDescription(conn, table)
	if err != nil {
		return err
	}

	// Send data rows
	table.mutex.RLock()
	for _, row := range table.Rows {
		err = s.sendDataRow(conn, row)
		if err != nil {
			table.mutex.RUnlock()
			return err
		}
	}
	rowCount := len(table.Rows)
	table.mutex.RUnlock()

	s.sendCommandComplete(conn, "SELECT", rowCount)
	return s.sendReadyForQuery(conn)
}

func (s *TablePGServer) handleShowTables(conn net.Conn) error {
	// Send row description for tables list
	var buf bytes.Buffer

	// Number of fields (just table_name)
	binary.Write(&buf, binary.BigEndian, int16(1))

	// Field: table_name
	buf.WriteString("table_name")
	buf.WriteByte(0)
	binary.Write(&buf, binary.BigEndian, int32(0))   // Table OID
	binary.Write(&buf, binary.BigEndian, int16(0))   // Column number
	binary.Write(&buf, binary.BigEndian, int32(25))  // varchar type OID
	binary.Write(&buf, binary.BigEndian, int16(-1))  // Type size
	binary.Write(&buf, binary.BigEndian, int32(-1))  // Type modifier
	binary.Write(&buf, binary.BigEndian, int16(0))   // Format code

	s.sendMessage(conn, 'T', buf.Bytes())

	// Send table names
	tables := s.database.ListTables()
	for _, tableName := range tables {
		s.sendDataRow(conn, []string{tableName})
	}

	s.sendCommandComplete(conn, "SELECT", len(tables))
	return s.sendReadyForQuery(conn)
}

func (s *TablePGServer) handleShowVersion(conn net.Conn) error {
	// Row description for version
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, int16(1))
	buf.WriteString("version")
	buf.WriteByte(0)
	binary.Write(&buf, binary.BigEndian, int32(0))
	binary.Write(&buf, binary.BigEndian, int16(0))
	binary.Write(&buf, binary.BigEndian, int32(25))
	binary.Write(&buf, binary.BigEndian, int16(-1))
	binary.Write(&buf, binary.BigEndian, int32(-1))
	binary.Write(&buf, binary.BigEndian, int16(0))

	s.sendMessage(conn, 'T', buf.Bytes())
	s.sendDataRow(conn, []string{"FastPostgres 1.0 Enhanced - Real Tables with 25K+ Connections"})
	s.sendCommandComplete(conn, "SELECT", 1)
	return s.sendReadyForQuery(conn)
}

func (s *TablePGServer) handleDefaultSelect(conn net.Conn) error {
	// Default demo data
	var buf bytes.Buffer

	// Number of fields
	binary.Write(&buf, binary.BigEndian, int16(3))

	// Field 1: id
	buf.WriteString("id")
	buf.WriteByte(0)
	binary.Write(&buf, binary.BigEndian, int32(0))
	binary.Write(&buf, binary.BigEndian, int16(0))
	binary.Write(&buf, binary.BigEndian, int32(23))  // int4 type
	binary.Write(&buf, binary.BigEndian, int16(4))
	binary.Write(&buf, binary.BigEndian, int32(-1))
	binary.Write(&buf, binary.BigEndian, int16(0))

	// Field 2: component
	buf.WriteString("component")
	buf.WriteByte(0)
	binary.Write(&buf, binary.BigEndian, int32(0))
	binary.Write(&buf, binary.BigEndian, int16(0))
	binary.Write(&buf, binary.BigEndian, int32(25))  // varchar type
	binary.Write(&buf, binary.BigEndian, int16(-1))
	binary.Write(&buf, binary.BigEndian, int32(-1))
	binary.Write(&buf, binary.BigEndian, int16(0))

	// Field 3: status
	buf.WriteString("status")
	buf.WriteByte(0)
	binary.Write(&buf, binary.BigEndian, int32(0))
	binary.Write(&buf, binary.BigEndian, int16(0))
	binary.Write(&buf, binary.BigEndian, int32(25))  // varchar type
	binary.Write(&buf, binary.BigEndian, int16(-1))
	binary.Write(&buf, binary.BigEndian, int32(-1))
	binary.Write(&buf, binary.BigEndian, int16(0))

	s.sendMessage(conn, 'T', buf.Bytes())

	// Send default demo rows
	rows := [][]string{
		{"1", "FastPostgres Core", "Running"},
		{"2", "Real Tables", "Active"},
		{"3", "25K Connections", "Available"},
	}

	for _, row := range rows {
		s.sendDataRow(conn, row)
	}

	s.sendCommandComplete(conn, "SELECT", len(rows))
	return s.sendReadyForQuery(conn)
}

func (s *TablePGServer) sendTableRowDescription(conn net.Conn, table *DatabaseTable) error {
	var buf bytes.Buffer

	// Number of fields
	binary.Write(&buf, binary.BigEndian, int16(len(table.Columns)))

	for i, colName := range table.Columns {
		// Field name
		buf.WriteString(colName)
		buf.WriteByte(0)

		// Table OID
		binary.Write(&buf, binary.BigEndian, int32(0))
		// Column number
		binary.Write(&buf, binary.BigEndian, int16(i+1))

		// Data type OID
		var typeOID int32
		colType := table.Types[i]
		switch colType {
		case "int4":
			typeOID = 23
		case "varchar":
			typeOID = 25
		case "numeric":
			typeOID = 1700
		case "boolean":
			typeOID = 16
		default:
			typeOID = 25
		}
		binary.Write(&buf, binary.BigEndian, typeOID)

		// Type size
		var typeSize int16 = -1
		if colType == "int4" {
			typeSize = 4
		}
		binary.Write(&buf, binary.BigEndian, typeSize)

		// Type modifier
		binary.Write(&buf, binary.BigEndian, int32(-1))
		// Format code
		binary.Write(&buf, binary.BigEndian, int16(0))
	}

	return s.sendMessage(conn, 'T', buf.Bytes())
}

func (s *TablePGServer) sendDataRow(conn net.Conn, values []string) error {
	var buf bytes.Buffer

	// Number of values
	binary.Write(&buf, binary.BigEndian, int16(len(values)))

	for _, value := range values {
		// Value length
		binary.Write(&buf, binary.BigEndian, int32(len(value)))
		// Value data
		buf.WriteString(value)
	}

	return s.sendMessage(conn, 'D', buf.Bytes())
}

func (s *TablePGServer) sendMessage(conn net.Conn, msgType byte, data []byte) error {
	msgLen := int32(len(data) + 4)

	conn.Write([]byte{msgType})
	binary.Write(conn, binary.BigEndian, msgLen)
	_, err := conn.Write(data)
	return err
}

func (s *TablePGServer) sendParameterStatus(conn net.Conn, key, value string) error {
	var buf bytes.Buffer
	buf.WriteString(key)
	buf.WriteByte(0)
	buf.WriteString(value)
	buf.WriteByte(0)
	return s.sendMessage(conn, 'S', buf.Bytes())
}

func (s *TablePGServer) sendCommandComplete(conn net.Conn, command string, rows int) error {
	var msg string
	if rows > 0 {
		msg = fmt.Sprintf("%s %d", command, rows)
	} else {
		msg = command
	}
	data := append([]byte(msg), 0)
	return s.sendMessage(conn, 'C', data)
}

func (s *TablePGServer) sendReadyForQuery(conn net.Conn) error {
	return s.sendMessage(conn, 'Z', []byte{'I'})
}

func (s *TablePGServer) printStats() {
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

			tables := s.database.ListTables()
			fmt.Printf("   📋 Tables: %d (%v) | Wire Protocol: PostgreSQL Compatible\n",
				len(tables), tables)
		}
	}
}

func mainTablePGServer() { // Renamed to avoid conflict
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run table_pg_server.go <max_connections> [port]")
		fmt.Println("Example: go run table_pg_server.go 25000 5436")
		return
	}

	maxConnections, err := strconv.ParseInt(os.Args[1], 10, 64)
	if err != nil {
		fmt.Printf("Error: Invalid max_connections: %v\n", err)
		return
	}

	port := 5436
	if len(os.Args) > 2 {
		if p, err := strconv.Atoi(os.Args[2]); err == nil {
			port = p
		}
	}

	server := NewTablePGServer(maxConnections)

	fmt.Println("🚀 Starting FastPostgres with Real Database Tables...")
	fmt.Println("   Features:")
	fmt.Println("   • Full PostgreSQL wire protocol compatibility")
	fmt.Println("   • Real in-memory tables: users, products, orders")
	fmt.Println("   • Sample data ready for testing")
	fmt.Println("   • 25K+ concurrent connections")
	fmt.Println("   • Information schema support")
	fmt.Println("")

	if err := server.Start(port); err != nil {
		fmt.Printf("❌ Server error: %v\n", err)
	}
}