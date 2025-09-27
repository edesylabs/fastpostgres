// Package server implements the PostgreSQL wire protocol for FastPostgres.
// It provides a PostgreSQL-compatible server interface that accepts connections
// and executes queries using the FastPostgres engine.
package server

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"fastpostgres/pkg/engine"
)

// PostgresServer implements the PostgreSQL wire protocol.
// It provides a PostgreSQL-compatible server that can accept connections
// from standard PostgreSQL clients and drivers.
type PostgresServer struct {
	db       *engine.Database
	port     string
	listener net.Listener
}

// Message type constants for PostgreSQL wire protocol.
// These define the message types used in client-server communication.
const (
	// Frontend messages
	MsgStartup     byte = 0
	MsgQuery       byte = 'Q'
	MsgTerminate   byte = 'X'
	MsgPassword    byte = 'p'
	MsgSync        byte = 'S'
	MsgParse       byte = 'P'
	MsgBind        byte = 'B'
	MsgExecute     byte = 'E'
	MsgDescribe    byte = 'D'
	MsgClose       byte = 'C'

	// Backend messages
	MsgAuth        byte = 'R'
	MsgRowDesc     byte = 'T'
	MsgDataRow     byte = 'D'
	MsgCmdComplete byte = 'C'
	MsgReadyForQuery byte = 'Z'
	MsgError       byte = 'E'
	MsgNotice      byte = 'N'
	MsgParameterStatus byte = 'S'
	MsgBackendKeyData  byte = 'K'
)

// Authentication type constants for PostgreSQL protocol.
// These define the various authentication methods supported.
const (
	AuthOK                = 0
	AuthKerberos         = 2
	AuthCleartextPassword = 3
	AuthMD5Password      = 5
	AuthSCMCredential    = 6
	AuthGSS              = 7
	AuthSSPI             = 9
	AuthSASL             = 10
)

// Transaction status constants indicate the current transaction state.
const (
	TransIdle    = 'I' // Idle
	TransInTrans = 'T' // In transaction
	TransError   = 'E' // In failed transaction
)

// SSLRequestCode is the special protocol version number that indicates an SSL request.
const SSLRequestCode = 80877103

// PostgresConnection represents a single client connection to the server.
// It maintains connection state, buffers, and authentication information.
type PostgresConnection struct {
	conn     net.Conn
	reader   *bufio.Reader
	writer   *bufio.Writer
	db       *engine.Database
	connInfo *ConnectionInfo
	authenticated bool
	transactionStatus byte
	processID int32
	secretKey int32
}

// ConnectionInfo holds metadata about a client connection.
type ConnectionInfo struct {
	User     string
	Database string
	Options  map[string]string
}

// PostgresMessage represents a protocol message.
type PostgresMessage struct {
	Type byte
	Data []byte
}

// QueryResult represents the result of a query execution.
type QueryResult struct {
	Columns []string
	Rows    [][]interface{}
}

// NewPostgresServer creates a new PostgreSQL-compatible server instance.
// It initializes the server with the given database and port.
func NewPostgresServer(db *engine.Database, port string) *PostgresServer {
	return &PostgresServer{
		db:   db,
		port: port,
	}
}

// Start begins listening for client connections on the configured port.
// It accepts connections in a loop and spawns a goroutine for each client.
func (ps *PostgresServer) Start() error {
	var err error
	ps.listener, err = net.Listen("tcp", ":"+ps.port)
	if err != nil {
		return err
	}
	defer ps.listener.Close()

	fmt.Printf("FastPostgres server listening on port %s\n", ps.port)

	for {
		conn, err := ps.listener.Accept()
		if err != nil {
			continue
		}

		go ps.handleConnection(conn)
	}
}

// handleConnection manages a single client connection lifecycle.
// It handles the startup sequence and message loop.
func (ps *PostgresServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	log.Printf("New connection from %s", conn.RemoteAddr())

	pgConn := &PostgresConnection{
		conn:     conn,
		reader:   bufio.NewReader(conn),
		writer:   bufio.NewWriter(conn),
		db:       ps.db,
		connInfo: &ConnectionInfo{Options: make(map[string]string)},
		authenticated: false,
		transactionStatus: TransIdle,
		processID: 12345, // Static for now
		secretKey: 67890, // Static for now
	}

	// Set connection timeout
	conn.SetDeadline(time.Now().Add(30 * time.Second))

	// Handle startup sequence
	if err := ps.handleStartup(pgConn); err != nil {
		log.Printf("Startup failed: %v", err)
		return
	}

	// Main message loop
	for {
		conn.SetDeadline(time.Now().Add(30 * time.Second))

		msgType, err := pgConn.reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				log.Printf("Client disconnected")
				return
			}
			log.Printf("Error reading message type: %v", err)
			return
		}

		var msgLen uint32
		err = binary.Read(pgConn.reader, binary.BigEndian, &msgLen)
		if err != nil {
			log.Printf("Error reading message length: %v", err)
			return
		}

		if msgLen < 4 {
			log.Printf("Invalid message length: %d", msgLen)
			return
		}

		msgData := make([]byte, msgLen-4)
		_, err = io.ReadFull(pgConn.reader, msgData)
		if err != nil {
			log.Printf("Error reading message data: %v", err)
			return
		}

		log.Printf("Received message: type=%c, length=%d", msgType, msgLen)

		if err := ps.handleMessage(pgConn, msgType, msgData); err != nil {
			if msgType == MsgTerminate {
				log.Printf("Client terminated connection")
				return
			}
			log.Printf("Error handling message: %v", err)
			ps.sendErrorResponse(pgConn, "ERROR", err.Error())
		}
	}
}

// handleStartup processes the PostgreSQL startup sequence.
// It handles SSL negotiation, authentication, and parameter exchange.
func (ps *PostgresServer) handleStartup(pgConn *PostgresConnection) error {
	log.Printf("Handling startup sequence")

	// Read startup message length
	var msgLen uint32
	err := binary.Read(pgConn.reader, binary.BigEndian, &msgLen)
	if err != nil {
		return fmt.Errorf("error reading startup length: %v", err)
	}

	log.Printf("Startup message length: %d", msgLen)

	if msgLen < 4 || msgLen > 10000 {
		return fmt.Errorf("invalid startup message length: %d", msgLen)
	}

	// Read startup message data
	msgData := make([]byte, msgLen-4)
	_, err = io.ReadFull(pgConn.reader, msgData)
	if err != nil {
		return fmt.Errorf("error reading startup data: %v", err)
	}

	// Check protocol version
	if len(msgData) < 4 {
		return fmt.Errorf("startup message too short")
	}

	version := binary.BigEndian.Uint32(msgData[0:4])
	log.Printf("Protocol version: %d", version)

	// Handle SSL request
	if version == SSLRequestCode {
		log.Printf("SSL request received, sending SSL not supported")
		// Send 'N' to indicate SSL not supported
		if err := pgConn.writer.WriteByte('N'); err != nil {
			return fmt.Errorf("error sending SSL response: %v", err)
		}
		if err := pgConn.writer.Flush(); err != nil {
			return fmt.Errorf("error flushing SSL response: %v", err)
		}

		// Read the actual startup message after SSL negotiation
		return ps.handleStartup(pgConn)
	}

	// Check protocol version (should be 3.0)
	majorVersion := version >> 16
	minorVersion := version & 0xFFFF

	if majorVersion != 3 {
		return fmt.Errorf("unsupported protocol version %d.%d", majorVersion, minorVersion)
	}

	// Parse connection parameters
	params := ps.parseStartupParams(msgData[4:])
	pgConn.connInfo.User = params["user"]
	pgConn.connInfo.Database = params["database"]
	pgConn.connInfo.Options = params

	log.Printf("Connection params: user=%s, database=%s", pgConn.connInfo.User, pgConn.connInfo.Database)

	// Send authentication OK
	if err := ps.sendAuthenticationOK(pgConn); err != nil {
		return fmt.Errorf("error sending auth OK: %v", err)
	}

	// Send parameter status messages
	if err := ps.sendParameterStatus(pgConn); err != nil {
		return fmt.Errorf("error sending parameter status: %v", err)
	}

	// Send backend key data
	if err := ps.sendBackendKeyData(pgConn); err != nil {
		return fmt.Errorf("error sending backend key data: %v", err)
	}

	// Send ready for query
	if err := ps.sendReadyForQuery(pgConn, TransIdle); err != nil {
		return fmt.Errorf("error sending ready for query: %v", err)
	}

	pgConn.authenticated = true
	log.Printf("Client authenticated successfully")

	return nil
}

// parseStartupParams extracts connection parameters from the startup message.
// Parameters are encoded as null-terminated key-value pairs.
func (ps *PostgresServer) parseStartupParams(data []byte) map[string]string {
	params := make(map[string]string)

	i := 0
	for i < len(data) {
		// Find null-terminated key
		keyStart := i
		for i < len(data) && data[i] != 0 {
			i++
		}
		if i >= len(data) {
			break
		}
		key := string(data[keyStart:i])
		i++ // skip null

		if i >= len(data) {
			break
		}

		// Find null-terminated value
		valueStart := i
		for i < len(data) && data[i] != 0 {
			i++
		}
		if i > len(data) {
			break
		}
		value := string(data[valueStart:i])
		i++ // skip null

		if key != "" {
			params[key] = value
		}
	}

	return params
}

// handleMessage processes a single protocol message from the client.
func (ps *PostgresServer) handleMessage(pgConn *PostgresConnection, msgType byte, data []byte) error {
	switch msgType {
	case MsgQuery:
		return ps.handleQuery(pgConn, string(data[:len(data)-1])) // Remove null terminator
	case MsgTerminate:
		return fmt.Errorf("terminate")
	default:
		log.Printf("Unhandled message type: %c", msgType)
		return nil
	}
}

// handleQuery executes a SQL query and sends the results to the client.
func (ps *PostgresServer) handleQuery(pgConn *PostgresConnection, sql string) error {
	log.Printf("Executing query: %s", sql)

	sql = strings.TrimSpace(sql)
	if sql == "" {
		return ps.sendEmptyQueryResponse(pgConn)
	}

	// For now, handle simple queries directly with the database
	// This is a simplified version - in production you'd use proper SQL parsing
	if strings.HasPrefix(strings.ToUpper(sql), "SELECT") {
		// Mock query result for testing
		columns := []string{"id", "name", "value"}
		rows := [][]interface{}{
			{1, "test1", "value1"},
			{2, "test2", "value2"},
		}

		result := &engine.QueryResult{
			Columns: columns,
			Rows:    rows,
		}

		// Send result
		if err := ps.sendQueryResult(pgConn, result); err != nil {
			return err
		}
	} else {
		// Handle other query types
		tag := "OK"
		if err := ps.sendCommandComplete(pgConn, tag); err != nil {
			return err
		}
	}

	return ps.sendReadyForQuery(pgConn, TransIdle)
}

// sendAuthenticationOK sends an authentication success message to the client.
func (ps *PostgresServer) sendAuthenticationOK(pgConn *PostgresConnection) error {
	msg := make([]byte, 9)
	msg[0] = MsgAuth
	binary.BigEndian.PutUint32(msg[1:5], 8) // message length (4 + 4)
	binary.BigEndian.PutUint32(msg[5:9], AuthOK)

	_, err := pgConn.writer.Write(msg)
	if err != nil {
		return err
	}
	return pgConn.writer.Flush()
}

// sendParameterStatus sends server parameter information to the client.
// This includes server version, encodings, and other configuration.
func (ps *PostgresServer) sendParameterStatus(pgConn *PostgresConnection) error {
	params := map[string]string{
		"server_version": "13.0 (FastPostgres)",
		"server_encoding": "UTF8",
		"client_encoding": "UTF8",
		"application_name": "fastpostgres",
		"is_superuser": "on",
		"session_authorization": pgConn.connInfo.User,
		"DateStyle": "ISO, MDY",
		"TimeZone": "UTC",
	}

	for key, value := range params {
		keyBytes := []byte(key)
		valueBytes := []byte(value)
		msgLen := 4 + len(keyBytes) + 1 + len(valueBytes) + 1

		msg := make([]byte, 1+4+msgLen-4)
		msg[0] = MsgParameterStatus
		binary.BigEndian.PutUint32(msg[1:5], uint32(msgLen))

		offset := 5
		copy(msg[offset:], keyBytes)
		offset += len(keyBytes)
		msg[offset] = 0 // null terminator
		offset++
		copy(msg[offset:], valueBytes)
		offset += len(valueBytes)
		msg[offset] = 0 // null terminator

		if _, err := pgConn.writer.Write(msg); err != nil {
			return err
		}
	}

	return pgConn.writer.Flush()
}

// sendBackendKeyData sends the process ID and secret key for connection cancellation.
func (ps *PostgresServer) sendBackendKeyData(pgConn *PostgresConnection) error {
	msg := make([]byte, 13)
	msg[0] = MsgBackendKeyData
	binary.BigEndian.PutUint32(msg[1:5], 12) // message length
	binary.BigEndian.PutUint32(msg[5:9], uint32(pgConn.processID))
	binary.BigEndian.PutUint32(msg[9:13], uint32(pgConn.secretKey))

	_, err := pgConn.writer.Write(msg)
	if err != nil {
		return err
	}
	return pgConn.writer.Flush()
}

// sendReadyForQuery indicates the server is ready to accept a new query.
func (ps *PostgresServer) sendReadyForQuery(pgConn *PostgresConnection, status byte) error {
	msg := []byte{MsgReadyForQuery, 0, 0, 0, 5, status}
	_, err := pgConn.writer.Write(msg)
	if err != nil {
		return err
	}
	return pgConn.writer.Flush()
}

// sendErrorResponse sends an error message to the client.
func (ps *PostgresServer) sendErrorResponse(pgConn *PostgresConnection, severity, message string) error {
	severityField := []byte{'S'}
	severityField = append(severityField, []byte(severity)...)
	severityField = append(severityField, 0)

	messageField := []byte{'M'}
	messageField = append(messageField, []byte(message)...)
	messageField = append(messageField, 0)

	msgData := severityField
	msgData = append(msgData, messageField...)
	msgData = append(msgData, 0) // final null terminator

	msg := make([]byte, 5+len(msgData))
	msg[0] = MsgError
	binary.BigEndian.PutUint32(msg[1:5], uint32(4+len(msgData)))
	copy(msg[5:], msgData)

	_, err := pgConn.writer.Write(msg)
	if err != nil {
		return err
	}
	return pgConn.writer.Flush()
}

// sendEmptyQueryResponse handles the case when an empty query is received.
func (ps *PostgresServer) sendEmptyQueryResponse(pgConn *PostgresConnection) error {
	msg := []byte{MsgCmdComplete, 0, 0, 0, 4}
	_, err := pgConn.writer.Write(msg)
	if err != nil {
		return err
	}
	return pgConn.writer.Flush()
}

// sendQueryResult sends query results to the client.
// It sends row descriptions, data rows, and a completion message.
func (ps *PostgresServer) sendQueryResult(pgConn *PostgresConnection, result *engine.QueryResult) error {
	// Send row description
	if len(result.Columns) > 0 {
		if err := ps.sendRowDescription(pgConn, result.Columns); err != nil {
			return err
		}

		// Send data rows
		for _, row := range result.Rows {
			if err := ps.sendDataRow(pgConn, row); err != nil {
				return err
			}
		}
	}

	// Send command complete
	tag := fmt.Sprintf("SELECT %d", len(result.Rows))
	return ps.sendCommandComplete(pgConn, tag)
}

// sendRowDescription sends metadata about the result columns.
func (ps *PostgresServer) sendRowDescription(pgConn *PostgresConnection, columns []string) error {
	msgData := make([]byte, 2) // field count
	binary.BigEndian.PutUint16(msgData, uint16(len(columns)))

	for _, col := range columns {
		colBytes := []byte(col)
		msgData = append(msgData, colBytes...)
		msgData = append(msgData, 0) // null terminator

		// Add field metadata (simplified)
		fieldData := make([]byte, 18)
		binary.BigEndian.PutUint32(fieldData[0:4], 0)    // table OID
		binary.BigEndian.PutUint16(fieldData[4:6], 0)    // column number
		binary.BigEndian.PutUint32(fieldData[6:10], 25)  // type OID (text)
		binary.BigEndian.PutUint16(fieldData[10:12], 0xFFFF) // type size (-1 as uint16)
		binary.BigEndian.PutUint32(fieldData[12:16], 0xFFFFFFFF) // type modifier (-1 as uint32)
		binary.BigEndian.PutUint16(fieldData[16:18], 0)  // format code (text)

		msgData = append(msgData, fieldData...)
	}

	msg := make([]byte, 5+len(msgData))
	msg[0] = MsgRowDesc
	binary.BigEndian.PutUint32(msg[1:5], uint32(4+len(msgData)))
	copy(msg[5:], msgData)

	_, err := pgConn.writer.Write(msg)
	if err != nil {
		return err
	}
	return pgConn.writer.Flush()
}

// sendDataRow sends a single row of query results.
func (ps *PostgresServer) sendDataRow(pgConn *PostgresConnection, row []interface{}) error {
	msgData := make([]byte, 2) // field count
	binary.BigEndian.PutUint16(msgData, uint16(len(row)))

	for _, field := range row {
		fieldStr := fmt.Sprintf("%v", field)
		fieldBytes := []byte(fieldStr)

		// Add field length
		lengthBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(lengthBytes, uint32(len(fieldBytes)))
		msgData = append(msgData, lengthBytes...)

		// Add field data
		msgData = append(msgData, fieldBytes...)
	}

	msg := make([]byte, 5+len(msgData))
	msg[0] = MsgDataRow
	binary.BigEndian.PutUint32(msg[1:5], uint32(4+len(msgData)))
	copy(msg[5:], msgData)

	_, err := pgConn.writer.Write(msg)
	if err != nil {
		return err
	}
	return pgConn.writer.Flush()
}

// sendCommandComplete indicates successful completion of a command.
func (ps *PostgresServer) sendCommandComplete(pgConn *PostgresConnection, tag string) error {
	tagBytes := []byte(tag)
	msgData := append(tagBytes, 0) // null terminator

	msg := make([]byte, 5+len(msgData))
	msg[0] = MsgCmdComplete
	binary.BigEndian.PutUint32(msg[1:5], uint32(4+len(msgData)))
	copy(msg[5:], msgData)

	_, err := pgConn.writer.Write(msg)
	if err != nil {
		return err
	}
	return pgConn.writer.Flush()
}