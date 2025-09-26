package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"fastpostgres/pkg/engine"
	"fastpostgres/pkg/query"
)

// PostgreSQL Wire Protocol Implementation
// Based on PostgreSQL Protocol 3.0 specification

// Message types (from client to server)
const (
	MSG_STARTUP    = 0    // No message type byte, just length + version
	MSG_QUERY      = 'Q'  // Simple query
	MSG_PARSE      = 'P'  // Parse (prepare statement)
	MSG_BIND       = 'B'  // Bind parameters
	MSG_EXECUTE    = 'E'  // Execute prepared statement
	MSG_DESCRIBE   = 'D'  // Describe statement/portal
	MSG_CLOSE      = 'C'  // Close statement/portal
	MSG_FLUSH      = 'H'  // Flush
	MSG_SYNC       = 'S'  // Sync
	MSG_TERMINATE  = 'X'  // Terminate connection
	MSG_PASSWORD   = 'p'  // Password response
)

// Message types (from server to client)
const (
	MSG_AUTH_REQUEST     = 'R'  // Authentication request
	MSG_PARAMETER_STATUS = 'S'  // Parameter status
	MSG_BACKEND_KEY_DATA = 'K'  // Backend key data
	MSG_READY_FOR_QUERY  = 'Z'  // Ready for query
	MSG_ROW_DESCRIPTION  = 'T'  // Row description
	MSG_DATA_ROW         = 'D'  // Data row
	MSG_COMMAND_COMPLETE = 'C'  // Command complete
	MSG_ERROR_RESPONSE   = 'E'  // Error response
	MSG_NOTICE_RESPONSE  = 'N'  // Notice response
	MSG_PARSE_COMPLETE   = '1'  // Parse complete
	MSG_BIND_COMPLETE    = '2'  // Bind complete
	MSG_NO_DATA          = 'n'  // No data
)

// Authentication types
const (
	AUTH_OK                = 0
	AUTH_KERBEROS_V5       = 2
	AUTH_CLEARTEXT_PASSWORD = 3
	AUTH_MD5_PASSWORD      = 5
	AUTH_SCM_CREDENTIAL    = 6
	AUTH_GSS               = 7
	AUTH_SSPI              = 9
	AUTH_SASL              = 10
)

// Transaction status
const (
	TRANS_IDLE    = 'I'  // Idle (not in transaction)
	TRANS_INTRANS = 'T'  // In transaction
	TRANS_ERROR   = 'E'  // In failed transaction
)

// PostgreSQL connection state
type PGConnection struct {
	conn         net.Conn
	database     *engine.Database
	authenticated bool
	transactionStatus byte
	parameters   map[string]string
	processID    int32
	secretKey    int32
}

// PostgreSQL protocol handler
type PGProtocolHandler struct {
	database *engine.Database
	parser   *query.SQLParser
	qEngine  *query.VectorizedEngine
	processCounter int32
}

func NewPGProtocolHandler(database *engine.Database) *PGProtocolHandler {
	return &PGProtocolHandler{
		database: database,
		parser:   query.NewSQLParser(),
		qEngine:  query.NewVectorizedEngine(),
		processCounter: 1000,
	}
}

func (h *PGProtocolHandler) HandleConnection(conn net.Conn) error {
	defer conn.Close()

	log.Printf("New connection from %s", conn.RemoteAddr())

	pgConn := &PGConnection{
		conn:              conn,
		database:          h.database,
		authenticated:     false,
		transactionStatus: TRANS_IDLE,
		parameters:        make(map[string]string),
		processID:         h.processCounter,
		secretKey:         h.processCounter * 1000,
	}
	h.processCounter++

	// Set connection timeout
	conn.SetDeadline(time.Now().Add(30 * time.Second))

	// Handle startup message first
	err := h.handleStartupMessage(pgConn)
	if err != nil {
		return fmt.Errorf("startup failed: %v", err)
	}

	// Handle subsequent messages
	for {
		conn.SetDeadline(time.Now().Add(30 * time.Second))

		msgType, msgData, err := h.readMessage(conn)
		if err != nil {
			if err == io.EOF {
				return nil // Clean disconnect
			}
			return fmt.Errorf("read message failed: %v", err)
		}

		err = h.handleMessage(pgConn, msgType, msgData)
		if err != nil {
			if err.Error() == "terminate" {
				return nil // Clean termination
			}
			// Send error response
			h.sendErrorResponse(pgConn, "ERROR", err.Error())
			continue
		}
	}
}

func (h *PGProtocolHandler) handleStartupMessage(pgConn *PGConnection) error {
	log.Printf("handleStartupMessage: reading startup message")
	// Read startup message (no message type byte, just length)
	var msgLen int32
	err := binary.Read(pgConn.conn, binary.BigEndian, &msgLen)
	if err != nil {
		log.Printf("handleStartupMessage: failed to read message length: %v", err)
		return err
	}
	log.Printf("handleStartupMessage: message length = %d", msgLen)

	if msgLen < 8 || msgLen > 10000 {
		return fmt.Errorf("invalid startup message length: %d", msgLen)
	}

	// Read the rest of the startup message
	msgData := make([]byte, msgLen-4)
	_, err = io.ReadFull(pgConn.conn, msgData)
	if err != nil {
		return err
	}

	// Parse protocol version
	if len(msgData) < 4 {
		return fmt.Errorf("startup message too short")
	}

	version := binary.BigEndian.Uint32(msgData[0:4])
	majorVersion := version >> 16
	minorVersion := version & 0xFFFF

	// Check for SSL request (special case)
	if version == 80877103 {
		log.Printf("SSL request received, sending SSL not supported response")
		// SSL not supported, send 'N'
		_, err := pgConn.conn.Write([]byte{'N'})
		if err != nil {
			return fmt.Errorf("failed to send SSL response: %v", err)
		}
		log.Printf("SSL response sent, waiting for actual startup message")
		return h.handleStartupMessage(pgConn) // Read actual startup message
	}

	if majorVersion != 3 {
		return fmt.Errorf("unsupported protocol version %d.%d", majorVersion, minorVersion)
	}

	// Parse parameters (null-terminated key-value pairs)
	params := h.parseParameters(msgData[4:])
	pgConn.parameters = params

	// Send authentication OK
	err = h.sendAuthenticationOK(pgConn)
	if err != nil {
		return err
	}

	// Send parameter status messages
	err = h.sendParameterStatuses(pgConn)
	if err != nil {
		return err
	}

	// Send backend key data
	err = h.sendBackendKeyData(pgConn)
	if err != nil {
		return err
	}

	// Send ready for query
	err = h.sendReadyForQuery(pgConn, TRANS_IDLE)
	if err != nil {
		return err
	}

	pgConn.authenticated = true
	return nil
}

func (h *PGProtocolHandler) parseParameters(data []byte) map[string]string {
	params := make(map[string]string)

	for i := 0; i < len(data); {
		// Find null terminator for key
		keyStart := i
		for i < len(data) && data[i] != 0 {
			i++
		}
		if i >= len(data) {
			break
		}
		key := string(data[keyStart:i])
		i++ // Skip null terminator

		// Find null terminator for value
		valueStart := i
		for i < len(data) && data[i] != 0 {
			i++
		}
		if i >= len(data) {
			break
		}
		value := string(data[valueStart:i])
		i++ // Skip null terminator

		if key == "" {
			break // End of parameters
		}
		params[key] = value
	}

	return params
}

func (h *PGProtocolHandler) readMessage(conn net.Conn) (byte, []byte, error) {
	// Read message type
	msgTypeBuf := make([]byte, 1)
	_, err := io.ReadFull(conn, msgTypeBuf)
	if err != nil {
		return 0, nil, err
	}
	msgType := msgTypeBuf[0]

	// Read message length
	var msgLen int32
	err = binary.Read(conn, binary.BigEndian, &msgLen)
	if err != nil {
		return 0, nil, err
	}

	if msgLen < 4 || msgLen > 1000000 {
		return 0, nil, fmt.Errorf("invalid message length: %d", msgLen)
	}

	// Read message data (length includes the 4-byte length field itself)
	msgData := make([]byte, msgLen-4)
	_, err = io.ReadFull(conn, msgData)
	if err != nil {
		return 0, nil, err
	}

	return msgType, msgData, nil
}

func (h *PGProtocolHandler) handleMessage(pgConn *PGConnection, msgType byte, msgData []byte) error {
	if !pgConn.authenticated {
		return fmt.Errorf("not authenticated")
	}

	switch msgType {
	case MSG_QUERY:
		return h.handleQuery(pgConn, msgData)
	case MSG_TERMINATE:
		return fmt.Errorf("terminate")
	case MSG_SYNC:
		return h.sendReadyForQuery(pgConn, pgConn.transactionStatus)
	default:
		// For now, just acknowledge unknown messages
		return nil
	}
}

func (h *PGProtocolHandler) handleQuery(pgConn *PGConnection, msgData []byte) error {
	// Extract SQL query (null-terminated string)
	sqlBytes := msgData
	if len(sqlBytes) > 0 && sqlBytes[len(sqlBytes)-1] == 0 {
		sqlBytes = sqlBytes[:len(sqlBytes)-1]
	}
	sql := string(sqlBytes)
	sql = strings.TrimSpace(sql)

	if sql == "" {
		return h.sendCommandComplete(pgConn, "EMPTY QUERY", 0)
	}

	// Simple query execution
	return h.executeSimpleQuery(pgConn, sql)
}

func (h *PGProtocolHandler) executeSimpleQuery(pgConn *PGConnection, sql string) error {
	sqlUpper := strings.ToUpper(strings.TrimSpace(sql))

	switch {
	case strings.HasPrefix(sqlUpper, "SELECT"):
		return h.handleSelect(pgConn, sql)
	case strings.HasPrefix(sqlUpper, "INSERT"):
		return h.handleInsert(pgConn, sql)
	case strings.HasPrefix(sqlUpper, "CREATE"):
		return h.handleCreate(pgConn, sql)
	case strings.HasPrefix(sqlUpper, "DROP"):
		return h.handleDrop(pgConn, sql)
	case strings.HasPrefix(sqlUpper, "BEGIN"):
		pgConn.transactionStatus = TRANS_INTRANS
		err := h.sendCommandComplete(pgConn, "BEGIN", 0)
		if err != nil {
			return err
		}
		return h.sendReadyForQuery(pgConn, TRANS_INTRANS)
	case strings.HasPrefix(sqlUpper, "COMMIT"):
		pgConn.transactionStatus = TRANS_IDLE
		err := h.sendCommandComplete(pgConn, "COMMIT", 0)
		if err != nil {
			return err
		}
		return h.sendReadyForQuery(pgConn, TRANS_IDLE)
	case strings.HasPrefix(sqlUpper, "ROLLBACK"):
		pgConn.transactionStatus = TRANS_IDLE
		err := h.sendCommandComplete(pgConn, "ROLLBACK", 0)
		if err != nil {
			return err
		}
		return h.sendReadyForQuery(pgConn, TRANS_IDLE)
	case strings.HasPrefix(sqlUpper, "SHOW"):
		return h.handleShow(pgConn, sql)
	default:
		// Generic command
		err := h.sendCommandComplete(pgConn, "OK", 0)
		if err != nil {
			return err
		}
		return h.sendReadyForQuery(pgConn, pgConn.transactionStatus)
	}
}

func (h *PGProtocolHandler) handleSelect(pgConn *PGConnection, sql string) error {
	log.Printf("Executing SELECT: %s", sql)

	// Handle special PostgreSQL queries first
	if h.handleSpecialSelect(pgConn, sql) {
		return nil
	}

	// Parse the SQL query
	plan, err := h.parser.Parse(sql)
	if err != nil {
		log.Printf("Parse error: %v", err)
		return h.sendErrorResponse(pgConn, "ERROR", fmt.Sprintf("Parse error: %v", err))
	}

	// Get the table
	tableInterface, exists := h.database.Tables.Load(plan.TableName)
	if !exists {
		log.Printf("Table not found: %s", plan.TableName)
		return h.sendErrorResponse(pgConn, "ERROR", fmt.Sprintf("Table '%s' does not exist", plan.TableName))
	}

	table := tableInterface.(*engine.Table)

	// Execute the query
	result, err := h.qEngine.ExecuteSelect(plan, table)
	if err != nil {
		log.Printf("Execution error: %v", err)
		return h.sendErrorResponse(pgConn, "ERROR", fmt.Sprintf("Execution error: %v", err))
	}

	// Send row description
	columns := make([]ColumnInfo, len(result.Columns))
	for i, colName := range result.Columns {
		dataType := "text"
		typeSize := int32(-1)

		if i < len(result.Types) {
			switch result.Types[i] {
			case engine.TypeInt32:
				dataType = "int4"
				typeSize = 4
			case engine.TypeInt64:
				dataType = "int8"
				typeSize = 8
			case engine.TypeString:
				dataType = "text"
				typeSize = -1
			case engine.TypeFloat64:
				dataType = "float8"
				typeSize = 8
			}
		}

		columns[i] = ColumnInfo{
			Name:     colName,
			DataType: dataType,
			TypeSize: typeSize,
		}
	}

	err = h.sendRowDescription(pgConn, columns)
	if err != nil {
		return err
	}

	// Send data rows
	for _, row := range result.Rows {
		stringRow := make([]string, len(row))
		for i, value := range row {
			if value == nil {
				stringRow[i] = ""
			} else {
				stringRow[i] = fmt.Sprintf("%v", value)
			}
		}

		err = h.sendDataRow(pgConn, stringRow)
		if err != nil {
			return err
		}
	}

	// Send command complete
	err = h.sendCommandComplete(pgConn, "SELECT", len(result.Rows))
	if err != nil {
		return err
	}

	return h.sendReadyForQuery(pgConn, pgConn.transactionStatus)
}

func (h *PGProtocolHandler) handleSpecialSelect(pgConn *PGConnection, sql string) bool {
	sqlUpper := strings.ToUpper(strings.TrimSpace(sql))

	switch {
	case strings.Contains(sqlUpper, "VERSION()"):
		return h.sendVersionResult(pgConn)
	case strings.Contains(sqlUpper, "CURRENT_DATABASE()"):
		return h.sendCurrentDatabaseResult(pgConn)
	case strings.Contains(sqlUpper, "PG_CATALOG") || strings.Contains(sqlUpper, "INFORMATION_SCHEMA"):
		return h.sendEmptyResult(pgConn, "SELECT")
	case sqlUpper == "SELECT 1":
		return h.sendSelectOneResult(pgConn)
	}

	return false
}

func (h *PGProtocolHandler) sendVersionResult(pgConn *PGConnection) bool {
	h.sendRowDescription(pgConn, []ColumnInfo{
		{Name: "version", DataType: "text", TypeSize: -1},
	})
	h.sendDataRow(pgConn, []string{"FastPostgres 1.0.0 on Go, PostgreSQL 13.0 compatible"})
	h.sendCommandComplete(pgConn, "SELECT", 1)
	h.sendReadyForQuery(pgConn, pgConn.transactionStatus)
	return true
}

func (h *PGProtocolHandler) sendCurrentDatabaseResult(pgConn *PGConnection) bool {
	h.sendRowDescription(pgConn, []ColumnInfo{
		{Name: "current_database", DataType: "text", TypeSize: -1},
	})
	h.sendDataRow(pgConn, []string{"fastpostgres"})
	h.sendCommandComplete(pgConn, "SELECT", 1)
	h.sendReadyForQuery(pgConn, pgConn.transactionStatus)
	return true
}

func (h *PGProtocolHandler) sendSelectOneResult(pgConn *PGConnection) bool {
	h.sendRowDescription(pgConn, []ColumnInfo{
		{Name: "?column?", DataType: "int4", TypeSize: 4},
	})
	h.sendDataRow(pgConn, []string{"1"})
	h.sendCommandComplete(pgConn, "SELECT", 1)
	h.sendReadyForQuery(pgConn, pgConn.transactionStatus)
	return true
}

func (h *PGProtocolHandler) sendEmptyResult(pgConn *PGConnection, command string) bool {
	h.sendCommandComplete(pgConn, command, 0)
	h.sendReadyForQuery(pgConn, pgConn.transactionStatus)
	return true
}

func (h *PGProtocolHandler) handleInsert(pgConn *PGConnection, sql string) error {
	log.Printf("Executing INSERT: %s", sql)

	// For now, just acknowledge INSERT commands
	// In a full implementation, this would parse and execute the INSERT
	err := h.sendCommandComplete(pgConn, "INSERT 0 1", 0)
	if err != nil {
		return err
	}
	return h.sendReadyForQuery(pgConn, pgConn.transactionStatus)
}

func (h *PGProtocolHandler) handleCreate(pgConn *PGConnection, sql string) error {
	log.Printf("Executing CREATE: %s", sql)

	// For now, just acknowledge CREATE commands
	// In a full implementation, this would actually create tables/indexes
	if strings.Contains(strings.ToUpper(sql), "TABLE") {
		err := h.sendCommandComplete(pgConn, "CREATE TABLE", 0)
		if err != nil {
			return err
		}
	} else {
		err := h.sendCommandComplete(pgConn, "CREATE", 0)
		if err != nil {
			return err
		}
	}
	return h.sendReadyForQuery(pgConn, pgConn.transactionStatus)
}

func (h *PGProtocolHandler) handleDrop(pgConn *PGConnection, sql string) error {
	// Simple DROP acknowledgment
	err := h.sendCommandComplete(pgConn, "DROP", 0)
	if err != nil {
		return err
	}
	return h.sendReadyForQuery(pgConn, pgConn.transactionStatus)
}

func (h *PGProtocolHandler) handleShow(pgConn *PGConnection, sql string) error {
	sqlUpper := strings.ToUpper(sql)

	if strings.Contains(sqlUpper, "VERSION") {
		// Send row description
		err := h.sendRowDescription(pgConn, []ColumnInfo{
			{Name: "version", DataType: "text", TypeSize: -1},
		})
		if err != nil {
			return err
		}

		// Send version row
		err = h.sendDataRow(pgConn, []string{"FastPostgres 1.0 (PostgreSQL 13.0 compatible)"})
		if err != nil {
			return err
		}

		err = h.sendCommandComplete(pgConn, "SELECT", 1)
		if err != nil {
			return err
		}
	} else {
		// Generic SHOW command
		err := h.sendCommandComplete(pgConn, "SHOW", 0)
		if err != nil {
			return err
		}
	}

	return h.sendReadyForQuery(pgConn, pgConn.transactionStatus)
}

type ColumnInfo struct {
	Name     string
	DataType string
	TypeSize int32
}

// Protocol message senders
func (h *PGProtocolHandler) sendMessage(conn net.Conn, msgType byte, data []byte) error {
	// Calculate total message length (4 bytes for length + data)
	msgLen := int32(len(data) + 4)

	// Write message type
	_, err := conn.Write([]byte{msgType})
	if err != nil {
		return err
	}

	// Write message length
	err = binary.Write(conn, binary.BigEndian, msgLen)
	if err != nil {
		return err
	}

	// Write data
	_, err = conn.Write(data)
	return err
}

func (h *PGProtocolHandler) sendAuthenticationOK(pgConn *PGConnection) error {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, AUTH_OK)
	return h.sendMessage(pgConn.conn, MSG_AUTH_REQUEST, data)
}

func (h *PGProtocolHandler) sendParameterStatuses(pgConn *PGConnection) error {
	// Send common parameter status messages
	params := map[string]string{
		"server_version":        "13.0 (FastPostgres 1.0)",
		"server_encoding":       "UTF8",
		"client_encoding":       "UTF8",
		"application_name":      "",
		"is_superuser":          "on",
		"session_authorization": "fastpostgres",
		"DateStyle":            "ISO, MDY",
		"IntervalStyle":        "postgres",
		"TimeZone":             "UTC",
	}

	for key, value := range params {
		var buf bytes.Buffer
		buf.WriteString(key)
		buf.WriteByte(0)
		buf.WriteString(value)
		buf.WriteByte(0)

		err := h.sendMessage(pgConn.conn, MSG_PARAMETER_STATUS, buf.Bytes())
		if err != nil {
			return err
		}
	}

	return nil
}

func (h *PGProtocolHandler) sendBackendKeyData(pgConn *PGConnection) error {
	data := make([]byte, 8)
	binary.BigEndian.PutUint32(data[0:4], uint32(pgConn.processID))
	binary.BigEndian.PutUint32(data[4:8], uint32(pgConn.secretKey))
	return h.sendMessage(pgConn.conn, MSG_BACKEND_KEY_DATA, data)
}

func (h *PGProtocolHandler) sendReadyForQuery(pgConn *PGConnection, status byte) error {
	return h.sendMessage(pgConn.conn, MSG_READY_FOR_QUERY, []byte{status})
}

func (h *PGProtocolHandler) sendRowDescription(pgConn *PGConnection, columns []ColumnInfo) error {
	var buf bytes.Buffer

	// Number of fields
	binary.Write(&buf, binary.BigEndian, int16(len(columns)))

	for _, col := range columns {
		// Field name
		buf.WriteString(col.Name)
		buf.WriteByte(0)

		// Table OID (0 for now)
		binary.Write(&buf, binary.BigEndian, int32(0))

		// Column attribute number (0 for now)
		binary.Write(&buf, binary.BigEndian, int16(0))

		// Data type OID (simplified mapping)
		var typeOID int32
		switch col.DataType {
		case "int4":
			typeOID = 23
		case "varchar", "text":
			typeOID = 25
		case "timestamp":
			typeOID = 1114
		default:
			typeOID = 25 // Default to text
		}
		binary.Write(&buf, binary.BigEndian, typeOID)

		// Data type size
		binary.Write(&buf, binary.BigEndian, col.TypeSize)

		// Type modifier (-1 for now)
		binary.Write(&buf, binary.BigEndian, int32(-1))

		// Format code (0 = text)
		binary.Write(&buf, binary.BigEndian, int16(0))
	}

	return h.sendMessage(pgConn.conn, MSG_ROW_DESCRIPTION, buf.Bytes())
}

func (h *PGProtocolHandler) sendDataRow(pgConn *PGConnection, values []string) error {
	var buf bytes.Buffer

	// Number of column values
	binary.Write(&buf, binary.BigEndian, int16(len(values)))

	for _, value := range values {
		if value == "" {
			// NULL value
			binary.Write(&buf, binary.BigEndian, int32(-1))
		} else {
			// Value length
			binary.Write(&buf, binary.BigEndian, int32(len(value)))
			// Value data
			buf.WriteString(value)
		}
	}

	return h.sendMessage(pgConn.conn, MSG_DATA_ROW, buf.Bytes())
}

func (h *PGProtocolHandler) sendCommandComplete(pgConn *PGConnection, command string, rows int) error {
	var msg string
	if rows > 0 {
		msg = fmt.Sprintf("%s %d", command, rows)
	} else {
		msg = command
	}

	data := append([]byte(msg), 0)
	return h.sendMessage(pgConn.conn, MSG_COMMAND_COMPLETE, data)
}

func (h *PGProtocolHandler) sendErrorResponse(pgConn *PGConnection, severity, message string) error {
	var buf bytes.Buffer

	// Severity
	buf.WriteByte('S')
	buf.WriteString(severity)
	buf.WriteByte(0)

	// Message
	buf.WriteByte('M')
	buf.WriteString(message)
	buf.WriteByte(0)

	// Terminator
	buf.WriteByte(0)

	return h.sendMessage(pgConn.conn, MSG_ERROR_RESPONSE, buf.Bytes())
}