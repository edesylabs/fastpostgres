package server

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"fastpostgres/pkg/engine"
	"fastpostgres/pkg/query"
)

// PostgreSQL Wire Protocol implementation
type PostgresServer struct {
	db       *engine.Database
	port     string
	listener net.Listener
}

// PostgreSQL message types
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

type PostgresConnection struct {
	conn     net.Conn
	reader   *bufio.Reader
	writer   *bufio.Writer
	db       *engine.Database
	connInfo *ConnectionInfo
}

type ConnectionInfo struct {
	User     string
	Database string
	Options  map[string]string
}

type PostgresMessage struct {
	Type byte
	Data []byte
}

func NewPostgresServer(db *engine.Database, port string) *PostgresServer {
	return &PostgresServer{
		db:   db,
		port: port,
	}
}

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

func (ps *PostgresServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	pgConn := &PostgresConnection{
		conn:     conn,
		reader:   bufio.NewReader(conn),
		writer:   bufio.NewWriter(conn),
		db:       ps.db,
		connInfo: &ConnectionInfo{Options: make(map[string]string)},
	}

	// Handle startup sequence
	if err := pgConn.handleStartup(); err != nil {
		pgConn.sendError("08P01", err.Error())
		return
	}

	// Main message loop
	for {
		msg, err := pgConn.readMessage()
		if err != nil {
			if err != io.EOF {
				pgConn.sendError("08P01", err.Error())
			}
			return
		}

		if err := pgConn.handleMessage(msg); err != nil {
			pgConn.sendError("XX000", err.Error())
			continue
		}
	}
}

func (pc *PostgresConnection) handleStartup() error {
	// Read startup message
	length, err := pc.readInt32()
	if err != nil {
		return err
	}

	if length < 8 {
		return fmt.Errorf("invalid startup message length")
	}

	protocolVersion, err := pc.readInt32()
	if err != nil {
		return err
	}

	// Protocol version 3.0
	if protocolVersion != 196608 {
		return fmt.Errorf("unsupported protocol version: %d", protocolVersion)
	}

	// Read parameters
	remaining := int(length) - 8
	paramData := make([]byte, remaining)
	if _, err := io.ReadFull(pc.reader, paramData); err != nil {
		return err
	}

	params := strings.Split(string(paramData[:len(paramData)-1]), "\x00")
	for i := 0; i < len(params)-1; i += 2 {
		if i+1 < len(params) {
			key := params[i]
			value := params[i+1]
			pc.connInfo.Options[key] = value

			if key == "user" {
				pc.connInfo.User = value
			} else if key == "database" {
				pc.connInfo.Database = value
			}
		}
	}

	// Send authentication OK (no password required for now)
	pc.sendAuthOK()

	// Send parameter status messages
	pc.sendParameterStatus("server_version", "13.0 (FastPostgres 1.0)")
	pc.sendParameterStatus("server_encoding", "UTF8")
	pc.sendParameterStatus("client_encoding", "UTF8")
	pc.sendParameterStatus("is_superuser", "on")
	pc.sendParameterStatus("session_authorization", pc.connInfo.User)

	// Send backend key data
	pc.sendBackendKeyData()

	// Send ready for query
	pc.sendReadyForQuery('I')

	return nil
}

func (pc *PostgresConnection) handleMessage(msg *PostgresMessage) error {
	switch msg.Type {
	case MsgQuery:
		return pc.handleQuery(string(msg.Data[:len(msg.Data)-1])) // Remove null terminator

	case MsgTerminate:
		return fmt.Errorf("connection terminated")

	case MsgSync:
		pc.sendReadyForQuery('I')
		return nil

	case MsgParse:
		// Handle prepared statement parsing
		return pc.handleParse(msg.Data)

	case MsgBind:
		// Handle parameter binding
		return pc.handleBind(msg.Data)

	case MsgExecute:
		// Handle statement execution
		return pc.handleExecute(msg.Data)

	case MsgDescribe:
		// Handle statement description
		return pc.handleDescribe(msg.Data)

	default:
		return fmt.Errorf("unsupported message type: %c", msg.Type)
	}
}

func (pc *PostgresConnection) handleQuery(sql string) error {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		pc.sendCommandComplete("SELECT 0")
		pc.sendReadyForQuery('I')
		return nil
	}

	// Parse SQL
	parser := query.NewSQLParser()
	plan, err := parser.Parse(sql)
	if err != nil {
		pc.sendError("42601", "syntax error: "+err.Error())
		pc.sendReadyForQuery('I')
		return nil
	}

	// Execute query
	result, err := pc.executeQuery(plan)
	if err != nil {
		pc.sendError("XX000", err.Error())
		pc.sendReadyForQuery('I')
		return nil
	}

	// Send result
	if err := pc.sendResult(result); err != nil {
		return err
	}

	pc.sendReadyForQuery('I')
	return nil
}

func (pc *PostgresConnection) executeQuery(plan *engine.QueryPlan) (*engine.QueryResult, error) {
	switch plan.Type {
	case engine.QuerySelect:
		return pc.executeSelect(plan)
	case engine.QueryInsert:
		return pc.executeInsert(plan)
	default:
		return nil, fmt.Errorf("unsupported query type")
	}
}

func (pc *PostgresConnection) executeSelect(plan *engine.QueryPlan) (*engine.QueryResult, error) {
	// Get table
	tableInterface, exists := pc.db.Tables.Load(plan.TableName)
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", plan.TableName)
	}

	table := tableInterface.(*engine.Table)

	// Use vectorized engine for execution
	engine := query.NewVectorizedEngine()
	return engine.ExecuteSelect(plan, table)
}

func (pc *PostgresConnection) executeInsert(plan *engine.QueryPlan) (*engine.QueryResult, error) {
	// Simplified INSERT implementation
	return &engine.QueryResult{
		Rows: [][]interface{}{},
		Stats: engine.QueryStats{
			RowsAffected: 1,
		},
	}, nil
}

func (pc *PostgresConnection) sendResult(result *engine.QueryResult) error {
	if len(result.Rows) > 0 {
		// Send row description
		pc.sendRowDescription(result.Columns, result.Types)

		// Send data rows
		for _, row := range result.Rows {
			pc.sendDataRow(row)
		}
	}

	// Send command complete
	pc.sendCommandComplete(fmt.Sprintf("SELECT %d", len(result.Rows)))
	return nil
}

// Protocol message handlers
func (pc *PostgresConnection) handleParse(data []byte) error {
	// Simplified prepared statement parsing
	pc.sendParseComplete()
	return nil
}

func (pc *PostgresConnection) handleBind(data []byte) error {
	// Simplified parameter binding
	pc.sendBindComplete()
	return nil
}

func (pc *PostgresConnection) handleExecute(data []byte) error {
	// Simplified statement execution
	pc.sendCommandComplete("SELECT 0")
	return nil
}

func (pc *PostgresConnection) handleDescribe(data []byte) error {
	// Simplified statement description
	return nil
}

// Protocol message senders
func (pc *PostgresConnection) sendAuthOK() {
	pc.writeMessage(MsgAuth, []byte{0, 0, 0, 0})
	pc.writer.Flush()
}

func (pc *PostgresConnection) sendParameterStatus(name, value string) {
	data := []byte(name + "\x00" + value + "\x00")
	pc.writeMessage(MsgParameterStatus, data)
	pc.writer.Flush()
}

func (pc *PostgresConnection) sendBackendKeyData() {
	data := make([]byte, 8)
	binary.BigEndian.PutUint32(data[0:4], 12345) // Process ID
	binary.BigEndian.PutUint32(data[4:8], 67890) // Secret key
	pc.writeMessage(MsgBackendKeyData, data)
	pc.writer.Flush()
}

func (pc *PostgresConnection) sendReadyForQuery(status byte) {
	pc.writeMessage(MsgReadyForQuery, []byte{status})
	pc.writer.Flush()
}

func (pc *PostgresConnection) sendRowDescription(columns []string, types []engine.DataType) {
	data := make([]byte, 2) // Field count
	binary.BigEndian.PutUint16(data, uint16(len(columns)))

	for i, colName := range columns {
		// Field name
		data = append(data, []byte(colName)...)
		data = append(data, 0) // Null terminator

		// Table OID (0 = no table)
		oid := make([]byte, 4)
		data = append(data, oid...)

		// Attribute number (0)
		attnum := make([]byte, 2)
		data = append(data, attnum...)

		// Type OID
		typeOID := pc.getPostgresTypeOID(types[i])
		typeOIDBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(typeOIDBytes, uint32(typeOID))
		data = append(data, typeOIDBytes...)

		// Type size (-1 = variable)
		typeSize := make([]byte, 2)
		binary.BigEndian.PutUint16(typeSize, 65535) // -1 as unsigned
		data = append(data, typeSize...)

		// Type modifier (-1)
		typeMod := make([]byte, 4)
		binary.BigEndian.PutUint32(typeMod, 4294967295) // -1 as unsigned
		data = append(data, typeMod...)

		// Format code (0 = text)
		format := make([]byte, 2)
		data = append(data, format...)
	}

	pc.writeMessage(MsgRowDesc, data)
	pc.writer.Flush()
}

func (pc *PostgresConnection) sendDataRow(row []interface{}) {
	data := make([]byte, 2) // Field count
	binary.BigEndian.PutUint16(data, uint16(len(row)))

	for _, value := range row {
		if value == nil {
			// Null value
			length := make([]byte, 4)
			binary.BigEndian.PutUint32(length, 4294967295) // -1 as unsigned
			data = append(data, length...)
		} else {
			valueStr := pc.formatValue(value)
			valueBytes := []byte(valueStr)

			// Value length
			length := make([]byte, 4)
			binary.BigEndian.PutUint32(length, uint32(len(valueBytes)))
			data = append(data, length...)

			// Value data
			data = append(data, valueBytes...)
		}
	}

	pc.writeMessage(MsgDataRow, data)
	pc.writer.Flush()
}

func (pc *PostgresConnection) sendCommandComplete(tag string) {
	data := []byte(tag + "\x00")
	pc.writeMessage(MsgCmdComplete, data)
	pc.writer.Flush()
}

func (pc *PostgresConnection) sendError(code, message string) {
	data := []byte("S" + "ERROR" + "\x00")
	data = append(data, []byte("C"+code+"\x00")...)
	data = append(data, []byte("M"+message+"\x00")...)
	data = append(data, 0) // Null terminator

	pc.writeMessage(MsgError, data)
	pc.writer.Flush()
}

func (pc *PostgresConnection) sendParseComplete() {
	pc.writeMessage('1', []byte{})
	pc.writer.Flush()
}

func (pc *PostgresConnection) sendBindComplete() {
	pc.writeMessage('2', []byte{})
	pc.writer.Flush()
}

// Low-level message I/O
func (pc *PostgresConnection) readMessage() (*PostgresMessage, error) {
	msgType, err := pc.reader.ReadByte()
	if err != nil {
		return nil, err
	}

	length, err := pc.readInt32()
	if err != nil {
		return nil, err
	}

	if length < 4 {
		return nil, fmt.Errorf("invalid message length: %d", length)
	}

	dataLen := length - 4
	data := make([]byte, dataLen)
	if dataLen > 0 {
		if _, err := io.ReadFull(pc.reader, data); err != nil {
			return nil, err
		}
	}

	return &PostgresMessage{Type: msgType, Data: data}, nil
}

func (pc *PostgresConnection) writeMessage(msgType byte, data []byte) {
	pc.writer.WriteByte(msgType)

	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(data)+4))
	pc.writer.Write(length)

	if len(data) > 0 {
		pc.writer.Write(data)
	}
}

func (pc *PostgresConnection) readInt32() (int32, error) {
	bytes := make([]byte, 4)
	if _, err := io.ReadFull(pc.reader, bytes); err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(bytes)), nil
}

func (pc *PostgresConnection) getPostgresTypeOID(dataType engine.DataType) int {
	switch dataType {
	case engine.TypeInt32:
		return 23 // INT4OID
	case engine.TypeInt64:
		return 20 // INT8OID
	case engine.TypeFloat64:
		return 701 // FLOAT8OID
	case engine.TypeString:
		return 25 // TEXTOID
	case engine.TypeBool:
		return 16 // BOOLOID
	case engine.TypeTimestamp:
		return 1114 // TIMESTAMPOID
	default:
		return 25 // Default to TEXT
	}
}

func (pc *PostgresConnection) formatValue(value interface{}) string {
	switch v := value.(type) {
	case int64:
		return strconv.FormatInt(v, 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case string:
		return v
	case bool:
		if v {
			return "t"
		}
		return "f"
	case time.Time:
		return v.Format("2006-01-02 15:04:05")
	default:
		return fmt.Sprintf("%v", v)
	}
}