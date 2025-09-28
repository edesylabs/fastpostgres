package server

import (
	"encoding/binary"
	"fmt"
	"log"
	"strconv"
	"strings"

	"fastpostgres/pkg/engine"
)

// handleParse handles the Parse message for prepared statements
func (ps *PostgresServer) handleParse(pgConn *PostgresConnection, data []byte) error {
	// Parse message format:
	// String(statementName) - name of prepared statement (empty for unnamed)
	// String(query) - SQL query
	// Int16(numParams) - number of parameter data types
	// For each parameter: Int32(paramType)

	offset := 0

	// Read statement name
	stmtNameEnd := offset
	for stmtNameEnd < len(data) && data[stmtNameEnd] != 0 {
		stmtNameEnd++
	}
	stmtName := string(data[offset:stmtNameEnd])
	offset = stmtNameEnd + 1

	// Read query
	queryEnd := offset
	for queryEnd < len(data) && data[queryEnd] != 0 {
		queryEnd++
	}
	query := string(data[offset:queryEnd])
	offset = queryEnd + 1

	log.Printf("Parse: statement='%s', query='%s'", stmtName, query)

	// Read number of parameters
	if offset+2 > len(data) {
		return ps.sendErrorResponse(pgConn, "ERROR", "Invalid Parse message: missing parameter count")
	}
	numParams := binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Read parameter types
	paramTypes := make([]int32, numParams)
	for i := uint16(0); i < numParams; i++ {
		if offset+4 > len(data) {
			break
		}
		paramTypes[i] = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
	}

	// Parse the SQL query (without parameter substitution for now)
	plan, err := ps.parser.Parse(query)
	if err != nil {
		log.Printf("SQL parse error: %v", err)
		return ps.sendErrorResponse(pgConn, "ERROR", fmt.Sprintf("SQL parse error: %v", err))
	}

	// Store prepared statement
	prepStmt := &PreparedStatement{
		Name:       stmtName,
		Query:      query,
		Plan:       plan,
		ParamTypes: paramTypes,
	}

	if stmtName == "" {
		stmtName = "unnamed"
	}
	pgConn.preparedStmts[stmtName] = prepStmt

	// Send ParseComplete
	return ps.sendParseComplete(pgConn)
}

// handleBind handles the Bind message
func (ps *PostgresServer) handleBind(pgConn *PostgresConnection, data []byte) error {
	// Bind message format:
	// String(portalName)
	// String(statementName)
	// Int16(numFormatCodes)
	// For each: Int16(formatCode)
	// Int16(numParams)
	// For each parameter: Int32(length), Byte[length](value)
	// Int16(numResultFormatCodes)
	// For each: Int16(formatCode)

	offset := 0

	// Read portal name
	portalNameEnd := offset
	for portalNameEnd < len(data) && data[portalNameEnd] != 0 {
		portalNameEnd++
	}
	portalName := string(data[offset:portalNameEnd])
	offset = portalNameEnd + 1

	// Read statement name
	stmtNameEnd := offset
	for stmtNameEnd < len(data) && data[stmtNameEnd] != 0 {
		stmtNameEnd++
	}
	stmtName := string(data[offset:stmtNameEnd])
	offset = stmtNameEnd + 1

	if stmtName == "" {
		stmtName = "unnamed"
	}

	log.Printf("Bind: portal='%s', statement='%s'", portalName, stmtName)

	// Get prepared statement
	prepStmt, exists := pgConn.preparedStmts[stmtName]
	if !exists {
		return ps.sendErrorResponse(pgConn, "ERROR", fmt.Sprintf("Prepared statement '%s' not found", stmtName))
	}

	// Skip format codes for now
	if offset+2 > len(data) {
		return ps.sendErrorResponse(pgConn, "ERROR", "Invalid Bind message: missing format code count")
	}
	numFormatCodes := binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2
	offset += int(numFormatCodes) * 2 // Skip format codes

	// Read parameters
	if offset+2 > len(data) {
		return ps.sendErrorResponse(pgConn, "ERROR", "Invalid Bind message: missing parameter count")
	}
	numParams := binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	params := make([]interface{}, numParams)
	for i := uint16(0); i < numParams; i++ {
		if offset+4 > len(data) {
			break
		}
		paramLen := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4

		if paramLen == -1 {
			// NULL parameter
			params[i] = nil
		} else if paramLen > 0 {
			if offset+int(paramLen) > len(data) {
				return ps.sendErrorResponse(pgConn, "ERROR", "Invalid Bind message: parameter data out of bounds")
			}
			paramValue := string(data[offset : offset+int(paramLen)])
			params[i] = parseParameter(paramValue)
			offset += int(paramLen)
		}
	}

	// Create portal
	portal := &Portal{
		Name:      portalName,
		Statement: prepStmt,
		Params:    params,
	}

	if portalName == "" {
		portalName = "unnamed"
	}
	pgConn.portals[portalName] = portal

	// Send BindComplete
	return ps.sendBindComplete(pgConn)
}

// handleExecute handles the Execute message
func (ps *PostgresServer) handleExecute(pgConn *PostgresConnection, data []byte) error {
	// Execute message format:
	// String(portalName)
	// Int32(maxRows) - 0 means no limit

	offset := 0

	// Read portal name
	portalNameEnd := offset
	for portalNameEnd < len(data) && data[portalNameEnd] != 0 {
		portalNameEnd++
	}
	portalName := string(data[offset:portalNameEnd])
	offset = portalNameEnd + 1

	if portalName == "" {
		portalName = "unnamed"
	}

	log.Printf("Execute: portal='%s'", portalName)

	// Get portal
	portal, exists := pgConn.portals[portalName]
	if !exists {
		return ps.sendErrorResponse(pgConn, "ERROR", fmt.Sprintf("Portal '%s' not found", portalName))
	}

	// Substitute parameters in the query
	query := portal.Statement.Query
	for i, param := range portal.Params {
		placeholder := fmt.Sprintf("$%d", i+1)
		var replacement string
		if param == nil {
			replacement = "NULL"
		} else {
			switch v := param.(type) {
			case string:
				replacement = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
			case int, int32, int64:
				replacement = fmt.Sprintf("%v", v)
			default:
				replacement = fmt.Sprintf("'%v'", v)
			}
		}
		query = strings.ReplaceAll(query, placeholder, replacement)
	}

	log.Printf("Execute query after parameter substitution: %s", query)

	// Re-parse with substituted parameters
	plan, err := ps.parser.Parse(query)
	if err != nil {
		log.Printf("SQL parse error: %v", err)
		return ps.sendErrorResponse(pgConn, "ERROR", fmt.Sprintf("SQL parse error: %v", err))
	}

	// Execute the query
	switch plan.Type {
	case engine.QuerySelect:
		// Get the table
		tableInterface, exists := ps.db.Tables.Load(plan.TableName)
		if !exists {
			return ps.sendErrorResponse(pgConn, "ERROR", fmt.Sprintf("Table not found: %s", plan.TableName))
		}

		table := tableInterface.(*engine.Table)

		// Execute the query
		var result *engine.QueryResult
		if plan.HasAggregates() {
			result, err = ps.queryEngine.ExecuteAggregateQuery(plan, table)
		} else {
			result, err = ps.queryEngine.ExecuteSelect(plan, table)
		}

		if err != nil {
			log.Printf("Query execution error: %v", err)
			return ps.sendErrorResponse(pgConn, "ERROR", fmt.Sprintf("Query execution error: %v", err))
		}

		// Send result
		return ps.sendQueryResult(pgConn, result)

	case engine.QueryInsert, engine.QueryUpdate, engine.QueryDelete:
		tag := "OK"
		return ps.sendCommandComplete(pgConn, tag)

	default:
		tag := "OK"
		return ps.sendCommandComplete(pgConn, tag)
	}
}

// handleDescribe handles the Describe message
func (ps *PostgresServer) handleDescribe(pgConn *PostgresConnection, data []byte) error {
	// Describe message format:
	// Byte(type) - 'S' for statement, 'P' for portal
	// String(name)

	if len(data) < 2 {
		return ps.sendErrorResponse(pgConn, "ERROR", "Invalid Describe message")
	}

	descType := data[0]
	nameEnd := 1
	for nameEnd < len(data) && data[nameEnd] != 0 {
		nameEnd++
	}
	name := string(data[1:nameEnd])

	if name == "" {
		name = "unnamed"
	}

	log.Printf("Describe: type='%c', name='%s'", descType, name)

	if descType == 'S' {
		// Describe statement - send ParameterDescription then NoData
		return ps.sendNoData(pgConn)
	} else if descType == 'P' {
		// Describe portal - send RowDescription
		portal, exists := pgConn.portals[name]
		if exists && portal.Statement.Plan.Type == engine.QuerySelect {
			// For now, send NoData as we don't have full type information
			return ps.sendNoData(pgConn)
		}
		return ps.sendNoData(pgConn)
	}

	return nil
}

// handleClose handles the Close message
func (ps *PostgresServer) handleClose(pgConn *PostgresConnection, data []byte) error {
	// Close message format:
	// Byte(type) - 'S' for statement, 'P' for portal
	// String(name)

	if len(data) < 2 {
		return nil
	}

	closeType := data[0]
	nameEnd := 1
	for nameEnd < len(data) && data[nameEnd] != 0 {
		nameEnd++
	}
	name := string(data[1:nameEnd])

	if name == "" {
		name = "unnamed"
	}

	log.Printf("Close: type='%c', name='%s'", closeType, name)

	if closeType == 'S' {
		delete(pgConn.preparedStmts, name)
	} else if closeType == 'P' {
		delete(pgConn.portals, name)
	}

	// Send CloseComplete
	return ps.sendCloseComplete(pgConn)
}

// Helper functions

func parseParameter(value string) interface{} {
	// Try to parse as integer
	if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
		return intVal
	}

	// Try to parse as float
	if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
		return floatVal
	}

	// Return as string
	return value
}

func (ps *PostgresServer) sendParseComplete(pgConn *PostgresConnection) error {
	msg := []byte{'1', 0, 0, 0, 4} // '1' is ParseComplete
	_, err := pgConn.writer.Write(msg)
	if err != nil {
		return err
	}
	return pgConn.writer.Flush()
}

func (ps *PostgresServer) sendBindComplete(pgConn *PostgresConnection) error {
	msg := []byte{'2', 0, 0, 0, 4} // '2' is BindComplete
	_, err := pgConn.writer.Write(msg)
	if err != nil {
		return err
	}
	return pgConn.writer.Flush()
}

func (ps *PostgresServer) sendCloseComplete(pgConn *PostgresConnection) error {
	msg := []byte{'3', 0, 0, 0, 4} // '3' is CloseComplete
	_, err := pgConn.writer.Write(msg)
	if err != nil {
		return err
	}
	return pgConn.writer.Flush()
}

func (ps *PostgresServer) sendNoData(pgConn *PostgresConnection) error {
	msg := []byte{'n', 0, 0, 0, 4} // 'n' is NoData
	_, err := pgConn.writer.Write(msg)
	if err != nil {
		return err
	}
	return pgConn.writer.Flush()
}