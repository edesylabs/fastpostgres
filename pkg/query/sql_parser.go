package query

import (
	"fmt"
	"strconv"
	"strings"

	"fastpostgres/pkg/engine"
)

// SQL Parser for FastPostgres
type SQLParser struct {
	keywords map[string]TokenType
	operators map[string]engine.FilterOperator
}

type TokenType int

const (
	TokenSelect TokenType = iota
	TokenFrom
	TokenWhere
	TokenInsert
	TokenInto
	TokenValues
	TokenUpdate
	TokenSet
	TokenDelete
	TokenCreate
	TokenTable
	TokenIndex
	TokenOrder
	TokenBy
	TokenGroup
	TokenHaving
	TokenLimit
	TokenOffset
	TokenJoin
	TokenInner
	TokenLeft
	TokenRight
	TokenFull
	TokenOn
	TokenAnd
	TokenOr
	TokenNot
	TokenIdentifier
	TokenString
	TokenNumber
	TokenOperator
	TokenComma
	TokenSemicolon
	TokenOpenParen
	TokenCloseParen
	TokenEOF
)

type Token struct {
	Type  TokenType
	Value string
	Pos   int
}

// NewSQLParser creates a new SQL parser
func NewSQLParser() *SQLParser {
	parser := &SQLParser{
		keywords: make(map[string]TokenType),
		operators: make(map[string]engine.FilterOperator),
	}

	// Initialize keywords
	parser.keywords["SELECT"] = TokenSelect
	parser.keywords["FROM"] = TokenFrom
	parser.keywords["WHERE"] = TokenWhere
	parser.keywords["INSERT"] = TokenInsert
	parser.keywords["INTO"] = TokenInto
	parser.keywords["VALUES"] = TokenValues
	parser.keywords["UPDATE"] = TokenUpdate
	parser.keywords["SET"] = TokenSet
	parser.keywords["DELETE"] = TokenDelete
	parser.keywords["CREATE"] = TokenCreate
	parser.keywords["TABLE"] = TokenTable
	parser.keywords["INDEX"] = TokenIndex
	parser.keywords["ORDER"] = TokenOrder
	parser.keywords["BY"] = TokenBy
	parser.keywords["GROUP"] = TokenGroup
	parser.keywords["HAVING"] = TokenHaving
	parser.keywords["LIMIT"] = TokenLimit
	parser.keywords["OFFSET"] = TokenOffset
	parser.keywords["JOIN"] = TokenJoin
	parser.keywords["INNER"] = TokenInner
	parser.keywords["LEFT"] = TokenLeft
	parser.keywords["RIGHT"] = TokenRight
	parser.keywords["FULL"] = TokenFull
	parser.keywords["ON"] = TokenOn
	parser.keywords["AND"] = TokenAnd
	parser.keywords["OR"] = TokenOr
	parser.keywords["NOT"] = TokenNot

	// Initialize operators
	parser.operators["="] = engine.OpEqual
	parser.operators["!="] = engine.OpNotEqual
	parser.operators["<>"] = engine.OpNotEqual
	parser.operators["<"] = engine.OpLess
	parser.operators["<="] = engine.OpLessEqual
	parser.operators[">"] = engine.OpGreater
	parser.operators[">="] = engine.OpGreaterEqual
	parser.operators["LIKE"] = engine.OpLike
	parser.operators["IN"] = engine.OpIn
	parser.operators["BETWEEN"] = engine.OpBetween

	return parser
}

// Tokenize breaks SQL into tokens
func (p *SQLParser) Tokenize(sql string) ([]*Token, error) {
	var tokens []*Token
	sql = strings.TrimSpace(sql)
	pos := 0

	for pos < len(sql) {
		// Skip whitespace
		for pos < len(sql) && isWhitespace(sql[pos]) {
			pos++
		}

		if pos >= len(sql) {
			break
		}

		char := sql[pos]

		switch {
		case char == ';':
			tokens = append(tokens, &Token{TokenSemicolon, ";", pos})
			pos++
		case char == ',':
			tokens = append(tokens, &Token{TokenComma, ",", pos})
			pos++
		case char == '(':
			tokens = append(tokens, &Token{TokenOpenParen, "(", pos})
			pos++
		case char == ')':
			tokens = append(tokens, &Token{TokenCloseParen, ")", pos})
			pos++
		case char == '\'':
			// String literal
			token, newPos, err := p.parseString(sql, pos)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, token)
			pos = newPos
		case isDigit(char):
			// Number
			token, newPos := p.parseNumber(sql, pos)
			tokens = append(tokens, token)
			pos = newPos
		case isAlpha(char):
			// Identifier or keyword
			token, newPos := p.parseIdentifier(sql, pos)
			tokens = append(tokens, token)
			pos = newPos
		case isOperator(char):
			// Operator
			token, newPos := p.parseOperator(sql, pos)
			tokens = append(tokens, token)
			pos = newPos
		default:
			return nil, fmt.Errorf("unexpected character at position %d: %c", pos, char)
		}
	}

	tokens = append(tokens, &Token{TokenEOF, "", pos})
	return tokens, nil
}

func (p *SQLParser) parseString(sql string, pos int) (*Token, int, error) {
	start := pos
	pos++ // Skip opening quote

	for pos < len(sql) && sql[pos] != '\'' {
		if sql[pos] == '\\' && pos+1 < len(sql) {
			pos += 2 // Skip escaped character
		} else {
			pos++
		}
	}

	if pos >= len(sql) {
		return nil, 0, fmt.Errorf("unterminated string literal at position %d", start)
	}

	pos++ // Skip closing quote
	value := sql[start+1 : pos-1] // Extract string content
	return &Token{TokenString, value, start}, pos, nil
}

func (p *SQLParser) parseNumber(sql string, pos int) (*Token, int) {
	start := pos
	for pos < len(sql) && (isDigit(sql[pos]) || sql[pos] == '.') {
		pos++
	}
	return &Token{TokenNumber, sql[start:pos], start}, pos
}

func (p *SQLParser) parseIdentifier(sql string, pos int) (*Token, int) {
	start := pos
	for pos < len(sql) && (isAlphaNumeric(sql[pos]) || sql[pos] == '_') {
		pos++
	}

	value := strings.ToUpper(sql[start:pos])
	tokenType := TokenIdentifier

	if keywordType, exists := p.keywords[value]; exists {
		tokenType = keywordType
	}

	return &Token{tokenType, value, start}, pos
}

func (p *SQLParser) parseOperator(sql string, pos int) (*Token, int) {
	start := pos

	// Handle multi-character operators
	if pos+1 < len(sql) {
		twoChar := sql[pos:pos+2]
		if twoChar == "<=" || twoChar == ">=" || twoChar == "!=" || twoChar == "<>" {
			return &Token{TokenOperator, twoChar, start}, pos + 2
		}
	}

	// Single character operators
	return &Token{TokenOperator, string(sql[pos]), start}, pos + 1
}

// Parse SQL into query plan
func (p *SQLParser) Parse(sql string) (*engine.QueryPlan, error) {
	tokens, err := p.Tokenize(sql)
	if err != nil {
		return nil, err
	}

	parser := &tokenParser{
		tokens: tokens,
		pos:    0,
		parser: p,
	}

	return parser.parseQuery()
}

type tokenParser struct {
	tokens []*Token
	pos    int
	parser *SQLParser
}

func (tp *tokenParser) parseQuery() (*engine.QueryPlan, error) {
	if tp.pos >= len(tp.tokens) {
		return nil, fmt.Errorf("empty query")
	}

	switch tp.tokens[tp.pos].Type {
	case TokenSelect:
		return tp.parseSelect()
	case TokenInsert:
		return tp.parseInsert()
	case TokenUpdate:
		return tp.parseUpdate()
	case TokenDelete:
		return tp.parseDelete()
	case TokenCreate:
		return tp.parseCreate()
	default:
		return nil, fmt.Errorf("unsupported query type: %s", tp.tokens[tp.pos].Value)
	}
}

func (tp *tokenParser) parseSelect() (*engine.QueryPlan, error) {
	plan := &engine.QueryPlan{
		Type: engine.QuerySelect,
	}

	// SELECT
	if !tp.consume(TokenSelect) {
		return nil, fmt.Errorf("expected SELECT")
	}

	// Column list
	columns, err := tp.parseColumnList()
	if err != nil {
		return nil, err
	}
	plan.Columns = columns

	// FROM
	if !tp.consume(TokenFrom) {
		return nil, fmt.Errorf("expected FROM")
	}

	// Table name
	if tp.current().Type != TokenIdentifier {
		return nil, fmt.Errorf("expected table name")
	}
	plan.TableName = strings.ToLower(tp.current().Value)
	tp.advance()

	// Optional JOIN clause
	joins, err := tp.parseJoins()
	if err != nil {
		return nil, err
	}
	plan.Joins = joins

	// Optional WHERE clause
	if tp.current().Type == TokenWhere {
		tp.advance()
		filters, err := tp.parseWhereClause()
		if err != nil {
			return nil, err
		}
		plan.Filters = filters
	}

	// Optional ORDER BY clause
	if tp.current().Type == TokenOrder {
		tp.advance()
		if !tp.consume(TokenBy) {
			return nil, fmt.Errorf("expected BY after ORDER")
		}
		orderBy, err := tp.parseOrderBy()
		if err != nil {
			return nil, err
		}
		plan.OrderBy = orderBy
	}

	// Optional LIMIT clause
	if tp.current().Type == TokenLimit {
		tp.advance()
		if tp.current().Type != TokenNumber {
			return nil, fmt.Errorf("expected number after LIMIT")
		}
		limit, _ := strconv.ParseInt(tp.current().Value, 10, 64)
		plan.Limit = limit
		tp.advance()

		// Optional OFFSET
		if tp.current().Type == TokenOffset {
			tp.advance()
			if tp.current().Type != TokenNumber {
				return nil, fmt.Errorf("expected number after OFFSET")
			}
			offset, _ := strconv.ParseInt(tp.current().Value, 10, 64)
			plan.Offset = offset
			tp.advance()
		}
	}

	return plan, nil
}

func (tp *tokenParser) parseColumnList() ([]string, error) {
	var columns []string

	for {
		if tp.current().Type == TokenOperator && tp.current().Value == "*" {
			// SELECT * case
			columns = append(columns, "*")
			tp.advance()
			break
		} else if tp.current().Type == TokenIdentifier {
			// Check if this is a function call
			if tp.peek().Type == TokenOpenParen {
				// This is a function call like COUNT(*)
				funcCall, err := tp.parseFunctionCall()
				if err != nil {
					return nil, err
				}
				columns = append(columns, funcCall)
			} else {
				// Regular column name
				columns = append(columns, strings.ToLower(tp.current().Value))
				tp.advance()
			}
		} else {
			return nil, fmt.Errorf("expected column name or function")
		}

		if tp.current().Type != TokenComma {
			break
		}
		tp.advance() // Skip comma
	}

	return columns, nil
}

func (tp *tokenParser) parseFunctionCall() (string, error) {
	// Parse function calls like COUNT(*), SUM(column), etc.
	funcName := tp.current().Value
	tp.advance()

	if !tp.consume(TokenOpenParen) {
		return "", fmt.Errorf("expected opening parenthesis")
	}

	var args []string
	for tp.current().Type != TokenCloseParen && tp.current().Type != TokenEOF {
		if tp.current().Type == TokenOperator && tp.current().Value == "*" {
			args = append(args, "*")
			tp.advance()
		} else if tp.current().Type == TokenIdentifier {
			args = append(args, strings.ToLower(tp.current().Value))
			tp.advance()
		} else {
			return "", fmt.Errorf("unexpected token in function call")
		}

		if tp.current().Type == TokenComma {
			tp.advance()
		}
	}

	if !tp.consume(TokenCloseParen) {
		return "", fmt.Errorf("expected closing parenthesis")
	}

	// Reconstruct function call
	result := strings.ToUpper(funcName) + "("
	for i, arg := range args {
		if i > 0 {
			result += ","
		}
		result += arg
	}
	result += ")"

	return result, nil
}

func (tp *tokenParser) peek() *Token {
	if tp.pos+1 < len(tp.tokens) {
		return tp.tokens[tp.pos+1]
	}
	return &Token{TokenEOF, "", -1}
}

func (tp *tokenParser) parseWhereClause() ([]*engine.FilterExpression, error) {
	// Simplified WHERE clause parsing
	// In a real implementation, this would handle complex expressions with proper precedence

	var filters []*engine.FilterExpression

	for tp.current().Type == TokenIdentifier {
		filter := &engine.FilterExpression{}

		// Column name
		filter.Column = strings.ToLower(tp.current().Value)
		tp.advance()

		// Operator
		if tp.current().Type != TokenOperator {
			return nil, fmt.Errorf("expected operator")
		}

		opStr := tp.current().Value
		if op, exists := tp.parser.operators[opStr]; exists {
			filter.Operator = op
		} else {
			return nil, fmt.Errorf("unsupported operator: %s", opStr)
		}
		tp.advance()

		// Value
		switch tp.current().Type {
		case TokenNumber:
			value, _ := strconv.ParseInt(tp.current().Value, 10, 64)
			filter.Value = value
		case TokenString:
			filter.Value = tp.current().Value
		case TokenIdentifier:
			filter.Value = tp.current().Value
		default:
			return nil, fmt.Errorf("expected value")
		}
		tp.advance()

		filters = append(filters, filter)

		// Handle AND (simplified - no OR support yet)
		if tp.current().Type == TokenAnd {
			tp.advance()
		} else {
			break
		}
	}

	return filters, nil
}

func (tp *tokenParser) parseOrderBy() ([]*engine.OrderExpression, error) {
	var orderBy []*engine.OrderExpression

	for {
		if tp.current().Type != TokenIdentifier {
			return nil, fmt.Errorf("expected column name in ORDER BY")
		}

		order := &engine.OrderExpression{
			Column: tp.current().Value,
			Desc:   false,
		}
		tp.advance()

		// Check for DESC
		if tp.current().Type == TokenIdentifier && strings.ToUpper(tp.current().Value) == "DESC" {
			order.Desc = true
			tp.advance()
		} else if tp.current().Type == TokenIdentifier && strings.ToUpper(tp.current().Value) == "ASC" {
			tp.advance() // Skip ASC (default)
		}

		orderBy = append(orderBy, order)

		if tp.current().Type != TokenComma {
			break
		}
		tp.advance() // Skip comma
	}

	return orderBy, nil
}

func (tp *tokenParser) parseInsert() (*engine.QueryPlan, error) {
	// Simplified INSERT parsing
	plan := &engine.QueryPlan{Type: engine.QueryInsert}

	if !tp.consume(TokenInsert) {
		return nil, fmt.Errorf("expected INSERT")
	}

	if !tp.consume(TokenInto) {
		return nil, fmt.Errorf("expected INTO")
	}

	if tp.current().Type != TokenIdentifier {
		return nil, fmt.Errorf("expected table name")
	}
	plan.TableName = tp.current().Value
	tp.advance()

	// Column list (optional)
	if tp.current().Type == TokenOpenParen {
		tp.advance()
		columns, err := tp.parseColumnList()
		if err != nil {
			return nil, err
		}
		plan.Columns = columns

		if !tp.consume(TokenCloseParen) {
			return nil, fmt.Errorf("expected closing parenthesis")
		}
	}

	if !tp.consume(TokenValues) {
		return nil, fmt.Errorf("expected VALUES")
	}

	// Parse VALUES clause (simplified)
	// In a real implementation, this would parse multiple value tuples

	return plan, nil
}

func (tp *tokenParser) parseUpdate() (*engine.QueryPlan, error) {
	return nil, fmt.Errorf("UPDATE not implemented yet")
}

func (tp *tokenParser) parseDelete() (*engine.QueryPlan, error) {
	return nil, fmt.Errorf("DELETE not implemented yet")
}

func (tp *tokenParser) parseCreate() (*engine.QueryPlan, error) {
	return nil, fmt.Errorf("CREATE not implemented yet")
}

// Helper methods
func (tp *tokenParser) current() *Token {
	if tp.pos < len(tp.tokens) {
		return tp.tokens[tp.pos]
	}
	return &Token{TokenEOF, "", -1}
}

func (tp *tokenParser) advance() {
	if tp.pos < len(tp.tokens) {
		tp.pos++
	}
}

func (tp *tokenParser) consume(expectedType TokenType) bool {
	if tp.current().Type == expectedType {
		tp.advance()
		return true
	}
	return false
}

// Character classification helpers
func isWhitespace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isAlpha(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isAlphaNumeric(ch byte) bool {
	return isAlpha(ch) || isDigit(ch)
}

func isOperator(ch byte) bool {
	operators := "=<>!+-*/%"
	return strings.ContainsRune(operators, rune(ch))
}

// Parse JOIN clauses
func (tp *tokenParser) parseJoins() ([]*engine.JoinExpression, error) {
	var joins []*engine.JoinExpression

	for {
		// Check for JOIN keywords
		if tp.current().Type == TokenInner || tp.current().Type == TokenLeft ||
		   tp.current().Type == TokenRight || tp.current().Type == TokenFull ||
		   tp.current().Type == TokenJoin {

			join := &engine.JoinExpression{}

			// Parse join type
			if tp.current().Type == TokenInner {
				join.Type = engine.InnerJoin
				tp.advance()
				if !tp.consume(TokenJoin) {
					return nil, fmt.Errorf("expected JOIN after INNER")
				}
			} else if tp.current().Type == TokenLeft {
				join.Type = engine.LeftJoin
				tp.advance()
				if !tp.consume(TokenJoin) {
					return nil, fmt.Errorf("expected JOIN after LEFT")
				}
			} else if tp.current().Type == TokenRight {
				join.Type = engine.RightJoin
				tp.advance()
				if !tp.consume(TokenJoin) {
					return nil, fmt.Errorf("expected JOIN after RIGHT")
				}
			} else if tp.current().Type == TokenFull {
				join.Type = engine.FullJoin
				tp.advance()
				if !tp.consume(TokenJoin) {
					return nil, fmt.Errorf("expected JOIN after FULL")
				}
			} else if tp.current().Type == TokenJoin {
				join.Type = engine.InnerJoin // Default to INNER JOIN
				tp.advance()
			}

			// Parse table name
			if tp.current().Type != TokenIdentifier {
				return nil, fmt.Errorf("expected table name after JOIN")
			}
			join.Table = strings.ToLower(tp.current().Value)
			tp.advance()

			// Parse ON clause
			if !tp.consume(TokenOn) {
				return nil, fmt.Errorf("expected ON after JOIN table")
			}

			// Parse join condition: left_table.column = right_table.column
			if tp.current().Type != TokenIdentifier {
				return nil, fmt.Errorf("expected column name in ON clause")
			}

			// Simple column name for now
			join.LeftCol = strings.ToLower(tp.current().Value)
			tp.advance()

			// Expect equals operator
			if tp.current().Type != TokenOperator || tp.current().Value != "=" {
				return nil, fmt.Errorf("expected '=' in JOIN condition")
			}
			tp.advance()

			// Parse right side
			if tp.current().Type != TokenIdentifier {
				return nil, fmt.Errorf("expected column name after '=' in JOIN")
			}
			join.RightCol = strings.ToLower(tp.current().Value)
			tp.advance()

			joins = append(joins, join)
		} else {
			break
		}
	}

	return joins, nil
}

// Query optimizer
type QueryOptimizer struct {
	statistics map[string]*TableStats
}

type TableStats struct {
	RowCount      int64
	ColumnStats   map[string]*ColumnStats
	IndexStats    map[string]*IndexStats
}

type ColumnStats struct {
	Cardinality int64
	MinValue    interface{}
	MaxValue    interface{}
	NullCount   int64
}

type IndexStats struct {
	Height      int
	Pages       int64
	Selectivity float64
}

func NewQueryOptimizer() *QueryOptimizer {
	return &QueryOptimizer{
		statistics: make(map[string]*TableStats),
	}
}

func (qo *QueryOptimizer) OptimizeQuery(plan *engine.QueryPlan) *engine.QueryPlan {
	// Cost-based optimization
	optimized := *plan
	optimized.Optimized = true

	// Push down filters
	qo.pushDownFilters(&optimized)

	// Choose optimal join order
	qo.optimizeJoinOrder(&optimized)

	// Select best indexes
	qo.selectIndexes(&optimized)

	return &optimized
}

func (qo *QueryOptimizer) pushDownFilters(plan *engine.QueryPlan) {
	// Move filters as close to data as possible
	// This reduces the amount of data that needs to be processed
}

func (qo *QueryOptimizer) optimizeJoinOrder(plan *engine.QueryPlan) {
	// Use dynamic programming to find optimal join order
	// Consider table sizes and selectivity
}

func (qo *QueryOptimizer) selectIndexes(plan *engine.QueryPlan) {
	// Choose the most selective indexes for filtering
}