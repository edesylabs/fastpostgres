package storage

import (
	"strings"

	"fastpostgres/pkg/engine"
)

// Aggregate function types
type AggregateType uint8

const (
	AggCount AggregateType = iota
	AggSum
	AggAvg
	AggMin
	AggMax
)

type AggregateFunction struct {
	Type   AggregateType
	Column string
	Alias  string
}

// Note: Methods for QueryPlan have been moved to pkg/engine to avoid circular import dependencies.
// These functions provide alternative implementations:

func HasAggregates(qp *engine.QueryPlan) bool {
	for _, col := range qp.Columns {
		if isAggregateFunction(col) {
			return true
		}
	}
	return false
}

func GetAggregates(qp *engine.QueryPlan) []*AggregateFunction {
	var aggregates []*AggregateFunction

	for _, col := range qp.Columns {
		if agg := parseAggregateFunction(col); agg != nil {
			aggregates = append(aggregates, agg)
		}
	}

	return aggregates
}

func isAggregateFunction(column string) bool {
	upper := strings.ToUpper(column)
	return strings.HasPrefix(upper, "COUNT(") ||
		   strings.HasPrefix(upper, "SUM(") ||
		   strings.HasPrefix(upper, "AVG(") ||
		   strings.HasPrefix(upper, "MIN(") ||
		   strings.HasPrefix(upper, "MAX(")
}

func parseAggregateFunction(column string) *AggregateFunction {
	upper := strings.ToUpper(column)

	var aggType AggregateType
	var funcName string

	if strings.HasPrefix(upper, "COUNT(") {
		aggType = AggCount
		funcName = "COUNT"
	} else if strings.HasPrefix(upper, "SUM(") {
		aggType = AggSum
		funcName = "SUM"
	} else if strings.HasPrefix(upper, "AVG(") {
		aggType = AggAvg
		funcName = "AVG"
	} else if strings.HasPrefix(upper, "MIN(") {
		aggType = AggMin
		funcName = "MIN"
	} else if strings.HasPrefix(upper, "MAX(") {
		aggType = AggMax
		funcName = "MAX"
	} else {
		return nil
	}

	// Extract column name from function call
	start := len(funcName) + 1
	end := strings.LastIndex(upper, ")")
	if end <= start {
		return nil
	}

	columnName := strings.TrimSpace(column[start:end])
	if columnName == "*" {
		columnName = "*"
	} else {
		columnName = strings.ToLower(columnName)
	}

	return &AggregateFunction{
		Type:   aggType,
		Column: columnName,
		Alias:  strings.ToLower(funcName + "(" + columnName + ")"),
	}
}

// Note: VectorizedEngine methods for aggregate queries have been moved to pkg/query
// to avoid circular import dependencies.