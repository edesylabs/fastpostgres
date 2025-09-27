// Package query provides advanced query result caching.
// It implements LRU-based caching with automatic expiration and invalidation.
package query

import (
	"crypto/md5"
	"fmt"
	"sync"
	"time"

	"fastpostgres/pkg/engine"
	"fastpostgres/pkg/storage"
)

// QueryCacheEngine manages cached query results with expiration and invalidation.
type QueryCacheEngine struct {
	cache          map[string]*CachedResult
	accessTimes    map[string]time.Time
	maxSize        int
	maxAge         time.Duration
	hitCount       int64
	missCount      int64
	evictionCount  int64
	mu             sync.RWMutex
	cleanupTicker  *time.Ticker
	stopCleanup    chan bool
}

// CachedResult stores a query result with metadata and statistics.
type CachedResult struct {
	Result       *engine.QueryResult
	CreatedAt    time.Time
	LastAccessed time.Time
	AccessCount  int64
	QueryHash    string
	TableNames   []string // For invalidation
	EstimatedMemory int64
}

// CacheStats provides metrics about cache performance.
type CacheStats struct {
	HitCount      int64
	MissCount     int64
	EvictionCount int64
	TotalQueries  int64
	HitRatio      float64
	CacheSize     int
	MemoryUsage   int64
}

// CacheConfig configures cache behavior and limits.
type CacheConfig struct {
	MaxSize        int
	MaxAge         time.Duration
	CleanupInterval time.Duration
	MaxMemoryMB    int64
}

// NewQueryCacheEngine creates a query cache with automatic cleanup.
func NewQueryCacheEngine(config *CacheConfig) *QueryCacheEngine {
	if config == nil {
		config = &CacheConfig{
			MaxSize:        1000,
			MaxAge:         30 * time.Minute,
			CleanupInterval: 5 * time.Minute,
			MaxMemoryMB:    100,
		}
	}

	qce := &QueryCacheEngine{
		cache:       make(map[string]*CachedResult),
		accessTimes: make(map[string]time.Time),
		maxSize:     config.MaxSize,
		maxAge:      config.MaxAge,
		stopCleanup: make(chan bool),
	}

	// Start background cleanup routine
	qce.startCleanupRoutine(config.CleanupInterval)

	return qce
}

// Get retrieves a cached query result if it exists and is valid.
func (qce *QueryCacheEngine) Get(queryHash string) (*engine.QueryResult, bool) {
	qce.mu.RLock()
	cached, exists := qce.cache[queryHash]
	qce.mu.RUnlock()

	if !exists {
		qce.mu.Lock()
		qce.missCount++
		qce.mu.Unlock()
		return nil, false
	}

	// Check if expired
	if time.Since(cached.CreatedAt) > qce.maxAge {
		qce.mu.Lock()
		delete(qce.cache, queryHash)
		delete(qce.accessTimes, queryHash)
		qce.evictionCount++
		qce.missCount++
		qce.mu.Unlock()
		return nil, false
	}

	// Update access statistics
	qce.mu.Lock()
	cached.LastAccessed = time.Now()
	cached.AccessCount++
	qce.accessTimes[queryHash] = cached.LastAccessed
	qce.hitCount++
	qce.mu.Unlock()

	return cached.Result, true
}

// Put stores a query result in the cache with automatic eviction.
func (qce *QueryCacheEngine) Put(queryHash string, result *engine.QueryResult, tableNames []string) {
	qce.mu.Lock()
	defer qce.mu.Unlock()

	// Check if cache is full and needs eviction
	if len(qce.cache) >= qce.maxSize {
		qce.evictLRU()
	}

	// Calculate estimated memory usage
	memoryUsage := qce.estimateMemoryUsage(result)

	cached := &CachedResult{
		Result:          result,
		CreatedAt:       time.Now(),
		LastAccessed:    time.Now(),
		AccessCount:     0,
		QueryHash:       queryHash,
		TableNames:      tableNames,
		EstimatedMemory: memoryUsage,
	}

	qce.cache[queryHash] = cached
	qce.accessTimes[queryHash] = cached.LastAccessed
}

// InvalidateTable removes cached results for queries involving the table.
func (qce *QueryCacheEngine) InvalidateTable(tableName string) int {
	qce.mu.Lock()
	defer qce.mu.Unlock()

	var toDelete []string
	for hash, cached := range qce.cache {
		for _, table := range cached.TableNames {
			if table == tableName {
				toDelete = append(toDelete, hash)
				break
			}
		}
	}

	for _, hash := range toDelete {
		delete(qce.cache, hash)
		delete(qce.accessTimes, hash)
		qce.evictionCount++
	}

	return len(toDelete)
}

// Clear removes all entries from the cache.
func (qce *QueryCacheEngine) Clear() {
	qce.mu.Lock()
	defer qce.mu.Unlock()

	qce.evictionCount += int64(len(qce.cache))
	qce.cache = make(map[string]*CachedResult)
	qce.accessTimes = make(map[string]time.Time)
}

// GetStats returns current cache statistics.
func (qce *QueryCacheEngine) GetStats() *CacheStats {
	qce.mu.RLock()
	defer qce.mu.RUnlock()

	totalQueries := qce.hitCount + qce.missCount
	hitRatio := float64(0)
	if totalQueries > 0 {
		hitRatio = float64(qce.hitCount) / float64(totalQueries)
	}

	var memoryUsage int64
	for _, cached := range qce.cache {
		memoryUsage += cached.EstimatedMemory
	}

	return &CacheStats{
		HitCount:      qce.hitCount,
		MissCount:     qce.missCount,
		EvictionCount: qce.evictionCount,
		TotalQueries:  totalQueries,
		HitRatio:      hitRatio,
		CacheSize:     len(qce.cache),
		MemoryUsage:   memoryUsage,
	}
}

// evictLRU removes the least recently used entry
func (qce *QueryCacheEngine) evictLRU() {
	if len(qce.cache) == 0 {
		return
	}

	var oldestHash string
	var oldestTime time.Time = time.Now()

	for hash, accessTime := range qce.accessTimes {
		if accessTime.Before(oldestTime) {
			oldestTime = accessTime
			oldestHash = hash
		}
	}

	if oldestHash != "" {
		delete(qce.cache, oldestHash)
		delete(qce.accessTimes, oldestHash)
		qce.evictionCount++
	}
}

// estimateMemoryUsage calculates approximate memory usage of a query result
func (qce *QueryCacheEngine) estimateMemoryUsage(result *engine.QueryResult) int64 {
	var size int64

	// Column names
	for _, col := range result.Columns {
		size += int64(len(col))
	}

	// Data types
	size += int64(len(result.Types) * 1) // 1 byte per DataType

	// Result rows
	for _, row := range result.Rows {
		for _, value := range row {
			if value == nil {
				size += 8 // pointer size
			} else {
				switch v := value.(type) {
				case string:
					size += int64(len(v))
				case int64:
					size += 8
				case int32:
					size += 4
				case float64:
					size += 8
				default:
					size += 16 // conservative estimate
				}
			}
		}
	}

	return size
}

// startCleanupRoutine starts background cleanup of expired entries
func (qce *QueryCacheEngine) startCleanupRoutine(interval time.Duration) {
	qce.cleanupTicker = time.NewTicker(interval)

	go func() {
		for {
			select {
			case <-qce.cleanupTicker.C:
				qce.cleanupExpired()
			case <-qce.stopCleanup:
				qce.cleanupTicker.Stop()
				return
			}
		}
	}()
}

// cleanupExpired removes expired cache entries
func (qce *QueryCacheEngine) cleanupExpired() {
	qce.mu.Lock()
	defer qce.mu.Unlock()

	now := time.Now()
	var toDelete []string

	for hash, cached := range qce.cache {
		if now.Sub(cached.CreatedAt) > qce.maxAge {
			toDelete = append(toDelete, hash)
		}
	}

	for _, hash := range toDelete {
		delete(qce.cache, hash)
		delete(qce.accessTimes, hash)
		qce.evictionCount++
	}
}

// Stop shuts down the cache engine
func (qce *QueryCacheEngine) Stop() {
	if qce.cleanupTicker != nil {
		qce.stopCleanup <- true
	}
}

// GenerateQueryHash creates an MD5 hash for a query and its parameters.
func GenerateQueryHash(sql string, params ...interface{}) string {
	hasher := md5.New()
	hasher.Write([]byte(sql))

	for _, param := range params {
		hasher.Write([]byte(fmt.Sprintf("%v", param)))
	}

	return fmt.Sprintf("%x", hasher.Sum(nil))
}

// Note: Integration with the main database engine moved to engine package to avoid circular dependencies.
// Use NewQueryCacheEngine directly and assign to Database.QueryCache field.
/*
func (db *engine.Database) SetupQueryCache(config *CacheConfig) {
	if db.QueryCache == nil {
		db.QueryCache = &engine.QueryCache{}
	}

	// Initialize the advanced cache engine
	db.QueryCache.engine = NewQueryCacheEngine(config)
}
*/

// EnhancedQueryCache extends the basic cache with advanced features.
type EnhancedQueryCache struct {
	*engine.QueryCache
	engine *QueryCacheEngine
}

// CachedExecuteSelect executes a query with result caching.
func (ve *VectorizedEngine) CachedExecuteSelect(plan *engine.QueryPlan, table *engine.Table, cacheEngine *QueryCacheEngine) (*engine.QueryResult, error) {
	// Generate cache key from query plan
	queryHash := ve.generatePlanHash(plan, table.Name)

	// Try to get from cache first
	if cachedResult, hit := cacheEngine.Get(queryHash); hit {
		return cachedResult, nil
	}

	// Cache miss - execute the query
	var result *engine.QueryResult
	var err error

	if plan.HasAggregates() {
		result, err = ve.ExecuteAggregateQuery(plan, table)
	} else {
		result, err = ve.ExecuteSelect(plan, table)
	}

	if err != nil {
		return nil, err
	}

	// Cache the result
	tableNames := []string{table.Name}
	cacheEngine.Put(queryHash, result, tableNames)

	return result, nil
}

// CachedExecuteIndexedSelect executes an indexed SELECT query with caching
func (ve *VectorizedEngine) CachedExecuteIndexedSelect(plan *engine.QueryPlan, table *engine.Table, indexManager *storage.IndexManager, cacheEngine *QueryCacheEngine) (*engine.QueryResult, error) {
	// Generate cache key
	queryHash := ve.generatePlanHash(plan, table.Name)

	// Try cache first
	if cachedResult, hit := cacheEngine.Get(queryHash); hit {
		return cachedResult, nil
	}

	// Cache miss - execute query
	result, err := ve.ExecuteSelect(plan, table)
	if err != nil {
		return nil, err
	}

	// Cache the result
	tableNames := []string{table.Name}
	cacheEngine.Put(queryHash, result, tableNames)

	return result, nil
}

// generatePlanHash creates a unique hash for a query plan
func (ve *VectorizedEngine) generatePlanHash(plan *engine.QueryPlan, tableName string) string {
	hashData := fmt.Sprintf("table:%s|type:%d|cols:%v|filters:%v|joins:%v|orderby:%v|groupby:%v|limit:%d|offset:%d",
		plan.TableName,
		plan.Type,
		plan.Columns,
		plan.Filters,
		plan.Joins,
		plan.OrderBy,
		plan.GroupBy,
		plan.Limit,
		plan.Offset,
	)

	return GenerateQueryHash(hashData)
}

// CacheWarmer pre-executes common queries to populate the cache.
type CacheWarmer struct {
	cacheEngine *QueryCacheEngine
	database    *engine.Database
	engine      *VectorizedEngine
	warmQueries []WarmQuery
}

// WarmQuery describes a query to pre-execute for cache warming.
type WarmQuery struct {
	SQL       string
	TableName string
	Frequency int // How often this query is expected
}

func NewCacheWarmer(cacheEngine *QueryCacheEngine, db *engine.Database, engine *VectorizedEngine) *CacheWarmer {
	return &CacheWarmer{
		cacheEngine: cacheEngine,
		database:    db,
		engine:      engine,
		warmQueries: make([]WarmQuery, 0),
	}
}

// AddWarmQuery adds a query to the warming list
func (cw *CacheWarmer) AddWarmQuery(sql, tableName string, frequency int) {
	cw.warmQueries = append(cw.warmQueries, WarmQuery{
		SQL:       sql,
		TableName: tableName,
		Frequency: frequency,
	})
}

// WarmCache executes all warm queries to populate the cache.
func (cw *CacheWarmer) WarmCache() error {
	parser := NewSQLParser()

	for _, warmQuery := range cw.warmQueries {
		plan, err := parser.Parse(warmQuery.SQL)
		if err != nil {
			continue // Skip invalid queries
		}

		tableInterface, exists := cw.database.Tables.Load(plan.TableName)
		if !exists {
			continue
		}

		table := tableInterface.(*engine.Table)

		// Execute and cache the query
		_, err = cw.engine.CachedExecuteSelect(plan, table, cw.cacheEngine)
		if err != nil {
			fmt.Printf("Warning: Failed to warm cache for query: %s (%v)\n", warmQuery.SQL, err)
		}
	}

	return nil
}

// Cache invalidation strategies
type InvalidationStrategy interface {
	ShouldInvalidate(tableName string, operation string) bool
}

type TimeBasedInvalidation struct {
	MaxAge time.Duration
}

func (tbi *TimeBasedInvalidation) ShouldInvalidate(tableName string, operation string) bool {
	// Time-based invalidation is handled automatically by the cache engine
	return false
}

type WriteBasedInvalidation struct {
	InvalidateOnWrite bool
}

func (wbi *WriteBasedInvalidation) ShouldInvalidate(tableName string, operation string) bool {
	if wbi.InvalidateOnWrite {
		return operation == "INSERT" || operation == "UPDATE" || operation == "DELETE"
	}
	return false
}

// Note: Integration helper moved to engine package to avoid circular dependencies.
/*
func (db *engine.Database) InvalidateCacheOnWrite(tableName string, operation string) {
	if db.QueryCache != nil && db.QueryCache.engine != nil {
		// Simple strategy: invalidate all queries for the modified table
		count := db.QueryCache.engine.InvalidateTable(tableName)
		if count > 0 {
			fmt.Printf("Invalidated %d cached queries for table %s due to %s operation\n",
				count, tableName, operation)
		}
	}
}
*/