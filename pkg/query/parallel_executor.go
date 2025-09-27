package query

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"fastpostgres/pkg/engine"
)

type WorkStealingScheduler struct {
	workers      []*Worker
	numWorkers   int
	globalQueue  *WorkQueue
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

type Worker struct {
	id          int
	localQueue  *WorkQueue
	scheduler   *WorkStealingScheduler
	stealing    atomic.Bool
	tasksRun    atomic.Uint64
	tasksStolen atomic.Uint64
}

type WorkQueue struct {
	tasks []Task
	mu    sync.Mutex
	cond  *sync.Cond
}

type Task interface {
	Execute() interface{}
	Priority() int
}

type ScanTask struct {
	table      *engine.Table
	startRow   uint64
	endRow     uint64
	columns    []*engine.Column
	filters    []*engine.FilterExpression
	resultChan chan *ScanResult
	priority   int
}

type ScanResult struct {
	rows      [][]interface{}
	startRow  uint64
	endRow    uint64
	matchedRows uint64
}

type AggregateTask struct {
	column     *engine.Column
	startRow   uint64
	endRow     uint64
	aggType    AggregateType
	resultChan chan *AggregateResult
	priority   int
}

type AggregateResult struct {
	sum   int64
	min   int64
	max   int64
	count uint64
	valid bool
}

func NewWorkStealingScheduler(numWorkers int) *WorkStealingScheduler {
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}

	ctx, cancel := context.WithCancel(context.Background())
	scheduler := &WorkStealingScheduler{
		workers:     make([]*Worker, numWorkers),
		numWorkers:  numWorkers,
		globalQueue: NewWorkQueue(),
		ctx:         ctx,
		cancel:      cancel,
	}

	for i := 0; i < numWorkers; i++ {
		worker := &Worker{
			id:         i,
			localQueue: NewWorkQueue(),
			scheduler:  scheduler,
		}
		scheduler.workers[i] = worker
	}

	return scheduler
}

func NewWorkQueue() *WorkQueue {
	wq := &WorkQueue{
		tasks: make([]Task, 0, 256),
	}
	wq.cond = sync.NewCond(&wq.mu)
	return wq
}

func (wq *WorkQueue) Push(task Task) {
	wq.mu.Lock()
	wq.tasks = append(wq.tasks, task)
	wq.cond.Signal()
	wq.mu.Unlock()
}

func (wq *WorkQueue) Pop() Task {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	if len(wq.tasks) == 0 {
		return nil
	}

	task := wq.tasks[len(wq.tasks)-1]
	wq.tasks = wq.tasks[:len(wq.tasks)-1]
	return task
}

func (wq *WorkQueue) Steal() Task {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	if len(wq.tasks) == 0 {
		return nil
	}

	task := wq.tasks[0]
	wq.tasks = wq.tasks[1:]
	return task
}

func (wq *WorkQueue) Size() int {
	wq.mu.Lock()
	defer wq.mu.Unlock()
	return len(wq.tasks)
}

func (s *WorkStealingScheduler) Start() {
	for _, worker := range s.workers {
		s.wg.Add(1)
		go worker.Run(s.ctx, &s.wg)
	}
}

func (s *WorkStealingScheduler) Stop() {
	s.cancel()
	s.wg.Wait()
}

func (s *WorkStealingScheduler) Submit(task Task) {
	workerID := int(atomic.AddUint64(new(uint64), 1)) % s.numWorkers
	s.workers[workerID].localQueue.Push(task)
}

func (w *Worker) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			task := w.getTask()
			if task != nil {
				task.Execute()
				w.tasksRun.Add(1)
			}
		}
	}
}

func (w *Worker) getTask() Task {
	task := w.localQueue.Pop()
	if task != nil {
		return task
	}

	task = w.scheduler.globalQueue.Pop()
	if task != nil {
		return task
	}

	return w.stealWork()
}

func (w *Worker) stealWork() Task {
	if !w.stealing.CompareAndSwap(false, true) {
		return nil
	}
	defer w.stealing.Store(false)

	for i := 0; i < w.scheduler.numWorkers; i++ {
		victimID := (w.id + i + 1) % w.scheduler.numWorkers
		victim := w.scheduler.workers[victimID]

		if task := victim.localQueue.Steal(); task != nil {
			w.tasksStolen.Add(1)
			return task
		}
	}

	return nil
}

func (st *ScanTask) Execute() interface{} {
	result := &ScanResult{
		rows:     make([][]interface{}, 0, st.endRow-st.startRow),
		startRow: st.startRow,
		endRow:   st.endRow,
	}

	selection := make([]bool, st.endRow-st.startRow)
	for i := range selection {
		selection[i] = true
	}

	allTableCols := st.table.Columns
	for _, filter := range st.filters {
		applyFilterToPartition(allTableCols, filter, selection, st.startRow, st.endRow)
	}

	for i := st.startRow; i < st.endRow; i++ {
		if selection[i-st.startRow] {
			row := make([]interface{}, len(st.columns))
			for j, col := range st.columns {
				switch col.Type {
				case engine.TypeInt64:
					if val, ok := col.GetInt64(i); ok {
						row[j] = val
					} else {
						row[j] = nil
					}
				case engine.TypeString:
					if val, ok := col.GetString(i); ok {
						row[j] = val
					} else {
						row[j] = nil
					}
				default:
					row[j] = nil
				}
			}
			result.rows = append(result.rows, row)
			result.matchedRows++
		}
	}

	st.resultChan <- result
	return result
}

func (st *ScanTask) Priority() int {
	return st.priority
}

func applyFilterToPartition(columns []*engine.Column, filter *engine.FilterExpression, selection []bool, startRow, endRow uint64) {
	var targetCol *engine.Column
	for _, col := range columns {
		if col.Name == filter.Column {
			targetCol = col
			break
		}
	}

	if targetCol == nil {
		for i := range selection {
			selection[i] = false
		}
		return
	}

	switch targetCol.Type {
	case engine.TypeInt64:
		applyInt64FilterPartition(targetCol, filter, selection, startRow, endRow)
	case engine.TypeString:
		applyStringFilterPartition(targetCol, filter, selection, startRow, endRow)
	default:
		for i := range selection {
			selection[i] = false
		}
	}
}

func applyInt64FilterPartition(col *engine.Column, filter *engine.FilterExpression, selection []bool, startRow, endRow uint64) {
	if filterValue, ok := filter.Value.(int64); ok {
		data := (*[]int64)(col.Data)

		for i := startRow; i < endRow; i++ {
			if !selection[i-startRow] {
				continue
			}

			if i >= uint64(len(*data)) || col.Nulls[i] {
				selection[i-startRow] = false
				continue
			}

			value := (*data)[i]

			switch filter.Operator {
			case engine.OpEqual:
				selection[i-startRow] = (value == filterValue)
			case engine.OpNotEqual:
				selection[i-startRow] = (value != filterValue)
			case engine.OpLess:
				selection[i-startRow] = (value < filterValue)
			case engine.OpLessEqual:
				selection[i-startRow] = (value <= filterValue)
			case engine.OpGreater:
				selection[i-startRow] = (value > filterValue)
			case engine.OpGreaterEqual:
				selection[i-startRow] = (value >= filterValue)
			default:
				selection[i-startRow] = false
			}
		}
	}
}

func applyStringFilterPartition(col *engine.Column, filter *engine.FilterExpression, selection []bool, startRow, endRow uint64) {
	if filterValue, ok := filter.Value.(string); ok {
		data := (*[]string)(col.Data)

		for i := startRow; i < endRow; i++ {
			if !selection[i-startRow] {
				continue
			}

			if i >= uint64(len(*data)) || col.Nulls[i] {
				selection[i-startRow] = false
				continue
			}

			value := (*data)[i]

			switch filter.Operator {
			case engine.OpEqual:
				selection[i-startRow] = (value == filterValue)
			case engine.OpNotEqual:
				selection[i-startRow] = (value != filterValue)
			default:
				selection[i-startRow] = false
			}
		}
	}
}

func (at *AggregateTask) Execute() interface{} {
	result := &AggregateResult{
		valid: true,
	}

	data := (*[]int64)(at.column.Data)

	switch at.aggType {
	case AggSum, AggAvg:
		sum := int64(0)
		count := uint64(0)
		for i := at.startRow; i < at.endRow && i < uint64(len(*data)); i++ {
			if !at.column.Nulls[i] {
				sum += (*data)[i]
				count++
			}
		}
		result.sum = sum
		result.count = count

	case AggMin:
		if at.startRow < uint64(len(*data)) {
			min := (*data)[at.startRow]
			for i := at.startRow + 1; i < at.endRow && i < uint64(len(*data)); i++ {
				if !at.column.Nulls[i] && (*data)[i] < min {
					min = (*data)[i]
				}
			}
			result.min = min
		}

	case AggMax:
		if at.startRow < uint64(len(*data)) {
			max := (*data)[at.startRow]
			for i := at.startRow + 1; i < at.endRow && i < uint64(len(*data)); i++ {
				if !at.column.Nulls[i] && (*data)[i] > max {
					max = (*data)[i]
				}
			}
			result.max = max
		}

	case AggCount:
		count := uint64(0)
		for i := at.startRow; i < at.endRow && i < uint64(len(*data)); i++ {
			if !at.column.Nulls[i] {
				count++
			}
		}
		result.count = count
	}

	at.resultChan <- result
	return result
}

func (at *AggregateTask) Priority() int {
	return at.priority
}

type ParallelExecutor struct {
	scheduler     *WorkStealingScheduler
	numWorkers    int
	partitionSize uint64
}

func NewParallelExecutor(numWorkers int) *ParallelExecutor {
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}

	scheduler := NewWorkStealingScheduler(numWorkers)
	scheduler.Start()

	return &ParallelExecutor{
		scheduler:     scheduler,
		numWorkers:    numWorkers,
		partitionSize: 10000,
	}
}

func (pe *ParallelExecutor) Stop() {
	pe.scheduler.Stop()
}

func (pe *ParallelExecutor) ExecuteParallelScan(table *engine.Table, columns []*engine.Column, filters []*engine.FilterExpression) [][]interface{} {
	rowCount := table.RowCount
	if rowCount == 0 {
		return nil
	}

	numPartitions := (rowCount + pe.partitionSize - 1) / pe.partitionSize
	resultChan := make(chan *ScanResult, numPartitions)

	for i := uint64(0); i < numPartitions; i++ {
		startRow := i * pe.partitionSize
		endRow := startRow + pe.partitionSize
		if endRow > rowCount {
			endRow = rowCount
		}

		task := &ScanTask{
			table:      table,
			startRow:   startRow,
			endRow:     endRow,
			columns:    columns,
			filters:    filters,
			resultChan: resultChan,
			priority:   1,
		}

		pe.scheduler.Submit(task)
	}

	results := make([]*ScanResult, 0, numPartitions)
	for i := uint64(0); i < numPartitions; i++ {
		result := <-resultChan
		results = append(results, result)
	}
	close(resultChan)

	totalRows := uint64(0)
	for _, result := range results {
		totalRows += result.matchedRows
	}

	allRows := make([][]interface{}, 0, totalRows)
	for _, result := range results {
		allRows = append(allRows, result.rows...)
	}

	return allRows
}

func (pe *ParallelExecutor) ExecuteParallelAggregate(column *engine.Column, aggType AggregateType) *AggregateResult {
	length := column.Length
	if length == 0 {
		return &AggregateResult{valid: false}
	}

	numPartitions := (length + pe.partitionSize - 1) / pe.partitionSize
	resultChan := make(chan *AggregateResult, numPartitions)

	for i := uint64(0); i < numPartitions; i++ {
		startRow := i * pe.partitionSize
		endRow := startRow + pe.partitionSize
		if endRow > length {
			endRow = length
		}

		task := &AggregateTask{
			column:     column,
			startRow:   startRow,
			endRow:     endRow,
			aggType:    aggType,
			resultChan: resultChan,
			priority:   2,
		}

		pe.scheduler.Submit(task)
	}

	localResults := make([]*AggregateResult, 0, numPartitions)
	for i := uint64(0); i < numPartitions; i++ {
		result := <-resultChan
		localResults = append(localResults, result)
	}
	close(resultChan)

	return pe.mergeAggregateResults(localResults, aggType)
}

func (pe *ParallelExecutor) mergeAggregateResults(results []*AggregateResult, aggType AggregateType) *AggregateResult {
	final := &AggregateResult{valid: true}

	switch aggType {
	case AggSum, AggAvg:
		totalSum := int64(0)
		totalCount := uint64(0)
		for _, r := range results {
			totalSum += r.sum
			totalCount += r.count
		}
		final.sum = totalSum
		final.count = totalCount

	case AggMin:
		if len(results) > 0 {
			min := results[0].min
			for _, r := range results[1:] {
				if r.min < min {
					min = r.min
				}
			}
			final.min = min
		}

	case AggMax:
		if len(results) > 0 {
			max := results[0].max
			for _, r := range results[1:] {
				if r.max > max {
					max = r.max
				}
			}
			final.max = max
		}

	case AggCount:
		totalCount := uint64(0)
		for _, r := range results {
			totalCount += r.count
		}
		final.count = totalCount
	}

	return final
}

type NUMAAllocator struct {
	pools    []*MemoryPool
	numNodes int
}

type MemoryPool struct {
	nodeID    int
	buffers   chan []byte
	blockSize int
	allocated atomic.Uint64
	freed     atomic.Uint64
}

func NewNUMAAllocator() *NUMAAllocator {
	numNodes := runtime.NumCPU() / 8
	if numNodes < 1 {
		numNodes = 1
	}

	allocator := &NUMAAllocator{
		pools:    make([]*MemoryPool, numNodes),
		numNodes: numNodes,
	}

	for i := 0; i < numNodes; i++ {
		allocator.pools[i] = &MemoryPool{
			nodeID:    i,
			buffers:   make(chan []byte, 1024),
			blockSize: 65536,
		}
	}

	return allocator
}

func (na *NUMAAllocator) Allocate(size int, nodeHint int) []byte {
	if nodeHint < 0 || nodeHint >= na.numNodes {
		nodeHint = 0
	}

	pool := na.pools[nodeHint]

	select {
	case buf := <-pool.buffers:
		if len(buf) >= size {
			return buf[:size]
		}
	default:
	}

	buf := make([]byte, size)
	pool.allocated.Add(1)
	return buf
}

func (na *NUMAAllocator) Free(buf []byte, nodeHint int) {
	if nodeHint < 0 || nodeHint >= na.numNodes {
		nodeHint = 0
	}

	pool := na.pools[nodeHint]

	select {
	case pool.buffers <- buf:
		pool.freed.Add(1)
	default:
	}
}

func (na *NUMAAllocator) GetNodeForCPU(cpuID int) int {
	return cpuID % na.numNodes
}

func (na *NUMAAllocator) Stats() map[int]MemoryStats {
	stats := make(map[int]MemoryStats)
	for i, pool := range na.pools {
		stats[i] = MemoryStats{
			NodeID:    pool.nodeID,
			Allocated: pool.allocated.Load(),
			Freed:     pool.freed.Load(),
			Pooled:    uint64(len(pool.buffers)),
		}
	}
	return stats
}

type MemoryStats struct {
	NodeID    int
	Allocated uint64
	Freed     uint64
	Pooled    uint64
}

func AlignPointer(ptr unsafe.Pointer, alignment uintptr) unsafe.Pointer {
	addr := uintptr(ptr)
	misalignment := addr % alignment
	if misalignment != 0 {
		addr += alignment - misalignment
	}
	return unsafe.Pointer(addr)
}

func GetCacheLineSize() int {
	return 64
}