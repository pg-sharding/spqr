package balancer

import (
	"github.com/wal-g/tracelog"
	"math/big"
	"math/rand"

	"math"
	"sort"
	"sync"
	"time"
)

// TODO use only one place to store strings
var (
	// that ones used by planner and workers
	leftBorderToShard   map[string]Shard
	shardToLeftBorders   map[Shard]map[string]bool
	leftBordersToRightBorders map[string]string
	allShardsStats   	map[Shard]Stats
	keyRangeStats   	map[string]Stats
	// sorted list of left borders
	leftBorders			[]string
	muLeftBorders		sync.Mutex

	muPlanerWorker sync.Mutex

	// that ones used only by workers
	lockedLeftBorders   	map[KeyRange]bool
	muLockedLeftBorders  	sync.Mutex

	// that ones used only by planner
	keyStats			map[string]Stats
	muKeyStats			sync.Mutex

	bestTask  Task
	taskTop	  []Task
	plannerCount int
	muTaskTop sync.Mutex

	_database DatabaseInterface
	_spqr SpqrInterface
	// no need to lock
	taskQueue			TaskQueue

	averageStats 		Stats
	retryTime    		= time.Second * 5
	workerRetryTime		= time.Millisecond * 500
	okLoad 				= 0.2
	EPS					= 0.0001
	workersCount		= 1
)

func _init() {
	// TODO: init all ^
	allShardsStats = map[Shard]Stats{}
	keyRangeStats = map[string]Stats{}
	lockedLeftBorders = map[KeyRange]bool{}
	shardToLeftBorders = map[Shard]map[string]bool{}
	leftBorderToShard = map[string]Shard{}
	taskTop = []Task{}
	taskQueue = *InitQueue()

	for {
		// shardToLeftBorders should be inited here
		err := _spqr.initKeyRanges()
		if err == nil {
			break
		}
		tracelog.ErrorLogger.PrintError(err)
		time.Sleep(retryTime)
	}

	for shard, ranges := range shardToLeftBorders {
		for left := range ranges {
			leftBorderToShard[left] = shard
			leftBorders = append(leftBorders, left)
		}
	}
	sort.Sort(LikeNumbers(leftBorders))

	// Init random seed
	rand.Seed(time.Now().UTC().UnixNano())
}

func findRange(s *string, left, right int, _leftBorders *[]string) int {
	//TODO ensure that right key range contains keys from smth to 'infinity'

	middle := (right + left) / 2
	if left == right + 1 {
		return left
	}

	if less(s, &(*_leftBorders)[middle]) {
		return findRange(s, left, middle, _leftBorders)
	}

	return findRange(s, middle, right, _leftBorders)
}

func findRangeEntry(s *string, _leftBorders *[]string) int {
	defer muLeftBorders.Unlock()
	muLeftBorders.Lock()
	return findRange(s, 0, len(*_leftBorders), _leftBorders)
}

func tryToUpdateShardStats(shard Shard, wg *sync.WaitGroup)  {
	defer wg.Done()
	var keyRanges []KeyRange
	for left := range shardToLeftBorders[shard] {
		keyRanges = append(keyRanges, KeyRange{left, leftBordersToRightBorders[left]})
	}
	_stats, err := _database.getShardStats(shard, keyRanges)

	if err != nil {
		tracelog.ErrorLogger.PrintError(err)
		return
	}
	muAllStats.Lock()
	shardStats := Stats{}
	allShardsStats[shard] = Stats{reads: 0, writes: 0, user_time: 0, system_time: 0, size: 0}
	for keyRange, keysMap := range _stats {
		for _, _keyStats := range keysMap {
			_, ok := keyRangeStats[keyRange]
			if !ok {
				keyRangeStats[keyRange] = Stats{}
			}
			AddStats(keyRangeStats[keyRange], _keyStats)
			AddStats(shardStats, _keyStats)
		}
	}
	muAllStats.Unlock()
}

func updateAllStats() {
	var wg sync.WaitGroup
	for shard := range shardToLeftBorders {
		wg.Add(1)
		go tryToUpdateShardStats(shard, &wg)
	}
	wg.Wait()
	averageStats = Stats{}
	for _, _keyRangeStats := range keyRangeStats {
		//TODO check for stats overflow
		AddStats(averageStats, _keyRangeStats)
	}
	DivideStats(averageStats, float64(len(shardToLeftBorders)))
}

// TODO don't know how to name it
func getFooByStats(stats Stats) float64 {
	var res float64 = 0
	var delta = math.Abs(stats.user_time - averageStats.user_time)

	if delta > okLoad * stats.user_time {
		res += math.Pow(delta / averageStats.user_time * 100, 2)
	}

	delta = math.Abs(stats.system_time - averageStats.system_time)
	if delta > okLoad * stats.system_time {
		res += math.Pow(delta / averageStats.system_time * 100, 2)
	}

	delta = math.Abs((float64)(stats.reads - averageStats.reads))
	if delta > okLoad * (float64)(stats.reads) {
		res += math.Pow(delta / (float64)(averageStats.reads) * 100, 2)
	}

	delta = math.Abs((float64)(stats.writes - averageStats.writes))
	if delta > okLoad * (float64)(stats.writes) {
		res += math.Pow(delta / (float64)(averageStats.writes) * 100, 2)
	}

	delta = math.Abs((float64)(stats.size - averageStats.size))
	if delta > okLoad * (float64)(stats.size) {
		res += math.Pow(delta / (float64)(averageStats.size) * 100, 2)
	}

	return res
}

func setTaskProfitByLoad(startRange KeyRange, task *Task, keyStatsInRange map[string]map[string]Stats) {
	oldShard := allShardsStats[task.shardFrom]
	newShard := allShardsStats[task.shardTo]
	for key, stat := range keyStatsInRange[startRange.left] {
		if less(&task.keyRange.left, &key) && less(&key, &task.keyRange.right) {
			SubtractStats(oldShard, stat)
			AddStats(newShard, stat)
		}
	}
	task.profit = -getFooByStats(allShardsStats[task.shardFrom]) + getFooByStats(oldShard)
	task.profit += -getFooByStats(allShardsStats[task.shardTo]) + getFooByStats(newShard)
}

func setTaskProfitByLength(startRange KeyRange, task *Task) {
	leftGluing := false
	rightGluing := false
	task.profit -= logLength(startRange)
	//task.profit += math.Pow(logLength(task.keyRange), 2)
	if less(&startRange.left, &task.keyRange.left) {
		task.profit += logLength(KeyRange{
			startRange.left,
			task.keyRange.left,
		})
	} else {
		leftGluing = true
	}
	if less(&task.keyRange.right, &startRange.right) {
		task.profit += logLength(KeyRange{
			task.keyRange.right,
			startRange.right,
		})
	} else {
		rightGluing = true
	}

	leftNeighbor := ""
	rightNeighbor := ""
	if leftGluing {
		leftBorderIndex := findRangeEntry(&task.keyRange.left, &leftBorders)
		if leftBorderIndex - 1 >= 0 {
			leftNeighbor = leftBorders[leftBorderIndex - 1]
		} else {
			leftGluing = false
		}
		leftGluing = leftGluing && leftBorderToShard[leftNeighbor] == task.shardTo
	}
	if rightGluing {
		rightBorderIndex := findRangeEntry(&task.keyRange.right, &leftBorders)
		rightNeighbor = leftBorders[rightBorderIndex]
		rightGluing = rightGluing && leftBorderToShard[rightNeighbor] == task.shardTo
	}
	if rightGluing && leftGluing {
		task.profit -= logLength(KeyRange{
			leftNeighbor,
			leftBordersToRightBorders[leftNeighbor],
		})
		task.profit -= logLength(KeyRange{
			rightNeighbor,
			leftBordersToRightBorders[rightNeighbor],
		})
		task.profit += logLength(KeyRange{
			leftNeighbor,
			leftBordersToRightBorders[rightNeighbor],
		})
	}
}

// TODO mb use more complex function
func getProbabilityToCalculate(n int) float64 {
	return math.Min(1.0, 1.0 / (0.01 * (float64)(n)))
}

func getSubrange(key *string, keyRange KeyRange, subrangeLen *big.Int) KeyRange {
	left := keyToBigInt(&keyRange.left)
	right := keyToBigInt(&keyRange.right)
	keyInt := keyToBigInt(key)
	n := new(big.Int)
	n.Div(new(big.Int).Sub(keyInt, left), subrangeLen)
	leftSub := big.NewInt(0)

	if n.Cmp(left) > 0 {
		leftSub.Add(left, new(big.Int).Mul(subrangeLen, n))
	} else {
		leftSub.Set(left)
	}

	rightSub := new(big.Int).Add(leftSub, subrangeLen)

	if rightSub.Cmp(right) > 0 {
		rightSub.Set(right)
	}
	return KeyRange{left: *bigIntToKey(leftSub), right: *bigIntToKey(rightSub)}
}

//TODO mb add cache
func getSubrangeLength(keyRange KeyRange) *big.Int {
	size := keyRangeStats[keyRange.left].size
	partSize := uint64(float64(size) / math.Pow(10, 3))
	minPart := uint64(math.Pow(2, 24))
	if partSize < minPart {
		partSize = minPart
	}
	partsCount := size / partSize
	left := keyToBigInt(&keyRange.left)
	right := keyToBigInt(&keyRange.right)
	return new(big.Int).Div(right.Sub(right, left), new(big.Int).SetInt64(int64(partsCount)))
}

func brutTasksFromShard(shard Shard,
	sortedShards *[]Shard,
	shardStats map[string]map[string]Stats) {

	for left := range shardToLeftBorders[shard] {
		for _, shardTo := range *sortedShards {
			keyRange := KeyRange{left: left,
				right: leftBordersToRightBorders[left]}
			subrangeLen := getSubrangeLength(keyRange)
			subrange := getSubrange(&left, keyRange, subrangeLen)
			task := Task{shardFrom: shard, shardTo: shardTo, keyRange: subrange}
			setTaskProfitByLoad(keyRange, &task, shardStats)
			setTaskProfitByLength(keyRange, &task)
			if task.profit > bestTask.profit {
				bestTask = task
			}
		}
	}
}

func isTaskValid(task *Task) bool {
	leftIntervalIndex := findRangeEntry(&task.keyRange.left, &leftBorders)
	rightIntervalIndex := findRangeEntry(&task.keyRange.right, &leftBorders)
	return leftIntervalIndex == rightIntervalIndex
}

func applyRemoveRanges(kr KeyRange, stats Stats, shard Shard, toRemove *map[string]bool) {
	SubtractStats(allShardsStats[shard], stats)
	delete(keyRangeStats, kr.left)
	delete(leftBorderToShard, kr.left)
	delete(shardToLeftBorders[shard], kr.left)
	delete(leftBordersToRightBorders, kr.left)
	delete(keyRangeStats, kr.left)
	(*toRemove)[kr.left] = true
}
func applyAppendRanges(kr KeyRange, stats Stats, shard Shard, toAppend *map[string]bool) {
	AddStats(allShardsStats[shard], stats)
	keyRangeStats[kr.left] = stats
	leftBorderToShard[kr.left] = shard
	shardToLeftBorders[shard][kr.left] = true
	leftBordersToRightBorders[kr.left] = kr.right
	keyRangeStats[kr.left] = stats
	(*toAppend)[kr.left] = true
}

func applyTask(task Task) {
	var removeLeftBorders map[string]bool
	var appendLeftBorders map[string]bool
	for kr, stats := range task.oldKeyRangesOnShardFrom {
		applyRemoveRanges(kr, stats, task.shardFrom, &removeLeftBorders)
	}
	for kr, stats := range task.oldKeyRangesOnShardTo {
		applyRemoveRanges(kr, stats, task.shardTo, &removeLeftBorders)
	}
	for kr, stats := range task.newKeyRangesOnShardFrom {
		applyAppendRanges(kr, stats, task.shardFrom, &appendLeftBorders)
	}
	for kr, stats := range task.newKeyRangesOnShardTo {
		applyAppendRanges(kr, stats, task.shardTo, &appendLeftBorders)
	}
	for left := range removeLeftBorders {
		i := findRangeEntry(&left, &leftBorders)
		if i == len(leftBorders) - 1 {
			leftBorders = append(leftBorders[:i])
		} else {
			leftBorders = append(leftBorders[:i], leftBorders[i + 1:]...)
		}
	}
	for left := range removeLeftBorders {
		leftBorders = append(leftBorders, left)
	}
	sort.Sort(LikeNumbers(leftBorders))
}

func runWorker(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		task, ok := PopQueue(&taskQueue)
		if !ok {
			time.Sleep(workerRetryTime)
			continue
		}
		if !isTaskValid(&task) {
			continue
		}
		err := _database.startTransfer(task)
		if err != nil {
			tracelog.ErrorLogger.PrintError(err)
			continue
		}
		// TODO merge intervals
	}
}

func brutTasks(shards *[]Shard) {
	shard := (*shards)[len(*shards) - 1]
	var keyRanges []KeyRange
	for left := range shardToLeftBorders[shard] {
		keyRanges = append(keyRanges, KeyRange{left, leftBordersToRightBorders[left]})
	}
	shardStats, err := _database.getShardStats(shard, keyRanges)

	if err != nil {
		tracelog.ErrorLogger.PrintError(err)
		return
	}

	for i := 0; i < plannerCount; i++ {
		brutTasksFromShard(shard, shards, shardStats)
		taskTop = append(taskTop, bestTask)
		applyTask(bestTask)
	}
}

func justBrutForceStrategy() {
	var shards []Shard
	for {
		updateAllStats()
		sort.Sort(ShardsByFoo(shards))
		brutTasks(&shards)
		muTaskTop.Lock()
		for _, t := range taskTop {
			AddQueue(&taskQueue, t)
		}
		muTaskTop.Unlock()
	}
}

func _main() {
	_init()
	var wg sync.WaitGroup
	for i := 0; i < workersCount; i++ {
		wg.Add(1)
		go runWorker(&wg)
	}
	justBrutForceStrategy()
	wg.Wait()
}
