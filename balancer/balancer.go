package main

import (
	"errors"
	"fmt"
	//"github.com/wal-g/tracelog"
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
	leftBorderToShard   map[string]Shard // 10^4 * (50 + 4)
	shardToLeftBorders   map[Shard]map[string]bool // 10^4 * (4 + 4 + 50)
	leftBordersToRightBorders map[string]string // 10^4 * 100
	allShardsStats   	map[Shard]Stats // 10^3 * (4 + 8 * 5)
	keyRangeStats   	map[string]Stats // 10^4 * 8 * 5
	// average distance between two keys
	keyDistanceByRanges	map[string]*big.Int
	keysOnShard			map[Shard]uint64
	avgKeysOnShard		uint64
	// sorted list of left borders
	leftBorders			[]string // 10^4 * 50
	// sorted list of shards
	shards 				[]Shard
	muLeftBorders		sync.Mutex

	// that ones used only by planner
	keyStats			map[string]Stats
	muKeyStats			sync.Mutex

	bestTask  Task

	splits int

	_installation InstallationInterface
	_coordinator  CoordinatorInterface
	_db           DatabaseInterface
	// no need to lock

	reloadRequired		bool
	muReload			sync.Mutex

	averageStats 		Stats
	retryTime    		= time.Second * 5
	workerRetryTime		= time.Millisecond * 1000
	okLoad 				= 0.2
	workersCount		= 2
	plannerCount		= 100
	loadK				= 0.9
	keysInOneTransfer	= new(big.Int).SetInt64(1000)
	plannerRetryTime	= time.Millisecond * 250
)

func _init() {
	splits = 0
	keyDistanceByRanges = map[string]*big.Int{}
	keysOnShard = map[Shard]uint64{}
	bestTask = Task{}
	shards = []Shard{}
	leftBorders = []string{}
	keyStats = map[string]Stats{}
	allShardsStats = map[Shard]Stats{}
	keyRangeStats = map[string]Stats{}
	shardToLeftBorders = map[Shard]map[string]bool{}
	leftBorderToShard = map[string]Shard{}
	leftBordersToRightBorders = map[string]string{}

	var shardToKeyRanges map[Shard][]KeyRange
	var err error

	for {
		// shardToLeftBorders should be inited here
		shardToKeyRanges, err = _coordinator.initKeyRanges()
		if err == nil {
			break
		}
		//tracelog.ErrorLogger.PrintError(err)
		fmt.Println("Error: trying to init state by coordinator, but got an error ", err)
		time.Sleep(retryTime)
	}
	// TODO parallel
	for shard, keyRanges := range shardToKeyRanges {
		shardDistances, _ := _installation.getKeyDistanceByRanges(shard, keyRanges)
		for rng, dist := range shardDistances {
			keyDistanceByRanges[rng] = dist
		}
	}
	keysAtAll := new(big.Int)
	for sh, keyRanges := range shardToKeyRanges {
		keysOnShard[sh] = 0
		shardToLeftBorders[sh] = map[string]bool{}
		for _, kr := range keyRanges {
			shardToLeftBorders[sh][kr.left] = true
			leftBordersToRightBorders[kr.left] = kr.right
			keysOnShard[sh] += uint64(new(big.Int).Div(getLength(kr), keyDistanceByRanges[kr.left]).Int64())
			// fmt.Println(sh.id, keysOnShard[sh], kr)
		}
		keysAtAll.Add(keysAtAll, new(big.Int).SetUint64(keysOnShard[sh]))
	}
	avgKeysOnShard = uint64(new(big.Int).Div(keysAtAll, new(big.Int).SetInt64(int64(len(shardToKeyRanges)))).Int64())
	for shard, ranges := range shardToLeftBorders {
		for left := range ranges {
			leftBorderToShard[left] = shard
			leftBorders = append(leftBorders, left)
		}
	}
	sort.Sort(LikeNumbers(leftBorders))
	shards = []Shard{}
	for sh := range shardToLeftBorders {
		shards = append(shards, sh)
	}

	muReload.Lock()
	reloadRequired = false
	muReload.Unlock()
	// Init random seed
	rand.Seed(time.Now().UTC().UnixNano())
}

func _findRange(s *string, left, right int, _leftBorders *[]string) int {
	//TODO ensure that right key range contains keys from smth to 'infinity'

	middle := (right + left) / 2
	if left == right - 1 {
		return left
	}

	if less(s, &(*_leftBorders)[middle]) {
		return _findRange(s, left, middle, _leftBorders)
	}

	return _findRange(s, middle, right, _leftBorders)
}

func findRange(s *string, _leftBorders *[]string) int {
	defer muLeftBorders.Unlock()
	muLeftBorders.Lock()
	return _findRange(s, 0, len(*_leftBorders), _leftBorders)
}

func tryToUpdateShardStats(shard Shard, wg *sync.WaitGroup)  {
	defer wg.Done()
	var keyRanges []KeyRange
	for left := range shardToLeftBorders[shard] {
		keyRanges = append(keyRanges, KeyRange{left, leftBordersToRightBorders[left]})
	}
	_stats, err := _installation.getShardStats(shard, keyRanges)

	if err != nil {
		//tracelog.ErrorLogger.PrintError(err)
		fmt.Println("Error: ", err)
		return
	}

	allShardsStats[shard] = Stats{}
	for keyRange, keysMap := range _stats {
		for _, _keyStats := range keysMap {
			_, ok := keyRangeStats[keyRange]
			if !ok {
				keyRangeStats[keyRange] = Stats{}
			}
			keyRangeStats[keyRange] = AddStats(keyRangeStats[keyRange], _keyStats)
			allShardsStats[shard] = AddStats(allShardsStats[shard], _keyStats)
		}
	}
}

func updateAllStats() {
	var wg sync.WaitGroup
	for shard := range shardToLeftBorders {
		wg.Add(1)
		tryToUpdateShardStats(shard, &wg)
	}
	wg.Wait()
	averageStats = Stats{}
	for _, _keyRangeStats := range keyRangeStats {
		//TODO check for stats overflow
		averageStats = AddStats(averageStats, _keyRangeStats)
	}
	if len(shardToLeftBorders) == 0 {
		fmt.Println("Error: shards count = 0")
		return
	}
	averageStats = DivideStats(averageStats, float64(len(shardToLeftBorders)))
}

func getFoo(value, avgValue float64, useAbs bool) float64 {
	var res float64 = 0
	var delta = value - avgValue
	if useAbs {
		// in that case, we increase res only if stat is greater than average one
		delta = math.Abs(delta)
	}
	if delta > okLoad * avgValue {
		res += math.Pow(delta / avgValue * 100, 2)
	}
	return res
}

// TODO don't know how to name it
func getFooByStats(stats Stats, useAbs bool) float64 {
	var res float64 = 0
	res += getFoo(stats.user_time, averageStats.user_time, useAbs)
	res += getFoo(stats.system_time, averageStats.system_time, useAbs)
	res += getFoo(float64(stats.reads), float64(averageStats.reads), useAbs)
	res += getFoo(float64(stats.writes), float64(averageStats.writes), useAbs)

	return res
}

func getStatsOnSubrange(rng, subrng KeyRange, keyStatsInRange *map[string]map[string]Stats) Stats {
	res := Stats{}
	for key, stat := range (*keyStatsInRange)[rng.left] {
		if !less(&key, &subrng.left) && less(&key, &subrng.right) {
			res = AddStats(res, stat)
		}
	}
	return res
}

func setTaskProfitByKeysCount(startRange KeyRange, task *Task, keyStatsInRange *map[string]map[string]Stats) float64 {
	keysDiff := uint64(new(big.Int).Div(getLength(task.keyRange), keyDistanceByRanges[startRange.left]).Int64())
	keysOnShardFrom := keysOnShard[task.shardFrom] - keysDiff
	keysOnShardTo := keysOnShard[task.shardTo] + keysDiff

	res := -getFoo(float64(keysOnShard[task.shardFrom]), float64(avgKeysOnShard), true)
	res -= getFoo(float64(keysOnShard[task.shardTo]), float64(avgKeysOnShard), true)
	res += getFoo(float64(keysOnShardFrom), float64(avgKeysOnShard), true)
	res += getFoo(float64(keysOnShardTo), float64(avgKeysOnShard), true)
	return res
}

func setTaskProfitByLoad(startRange KeyRange, task *Task, keyStatsInRange *map[string]map[string]Stats) float64 {
	shardFromStats := allShardsStats[task.shardFrom]
	shardToStats := allShardsStats[task.shardTo]
	s := getStatsOnSubrange(startRange, task.keyRange, keyStatsInRange)
	shardFromStats = SubtractStats(shardFromStats, s)
	shardToStats = AddStats(shardToStats, s)
	res := -getFooByStats(allShardsStats[task.shardFrom], true) + getFooByStats(shardFromStats, true)
	res += -getFooByStats(allShardsStats[task.shardTo], true) + getFooByStats(shardToStats, true)
	return res
}

func setTaskProfitByLength(startRange KeyRange, task *Task, keyStatsInRange *map[string]map[string]Stats) float64 {
	leftGluing := false
	rightGluing := false
	task.splitBy = []string{}
	res := -logLength(startRange)
	task.startKeyRange = startRange
	task.oldKeyRangesOnShardTo = map[KeyRange]Stats{}
	task.oldKeyRangesOnShardFrom = map[KeyRange]Stats{}
	task.newKeyRangesOnShardTo = map[KeyRange]Stats{}
	task.newKeyRangesOnShardFrom = map[KeyRange]Stats{}

	task.oldKeyRangesOnShardFrom[startRange] = keyRangeStats[startRange.left]
	if less(&startRange.left, &task.keyRange.left) {
		leftSubrng := KeyRange{
			startRange.left,
			task.keyRange.left,
		}
		res += logLength(leftSubrng)
		task.splitBy = append(task.splitBy, task.keyRange.left)
		task.newKeyRangesOnShardFrom[leftSubrng] = getStatsOnSubrange(startRange, leftSubrng, keyStatsInRange)
	} else {
		leftGluing = true
	}
	if less(&task.keyRange.right, &startRange.right) {
		rightSubrng := KeyRange{
			task.keyRange.right,
			startRange.right,
		}
		res += logLength(rightSubrng)
		task.splitBy = append(task.splitBy, task.keyRange.right)
		task.newKeyRangesOnShardFrom[rightSubrng] = getStatsOnSubrange(startRange, rightSubrng, keyStatsInRange)
	} else {
		rightGluing = true
	}

	leftNeighbor := ""
	rightNeighbor := ""
	if leftGluing {
		leftBorderIndex := findRange(&task.keyRange.left, &leftBorders)
		if leftBorderIndex - 1 >= 0 {
			leftNeighbor = leftBorders[leftBorderIndex - 1]
		} else {
			leftGluing = false
		}
		leftGluing = leftGluing && leftBorderToShard[leftNeighbor] == task.shardTo
	}
	if rightGluing {
		rightBorderIndex := findRange(&task.keyRange.right, &leftBorders)
		rightNeighbor = leftBorders[rightBorderIndex]
		rightGluing = rightGluing && leftBorderToShard[rightNeighbor] == task.shardTo
	}
	newRng := task.keyRange
	newStats := getStatsOnSubrange(startRange, task.keyRange, keyStatsInRange)
	if leftGluing {
		leftNeighborRng := KeyRange{
			leftNeighbor,
			leftBordersToRightBorders[leftNeighbor],
		}
		res -= logLength(leftNeighborRng)
		task.oldKeyRangesOnShardTo[leftNeighborRng] = keyRangeStats[leftNeighborRng.left]
		newStats = AddStats(newStats, keyRangeStats[leftNeighborRng.left])
		newRng.left = leftNeighborRng.left
	}
	if rightGluing {
		rightNeighborRng := KeyRange{
			rightNeighbor,
			leftBordersToRightBorders[rightNeighbor],
		}
		res -= logLength(rightNeighborRng)
		task.oldKeyRangesOnShardTo[rightNeighborRng] = keyRangeStats[rightNeighborRng.left]
		newStats = AddStats(newStats, keyRangeStats[rightNeighborRng.left])
		newRng.right = rightNeighborRng.right
	}
	res += logLength(newRng)
	task.newKeyRangesOnShardTo[newRng] = newStats
	return res
}

//TODO mb add cache
//TODO may be upper border of subrange is required
func getSubrange(leftKey *string, keyRange KeyRange) KeyRange {
	rightSubrange := KeyRange{*leftKey, keyRange.right}
	rightKeyInt := new(big.Int).Mul(keysInOneTransfer, keyDistanceByRanges[keyRange.left])
	rightKey := bigIntToKey(new(big.Int).Add(rightKeyInt, keyToBigInt(leftKey)))
	if !less(rightKey, &keyRange.right) {
		return rightSubrange
	}
	return KeyRange{*leftKey, *rightKey}
}

func brutTasksFromShard(shard Shard,
	shardStats map[string]map[string]Stats) {

	var subrangeLeftKeys []string
	for subrangeLeftKey := range shardToLeftBorders[shard] {
		subrangeLeftKeys = append(subrangeLeftKeys, subrangeLeftKey)
	}

	if splits > 0 {
		for leftBorder := range shardToLeftBorders[shard] {
			for key := range shardStats[leftBorder] {
				subrangeLeftKeys = append(subrangeLeftKeys[:1], key)
				break
			}
			break
		}
		splits -= 1
	}

	for _, subrangeLeftKey := range subrangeLeftKeys {
		for _, shardTo := range shards {
			if shardTo == shard {
				continue
			}
			leftBorder := leftBorders[findRange(&subrangeLeftKey, &leftBorders)]
			keyRange := KeyRange{left: leftBorder,
				right: leftBordersToRightBorders[leftBorder]}
			subrange := getSubrange(&subrangeLeftKey, keyRange)
			task := Task{shardFrom: shard, shardTo: shardTo, keyRange: subrange}
			load := setTaskProfitByLoad(keyRange, &task, &shardStats)
			space := setTaskProfitByKeysCount(keyRange, &task, &shardStats)
			length := setTaskProfitByLength(keyRange, &task, &shardStats)
			task.profit = (load + space) * loadK + length * (1 - loadK)
			if task.profit > -1 {
				t1 := task
				_ = setTaskProfitByLoad(keyRange, &t1, &shardStats)
				_ = setTaskProfitByLength(keyRange, &t1, &shardStats)
				_ = setTaskProfitByKeysCount(keyRange, &t1, &shardStats)
			}
			if task.profit < bestTask.profit {
				bestTask = task
			}
		}
	}
}

func applyRemoveRanges(kr KeyRange, stats Stats, shard Shard, toRemove *map[string]bool) {
	allShardsStats[shard] = SubtractStats(allShardsStats[shard], stats)
	delete(keyRangeStats, kr.left)
	delete(leftBorderToShard, kr.left)
	delete(shardToLeftBorders[shard], kr.left)
	delete(leftBordersToRightBorders, kr.left)
	delete(keyRangeStats, kr.left)
	(*toRemove)[kr.left] = true
}
func applyAppendRanges(kr KeyRange, stats Stats, shard Shard, toAppend *map[string]bool) {
	allShardsStats[shard] = AddStats(allShardsStats[shard], stats)
	keyRangeStats[kr.left] = stats
	leftBorderToShard[kr.left] = shard
	shardToLeftBorders[shard][kr.left] = true
	leftBordersToRightBorders[kr.left] = kr.right
	keyRangeStats[kr.left] = stats
	(*toAppend)[kr.left] = true
}

func isKeyFromRange(key *string, rng *KeyRange) bool {
	return !less(key, &rng.left) && less(key, &rng.right)
}

func getLength(kr KeyRange) *big.Int {
	return new(big.Int).Sub(keyToBigInt(&kr.right), keyToBigInt(&kr.left))
}

func getAvgKeyDistance(keyDist1, keyDist2, len1, len2 *big.Int) *big.Int {
	//   n + m        n + m         (n + m) * a * b
	// _________ = ____________ =  _________________
	// n/a + m/b    b*n + a*m        b * n + a * m
	//              ---------
	//                 a*b
	// n = len1; m = len2; a = keyDist1; b = keyDist2;

	num := new(big.Int).Mul(
		new(big.Int).Add(
			len1,
			len2,
			),
		new(big.Int).Mul(
			keyDist1,
			keyDist2,
			),
		)
	den := new(big.Int).Add(
		new(big.Int).Mul(
			keyDist2,
			len1,
			),
		new(big.Int).Mul(
			keyDist1,
			len2,
			),
		)
	return new(big.Int).Div(num, den)
}

func applyTask(task Task, shardStats *map[string]map[string]Stats) {
	if task.oldKeyRangesOnShardFrom == nil {
		return
	}
	removeLeftBorders := map[string]bool{}
	appendLeftBorders := map[string]bool{}
	if len(leftBorders) != len(leftBordersToRightBorders) {
		fmt.Println("hmm")
	}
	var oldDistance *big.Int

	for kr, stats := range task.oldKeyRangesOnShardFrom {
		applyRemoveRanges(kr, stats, task.shardFrom, &removeLeftBorders)
		oldDistance = keyDistanceByRanges[kr.left]
		delete(keyDistanceByRanges, kr.left)
	}
	newDistance := oldDistance
	newLen := getLength(task.keyRange)
	for kr, stats := range task.oldKeyRangesOnShardTo {
		applyRemoveRanges(kr, stats, task.shardTo, &removeLeftBorders)
		l := getLength(kr)
		newDistance = getAvgKeyDistance(newDistance, keyDistanceByRanges[kr.left], newLen, l)
		newLen.Add(newLen, l)
		delete(keyDistanceByRanges, kr.left)
	}

	for kr, stats := range task.newKeyRangesOnShardFrom {
		applyAppendRanges(kr, stats, task.shardFrom, &appendLeftBorders)
		keyDistanceByRanges[kr.left] = oldDistance
	}
	for kr, stats := range task.newKeyRangesOnShardTo {
		applyAppendRanges(kr, stats, task.shardTo, &appendLeftBorders)
		keyDistanceByRanges[kr.left] = newDistance
	}
	keysDiffOnShard := uint64(new(big.Int).Div(getLength(task.keyRange), oldDistance).Int64())
	if keysDiffOnShard >= keysOnShard[task.shardFrom] {
		keysDiffOnShard = keysOnShard[task.shardFrom]
	}
	keysOnShard[task.shardFrom] -= keysDiffOnShard
	keysOnShard[task.shardTo] += keysDiffOnShard

	newShardStats := map[string]map[string]Stats{}
	for newKr := range task.newKeyRangesOnShardFrom {
		newShardStats[newKr.left] = map[string]Stats{}
	}
	for key, stat := range (*shardStats)[task.keyRange.left] {
		for newKr := range task.newKeyRangesOnShardFrom {
			if isKeyFromRange(&key, &newKr) {
				newShardStats[newKr.left][key] = stat
			}
		}
	}
	delete(*shardStats, task.keyRange.left)
	for newK, v := range newShardStats {
		(*shardStats)[newK] = v
	}

	for left := range removeLeftBorders {
		i := findRange(&left, &leftBorders)
		if i == len(leftBorders) - 1 {
			leftBorders = leftBorders[:i]
		} else {
			leftBorders = append(leftBorders[:i], leftBorders[i + 1:]...)
		}
	}
	for left := range appendLeftBorders {
		leftBorders = append(leftBorders, left)
	}
	sort.Sort(LikeNumbers(leftBorders))
	q := len(leftBorders)
	w := len(leftBordersToRightBorders)
	if q != w {
		fmt.Println("hmm")
	}
	q = len(leftBorders)
	w = len(leftBordersToRightBorders)
	if q != w {
		lbm := map[string]bool{}
		for _, a := range leftBorders {
			_, ok := lbm[a]
			if ok {
				fmt.Println("hmm")
			}
			lbm[a] = true
			_, ok = leftBordersToRightBorders[a]
			if !ok {
				q += 1
				leftBorders = append(leftBorders, "sooqa")
				fmt.Println("hmm")
			}
		}
		for a := range leftBordersToRightBorders {
			_, ok := lbm[a]
			if !ok {
				q += 1
				fmt.Println("hmm")
			}
		}
		q = len(leftBorders)
		w = len(leftBordersToRightBorders)
		fmt.Println("hmm")
	}
}

//TODO: make possible parallel runTask
func runTask(task *Action) error {
	var err error

	for task.actionStage != done {
		switch task.actionStage {
		case plan:
			err = _coordinator.splitKeyRange(&task.keyRange.left)
			if err != nil {
				fmt.Println("Error: split problems with ", task.keyRange.left, err)
				return err
			}
			err = _coordinator.splitKeyRange(&task.keyRange.right)
			if err != nil {
				fmt.Println("Error: split problems with ", task.keyRange.right, err)
				return err
			}
			task.actionStage = split
		case split:
			//TODO ensure that task.KeyRange in one actual range
			err = _coordinator.lockKeyRange(task.keyRange)
			if err != nil {
				fmt.Println("Error: lock problems with range ", task.keyRange, err)
				return err
			}
			task.actionStage = lock
		case lock:
			err = _installation.startTransfer(*task)
			if err != nil {
				fmt.Println("Error: transfer problems with range ", task.keyRange, err)
				return err
			}
			task.actionStage = transfer
		case transfer:
			err = _coordinator.moveKeyRange(task.keyRange, task.fromShard, task.toShard)
			if err != nil {
				fmt.Println("Error: move problems with range ", task.keyRange, err)
				return err
			}
			task.actionStage = move
		case move:
			err = _coordinator.unlockKeyRange(task.keyRange)
			if err != nil {
				fmt.Println("Error: unlock problems with range ", task.keyRange, err)
				return err
			}
			task.actionStage = unlock
		case unlock:
			err = _installation.removeRange(task.keyRange, task.fromShard)
			if err != nil {
				fmt.Println("Error: delete problems with range ", task.keyRange, err)
				return err
			}
			task.actionStage = remove
		case remove:
			err = _coordinator.mergeKeyRanges(&task.keyRange.left)
			if err != nil {
				fmt.Println("Error: merge problems with border ", &task.keyRange.left, err)
				return err
			}
			err = _coordinator.mergeKeyRanges(&task.keyRange.right)
			if err != nil {
				fmt.Println("Error: merge problems with border ", &task.keyRange.right, err)
				return err
			}
			task.actionStage = merge

		default:
			return errors.New(fmt.Sprint("Not known actionStage: ", task.actionStage))
		}

		for {
			err = _db.Update(task)
			if err == nil {
				break
			}
			fmt.Println("Error: update action error ", task.id, err)
			time.Sleep(workerRetryTime)
		}

		if task.actionStage == done {
			for {
				err = _db.Delete(task)
				if err == nil {
					break
				}
				fmt.Println("Error: delete action error ", task.id, err)
				time.Sleep(workerRetryTime)
			}
		}
	}

	return nil
}

func runWorker() {
	i := 0
	for {
		action, ok := _db.GetAndRun()
		if !ok {
			fmt.Println("No tasks, wait")
			time.Sleep(workerRetryTime)
			continue
		}
		i += 1
		fmt.Println("tasks done: ", i)
		err := runTask(&action)
		if err != nil {
			fmt.Println("Error while task running: ", err)
			continue
		}
	}
}

func planTasks() {
	shard := shards[len(shards) - 1]
	var keyRanges []KeyRange
	for left := range shardToLeftBorders[shard] {
		keyRanges = append(keyRanges, KeyRange{left, leftBordersToRightBorders[left]})
	}
	shardStats, err := _installation.getShardStats(shard, keyRanges)

	if err != nil {
		//tracelog.ErrorLogger.PrintError(err)
		fmt.Println("Error: ", err)
		return
	}
	bestTask = Task{}
	for i := 0; i < plannerCount; i++ {
		brutTasksFromShard(shard, shardStats)
		//fmt.Println("Plan (id ", bestTask.id, ") to move ", bestTask.keyRange, " from ", bestTask.startKeyRange, " in shard ", bestTask.shardFrom,
		//	" to shard ", bestTask.shardTo)

		for _, sh := range shards{
			foo := getFooByShard(sh, false)
			fmt.Println("Shard stats without abs", sh.id, foo)
		}
		for _, sh := range shards{
			foo := getFooByShard(sh, true)
			fmt.Println("Shard stats with abs", sh.id, foo)
		}

		fmt.Println("Ranges count: ", len(leftBorderToShard))
		fmt.Println("Profit: ", bestTask.profit)

		fmt.Println("Range len", new(big.Int).Sub(keyToBigInt(&bestTask.keyRange.right), keyToBigInt(&bestTask.keyRange.left)))
		fmt.Println("To: ", bestTask.shardTo.id)

		fmt.Println("______________________")

		if bestTask.profit < 0 {
			bestAction := Action{
				id: 0,
				actionStage: plan,
				keyRange: bestTask.keyRange,
				fromShard: bestTask.shardFrom,
				toShard: bestTask.shardTo,
				isRunning: false,
			}

			err = _db.Insert(&bestAction)
			if err != nil {
				fmt.Println("Error: insert action error:", bestAction, err)
				continue
			}

			applyTask(bestTask, &shardStats)
		}
		if bestTask.profit > -1 {
			splits += 1
			// there is nothing else we can do
			bestTask = Task{}
			continue
		}
		bestTask = Task{}
	}
}

func justBrutForceStrategy() {
	var err error

	// TODO: merge all possible kr's? Or not.

	// TODO: mb we should wait til new stats is going to be collected.
	// If not, after transfer we will see, that transfered range is not loaded, but it might be not true

	muReload.Lock()
	reloadRequired = true
	muReload.Unlock()

	for {
		err = _db.MarkAllNotRunning()
		if err == nil {
			break
		}
		fmt.Println("Error: mark actions as not running ", err)
		time.Sleep(plannerRetryTime)
	}
	for j := 0; j < workersCount; j++ {
		fmt.Println("add worker")
		go runWorker()
	}
	for {
		muReload.Lock()
		reload := reloadRequired
		muReload.Unlock()
		if !reload {
			reload, err = _coordinator.isReloadRequired()
			if err != nil {
				fmt.Println("Error while spqr.isReloadRequired call: ", err)
				reload = true
			}
		}
		if reload {
			fmt.Println("Reload, tasks ", _db.Len())
			_init()
			updateAllStats()
			fmt.Println("Reload done")
		}

		sort.Sort(ShardsByFoo(shards))
		planTasks()

		for {
			fmt.Println("wait for workers")
			if _db.Len() == 0 {
				break
			}
			time.Sleep(plannerRetryTime)
		}
		fmt.Println("Tasks ", _db.Len())
	}
}

func _main(shards InstallationInterface, coordinator CoordinatorInterface, db DatabaseInterface) {
	_installation = shards
	_coordinator = coordinator
	_db = db

	justBrutForceStrategy()
}
