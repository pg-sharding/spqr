package pkg

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/wal-g/tracelog"
)

// TODO use only one place to store strings

type Balancer struct {
	// that ones used by planner and workers
	leftBorderToShard         map[string]Shard          // 10^4 * (50 + 4)
	shardToLeftBorders        map[Shard]map[string]bool // 10^4 * (4 + 4 + 50)
	leftBordersToRightBorders map[string]string         // 10^4 * 100
	allShardsStats            map[Shard]Stats           // 10^3 * (4 + 8 * 5)
	keyRangeStats             map[string]Stats          // 10^4 * 8 * 5
	// average distance between two keys
	keyDistanceByRanges map[string]*big.Int
	keysOnShard         map[Shard]uint64
	avgKeysOnShard      uint64
	// sorted list of left borders
	leftBorders []string // 10^4 * 50
	// sorted list of shards
	shards        []Shard
	muLeftBorders sync.Mutex

	// that ones used only by planner
	keyStats   map[string]Stats
	muKeyStats sync.Mutex

	bestTask Task

	splits int

	installation InstallationInterface
	coordinator  CoordinatorInterface
	db           DatabaseInterface
	// no need to actionStageLock

	reloadRequired bool
	muReload       sync.Mutex

	averageStats Stats

	retryTime         time.Duration
	workerRetryTime   time.Duration
	plannerRetryTime  time.Duration
	okLoad            float64
	loadK             float64
	keysInOneTransfer *big.Int
	workersCount      int
	plannerCount      int
}

//TODO think about schema changing

func (b *Balancer) Init(installation InstallationInterface, coordinator CoordinatorInterface, db DatabaseInterface) {
	//TODO actionStageMove constants somewhere, get values from config
	b.retryTime = time.Second * 5
	b.workerRetryTime = time.Second * 10
	b.okLoad = 0.2
	b.workersCount = 2
	b.plannerCount = 1
	b.loadK = 0.9
	b.keysInOneTransfer = new(big.Int).SetInt64(1000)
	b.plannerRetryTime = time.Millisecond * 25000

	b.installation = installation
	b.coordinator = coordinator
	b.db = db

	b.splits = 0
	b.keyDistanceByRanges = map[string]*big.Int{}
	b.keysOnShard = map[Shard]uint64{}
	b.bestTask = Task{}
	b.shards = []Shard{}
	b.leftBorders = []string{}
	b.keyStats = map[string]Stats{}
	b.allShardsStats = map[Shard]Stats{}
	b.keyRangeStats = map[string]Stats{}
	b.shardToLeftBorders = map[Shard]map[string]bool{}
	b.leftBorderToShard = map[string]Shard{}
	b.leftBordersToRightBorders = map[string]string{}

	var shardToKeyRanges = make(map[Shard][]KeyRange)
	var err error

	for {
		// shardToLeftBorders should be inited here
		shardToKeyRanges, err = b.coordinator.initKeyRanges()
		if err == nil {
			break
		}
		//tracelog.ErrorLogger.PrintError(err)
		fmt.Println("Error: trying to init state by coordinator, but got an error ", err)
		time.Sleep(b.retryTime)
	}
	// TODO parallel
	for shard, keyRanges := range shardToKeyRanges {
		shardDistances, err := b.installation.GetKeyDistanceByRanges(shard, keyRanges)
		if err != nil {
			tracelog.DebugLogger.PrintError(err)
			continue
		}
		for rng, dist := range shardDistances {
			b.keyDistanceByRanges[rng] = dist
		}
	}

	keysAtAll := new(big.Int)
	for sh, keyRanges := range shardToKeyRanges {
		b.keysOnShard[sh] = 0
		b.shardToLeftBorders[sh] = map[string]bool{}
		for _, kr := range keyRanges {
			b.shardToLeftBorders[sh][kr.left] = true
			b.leftBordersToRightBorders[kr.left] = kr.right

			krLength := getLength(kr)
			y := b.keyDistanceByRanges[kr.left]

			tracelog.InfoLogger.Printf("krLength: %v, y %v", krLength, y)

			b.keysOnShard[sh] += uint64(new(big.Int).Div(krLength, y).Int64())
			// fmt.Println(sh.id, b.keysOnShard[sh], kr)
		}
		keysAtAll.Add(keysAtAll, new(big.Int).SetUint64(b.keysOnShard[sh]))
	}

	b.avgKeysOnShard = uint64(new(big.Int).Div(keysAtAll, new(big.Int).SetInt64(int64(len(shardToKeyRanges)))).Int64())
	for shard, ranges := range b.shardToLeftBorders {
		for left := range ranges {
			b.leftBorderToShard[left] = shard
			b.leftBorders = append(b.leftBorders, left)
		}
	}

	sort.Sort(LikeNumbers(b.leftBorders))
	b.shards = []Shard{}
	for sh := range b.shardToLeftBorders {
		b.shards = append(b.shards, sh)
	}

	tracelog.InfoLogger.Printf("Shards: %#v", b.shards)

	b.muReload.Lock()
	b.reloadRequired = false
	b.muReload.Unlock()
	// Init random seed
	rand.Seed(time.Now().UTC().UnixNano())
}

func findRange(s *string, left, right int, leftBorders *[]string) int {
	//TODO ensure that right key range contains keys from smth to 'infinity'

	middle := (right + left) / 2
	if left == right-1 {
		return left
	}

	if less(s, &(*leftBorders)[middle]) {
		return findRange(s, left, middle, leftBorders)
	}

	return findRange(s, middle, right, leftBorders)
}

func (b *Balancer) FindRange(s *string, leftBorders *[]string) int {
	defer b.muLeftBorders.Unlock()
	b.muLeftBorders.Lock()
	return findRange(s, 0, len(*leftBorders), leftBorders)
}

func (b *Balancer) tryToUpdateShardStats(shard Shard, wg *sync.WaitGroup) {
	defer wg.Done()
	var keyRanges []KeyRange
	for left := range b.shardToLeftBorders[shard] {
		keyRanges = append(keyRanges, KeyRange{left, b.leftBordersToRightBorders[left]})
	}
	_stats, err := b.installation.GetShardStats(shard, keyRanges)

	if err != nil {
		//tracelog.ErrorLogger.PrintError(err)
		fmt.Println("Error: ", err)
		return
	}

	b.allShardsStats[shard] = Stats{}
	for keyRange, keysMap := range _stats {
		for _, keyStats := range keysMap {
			_, ok := b.keyRangeStats[keyRange]
			if !ok {
				b.keyRangeStats[keyRange] = Stats{}
			}
			b.keyRangeStats[keyRange] = AddStats(b.keyRangeStats[keyRange], keyStats)
			b.allShardsStats[shard] = AddStats(b.allShardsStats[shard], keyStats)
		}
	}
}

func (b *Balancer) updateAllStats() {
	var wg sync.WaitGroup
	for shard := range b.shardToLeftBorders {
		wg.Add(1)
		b.tryToUpdateShardStats(shard, &wg)
	}
	wg.Wait()
	b.averageStats = Stats{}
	for _, keyRangeStat := range b.keyRangeStats {
		//TODO check for stats overflow
		b.averageStats = AddStats(b.averageStats, keyRangeStat)
	}
	if len(b.shardToLeftBorders) == 0 {
		fmt.Println("Error: shards count = 0")
		return
	}
	b.averageStats = DivideStats(b.averageStats, float64(len(b.shardToLeftBorders)))
}

func (b *Balancer) getFoo(value, avgValue float64, useAbs bool) float64 {
	var res float64 = 0
	var delta = value - avgValue
	if useAbs {
		// in that case, we increase res only if stat is greater than average one
		delta = math.Abs(delta)
	}
	if delta > b.okLoad*avgValue {
		res += math.Pow(delta/avgValue*100, 2)
	}
	return res
}

// TODO don't know how to name it
func (b *Balancer) getFooByStats(stats Stats, useAbs bool) float64 {
	var res float64 = 0
	res += b.getFoo(stats.userTime, b.averageStats.userTime, useAbs)
	res += b.getFoo(stats.systemTime, b.averageStats.systemTime, useAbs)
	res += b.getFoo(float64(stats.reads), float64(b.averageStats.reads), useAbs)
	res += b.getFoo(float64(stats.writes), float64(b.averageStats.writes), useAbs)

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

func (b *Balancer) setTaskProfitByKeysCount(startRange KeyRange, task *Task, keyStatsInRange *map[string]map[string]Stats) float64 {
	keysDiff := uint64(new(big.Int).Div(getLength(task.keyRange), b.keyDistanceByRanges[startRange.left]).Int64())
	keysOnShardFrom := b.keysOnShard[task.shardFrom] - keysDiff
	keysOnShardTo := b.keysOnShard[task.shardTo] + keysDiff

	res := -b.getFoo(float64(b.keysOnShard[task.shardFrom]), float64(b.avgKeysOnShard), true)
	res -= b.getFoo(float64(b.keysOnShard[task.shardTo]), float64(b.avgKeysOnShard), true)
	res += b.getFoo(float64(keysOnShardFrom), float64(b.avgKeysOnShard), true)
	res += b.getFoo(float64(keysOnShardTo), float64(b.avgKeysOnShard), true)
	return res
}

func (b *Balancer) setTaskProfitByLoad(startRange KeyRange, task *Task, keyStatsInRange *map[string]map[string]Stats) float64 {
	shardFromStats := b.allShardsStats[task.shardFrom]
	shardToStats := b.allShardsStats[task.shardTo]
	s := getStatsOnSubrange(startRange, task.keyRange, keyStatsInRange)
	shardFromStats = SubtractStats(shardFromStats, s)
	shardToStats = AddStats(shardToStats, s)
	res := -b.getFooByStats(b.allShardsStats[task.shardFrom], true) + b.getFooByStats(shardFromStats, true)
	res += -b.getFooByStats(b.allShardsStats[task.shardTo], true) + b.getFooByStats(shardToStats, true)
	return res
}

func (b *Balancer) setTaskProfitByLength(startRange KeyRange, task *Task, keyStatsInRange *map[string]map[string]Stats) float64 {
	leftGluing := false
	rightGluing := false
	task.splitBy = []string{}
	res := -logLength(startRange)
	task.startKeyRange = startRange
	task.oldKeyRangesOnShardTo = map[KeyRange]Stats{}
	task.oldKeyRangesOnShardFrom = map[KeyRange]Stats{}
	task.newKeyRangesOnShardTo = map[KeyRange]Stats{}
	task.newKeyRangesOnShardFrom = map[KeyRange]Stats{}

	task.oldKeyRangesOnShardFrom[startRange] = b.keyRangeStats[startRange.left]
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
		leftBorderIndex := b.FindRange(&task.keyRange.left, &b.leftBorders)
		if leftBorderIndex-1 >= 0 {
			leftNeighbor = b.leftBorders[leftBorderIndex-1]
		} else {
			leftGluing = false
		}
		leftGluing = leftGluing && b.leftBorderToShard[leftNeighbor] == task.shardTo
	}
	if rightGluing {
		rightBorderIndex := b.FindRange(&task.keyRange.right, &b.leftBorders)
		rightNeighbor = b.leftBorders[rightBorderIndex]
		rightGluing = rightGluing && b.leftBorderToShard[rightNeighbor] == task.shardTo
	}
	newRng := task.keyRange
	newStats := getStatsOnSubrange(startRange, task.keyRange, keyStatsInRange)
	if leftGluing {
		leftNeighborRng := KeyRange{
			leftNeighbor,
			b.leftBordersToRightBorders[leftNeighbor],
		}
		res -= logLength(leftNeighborRng)
		task.oldKeyRangesOnShardTo[leftNeighborRng] = b.keyRangeStats[leftNeighborRng.left]
		newStats = AddStats(newStats, b.keyRangeStats[leftNeighborRng.left])
		newRng.left = leftNeighborRng.left
	}
	if rightGluing {
		rightNeighborRng := KeyRange{
			rightNeighbor,
			b.leftBordersToRightBorders[rightNeighbor],
		}
		res -= logLength(rightNeighborRng)
		task.oldKeyRangesOnShardTo[rightNeighborRng] = b.keyRangeStats[rightNeighborRng.left]
		newStats = AddStats(newStats, b.keyRangeStats[rightNeighborRng.left])
		newRng.right = rightNeighborRng.right
	}
	res += logLength(newRng)
	task.newKeyRangesOnShardTo[newRng] = newStats
	return res
}

//TODO mb add cache
//TODO may be upper border of subrange is required
func (b *Balancer) getSubrange(leftKey *string, keyRange KeyRange) KeyRange {
	rightSubrange := KeyRange{*leftKey, keyRange.right}
	rightKeyInt := new(big.Int).Mul(b.keysInOneTransfer, b.keyDistanceByRanges[keyRange.left])
	rightKey := bigIntToKey(new(big.Int).Add(rightKeyInt, keyToBigInt(leftKey)))
	if !less(rightKey, &keyRange.right) {
		return rightSubrange
	}
	return KeyRange{*leftKey, *rightKey}
}

func (b *Balancer) brutTasksFromShard(shard Shard,
	shardStats map[string]map[string]Stats) {

	var subrangeLeftKeys []string
	for subrangeLeftKey := range b.shardToLeftBorders[shard] {
		subrangeLeftKeys = append(subrangeLeftKeys, subrangeLeftKey)
	}

	if b.splits > 0 {
		for leftBorder := range b.shardToLeftBorders[shard] {
			for key := range shardStats[leftBorder] {
				subrangeLeftKeys = append(subrangeLeftKeys[:1], key)
				break
			}
			break
		}
		b.splits -= 1
	}

	for _, subrangeLeftKey := range subrangeLeftKeys {
		for _, shardTo := range b.shards {
			if shardTo == shard {
				continue
			}
			leftBorder := b.leftBorders[b.FindRange(&subrangeLeftKey, &b.leftBorders)]
			keyRange := KeyRange{left: leftBorder,
				right: b.leftBordersToRightBorders[leftBorder]}
			subrange := b.getSubrange(&subrangeLeftKey, keyRange)
			task := Task{shardFrom: shard, shardTo: shardTo, keyRange: subrange}
			load := b.setTaskProfitByLoad(keyRange, &task, &shardStats)
			space := b.setTaskProfitByKeysCount(keyRange, &task, &shardStats)
			length := b.setTaskProfitByLength(keyRange, &task, &shardStats)
			task.profit = (load+space)*b.loadK + length*(1-b.loadK)
			if task.profit > -1 {
				t1 := task
				_ = b.setTaskProfitByLoad(keyRange, &t1, &shardStats)
				_ = b.setTaskProfitByLength(keyRange, &t1, &shardStats)
				_ = b.setTaskProfitByKeysCount(keyRange, &t1, &shardStats)
			}
			if task.profit < b.bestTask.profit {
				b.bestTask = task
			}
		}
	}
}

func (b *Balancer) applyRemoveRanges(kr KeyRange, stats Stats, shard Shard, toRemove *map[string]bool) {
	b.allShardsStats[shard] = SubtractStats(b.allShardsStats[shard], stats)
	delete(b.keyRangeStats, kr.left)
	delete(b.leftBorderToShard, kr.left)
	delete(b.shardToLeftBorders[shard], kr.left)
	delete(b.leftBordersToRightBorders, kr.left)
	delete(b.keyRangeStats, kr.left)
	(*toRemove)[kr.left] = true
}
func (b *Balancer) applyAppendRanges(kr KeyRange, stats Stats, shard Shard, toAppend *map[string]bool) {
	b.allShardsStats[shard] = AddStats(b.allShardsStats[shard], stats)
	b.keyRangeStats[kr.left] = stats
	b.leftBorderToShard[kr.left] = shard
	b.shardToLeftBorders[shard][kr.left] = true
	b.leftBordersToRightBorders[kr.left] = kr.right
	b.keyRangeStats[kr.left] = stats
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

func (b *Balancer) applyTask(task Task, shardStats *map[string]map[string]Stats) {
	if task.oldKeyRangesOnShardFrom == nil {
		return
	}
	removeLeftBorders := map[string]bool{}
	appendLeftBorders := map[string]bool{}
	if len(b.leftBorders) != len(b.leftBordersToRightBorders) {
		fmt.Println("hmm")
	}
	var oldDistance *big.Int

	for kr, stats := range task.oldKeyRangesOnShardFrom {
		b.applyRemoveRanges(kr, stats, task.shardFrom, &removeLeftBorders)
		oldDistance = b.keyDistanceByRanges[kr.left]
		delete(b.keyDistanceByRanges, kr.left)
	}
	newDistance := oldDistance
	newLen := getLength(task.keyRange)
	for kr, stats := range task.oldKeyRangesOnShardTo {
		b.applyRemoveRanges(kr, stats, task.shardTo, &removeLeftBorders)
		l := getLength(kr)
		newDistance = getAvgKeyDistance(newDistance, b.keyDistanceByRanges[kr.left], newLen, l)
		newLen.Add(newLen, l)
		delete(b.keyDistanceByRanges, kr.left)
	}

	for kr, stats := range task.newKeyRangesOnShardFrom {
		b.applyAppendRanges(kr, stats, task.shardFrom, &appendLeftBorders)
		b.keyDistanceByRanges[kr.left] = oldDistance
	}
	for kr, stats := range task.newKeyRangesOnShardTo {
		b.applyAppendRanges(kr, stats, task.shardTo, &appendLeftBorders)
		b.keyDistanceByRanges[kr.left] = newDistance
	}
	keysDiffOnShard := uint64(new(big.Int).Div(getLength(task.keyRange), oldDistance).Int64())
	if keysDiffOnShard >= b.keysOnShard[task.shardFrom] {
		keysDiffOnShard = b.keysOnShard[task.shardFrom]
	}
	b.keysOnShard[task.shardFrom] -= keysDiffOnShard
	b.keysOnShard[task.shardTo] += keysDiffOnShard

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
		// TODO: fix panic: assignment to entry in nil map
		(*shardStats)[newK] = v
	}

	for left := range removeLeftBorders {
		i := b.FindRange(&left, &b.leftBorders)
		if i == len(b.leftBorders)-1 {
			b.leftBorders = b.leftBorders[:i]
		} else {
			b.leftBorders = append(b.leftBorders[:i], b.leftBorders[i+1:]...)
		}
	}
	for left := range appendLeftBorders {
		b.leftBorders = append(b.leftBorders, left)
	}
	sort.Sort(LikeNumbers(b.leftBorders))
	q := len(b.leftBorders)
	w := len(b.leftBordersToRightBorders)
	if q != w {
		fmt.Println("hmm")
	}
	q = len(b.leftBorders)
	w = len(b.leftBordersToRightBorders)
	if q != w {
		lbm := map[string]bool{}
		for _, a := range b.leftBorders {
			_, ok := lbm[a]
			if ok {
				fmt.Println("hmm")
			}
			lbm[a] = true
			_, ok = b.leftBordersToRightBorders[a]
			if !ok {
				q += 1
				b.leftBorders = append(b.leftBorders, "sooqa")
				fmt.Println("hmm")
			}
		}
		for a := range b.leftBordersToRightBorders {
			_, ok := lbm[a]
			if !ok {
				q += 1
				fmt.Println("hmm")
			}
		}
		q = len(b.leftBorders)
		w = len(b.leftBordersToRightBorders)
		fmt.Println("hmm")
	}
}

func (b *Balancer) runTask(task *Action) error {
	var err error

	for task.actionStage != actionStageDone {
		tracelog.InfoLogger.Printf("Action stage: %v", task.actionStage)

		switch task.actionStage {
		case actionStagePlan:
			// TODO: uncomment. Skip splitting for MVP
			//err = b.coordinator.splitKeyRange(&task.keyRange.left)
			//if err != nil {
			//	fmt.Println("Error: actionStageSplit problems with ", task.keyRange.left, err)
			//	return err
			//}
			//err = b.coordinator.splitKeyRange(&task.keyRange.right)
			//if err != nil {
			//	fmt.Println("Error: actionStageSplit problems with ", task.keyRange.right, err)
			//	return err
			//}
			task.actionStage = actionStageSplit
		case actionStageSplit:
			//TODO ensure that task.KeyRange in one actual range
			err = b.coordinator.lockKeyRange(task.keyRange)
			if err != nil {
				fmt.Println("Error: actionStageLock problems with range ", task.keyRange, err)
				return err
			}
			task.actionStage = actionStageLock
		case actionStageLock:
			err = b.installation.StartTransfer(*task)
			if err != nil {
				fmt.Println("Error: actionStageTransfer problems with range ", task.keyRange, err)
				return err
			}
			task.actionStage = actionStageTransfer
		case actionStageTransfer:
			err = b.coordinator.moveKeyRange(task.keyRange, task.toShard)
			if err != nil {
				fmt.Println("Error: actionStageMove problems with range ", task.keyRange, err)
				return err
			}
			task.actionStage = actionStageMove
		case actionStageMove:
			err = b.coordinator.unlockKeyRange(task.keyRange)
			if err != nil {
				fmt.Println("Error: actionStageUnlock problems with range ", task.keyRange, err)
				return err
			}
			task.actionStage = actionStageUnlock
		case actionStageUnlock:
			err = b.installation.RemoveRange(task.keyRange, task.fromShard)
			if err != nil {
				fmt.Println("Error: delete problems with range ", task.keyRange, err)
				return err
			}
			task.actionStage = actionStageRemove
		case actionStageRemove:
			err = b.coordinator.mergeKeyRanges(&task.keyRange.left)
			if err != nil {
				fmt.Println("Error: actionStageMerge problems with border ", &task.keyRange.left, err)
				return err
			}
			err = b.coordinator.mergeKeyRanges(&task.keyRange.right)
			if err != nil {
				fmt.Println("Error: actionStageMerge problems with border ", &task.keyRange.right, err)
				return err
			}
			task.actionStage = actionStageMerge

		default:
			return errors.New(fmt.Sprint("Not known actionStage: ", task.actionStage))
		}

		for {
			err = b.db.Update(task)
			if err == nil {
				break
			}
			fmt.Println("Error: update action error ", task.id, err)
			time.Sleep(b.workerRetryTime)
		}

		if task.actionStage == actionStageDone {
			for {
				err = b.db.Delete(task)
				if err == nil {
					break
				}
				fmt.Println("Error: delete action error ", task.id, err)
				time.Sleep(b.workerRetryTime)
			}
		}
	}

	return nil
}

func (b *Balancer) runWorker() {
	i := 0
	for {
		action, ok, err := b.db.GetAndRun()
		if err != nil {
			fmt.Println(err)
			time.Sleep(b.workerRetryTime)
			continue
		}
		if !ok {
			fmt.Println("No tasks, wait")
			time.Sleep(b.workerRetryTime)
			continue
		}
		i += 1
		fmt.Println("tasks actionStageDone: ", i)
		err = b.runTask(&action)
		if err != nil {
			fmt.Println("Error while task running: ", err)
			continue
		}
	}
}

func (b *Balancer) planTasks() {
	shard := b.shards[len(b.shards)-1]
	var keyRanges []KeyRange
	for left := range b.shardToLeftBorders[shard] {
		keyRanges = append(keyRanges, KeyRange{left, b.leftBordersToRightBorders[left]})
	}

	shardStats, err := b.installation.GetShardStats(shard, keyRanges)
	if err != nil {
		tracelog.ErrorLogger.PrintError(err)
		//fmt.Println("Error: ", err)
		return
	}

	tracelog.InfoLogger.Printf("Plan tasks. ShardStats: %#v", shardStats)

	b.bestTask = Task{}
	for i := 0; i < b.plannerCount; i++ {
		b.brutTasksFromShard(shard, shardStats)
		//fmt.Println("Plan (id ", b.bestTask.id, ") to actionStageMove ", b.bestTask.keyRange, " from ", b.bestTask.startKeyRange, " in shard ", b.bestTask.shardFrom,
		//	" to shard ", b.bestTask.shardTo)

		for _, sh := range b.shards {
			foo := b.getFooByShard(sh, false)
			fmt.Println("Shard stats without abs", sh.id, foo)
		}
		for _, sh := range b.shards {
			foo := b.getFooByShard(sh, true)
			fmt.Println("Shard stats with abs", sh.id, foo)
		}

		fmt.Println("Ranges count: ", len(b.leftBorderToShard))
		fmt.Println("Profit: ", b.bestTask.profit)

		fmt.Println("Range len", new(big.Int).Sub(keyToBigInt(&b.bestTask.keyRange.right), keyToBigInt(&b.bestTask.keyRange.left)))
		fmt.Println("To: ", b.bestTask.shardTo.id)

		fmt.Println("______________________")

		if b.bestTask.profit < 0 {
			bestAction := Action{
				id:          0,
				actionStage: actionStagePlan,
				keyRange:    b.bestTask.keyRange,
				fromShard:   b.bestTask.shardFrom,
				toShard:     b.bestTask.shardTo,
				isRunning:   false,
			}

			err = b.db.Insert(&bestAction)
			if err != nil {
				fmt.Println("Error: insert action error:", bestAction, err)
				continue
			}

			b.applyTask(b.bestTask, &shardStats)
		}
		if b.bestTask.profit > -1 {
			b.splits += 1
			// there is nothing else we can do
			b.bestTask = Task{}
			continue
		}
		b.bestTask = Task{}
	}
}

func (b *Balancer) getFooOfShardSize(shard Shard, useAbs bool) float64 {
	return b.getFoo(float64(b.keysOnShard[shard]), float64(b.avgKeysOnShard), useAbs)
}

func (b *Balancer) getFooByShard(shard Shard, useAbs bool) float64 {
	return b.getFooByStats(b.allShardsStats[shard], useAbs) + b.getFooOfShardSize(shard, useAbs)
}

//TODO add function MoveAllDataFromShard

func (b *Balancer) BrutForceStrategy() {
	var err error

	// TODO: actionStageMerge all possible kr's? Or not.

	// TODO: mb we should wait til new stats is going to be collected.
	// If not, after actionStageTransfer we will see, that transferred range is not loaded, but it might be not true

	b.muReload.Lock()
	b.reloadRequired = true
	b.muReload.Unlock()

	for {
		err = b.db.MarkAllNotRunning()
		if err == nil {
			break
		}
		fmt.Println("Error: mark actions as not running ", err)
		time.Sleep(b.plannerRetryTime)
	}

	for j := 0; j < b.workersCount; j++ {
		fmt.Println("add worker")
		go b.runWorker()
	}

	for {
		b.muReload.Lock()
		reload := b.reloadRequired
		b.muReload.Unlock()
		if !reload {
			tracelog.InfoLogger.Println("Check coordinator for reloading requirements")
			reload, err = b.coordinator.isReloadRequired()
			if err != nil {
				fmt.Println("Error while spqr.isReloadRequired call: ", err)
				reload = true
			}
		}
		if reload {
			activeTaskCounter, err := b.db.Len()
			if err != nil {
				fmt.Println("Error while selecting count of active tasks: ", err)
				b.reloadRequired = true
				continue
			}

			fmt.Println("Reload, tasks ", activeTaskCounter)
			b.Init(b.installation, b.coordinator, b.db)
			b.updateAllStats()
			fmt.Println("Reload actionStageDone")
		}

		sort.Slice(b.shards, func(i, j int) bool {
			return b.getFooByShard(b.shards[i], false) < b.getFooByShard(b.shards[j], false)
		})
		b.planTasks()

		var length uint64
		for {
			fmt.Println("wait for workers")
			length, err = b.db.Len()
			if err != nil {
				fmt.Println("Error while selecting count of active tasks: ", err)
				time.Sleep(time.Millisecond * time.Duration(defaultSleepMS))
				continue
			}
			if length == 0 {
				fmt.Println("No active tasks")
				break
			}
			time.Sleep(b.plannerRetryTime)
		}

	}
}
