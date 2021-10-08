package balancer

import (
	"math"
	"math/big"
	"sync"
)

type Shard struct {
	id int
}

type KeyRange struct {
	left string
	right string
}

//TODO mb add cache to that function
func keyToBigInt(key *string) *big.Int {
	num := big.NewInt(0)
	k := big.NewInt(256)
	chr := new(big.Int)
	for i := 0; i < len(*key); i++ {
		num.Mul(num, k)
		chr.SetInt64((int64)((*key)[i]))
		num.Add(num, chr)
	}
	return num
}

//TODO mb add cache to that function
func bigIntToKey(num *big.Int) *string {
	numCopy := new(big.Int).Set(num)
	k := big.NewInt(256)
	mod := new(big.Int)
	zero := big.NewInt(0)
	var charsArr []byte
	for numCopy.Cmp(zero) > 0 {
		numCopy.DivMod(numCopy, k, mod)
		charsArr = append(charsArr, byte(mod.Int64()))
	}
	for i := 0; i < len(charsArr) / 2; i++ {
		charsArr[i], charsArr[len(charsArr) - i] = charsArr[len(charsArr) - i], charsArr[i]
	}
	res := string(charsArr)
	return &res
}

func less(s1, s2 *string) bool {
	if len(*s1) < len(*s2) {
		return true
	}
	if len(*s1) > len(*s2) {
		return false
	}
	for i := 0; i < len(*s1); i++ {
		if (*s1)[i] == (*s2)[i] {
			continue
		}
		return (*s1)[i] < (*s2)[i]
	}
	return false
}

func logLength(keyRange KeyRange) float64 {
	//TODO check is order right
	var diff float64
	for i := 0; i <= len(keyRange.right); i++ {
		diff += (float64)(keyRange.right[i])
		if i < len(keyRange.left) {
			diff -= (float64)(keyRange.left[i])
		}
		diff *= math.Pow(2, 8)
	}
	return math.Pow(2 - math.Log2(-diff), 2)
}

type Stats struct {
	// total reads, in bytes
	reads uint64
	// total writes, in bytes
	writes uint64
	// total user CPU time used
	user_time float64
	// total system CPU time used
	system_time float64
	// total size in bytes
	size uint64
}

func AddStats(a, b Stats) {
	a.reads += b.reads
	a.writes += b.writes
	a.user_time += b.user_time
	a.system_time += a.system_time
	a.size += b.size
}

func SubtractStats(a, b Stats) {
	a.reads -= b.reads
	a.writes -= b.writes
	a.user_time -= b.user_time
	a.system_time -= a.system_time
	a.size -= b.size
}

func DivideStats(a Stats, k float64) {
	a.reads /= (uint64)(k)
	a.writes /= (uint64)(k)
	a.system_time /= k
	a.user_time /= k
	a.size /= (uint64)(k)
}

type ShardTransfers struct {
	mu sync.Mutex
	// shard name to count of transfers from/to this shard
	transfers map[string]int
}

type LockedKRIds struct {
	mu sync.Mutex
	locks map[KeyRange]bool
}

type AllStats struct {
	mu sync.Mutex
	kRIdsStats map[KeyRange]Stats
}

type SpqrInterface interface {
	lockKeyRange(rng KeyRange) error
	unlockKeyRange(rng KeyRange) error
	initKeyRanges() error
	splitKeyRange(rng KeyRange, border string) error
	mergeKeyRanges(leftRng, rightRng KeyRange) error
	moveKeyRange(rng KeyRange, shardFrom, shardTo Shard) error
}

type DatabaseInterface interface {
	getShardStats(shard Shard, keyRanges []KeyRange) (map[string]map[string]Stats, error)
	startTransfer(task Task) error
}


type LikeNumbers []string
func (a LikeNumbers) Len() int           { return len(a) }
func (a LikeNumbers) Less(i, j int) bool { return less(&a[i], &a[j]) }
func (a LikeNumbers) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

type TasksByProfitIncrease []Task
func (a TasksByProfitIncrease) Len() int           { return len(a) }
func (a TasksByProfitIncrease) Less(i, j int) bool { return a[i].profit < a[j].profit }
func (a TasksByProfitIncrease) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

type KeysByFoo []string
func (a KeysByFoo) Len() int           { return len(a) }
func (a KeysByFoo) Less(i, j int) bool {
	defer muKeyStats.Unlock()
	muKeyStats.Lock()
	return getFooByStats(keyStats[a[i]]) < getFooByStats(keyStats[a[j]]) }
func (a KeysByFoo) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

type ShardsByFoo []Shard
func (a ShardsByFoo) Len() int           { return len(a) }
func (a ShardsByFoo) Less(i, j int) bool {
	return getFooByStats(allShardsStats[a[i]]) < getFooByStats(allShardsStats[a[j]])
}
func (a ShardsByFoo) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
