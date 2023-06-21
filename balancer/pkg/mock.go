package pkg

import (
	"fmt"
	"math/big"
	"math/rand"
	"sort"
	"sync"

	"golang.yandex/hasql"

	"github.com/pg-sharding/spqr/pkg/models/kr"
)

type mock struct {
	shardToKeyRanges map[Shard]map[KeyRange]bool
	keyRangeToShard  map[KeyRange]Shard
	leftBorders      []string
	lockedKeyRanges  map[KeyRange]bool
	keys             map[string]bool
	ranges           map[KeyRange]bool

	// shard -> key -> stats
	stats map[Shard]map[string]Stats

	mu sync.Mutex
}

func (m *mock) init() {
	defer m.mu.Unlock()
	m.mu.Lock()
	m.leftBorders = []string{}
	m.lockedKeyRanges = map[KeyRange]bool{}
	m.keys = map[string]bool{}
	m.ranges = map[KeyRange]bool{}
	m.stats = map[Shard]map[string]Stats{}
	m.shardToKeyRanges = map[Shard]map[KeyRange]bool{}
	m.keyRangeToShard = map[KeyRange]Shard{}

	left := 0
	right := 1000000
	var _shards []Shard
	maxLen := 100
	for i := 0; i < 5; i++ {
		shard := Shard{id: i}
		_shards = append(_shards, shard)
		m.shardToKeyRanges[shard] = map[KeyRange]bool{}
		m.stats[shard] = map[string]Stats{}
	}
	i := left
	for i < right {
		diff := rand.Intn(maxLen) + 1
		if i+diff >= right {
			diff = right - i
		}

		s := Stats{
			reads:      (uint64)(rand.Intn(100)),
			writes:     (uint64)(rand.Intn(100)),
			userTime:   rand.Float64() * 10,
			systemTime: rand.Float64() * 10,
		}

		shard := _shards[rand.Intn(len(_shards))]
		kr := KeyRange{left: *bigIntToKey(big.NewInt(int64(i))), right: *bigIntToKey(big.NewInt(int64(i + diff)))}
		m.keyRangeToShard[kr] = shard
		m.shardToKeyRanges[shard][kr] = true
		m.leftBorders = append(m.leftBorders, kr.left)
		m.ranges[kr] = true
		j := i
		for j < i+diff {
			s1 := Stats{
				reads:      (uint64)(rand.Intn(10)) + s.reads,
				writes:     (uint64)(rand.Intn(10)) + s.writes,
				userTime:   rand.Float64()*1 + s.userTime,
				systemTime: rand.Float64()*1 + s.systemTime,
			}

			s1 = DivideStats(s1, 1/float64(shard.id+1)/float64(shard.id+1))
			m.stats[shard][*bigIntToKey(big.NewInt(int64(j)))] = s1
			m.keys[*bigIntToKey(big.NewInt(int64(j)))] = true
			j += rand.Intn(3) + 1
		}
		i += diff
	}
	sort.Sort(LikeNumbers(m.leftBorders))
	m.foo()
}

func (m *mock) GetShardStats(shard Shard, keyRanges []KeyRange) (map[string]map[string]Stats, error) {
	defer m.mu.Unlock()
	m.mu.Lock()
	m.foo()
	res := map[string]map[string]Stats{}
	var _leftBorders []string
	leftToRight := map[string]string{}

	for _, kr := range keyRanges {
		_leftBorders = append(_leftBorders, kr.left)
		leftToRight[kr.left] = kr.right
		res[kr.left] = map[string]Stats{}
	}
	sort.Sort(LikeNumbers(_leftBorders))
	for key, stats := range m.stats[shard] {
		i := findRange(&key, 0, len(m.leftBorders), &m.leftBorders)
		right := leftToRight[_leftBorders[i]]
		if less(&key, &right) {
			res[_leftBorders[i]][key] = stats
		}
	}
	m.foo()
	return res, nil
}

func (m *mock) StartTransfer(task Action) error {
	defer m.mu.Unlock()
	m.mu.Lock()
	m.foo()
	return nil
}

func (m *mock) RemoveRange(keyRange KeyRange, shard Shard) error {
	defer m.mu.Unlock()
	m.mu.Lock()
	m.foo()
	return nil
}

func (m *mock) lockKeyRange(rng KeyRange) error {
	defer m.mu.Unlock()
	m.mu.Lock()
	m.foo()
	_, ok := m.ranges[rng]
	if !ok {
		_ = m.leftBorders
		_ = m.ranges
		_ = m.shardToKeyRanges
		_ = m.stats
		return fmt.Errorf("key range does not exist")
	}
	if m.lockedKeyRanges[rng] {
		return fmt.Errorf("already locked")
	}
	m.lockedKeyRanges[rng] = true
	m.foo()
	return nil
}

func (m *mock) foo() {
	for i := range m.leftBorders {
		if i == 0 {
			continue
		}
		r := KeyRange{m.leftBorders[i-1], m.leftBorders[i]}
		_, ok := m.ranges[r]
		if !ok {
			if r.left == r.right {
				fmt.Println("???")
			}
			fmt.Println("foo")
		}
	}
	for r := range m.ranges {
		i := findRange(&r.left, 0, len(m.leftBorders), &m.leftBorders)
		if m.leftBorders[i] != r.left {
			fmt.Println("foo")
		}
	}
}

func (m *mock) unlockKeyRange(rng KeyRange) error {
	defer m.mu.Unlock()
	m.mu.Lock()
	m.foo()
	_, ok := m.ranges[rng]
	if !ok {
		return fmt.Errorf("key range does not exist")
	}
	_, ok = m.lockedKeyRanges[rng]
	if !ok {
		return fmt.Errorf("already unlocked")
	}
	delete(m.lockedKeyRanges, rng)
	m.foo()
	return nil
}

// TODO: is race is possible in actual spqr and database calls?

func (m *mock) initKeyRanges() (map[Shard][]KeyRange, error) {
	defer m.mu.Unlock()
	m.mu.Lock()
	m.foo()
	shardToRanges := map[Shard][]KeyRange{}
	for shard, ranges := range m.shardToKeyRanges {
		shardToRanges[shard] = []KeyRange{}
		for rng := range ranges {
			shardToRanges[shard] = append(shardToRanges[shard], rng)
		}
	}
	m.foo()
	return shardToRanges, nil
}

func (m *mock) GetKeyDistanceByRanges(shard Shard, keyRanges []KeyRange) (map[string]*big.Int, error) {
	_l := "10000"
	_r := "20000"
	dist := new(big.Int).Sub(keyToBigInt(&_r), keyToBigInt(&_l))
	_keyDistanceByRanges := map[string]*big.Int{}
	for _, rng := range keyRanges {
		_, ok := m.shardToKeyRanges[shard][rng]
		if !ok {
			continue
		}
		_keyDistanceByRanges[rng.left] = new(big.Int).Div(dist, new(big.Int).SetInt64(5000))
	}
	return _keyDistanceByRanges, nil
}

func (m *mock) splitKeyRange(border *string, _, _ string) error {
	defer m.mu.Unlock()
	m.mu.Lock()
	m.foo()
	leftInd := findRange(border, 0, len(m.leftBorders), &m.leftBorders)

	if leftInd == len(m.leftBorders)-1 {
		if m.leftBorders[leftInd] == *border {
			// not a problem, we just try to actionStageSplit by right border of all ranges
			return nil
		} else {
			// border greater than all ranges. TODO something here?
			return nil
		}
	}
	rng := KeyRange{left: m.leftBorders[leftInd], right: m.leftBorders[leftInd+1]}
	leftRng := KeyRange{left: rng.left, right: *border}
	rightRng := KeyRange{left: *border, right: rng.right}
	if *border == leftRng.left || *border == rightRng.right {
		// there is nothing we should do here
		return nil
	}
	_, ok := m.ranges[rng]
	if !ok {
		return fmt.Errorf("key range does not exist")
	}
	shard := m.keyRangeToShard[rng]
	delete(m.shardToKeyRanges[shard], rng)
	m.shardToKeyRanges[shard][leftRng] = true
	m.shardToKeyRanges[shard][rightRng] = true
	delete(m.keyRangeToShard, rng)
	m.keyRangeToShard[leftRng] = shard
	m.keyRangeToShard[rightRng] = shard
	if m.lockedKeyRanges[rng] {
		delete(m.lockedKeyRanges, rng)
		m.lockedKeyRanges[leftRng] = true
		m.lockedKeyRanges[rightRng] = true
	}
	delete(m.ranges, rng)
	m.ranges[leftRng] = true
	m.ranges[rightRng] = true
	m.leftBorders = append(m.leftBorders, rightRng.left)
	sort.Sort(LikeNumbers(m.leftBorders))
	m.foo()
	return nil
}

func (m *mock) mergeKeyRanges(border *string) error {
	defer m.mu.Unlock()
	m.mu.Lock()
	m.foo()
	rightInd := findRange(border, 0, len(m.leftBorders), &m.leftBorders)
	if rightInd == 0 || rightInd == len(m.leftBorders)-1 {
		if rightInd == 0 {
			// it's ok, just ignore actionStageSplit by left border
			return nil
		}
		if *border == m.leftBorders[rightInd] {
			// it's ok, just ignore actionStageSplit by right border
			return nil
		}
		// actionStageSplit by border greater than all ranges
		return fmt.Errorf("split by border '%v' greater than all ranges ", border)
	}
	leftRng := KeyRange{m.leftBorders[rightInd-1], m.leftBorders[rightInd]}
	rightRng := KeyRange{m.leftBorders[rightInd], m.leftBorders[rightInd+1]}
	if *border != leftRng.right || *border != rightRng.left {
		return fmt.Errorf("something goes wrong...")
	}
	if m.keyRangeToShard[leftRng] != m.keyRangeToShard[rightRng] {
		// it's ok, because we try actionStageMerge by every transfered border
		return nil
	}
	newRng := KeyRange{left: leftRng.left, right: rightRng.right}
	_, ok := m.ranges[leftRng]
	if !ok {
		return fmt.Errorf("key range %v does not exist", leftRng)
	}
	_, ok = m.ranges[rightRng]
	if !ok {
		return fmt.Errorf("key range %v does not exist", rightRng)
	}
	shard := m.keyRangeToShard[leftRng]
	if shard != m.keyRangeToShard[rightRng] {
		return fmt.Errorf("key ranges in different shards")
	}
	delete(m.shardToKeyRanges[shard], leftRng)
	delete(m.shardToKeyRanges[shard], rightRng)
	m.shardToKeyRanges[shard][newRng] = true
	delete(m.keyRangeToShard, leftRng)
	delete(m.keyRangeToShard, rightRng)
	m.keyRangeToShard[newRng] = shard
	if m.lockedKeyRanges[leftRng] || m.lockedKeyRanges[rightRng] {
		if m.lockedKeyRanges[leftRng] {
			delete(m.lockedKeyRanges, leftRng)
		}
		if m.lockedKeyRanges[rightRng] {
			delete(m.lockedKeyRanges, rightRng)
		}
		m.lockedKeyRanges[newRng] = true
	}
	delete(m.ranges, leftRng)
	delete(m.ranges, rightRng)
	m.ranges[newRng] = true
	j := 0
	for i, l := range m.leftBorders {
		if l == leftRng.right {
			j = i
		}
	}
	if j == len(m.leftBorders)-1 {
		m.leftBorders = m.leftBorders[:j]
	} else {
		m.leftBorders = append(m.leftBorders[:j], m.leftBorders[j+1:]...)
	}
	m.foo()
	return nil
}
func (m *mock) showKeyRanges() ([]*kr.KeyRange, error) {
	return []*kr.KeyRange{}, nil
}

func (m *mock) moveKeyRange(rng KeyRange, shardTo Shard) error {
	defer m.mu.Unlock()
	m.mu.Lock()
	m.foo()
	var shardFrom Shard
	found := false
	for shard := range m.shardToKeyRanges {
		_, ok := m.shardToKeyRanges[shard][rng]
		if ok {
			found = true
			shardFrom = shard
		}
	}
	if !found {
		m.foo()
		return fmt.Errorf("key ranges not found")
	}

	for key := range m.keys {
		if !less(&key, &rng.left) && less(&key, &rng.right) {
			m.stats[shardTo][key] = m.stats[shardFrom][key]
			delete(m.stats[shardFrom], key)
		}
	}
	m.shardToKeyRanges[shardTo][rng] = true
	delete(m.shardToKeyRanges[shardFrom], rng)
	m.keyRangeToShard[rng] = shardTo
	m.foo()
	return nil
}

func (m *mock) isReloadRequired() (bool, error) {
	defer m.mu.Unlock()
	m.mu.Lock()
	m.foo()
	if rand.Intn(10) < 2 {
		return true, nil
	}
	return false, nil
}

func (m *mock) Init(_, _, _, _, _ string, _ *map[int]*hasql.Cluster, _ int) error {
	return nil
}

// func local_test() {
// 	fmt.Println("1")
// 	m := mock{}
// 	fmt.Println("2")
// 	m.init()
// 	fmt.Println("3")
// 	db := MockDb{}
// 	_ = db.Init([]string{}, 0, "", "", "")
// 	b := Balancer{}
// 	b.Init(&m, &m, &m, &db)
// 	b.BrutForceStrategy()
// 	fmt.Println("4")
// }
