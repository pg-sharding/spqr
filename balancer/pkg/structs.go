package pkg

import (
	"math"
	"math/big"
)

type Shard struct {
	id int
}

type KeyRange struct {
	left  string
	right string
}

// TODO mb add cache to that function
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

// TODO mb add cache to that function
// TODO: add tests and check correctness.
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
	for i := 0; i < len(charsArr)/2; i++ {
		charsArr[i], charsArr[len(charsArr)-i-1] = charsArr[len(charsArr)-i-1], charsArr[i]
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

// можно придумать отображение получше, если знать максимальную длину ренджа, то есть если знать максимальный ключ.
// пока оставим так, мб потом поменяем...
func logLength(keyRange KeyRange) float64 {
	var diff float64
	k := math.Pow(2, 8)
	lenDiff := len(keyRange.right) - len(keyRange.left)
	for i := 0; i < len(keyRange.right); i++ {
		diff += (float64)(keyRange.right[i])
		if i >= lenDiff {
			diff -= (float64)(keyRange.left[i-lenDiff])
		}
		diff *= k
	}

	return 1.0 / diff
}

type Stats struct {
	// total reads, in bytes
	reads uint64 `db:"reads"`
	// total writes, in bytes
	writes uint64 `db:"writes"`
	// total user CPU time used
	userTime float64 `db:"user_time"`
	// total system CPU time used
	systemTime float64 `db:"system_time"`
}

func AddStats(a, b Stats) Stats {
	a.reads += b.reads
	a.writes += b.writes
	a.userTime += b.userTime
	a.systemTime += b.systemTime
	return a
}

func SubtractStats(a, b Stats) Stats {
	a.reads -= b.reads
	a.writes -= b.writes
	a.userTime -= b.userTime
	a.systemTime -= b.systemTime
	return a
}

func DivideStats(a Stats, k float64) Stats {
	a.reads = uint64(float64(a.reads) / k)
	a.writes = uint64(float64(a.writes) / k)
	a.systemTime /= k
	a.userTime /= k
	return a
}

type LikeNumbers []string

func (a LikeNumbers) Len() int           { return len(a) }
func (a LikeNumbers) Less(i, j int) bool { return less(&a[i], &a[j]) }
func (a LikeNumbers) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
