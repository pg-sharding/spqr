package balancer

import "errors"

type mock struct {
	shardToKeyRanges map[Shard]map[KeyRange]bool
	keyRangeToShard map[KeyRange]Shard
	leftBorders []string
	lockedKeyRanges map[KeyRange]bool
	keyStats map[string]Stats
	ranges map[KeyRange]bool

	// shard -> key -> stats
	stats map[Shard]map[string]Stats
}

func (m mock) init() {
	
}

func (m mock) getShardStats(shard Shard, keyRanges []KeyRange) (map[string]map[string]Stats, error) {
	res := map[string]map[string]Stats{}
	var leftBorders []string
	var leftToRight map[string]string

	for _, kr := range keyRanges {
		leftBorders = append(leftBorders, kr.left)
		leftToRight[kr.left] = kr.right
		res[kr.left] = map[string]Stats{}
	}

	for key, stats := range m.stats[shard] {
		i := findRangeEntry(&key, &leftBorders)
		right := leftToRight[leftBorders[i]]
		if less(&key, &right) {
			res[leftBorders[i]][key] = stats
		}
	}
	return res, nil
}

func (mock) startTransfer(task Task) error {
	return nil
}

func (m mock) lockKeyRange(rng KeyRange) error {
	_, ok := m.ranges[rng]
	if !ok {
		return errors.New("Key range does not exist")
	}
	if m.lockedKeyRanges[rng] {
		return errors.New("Already locked")
	}
	m.lockedKeyRanges[rng] = true
	return nil
}

func (m mock) unlockKeyRange(rng KeyRange) error {
	_, ok := m.ranges[rng]
	if !ok {
		return errors.New("Key range does not exist")
	}
	if !m.lockedKeyRanges[rng] {
		return errors.New("Already unlocked")
	}
	delete(m.lockedKeyRanges, rng)
	return nil
}

func (m mock) initKeyRanges() map[Shard][]KeyRange {
	res := map[Shard][]KeyRange{}
	for shard, ranges := range m.shardToKeyRanges {
		res[shard] = []KeyRange{}
		for rng := range ranges {
			res[shard] = append(res[shard], rng)
		}
	}
	return res
}

func (m mock) splitKeyRange(rng KeyRange, border string) error {
	leftRng := KeyRange{left: rng.left, right: border}
	rightRng := KeyRange{left: border, right: rng.right}
	_, ok := m.ranges[rng]
	if !ok {
		return errors.New("Key range does not exist")
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

	return nil
}

func (m mock) mergeKeyRanges(leftRng, rightRng KeyRange) error {
	newRng := KeyRange{left: leftRng.left, right: leftRng.right}
	_, ok := m.ranges[leftRng]
	if !ok {
		return errors.New("Key range does not exist")
	}
	_, ok = m.ranges[rightRng]
	if !ok {
		return errors.New("Key range does not exist")
	}
	shard := m.keyRangeToShard[leftRng]
	if shard != m.keyRangeToShard[rightRng] {
		return errors.New("Key ranges in different shards")
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
		if m.lockedKeyRanges[leftRng] {
			delete(m.lockedKeyRanges, rightRng)
		}
		m.lockedKeyRanges[newRng] = true
	}
	delete(m.ranges, leftRng)
	delete(m.ranges, rightRng)
	m.ranges[newRng] = true

	return nil
}

func (m mock) moveKeyRange(rng KeyRange, shardFrom, shardTo Shard) error {
	_, ok := m.shardToKeyRanges[shardFrom][rng]
	if !ok {
		return errors.New("Key ranges not in shardFrom")
	}
	for key := range keyStats {
		if !less(&key, &rng.left) && less(&key, &rng.right) {
			m.stats[shardTo][key] = m.stats[shardFrom][key]
			delete(m.stats[shardFrom], key)
		}
	}
	return nil
}
