package balancer

import (
	"sync"
)

type Task struct {
	keyRange KeyRange
	shardFrom Shard
	shardTo Shard

	splitBy []string

	oldKeyRangesOnShardFrom map[KeyRange]Stats
	oldKeyRangesOnShardTo map[KeyRange]Stats
	newKeyRangesOnShardFrom map[KeyRange]Stats
	newKeyRangesOnShardTo map[KeyRange]Stats

	profit float64
}

type TaskQueue struct {
	queue []Task
	mu sync.Mutex
}

func InitQueue() *TaskQueue {
	q := TaskQueue{}
	q.queue = []Task{}
	return &q
}

func PopQueue(tq *TaskQueue) (Task, bool) {
	defer tq.mu.Unlock()
	tq.mu.Lock()
	if len(tq.queue) == 0 {
		return Task{}, false
	}
	t := tq.queue[0]
	tq.queue = tq.queue[1:]
	return t, true
}

func AddQueue(tq *TaskQueue, t Task) {
	defer tq.mu.Unlock()
	tq.mu.Lock()
	tq.queue = append(tq.queue, t)
}
