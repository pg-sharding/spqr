package topology

type Shard struct {
	ID    string
	Hosts []string
}

func NewShard(ID string, hosts []string) *Shard {
	return &Shard{
		ID:    ID,
		Hosts: hosts,
	}
}
