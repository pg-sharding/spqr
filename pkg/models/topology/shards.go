package topology

type Shard struct {
	ID    string
	Hosts []string
}

// NewShard creates a new Shard instance with the specified ID and hosts.
// It returns a pointer to the created Shard.
//
// Parameters:
//   - ID: The ID of the shard.
//   - hosts: The list of hosts that the shard is distributed across.
//
// Returns:
//   - *Shard: A pointer to the created Shard.
func NewShard(ID string, hosts []string) *Shard {
	return &Shard{
		ID:    ID,
		Hosts: hosts,
	}
}
