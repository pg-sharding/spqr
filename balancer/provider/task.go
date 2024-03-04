package provider

type Task struct {
	shardFromId string
	shardToId   string
	krIdFrom    string
	krIdTo      string
	bound       []byte
}
