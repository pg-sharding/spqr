package provider

type Task struct {
	shardFromId string
	shardToId   string
	krIdForm    string
	krIdTo      string
	bound       []byte
}
