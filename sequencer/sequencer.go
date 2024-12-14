package sequencer

type SeqAM interface {
	Read() (int64, error)
	NextVal() (int64, error)
}

type SeqState struct {
	Tag int64
}
