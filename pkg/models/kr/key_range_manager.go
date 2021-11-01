package kr

type SplitKeyRange struct {
	Bound    KeyRangeBound
	SourceID string
	Krid     string
}

type UniteKeyRange struct {
	KeyRangeIDLeft  string
	KeyRangeIDRight string
}

type KeyRangeManager interface {
	KeyRanges() []*KeyRange

	AddKeyRange(kr *KeyRange) error
	Lock(krid string) (*KeyRange, error)
	UnLock(krid string) error
	Split(req *SplitKeyRange) error
	Unite(req *UniteKeyRange) error
}
