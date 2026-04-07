package session

type SimpleParamVisibility struct {
	name    string
	entries []ParamEntry

	globalMap map[string]string
}

func (s *SimpleParamVisibility) Commit() {
	i := len(s.entries) - 1
	for ; i >= 0 && s.entries[i].Tx > 0 && s.entries[i].IsLocal; i-- {
	}

	if i >= 0 {
		entry := s.entries[i]
		entry.Tx = 0
		s.entries = s.entries[:1]
		s.entries[0] = entry
	} else {
		s.entries = s.entries[:0]
	}

	s.updateInGlobal()
}

func (s *SimpleParamVisibility) Set(entry ParamEntry) {
	n := len(s.entries)
	if n == 0 {
		s.entries = append(s.entries, entry)
	} else {
		lastEntry := s.entries[n-1]
		if lastEntry.EqualIgnoringValue(entry) {
			s.entries[n-1] = entry
		} else {
			s.entries = append(s.entries, entry)
		}
	}

	s.updateInGlobal()
}

func (s *SimpleParamVisibility) RollbackTo(txCnt int) {
	i := len(s.entries) - 1
	for ; i >= 0 && s.entries[i].Tx > txCnt; i-- {
	}

	s.entries = s.entries[:i+1]

	s.updateInGlobal()
}

func (s *SimpleParamVisibility) updateInGlobal() {
	n := len(s.entries)
	if n > 0 {
		lastEntry := s.entries[n-1]
		if lastEntry.Action == ParamActionReset && lastEntry.Value == "" {
			delete(s.globalMap, s.name)
		} else {
			s.globalMap[s.name] = lastEntry.Value
		}
	} else {
		delete(s.globalMap, s.name)
	}
}

func (s *SimpleParamVisibility) CleanupStatementSet() {

}

func (s *SimpleParamVisibility) Reset(tx int, defaultValue *string) {
	val := ""
	if defaultValue != nil {
		val = *defaultValue
	}
	s.entries = append(s.entries, ParamEntry{
		Tx:     tx,
		Action: ParamActionReset,
		Value:  val,
	})
	s.updateInGlobal()
}
