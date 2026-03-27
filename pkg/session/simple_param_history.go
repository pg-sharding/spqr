package session

type SimpleParamHistory struct {
	name    string
	history []ParamEntry

	globalMap map[string]string
}

func (s *SimpleParamHistory) Commit() {
	i := len(s.history) - 1
	for ; i >= 0 && s.history[i].Tx > 0 && s.history[i].IsLocal; i-- {
	}

	if i >= 0 {
		entry := s.history[i]
		entry.Tx = 0
		s.history = []ParamEntry{entry}

	} else {
		s.history = nil
	}

	s.updateInGlobal()
}

func (s *SimpleParamHistory) Set(entry ParamEntry) {
	if len(s.history) == 0 {
		s.history = append(s.history, entry)
	} else {
		lastEntry := s.history[len(s.history)-1]
		if lastEntry.EqualIgnoringValue(entry) {
			s.history[len(s.history)-1] = entry
		} else {
			s.history = append(s.history, entry)
		}
	}

	s.updateInGlobal()
}

func (s *SimpleParamHistory) RollbackTo(txCnt int) {
	i := len(s.history) - 1
	for ; i >= 0 && s.history[i].Tx > txCnt; i-- {
	}

	if i >= 0 {
		s.history = s.history[:i+1]
	} else {
		s.history = nil
	}

	s.updateInGlobal()
}

func (s *SimpleParamHistory) updateInGlobal() {
	if len(s.history) > 0 {
		s.globalMap[s.name] = s.history[len(s.history)-1].Value
	} else {
		delete(s.globalMap, s.name)
	}
}

func (s *SimpleParamHistory) CleanupStatementSet() {

}
