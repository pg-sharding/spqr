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
		s.history = s.history[:1]
		s.history[0] = entry
	} else {
		s.history = s.history[:0]
	}

	s.updateInGlobal()
}

func (s *SimpleParamHistory) Set(entry ParamEntry) {
	n := len(s.history)
	if n == 0 {
		s.history = append(s.history, entry)
	} else {
		lastEntry := s.history[n-1]
		if lastEntry.EqualIgnoringValue(entry) {
			s.history[n-1] = entry
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

	s.history = s.history[:i+1]

	s.updateInGlobal()
}

func (s *SimpleParamHistory) updateInGlobal() {
	n := len(s.history)
	if n > 0 {
		lastEntry := s.history[n-1]
		if lastEntry.Action == ParamActionReset && lastEntry.Value == "" {
			delete(s.globalMap, s.name)
		} else {
			s.globalMap[s.name] = lastEntry.Value
		}
	} else {
		delete(s.globalMap, s.name)
	}
}

func (s *SimpleParamHistory) CleanupStatementSet() {

}

func (s *SimpleParamHistory) Reset(tx int, defaultValue *string) {
	val := ""
	if defaultValue != nil {
		val = *defaultValue
	}
	s.history = append(s.history, ParamEntry{
		Tx:     tx,
		Action: ParamActionReset,
		Value:  val,
	})
	s.updateInGlobal()
}
