package session

type VirtualParamVisibility struct {
	entries []ParamEntry
	name    string

	globalMap map[string]string
}

func (s *VirtualParamVisibility) Commit() {
	i := len(s.entries) - 1
	for ; i >= 0 && s.entries[i].Tx > 0; i-- {
		_, ok := s.entries[i].Levels[VirtualParamLevelTxBlock]
		if ok {
			break
		}
	}

	if i >= 0 {
		entry := s.entries[i]
		if entry.Tx != 0 {
			entry.Tx = 0
			delete(entry.Levels, VirtualParamLevelLocal)
			delete(entry.Levels, VirtualParamLevelStatement)
		}
		s.entries = []ParamEntry{entry}
	} else {
		s.entries = nil
	}

	s.updateInGlobal()
}

func (s *VirtualParamVisibility) get() (string, bool) {
	if len(s.entries) == 0 {
		return "", false
	}

	lastEntry := s.entries[len(s.entries)-1]
	if v, ok := lastEntry.Levels[VirtualParamLevelLocal]; ok {
		return v, true
	}
	if v, ok := lastEntry.Levels[VirtualParamLevelStatement]; ok {
		return v, true
	}
	if v, ok := lastEntry.Levels[VirtualParamLevelTxBlock]; ok {
		return v, true
	}

	return "", false
}

func (s *VirtualParamVisibility) Set(entry ParamEntry) {
	if len(s.entries) == 0 {
		s.entries = append(s.entries, entry)
	} else {
		lastEntry := s.entries[len(s.entries)-1]
		if lastEntry.EqualIgnoringValue(entry) {
			for lvl, v := range entry.Levels {
				s.entries[len(s.entries)-1].Levels[lvl] = v
			}
		} else {
			s.entries = append(s.entries, entry)
		}
	}

	s.updateInGlobal()
}

func (s *VirtualParamVisibility) updateInGlobal() {
	v, ok := s.get()

	if ok {
		s.globalMap[s.name] = v
	} else {
		delete(s.globalMap, s.name)
	}
}

func (s *VirtualParamVisibility) RollbackTo(txCnt int) {
	i := len(s.entries) - 1
	for ; i >= 0 && s.entries[i].Tx > txCnt; i-- {
	}

	if i >= 0 {
		s.entries = s.entries[:i+1]
	} else {
		s.entries = nil
	}

	s.updateInGlobal()
}

func (s *VirtualParamVisibility) CleanupStatementSet() {
	for i := range s.entries {
		delete(s.entries[i].Levels, VirtualParamLevelStatement)

		if len(s.entries[i].Levels) == 0 {
			s.entries = s.entries[:len(s.entries)-1]
		}
	}

	s.updateInGlobal()
}

func (s *VirtualParamVisibility) Reset(tx int, defaultValue *string) {
	if defaultValue != nil {
		s.globalMap[s.name] = *defaultValue
	} else {
		delete(s.globalMap, s.name)
	}
	s.entries = nil
}
