package session

type VirtualParamHistory struct {
	history []ParamEntry
	name    string

	globalMap map[string]string
}

func (v *VirtualParamHistory) Commit() {
	i := len(v.history) - 1
	for ; i >= 0 && v.history[i].Tx > 0; i-- {
		_, ok := v.history[i].Levels[VirtualParamLevelTxBlock]
		if ok {
			break
		}
	}

	if i >= 0 {
		entry := v.history[i]
		if entry.Tx != 0 {
			entry.Tx = 0
			delete(entry.Levels, VirtualParamLevelLocal)
			delete(entry.Levels, VirtualParamLevelStatement)
		}
		v.history = []ParamEntry{entry}
	} else {
		v.history = nil
	}

	v.updateInGlobal()
}

func (s *VirtualParamHistory) get() (string, bool) {
	if len(s.history) == 0 {
		return "", false
	}

	lastEntry := s.history[len(s.history)-1]
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

func (s *VirtualParamHistory) Set(entry ParamEntry) {
	if len(s.history) == 0 {
		s.history = append(s.history, entry)
	} else {
		lastEntry := s.history[len(s.history)-1]
		if lastEntry.EqualIgnoringValue(entry) {
			for lvl, v := range entry.Levels {
				s.history[len(s.history)-1].Levels[lvl] = v
			}
		} else {
			s.history = append(s.history, entry)
		}
	}

	s.updateInGlobal()
}

func (s *VirtualParamHistory) updateInGlobal() {
	v, ok := s.get()

	if ok {
		s.globalMap[s.name] = v
	} else {
		delete(s.globalMap, s.name)
	}
}

func (s *VirtualParamHistory) RollbackTo(txCnt int) {
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

func (s *VirtualParamHistory) CleanupStatementSet() {
	for i := range s.history {
		delete(s.history[i].Levels, VirtualParamLevelStatement)

		if len(s.history[i].Levels) == 0 {
			s.history = s.history[:len(s.history)-1]
		}
	}

	s.updateInGlobal()
}

func (s *VirtualParamHistory) Reset(tx int, defaultValue *string) {
	if defaultValue != nil {
		s.globalMap[s.name] = *defaultValue
	} else {
		delete(s.globalMap, s.name)
	}
	s.history = nil
}
