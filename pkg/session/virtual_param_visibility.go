package session

type VirtualParamVisibility struct {
	entries []ParamEntry
	name    string

	globalMap map[string]string
}

func (v *VirtualParamVisibility) Commit() {
	i := len(v.entries) - 1
	for ; i >= 0 && v.entries[i].Tx > 0; i-- {
		_, ok := v.entries[i].Levels[VirtualParamLevelTxBlock]
		if ok {
			break
		}
	}

	if i >= 0 {
		entry := v.entries[i]
		if entry.Tx != 0 {
			entry.Tx = 0
			delete(entry.Levels, VirtualParamLevelLocal)
			delete(entry.Levels, VirtualParamLevelStatement)
		}
		v.entries = []ParamEntry{entry}
	} else {
		v.entries = nil
	}

	v.updateInGlobal()
}

func (v *VirtualParamVisibility) get() (string, bool) {
	if len(v.entries) == 0 {
		return "", false
	}

	lastEntry := v.entries[len(v.entries)-1]
	if val, ok := lastEntry.Levels[VirtualParamLevelLocal]; ok {
		return val, true
	}
	if val, ok := lastEntry.Levels[VirtualParamLevelStatement]; ok {
		return val, true
	}
	if val, ok := lastEntry.Levels[VirtualParamLevelTxBlock]; ok {
		return val, true
	}

	return "", false
}

func (v *VirtualParamVisibility) Set(entry ParamEntry) {
	if len(v.entries) == 0 {
		v.entries = append(v.entries, entry)
	} else {
		lastEntry := v.entries[len(v.entries)-1]
		if lastEntry.EqualIgnoringValue(entry) {
			for lvl, val := range entry.Levels {
				v.entries[len(v.entries)-1].Levels[lvl] = val
			}
		} else {
			v.entries = append(v.entries, entry)
		}
	}

	v.updateInGlobal()
}

func (v *VirtualParamVisibility) updateInGlobal() {
	val, ok := v.get()

	if ok {
		v.globalMap[v.name] = val
	} else {
		delete(v.globalMap, v.name)
	}
}

func (v *VirtualParamVisibility) RollbackTo(txCnt int) {
	i := len(v.entries) - 1
	for ; i >= 0 && v.entries[i].Tx > txCnt; i-- {
	}

	if i >= 0 {
		v.entries = v.entries[:i+1]
	} else {
		v.entries = nil
	}

	v.updateInGlobal()
}

func (v *VirtualParamVisibility) CleanupStatementSet() {
	for i := range v.entries {
		delete(v.entries[i].Levels, VirtualParamLevelStatement)

		if len(v.entries[i].Levels) == 0 {
			v.entries = v.entries[:len(v.entries)-1]
		}
	}

	v.updateInGlobal()
}

func (v *VirtualParamVisibility) Reset(_ int, defaultValue *string) {
	if defaultValue != nil {
		v.globalMap[v.name] = *defaultValue
	} else {
		delete(v.globalMap, v.name)
	}
	v.entries = nil
}
