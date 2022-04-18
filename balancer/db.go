package main

import (
	"errors"
	"fmt"
	"sync"
)

type DatabaseInterface interface {
	Init() error
	Insert(action *Action) error
	Update(action *Action) error
	Delete(action *Action) error
	GetAndRun() (Action, bool)
	MarkAllNotRunning() error
	Len() uint64
	Clear() error
}

//TODO retries!

type MockDb struct {
	actions map[uint64]Action
	count   uint64

	lock sync.Mutex
}

func (m *MockDb) Len() uint64 {
	return m.count
}

func (m *MockDb) MarkAllNotRunning() error {
	defer m.lock.Unlock()
	m.lock.Lock()
	for k, e := range m.actions {
		e.isRunning = false
		m.actions[k] = e
	}
	return nil
}

func (m *MockDb) Init() error {
	defer m.lock.Unlock()
	m.lock.Lock()
	m.actions = map[uint64]Action{}
	m.count = 0
	return nil
}

func (m *MockDb) Insert(action *Action) error {
	defer m.lock.Unlock()
	m.lock.Lock()
	_, ok := m.actions[action.id]
	if ok {
		return errors.New(fmt.Sprint("Already in db: ", action))
	}
	maxId := uint64(0)

	for a := range m.actions {
		if a > maxId {
			maxId = a
		}
	}
	action.id = maxId + 1
	m.actions[action.id] = *action
	m.count += 1
	return nil
}

func (m *MockDb) Update(action *Action) error {
	defer m.lock.Unlock()
	m.lock.Lock()
	_, ok := m.actions[action.id]
	if !ok {
		return errors.New(fmt.Sprint("Action not in db: ", action))
	}
	m.actions[action.id] = *action
	return nil
}

func (m *MockDb) Delete(action *Action) error {
	defer m.lock.Unlock()
	m.lock.Lock()
	_, ok := m.actions[action.id]
	if !ok {
		return errors.New(fmt.Sprint("Action not in db: ", action))
	}
	if m.actions[action.id].actionStage != done {
		return errors.New(fmt.Sprint("Action stage shoud be done, but: ", action.actionStage))
	}
	m.count -= 1
	delete(m.actions, action.id)
	return nil
}

func (m *MockDb) Clear() error {
	defer m.lock.Unlock()
	m.lock.Lock()
	if m.count != 0 {
		return errors.New(fmt.Sprint("Action list is not empty"))
	}

	m.actions = map[uint64]Action{}
	m.count = 0
	return nil
}

func (m *MockDb) GetAndRun() (Action, bool) {
	defer m.lock.Unlock()
	m.lock.Lock()
	for k, e := range m.actions {
		if !e.isRunning {
			e.isRunning = true
			m.actions[k] = e
			return e, true
		}
	}

	return Action{}, false
}
