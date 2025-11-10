package qdb

import (
	"maps"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

type Command interface {
	Do() error
	Undo() error
}

func NewDeleteCommand[T any](m map[string]T, key string) *DeleteCommand[T] {
	return &DeleteCommand[T]{m: m, key: key}
}

type DeleteCommand[T any] struct {
	m       map[string]T
	key     string
	value   T
	present bool
}

func (c *DeleteCommand[T]) Do() error {
	c.value, c.present = c.m[c.key]
	delete(c.m, c.key)
	return nil
}

func (c *DeleteCommand[T]) Undo() error {
	if !c.present {
		delete(c.m, c.key)
	} else {
		c.m[c.key] = c.value
	}
	return nil
}

func NewUpdateCommand[T any](m map[string]T, key string, value T) *UpdateCommand[T] {
	return &UpdateCommand[T]{m: m, key: key, value: value}
}

type UpdateCommand[T any] struct {
	m         map[string]T
	key       string
	value     T
	prevValue T
	present   bool
}

func (c *UpdateCommand[T]) Do() error {
	c.prevValue, c.present = c.m[c.key]
	c.m[c.key] = c.value
	return nil
}

func (c *UpdateCommand[T]) Undo() error {
	if !c.present {
		delete(c.m, c.key)
	} else {
		c.m[c.key] = c.prevValue
	}
	return nil
}

func NewDropCommand[T any](m map[string]T) *DropCommand[T] {
	return &DropCommand[T]{m: m}
}

type DropCommand[T any] struct {
	m    map[string]T
	copy map[string]T
}

func (c *DropCommand[T]) Do() error {
	c.copy = make(map[string]T)
	maps.Copy(c.copy, c.m)
	for k := range c.m {
		delete(c.m, k)
	}
	return nil
}

func (c *DropCommand[T]) Undo() error {
	for k, v := range c.copy {
		c.m[k] = v
	}
	return nil
}

func NewCustomCommand(do func() error, undo func() error) *CustomCommand {
	return &CustomCommand{do: do, undo: undo}
}

type CustomCommand struct {
	do   func() error
	undo func() error
}

func (c *CustomCommand) Do() error {
	return c.do()
}

func (c *CustomCommand) Undo() error {
	return c.undo()
}

func doCommands(commands ...Command) (int, error) {
	for i, c := range commands {
		err := c.Do()
		if err != nil {
			return i, err
		}
	}
	return len(commands), nil
}

func undoCommands(commands ...Command) error {
	spqrlog.Zero.Info().Msg("memqdb: undo commands")
	for _, c := range commands {
		err := c.Undo()
		if err != nil {
			return err
		}
	}
	return nil
}

func ExecuteCommands(saver func() error, commands ...Command) error {
	completed, err := doCommands(commands...)
	if err == nil {
		err = saver()
	}
	if err != nil {
		undoErr := undoCommands(commands[:completed]...)
		if undoErr != nil {
			return fmt.Errorf("failed to undo command %s while: %s", undoErr.Error(), err.Error())
		}
		return err
	}
	return nil
}
