package qdb

type Command interface {
	Do()
	Undo()
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

func (c *DeleteCommand[T]) Do() {
	c.value, c.present = c.m[c.key]
	delete(c.m, c.key)
}

func (c *DeleteCommand[T]) Undo() {
	if !c.present {
		delete(c.m, c.key)
	} else {
		c.m[c.key] = c.value
	}
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

func (c *UpdateCommand[T]) Do() {
	c.prevValue, c.present = c.m[c.key]
	c.m[c.key] = c.value
}

func (c *UpdateCommand[T]) Undo() {
	if !c.present {
		delete(c.m, c.key)
	} else {
		c.m[c.key] = c.prevValue
	}
}

func NewDropCommand[T any](m map[string]T) *DropCommand[T] {
	return &DropCommand[T]{m: m}
}

type DropCommand[T any] struct {
	m    map[string]T
	copy map[string]T
}

func (c *DropCommand[T]) Do() {
	c.copy = make(map[string]T)
	for k, v := range c.m {
		c.copy[k] = v
	}
	for k := range c.m {
		delete(c.m, k)
	}
}

func (c *DropCommand[T]) Undo() {
	for k, v := range c.copy {
		c.m[k] = v
	}
}

func NewCustomCommand(do func(), undo func()) *CustomCommand {
	return &CustomCommand{do: do, undo: undo}
}

type CustomCommand struct {
	do   func()
	undo func()
}

func (c *CustomCommand) Do() {
	c.do()
}

func (c *CustomCommand) Undo() {
	c.undo()
}

func ExecuteCommands(saver func() error, commands ...Command) error {
	for _, c := range commands {
		c.Do()
	}
	err := saver()
	if err != nil {
		for _, c := range commands {
			c.Undo()
		}
		return err
	}
	return nil
}
