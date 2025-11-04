package engine

const (
	ASC = iota
	DESC
)

type SortableWithContext struct {
	Data      [][]string
	Col_index int
	Order     int
}

func (a SortableWithContext) Len() int      { return len(a.Data) }
func (a SortableWithContext) Swap(i, j int) { a.Data[i], a.Data[j] = a.Data[j], a.Data[i] }
func (a SortableWithContext) Less(i, j int) bool {
	if a.Order == ASC {
		return a.Data[i][a.Col_index] < a.Data[j][a.Col_index]
	} else {
		return a.Data[i][a.Col_index] > a.Data[j][a.Col_index]
	}
}
