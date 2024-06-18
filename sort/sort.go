package sort

import (
	"context"
	"sort"

	"www.velocidex.com/golang/vfilter/types"
)

type DefaultSorter struct{}

func (self DefaultSorter) Sort(ctx context.Context,
	scope types.Scope,
	input <-chan types.Row,
	key string,
	desc bool) <-chan types.Row {

	output_chan := make(chan types.Row)

	sort_ctx := &DefaultSorterCtx{
		OrderBy: key,
		Desc:    desc,
		Scope:   scope,
	}
	go func() {
		defer close(output_chan)

		// On exit from the function, sort our memory buffer
		// and dump it to the output chan.
		defer func() {
			// Sort ourselves
			sort.Sort(sort_ctx)

			// Dump everything to the output.
			for _, row := range sort_ctx.Items {
				select {
				case <-ctx.Done():
					return

				case output_chan <- row:
				}
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return

			case row, ok := <-input:
				if !ok {
					return
				}
				// Collect all the rows
				sort_ctx.Items = append(sort_ctx.Items, row)
			}
		}
	}()
	return output_chan
}

// The Default Sorter implements sorting in memory.
type DefaultSorterCtx struct {
	Items   []types.Row
	OrderBy string
	Desc    bool
	Scope   types.Scope
}

func (self *DefaultSorterCtx) Len() int {
	return len(self.Items)
}

func (self *DefaultSorterCtx) Less(i, j int) bool {
	element1, pres1 := self.Scope.Associative(
		self.Items[i], self.OrderBy)

	element2, pres2 := self.Scope.Associative(
		self.Items[j], self.OrderBy)

	if !pres1 || !pres2 {
		return false
	}

	// Sort NULL like an empty string because normally NULL
	// comparisons are not stable.
	if types.IsNil(element1) {
		element1 = ""
	}

	if types.IsNil(element2) {
		element2 = ""
	}

	if self.Desc {
		return !self.Scope.Lt(element1, element2)
	}

	return self.Scope.Lt(element1, element2)
}

func (self *DefaultSorterCtx) Swap(i, j int) {
	element1 := self.Items[i]
	self.Items[i] = self.Items[j]
	self.Items[j] = element1
}
