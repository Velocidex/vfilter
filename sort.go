package vfilter

type ResultSet struct {
	Items   []Row
	OrderBy string
	Desc    bool
	scope   *Scope
}

func (self *ResultSet) Len() int {
	return len(self.Items)
}

func (self *ResultSet) Less(i, j int) bool {
	element1, pres1 := self.scope.Associative(
		self.Items[i], self.OrderBy)
	element2, pres2 := self.scope.Associative(
		self.Items[j], self.OrderBy)

	if !pres1 || !pres2 {
		return false
	}

	if self.Desc {
		return !self.scope.Lt(element1, element2)
	}

	return self.scope.Lt(element1, element2)
}

func (self *ResultSet) Swap(i, j int) {
	element1 := self.Items[i]
	self.Items[i] = self.Items[j]
	self.Items[j] = element1
}
