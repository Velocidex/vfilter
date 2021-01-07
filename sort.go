package vfilter

import "www.velocidex.com/golang/vfilter/types"

type ResultSet struct {
	Items   []Row
	OrderBy string
	Desc    bool
	scope   types.Scope
}

func (self *ResultSet) Len() int {
	return len(self.Items)
}

func (self *ResultSet) Less(i, j int) bool {
	element1, pres1 := self.scope.Associative(
		self.Items[i], unquote_ident(self.OrderBy))

	element2, pres2 := self.scope.Associative(
		self.Items[j], unquote_ident(self.OrderBy))

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
