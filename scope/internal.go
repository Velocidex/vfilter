package scope

// The following functions manipulate internal scope state and should
// not be used publically.

func (self *Scope) IncDepth() {
	self.Lock()
	defer self.Unlock()
	self.stack_depth++
}

func (self *Scope) DecDepth() {
	self.Lock()
	defer self.Unlock()
	self.stack_depth--
}

func (self *Scope) GetDepth() int {
	self.Lock()
	defer self.Unlock()
	return self.stack_depth
}
