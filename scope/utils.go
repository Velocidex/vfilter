package scope

// Get a list of similar sounding plugins.
func (self *Scope) GetSimilarPlugins(name string) []string {
	return self.dispatcher.GetSimilarPlugins(name)
}
