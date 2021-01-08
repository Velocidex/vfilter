package scope

import (
	"sort"
	"strings"

	"www.velocidex.com/golang/vfilter/utils"
)

// Get a list of similar sounding plugins.
func (self *Scope) GetSimilarPlugins(name string) []string {
	result := []string{}
	parts := strings.Split(name, "_")

	self.Lock()
	defer self.Unlock()

	for _, part := range parts {
		for k, _ := range self.plugins {
			if strings.Contains(k, part) && !utils.InString(&result, k) {
				result = append(result, k)
			}
		}
	}

	sort.Strings(result)

	return result
}
