package utils

import "fmt"

const (
	debug = false
)

func DlvBreak() {}

func DebugPrint(format string) {
	if debug {
		fmt.Println(format)
	}
}
