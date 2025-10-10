package assert

import "fmt"

// Assert asserted a condition
func Assert(condition bool, errMsg string) {
	if !condition {
		panic(fmt.Sprintf("Assert failure: %s", errMsg))
	}
}

// Assertf asserted a condition
func Assertf(condition bool, errMsg string, a ...any) {
	if !condition {
		panic("Assert failure:  " + fmt.Sprintf(errMsg, a...))
	}
}
