package logging

import (
	"fmt"
	"os"
)

func Logf(msg string, a ...any) {
	fmt.Fprintf(os.Stderr, msg, a...)
}
