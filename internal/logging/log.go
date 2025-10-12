package logging

import "fmt"

var logch = make(chan string, 1000)

func init() {
	go func() {
		// single-threaded log printer
		for msg := range logch {
			fmt.Print(msg)
		}
	}()
}

func Logf(msg string, a ...any) {
	logch <- fmt.Sprintf(msg, a...)
}
