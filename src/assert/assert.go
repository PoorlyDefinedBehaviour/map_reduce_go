package assert

import "fmt"

func Equal[T comparable](expected, value T, msg ...string) {
	if len(msg) > 1 {
		panic("only one message is allowed")
	}

	if value != expected {
		if len(msg) > 0 {
			panic(fmt.Sprintf("expected '%+v' to equal '%+v': %s", value, expected, msg))
		} else {
			panic(fmt.Sprintf("expected '%+v' to equal '%+v'", value, expected))
		}
	}
}

func True(v bool, msg ...string) {
	Equal(true, v, msg...)
}
