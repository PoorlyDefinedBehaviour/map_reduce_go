package tracing

import (
	"context"
	"fmt"
	"strings"
)

func Info(ctx context.Context, msg string, keyValuePairs ...any) {
	log(ctx, "INFO", msg, keyValuePairs...)
}

func Error(ctx context.Context, msg string, keyValuePairs ...any) {
	log(ctx, "ERROR", msg, keyValuePairs...)
}

func log(_ context.Context, level, msg string, keyValuePairs ...any) {
	if len(keyValuePairs)%2 != 0 {
		panic("Keys must be followed by a value. Did you forget a value?")
	}

	buffer := strings.Builder{}
	if _, err := buffer.WriteString(fmt.Sprintf("[%s] %s", level, msg)); err != nil {
		panic(err)
	}

	for i := 0; i < len(keyValuePairs); i += 2 {
		k := keyValuePairs[i]
		v := keyValuePairs[i+1]

		if _, err := buffer.WriteString(fmt.Sprintf(" %+v=%+v", k, v)); err != nil {
			panic(err)
		}

	}

	fmt.Println(buffer.String())
}
