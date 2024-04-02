package log

import (
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strconv"
)

const (
	groupKey      = "error"
	errorKey      = "message"
	stackTraceKey = "stack"
)

func Err(err error) slog.Attr {
	if err == nil {
		return slog.Attr{}
	}

	msg := slog.String(errorKey, err.Error())

	var st StackTracer
	if errors.As(err, &st) {
		stack := slog.Any(stackTraceKey, LazyStr(func() string {
			var stack []string

			frames := st.Frames()
			stack = make([]string, len(frames))
			for i, frame := range frames {
				stack[i] = frame.File + `:` + strconv.Itoa(frame.Line)
			}
			return fmt.Sprint(stack)
		}))

		return slog.Any(groupKey, slog.GroupValue(msg, stack))
	}

	return slog.Any(groupKey, slog.GroupValue(msg))
}

type LazyStr func() string

func (ls LazyStr) String() string {
	return ls()
}

type StackTracer interface {
	Frames() []runtime.Frame
}
