package log

import "log/slog"

const errKey = "error"

func Err(err error) slog.Attr {
	return slog.Any(errKey, err)
}
