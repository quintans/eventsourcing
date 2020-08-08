package repo

import "strings"

func JoinAndEscape(s []string) string {
	fields := strings.Join(s, ", ")
	return Escape(fields)
}

func Escape(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
