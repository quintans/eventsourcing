//go:build unit

package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIfZero(t *testing.T) {
	def := time.Minute
	var d time.Duration

	out := IfZero(d, def)
	assert.Equal(t, def, out)

	out = IfZero(time.Second, def)
	assert.Equal(t, time.Second, out)
}
