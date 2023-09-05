//go:build unit

package eventid

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMonotonic(t *testing.T) {
	gen := NewGeneratorNow()

	last := Zero
	for i := 0; i < 10_000; i++ {
		id := gen.NewID()
		require.True(t, id.Compare(last) > 0)
		last = id
	}
}
