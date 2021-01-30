package encoding

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

type TestBase64 struct {
	Bin Base64
}

func TestBase64Marshall(t *testing.T) {
	test := TestBase64{
		Bin: []byte{1, 2, 3, 4, 5, 6},
	}
	b, err := json.Marshal(test)
	require.NoError(t, err)
	require.Equal(t, `{"Bin":"AQIDBAUG"}`, string(b))

	test2 := TestBase64{}
	err = json.Unmarshal(b, &test2)
	require.NoError(t, err)
	require.Equal(t, test, test2)
}
