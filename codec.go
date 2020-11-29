package eventstore

import (
	"encoding/json"
)

type JsonCodec struct{}

func (_ JsonCodec) Encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (_ JsonCodec) Decode(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
