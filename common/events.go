package common

import (
	"strings"

	"github.com/quintans/eventstore/encoding"
)

const (
	// countSplitter separates the eventID and the event count
	countSplitter = "-"

	// MinEventID is the lowest event ID
	MinEventID = ""
)

// NewMessageID creates a message ID by concatenating eventID and count
func NewMessageID(eventID string, count uint8) string {
	c := encoding.Marshal([]byte{count})
	return eventID + countSplitter + c
}

// SplitMessageID splits messageID into eventID and count
func SplitMessageID(messageID string) (eventID string, count uint8, err error) {
	if messageID == "" {
		return "", 0, nil
	}

	splits := strings.Split(messageID, countSplitter)
	id := splits[0]
	if len(splits) > 1 {
		b, err := encoding.Unmarshal(splits[1])
		if err != nil {
			return "", 0, err
		}
		count = uint8(b[0])
	}

	return id, count, nil
}
