package common

import (
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/quintans/eventstore/encoding"
	"github.com/quintans/eventstore/eventid"
)

const (
	// countSplitter separates the eventID and the event count
	countSplitter = "-"

	// MinEventID is the lowest event ID
	MinEventID = ""
)

func NewEventID(createdAt time.Time, aggregateID string, version uint32) string {
	var id uuid.UUID
	if aggregateID != "" {
		id, _ = uuid.Parse(aggregateID)
	} else {
		id = uuid.UUID{}
	}
	eid := eventid.New(createdAt, id, version)
	return eid.String()
}

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

func DelayEventID(eventID string, offset time.Duration) (string, error) {
	if eventID == "" {
		return eventID, nil
	}

	splits := strings.Split(eventID, countSplitter)

	e, err := eventid.DelayEventID(splits[0], offset)
	if err != nil {
		return "", err
	}

	if len(splits) > 1 {
		return e + countSplitter + splits[1], nil
	}

	return e, nil
}
