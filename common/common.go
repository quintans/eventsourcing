package common

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/quintans/eventstore/base32"
	"github.com/quintans/eventstore/eventid"
)

const (
	// countSplitter separates the eventID and the event count
	countSplitter = "-"
)

func NewEventID(createdAt time.Time, aggregateID string, version uint32) string {
	id, _ := uuid.Parse(aggregateID)
	eid := eventid.New(createdAt, id, version)
	return eid.String()
}

// NewMessageID creates a message ID by concatenating eventID and count
func NewMessageID(eventID string, count uint8) string {
	c := base32.Marshal([]byte{count})
	return eventID + countSplitter + c
}

// SplitMessageID splits messageID into eventID and count
func SplitMessageID(messageID string) (eventID string, count uint8, err error) {
	if messageID == "" {
		return "", 0, nil
	}

	splits := strings.Split(messageID, countSplitter)
	if len(splits) != 2 {
		return "", 0, fmt.Errorf("Bad formated message ID. Message ID '%s' does not '%s' separator", messageID, countSplitter)
	}
	id := splits[0]
	b, err := base32.Unmarshal(splits[1])
	if err != nil {
		return "", 0, err
	}

	return id, uint8(b[0]), nil
}

// Dereference returns the underlying struct dereference
func Dereference(i interface{}) interface{} {
	v := reflect.ValueOf(i)
	if v.Kind() != reflect.Ptr {
		return i
	}
	v = v.Elem()
	return v.Interface()
}
