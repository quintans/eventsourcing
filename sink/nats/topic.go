package nats

import (
	"fmt"

	"github.com/quintans/faults"
)

func ComposeTopic(topic string, partitionID uint32) (_ string, e error) {
	defer faults.Catch(&e, "ComposeTopic(topic=%s, partitionID=%d)", topic, partitionID)

	if topic == "" {
		return "", faults.New("topic root cannot be empty")
	}
	if partitionID < 1 {
		return "", faults.Errorf("the partitionID (%d) must be greater than  0", partitionID)
	}
	return fmt.Sprintf("%s#%d", topic, partitionID), nil
}
