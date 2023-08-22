package util

import (
	"strconv"

	"github.com/quintans/faults"
)

func PartitionTopic(topic string, hash, partitions uint32) (Topic, error) {
	m, err := WhichPartition(hash, partitions)
	if err != nil {
		return Topic{}, err
	}
	return NewPartitionedTopic(topic, m)
}

func WhichPartition(hash, partitions uint32) (uint32, error) {
	if partitions < 1 {
		return 0, faults.Errorf("the number of partitions (%d) must be greater than than 0", partitions)
	}
	return (hash % partitions) + 1, nil
}

type Topic struct {
	root      string
	partition uint32
}

func NewTopic(root string) (Topic, error) {
	return NewPartitionedTopic(root, 1)
}

func NewPartitionedTopic(root string, partitionID uint32) (Topic, error) {
	if root == "" {
		return Topic{}, faults.New("topic root cannot be empty")
	}
	if partitionID < 1 {
		return Topic{}, faults.Errorf("the partitions ID (%d) must be greater than than 0", partitionID)
	}
	return Topic{
		root:      root,
		partition: partitionID,
	}, nil
}

func (t Topic) String() string {
	return t.root + "#" + strconv.Itoa(int(t.partition))
}

func (t Topic) Root() string {
	return t.root
}

func (t Topic) Partition() uint32 {
	return t.partition
}

func (t Topic) IsZero() bool {
	return t == Topic{}
}
